import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";
import { describe } from "vite-plus/test";

import { ServerConfig } from "../../config.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "../IdAllocator.ts";
import { ProviderAdapterV2RuntimePolicy, type ProviderAdapterV2Event } from "../ProviderAdapter.ts";
import { makeDroidAdapterV2 } from "./DroidAdapterV2.ts";

const serverConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-droid-v2-live-",
}).pipe(Layer.provide(NodeServices.layer));

const testLayer = Layer.mergeAll(NodeServices.layer, idAllocatorLayer, serverConfigLayer);

const INSTANCE_ID = ProviderInstanceId.make("droid");
const THREAD_ID = ThreadId.make("thread-droid-live");
const MODEL = process.env.T3_DROID_LIVE_MODEL ?? "default";

// Spends Factory credits: run with T3_DROID_LIVE=1 against a signed-in `droid`.
describe.runIf(process.env.T3_DROID_LIVE === "1")("Droid V2 live adapter", () => {
  it.live(
    "runs a supervised turn with an approval against the real Droid CLI",
    () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const cwd = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-droid-live-" });
        const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
          runtimeMode: "approval-required",
          interactionMode: "default",
          cwd,
        });
        const modelSelection = { instanceId: INSTANCE_ID, model: MODEL };
        const adapter = makeDroidAdapterV2({
          instanceId: INSTANCE_ID,
          settings: { enabled: true, binaryPath: "droid", customModels: [] },
          environment: process.env,
          spawner: yield* ChildProcessSpawner.ChildProcessSpawner,
          fileSystem,
          crypto: yield* Crypto.Crypto,
          idAllocator: yield* IdAllocatorV2,
          serverConfig: yield* ServerConfig,
        });
        const runtime = yield* adapter.openSession({
          threadId: THREAD_ID,
          providerSessionId: ProviderSessionId.make("provider-session-droid-live"),
          modelSelection,
          runtimePolicy,
        });
        const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
        yield* runtime.events.pipe(
          Stream.runForEach((event) => Queue.offer(events, event)),
          Effect.forkScoped,
        );
        const providerThread = yield* runtime.ensureThread({
          threadId: THREAD_ID,
          modelSelection,
          runtimePolicy,
        });
        const now = yield* DateTime.now;
        const runId = RunId.make(`run:${THREAD_ID}:1`);
        yield* runtime.startTurn({
          appThread: {
            createdBy: "user",
            creationSource: "web",
            id: THREAD_ID,
            projectId: "project:live:droid" as OrchestrationV2AppThread["projectId"],
            title: "Droid live",
            providerInstanceId: INSTANCE_ID,
            modelSelection,
            runtimeMode: "approval-required",
            interactionMode: "default",
            branch: null,
            worktreePath: null,
            activeProviderThreadId: null,
            lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
            forkedFrom: null,
            createdAt: now,
            updatedAt: now,
            archivedAt: null,
            settledOverride: null,
            settledAt: null,
            lastVisitedAt: null,
            deletedAt: null,
          },
          threadId: THREAD_ID,
          runId,
          runOrdinal: 1,
          providerTurnOrdinal: 1,
          attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
          rootNodeId: NodeId.make(`node:${runId}:root`),
          providerThread,
          message: {
            messageId: `message:${THREAD_ID}:1` as never,
            text: "Create a file named hello.txt containing the word hi, then reply with exactly: done",
            attachments: [],
            createdBy: "user",
            creationSource: "web",
          },
          modelSelection,
          runtimePolicy,
        });

        const seen: Array<ProviderAdapterV2Event> = [];
        while (true) {
          const event = yield* Queue.take(events);
          seen.push(event);
          if (
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.status === "pending"
          ) {
            yield* runtime.respondToRuntimeRequest({
              requestId: event.runtimeRequest.id,
              decision: "accept",
            });
          }
          if (event.type === "turn.terminal") {
            assert.strictEqual(event.status, "completed");
            break;
          }
        }
        const items = seen.flatMap((event) =>
          event.type === "turn_item.updated" ? [event.turnItem] : [],
        );
        assert.isTrue(items.some((item) => item.type === "approval_request"));
        assert.isTrue(
          items.some((item) => item.type === "file_change" && item.status === "completed"),
        );
        assert.isTrue(
          items.some(
            (item) => item.type === "assistant_message" && item.text.toLowerCase().includes("done"),
          ),
        );
        const written = yield* fileSystem.readFileString((yield* Path.Path).join(cwd, "hello.txt"));
        assert.strictEqual(written.trim(), "hi");
      }).pipe(Effect.scoped, Effect.provide(testLayer)),
    180_000,
  );
});
