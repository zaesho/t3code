import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CheckpointId,
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import { IdAllocatorV2, layer as idAllocatorLayer } from "../IdAllocator.ts";
import {
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import {
  DROID_PROVIDER,
  droidAutonomyLevelForRuntimeMode,
  droidContextUsage,
  makeDroidAdapterV2,
  selectDroidPermissionOutcome,
} from "./DroidAdapterV2.ts";

const serverConfigLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-droid-v2-adapter-",
}).pipe(Layer.provide(NodeServices.layer));

const testLayer = Layer.mergeAll(NodeServices.layer, idAllocatorLayer, serverConfigLayer);

const MOCK_AGENT = new URL("../../../scripts/droid-mock-agent.ts", import.meta.url).pathname;
const DROID_INSTANCE_ID = ProviderInstanceId.make("droid");
const THREAD_ID = ThreadId.make("thread-droid-test");
const SESSION_ID = ProviderSessionId.make("provider-session-droid-test");

const policy = (
  runtimeMode: RuntimeMode = "full-access",
  interactionMode: "default" | "plan" = "default",
) => ProviderAdapterV2RuntimePolicy.make({ runtimeMode, interactionMode, cwd: process.cwd() });

const modelSelection = (model = "default"): ModelSelection => ({
  instanceId: DROID_INSTANCE_ID,
  model,
});

const openRuntime = Effect.fnUntraced(function* (
  scenario: string,
  extraEnv: NodeJS.ProcessEnv = {},
) {
  const adapter = makeDroidAdapterV2({
    instanceId: DROID_INSTANCE_ID,
    settings: { enabled: true, binaryPath: MOCK_AGENT, customModels: [] },
    environment: { ...process.env, T3_DROID_MOCK_SCENARIO: scenario, ...extraEnv },
    spawner: yield* ChildProcessSpawner.ChildProcessSpawner,
    fileSystem: yield* FileSystem.FileSystem,
    crypto: yield* Crypto.Crypto,
    idAllocator: yield* IdAllocatorV2,
    serverConfig: yield* ServerConfig,
  });
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: SESSION_ID,
    modelSelection: modelSelection(),
    runtimePolicy: policy(),
  });
  const emitted = yield* Queue.unbounded<ProviderAdapterV2Event>();
  const streamEnd = yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.exit,
    Effect.forkScoped,
  );
  const seen: Array<ProviderAdapterV2Event> = [];
  const takeEvent = <E extends ProviderAdapterV2Event>(
    predicate: (event: ProviderAdapterV2Event) => event is E,
  ) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(emitted);
        seen.push(event);
        if (predicate(event)) return event;
      }
    });
  return { runtime, takeEvent, seen, streamEnd };
});

const makeAppThread = Effect.fnUntraced(function* () {
  const now = yield* DateTime.now;
  return {
    createdBy: "user",
    creationSource: "web",
    id: THREAD_ID,
    projectId: "project:fixture:droid" as OrchestrationV2AppThread["projectId"],
    title: "Droid test thread",
    providerInstanceId: DROID_INSTANCE_ID,
    modelSelection: modelSelection(),
    runtimeMode: "full-access",
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
  } satisfies OrchestrationV2AppThread;
});

const startTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  options: {
    readonly runOrdinal?: number;
    readonly text?: string;
    readonly interactionMode?: "default" | "plan";
    readonly model?: string;
  } = {},
) {
  const runOrdinal = options.runOrdinal ?? 1;
  const runId = RunId.make(`run:${THREAD_ID}:${runOrdinal}`);
  yield* runtime.startTurn({
    appThread: yield* makeAppThread(),
    threadId: THREAD_ID,
    runId,
    runOrdinal,
    providerTurnOrdinal: runOrdinal,
    attemptId: RunAttemptId.make(`run-attempt:${runId}:1`),
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${THREAD_ID}:${runOrdinal}` as never,
      text: options.text ?? "Hello droid",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection: modelSelection(options.model),
    runtimePolicy: policy("full-access", options.interactionMode ?? "default"),
  });
});

const isTerminal = (
  event: ProviderAdapterV2Event,
): event is Extract<ProviderAdapterV2Event, { type: "turn.terminal" }> =>
  event.type === "turn.terminal";

const isPendingRequest = (
  event: ProviderAdapterV2Event,
): event is Extract<ProviderAdapterV2Event, { type: "runtime_request.updated" }> =>
  event.type === "runtime_request.updated" && event.runtimeRequest.status === "pending";

const isRunningTurn = (
  event: ProviderAdapterV2Event,
): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
  event.type === "provider_turn.updated" && event.providerTurn.status === "running";

const turnItems = (seen: ReadonlyArray<ProviderAdapterV2Event>) =>
  seen.flatMap((event) => (event.type === "turn_item.updated" ? [event.turnItem] : []));

const ensureThread = (runtime: ProviderAdapterV2SessionRuntime) =>
  runtime.ensureThread({
    threadId: THREAD_ID,
    modelSelection: modelSelection(),
    runtimePolicy: policy(),
  });

describe("DroidAdapterV2 protocol mapping", () => {
  it("binds decisions only to outcomes it recognizes", () => {
    const options = [
      { label: "Allow once", outcome: "proceed_once" },
      { label: "Always", outcome: "proceed_always" },
      { label: "Deny", outcome: "cancel" },
    ];
    assert.strictEqual(selectDroidPermissionOutcome(options, "accept"), "proceed_once");
    assert.strictEqual(selectDroidPermissionOutcome(options, "acceptForSession"), "proceed_always");
    assert.strictEqual(selectDroidPermissionOutcome(options, "decline"), "cancel");
    assert.isUndefined(
      selectDroidPermissionOutcome([{ label: "?", outcome: "future_grant" }], "accept"),
    );
  });

  it("maps runtime modes onto Droid autonomy levels", () => {
    assert.deepStrictEqual(
      (["approval-required", "auto-accept-edits", "auto", "full-access"] as const).map(
        droidAutonomyLevelForRuntimeMode,
      ),
      ["off", "low", "medium", "high"],
    );
  });

  it("uses the last call as the context meter", () => {
    const usage = droidContextUsage(
      {
        inputTokens: 100,
        outputTokens: 10,
        cacheCreationTokens: 5,
        cacheReadTokens: 50,
        thinkingTokens: 2,
      },
      { inputTokens: 7, cacheReadTokens: 2, outputTokens: 3 },
    );
    assert.strictEqual(usage.usedTokens, 12);
    assert.strictEqual(usage.totalProcessedTokens, 165);
    assert.isTrue(usage.compactsAutomatically);
  });
});

describe("DroidAdapterV2", () => {
  it.effect("streams a turn and settles it on Droid's terminal", () =>
    Effect.gen(function* () {
      const { runtime, takeEvent, seen } = yield* openRuntime("default");
      const providerThread = yield* ensureThread(runtime);
      assert.strictEqual(providerThread.nativeThreadRef?.nativeId, "mock-session-1");
      assert.strictEqual(providerThread.driver, DROID_PROVIDER);

      yield* startTurn(runtime, providerThread);
      const terminal = yield* takeEvent(isTerminal);
      assert.strictEqual(terminal.status, "completed");

      const items = turnItems(seen);
      const assistant = items.findLast((item) => item.type === "assistant_message");
      assert.isTrue(
        assistant?.type === "assistant_message" &&
          assistant.text === "hello from droid mock" &&
          assistant.streaming === false,
      );
      const reasoning = items.findLast((item) => item.type === "reasoning");
      assert.isTrue(reasoning?.type === "reasoning" && reasoning.text === "Mock thinking");
      const usage = seen.findLast(
        (event) => event.type === "provider_thread.updated" && event.providerThread.contextUsage,
      );
      assert.isTrue(
        usage?.type === "provider_thread.updated" &&
          usage.providerThread.contextUsage?.usedTokens === 12,
      );
      const settled = seen.findLast(
        (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
      );
      assert.isDefined(settled?.providerTurn.nativeTurnRef?.nativeId);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("routes approvals through runtime requests and answers Droid", () =>
    Effect.gen(function* () {
      const { runtime, takeEvent, seen } = yield* openRuntime("permission");
      const providerThread = yield* ensureThread(runtime);
      yield* startTurn(runtime, providerThread);

      const pending = yield* takeEvent(isPendingRequest);
      assert.strictEqual(pending.runtimeRequest.kind, "command");
      const approval = yield* takeEvent(
        (event): event is Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }> =>
          event.type === "turn_item.updated" && event.turnItem.type === "approval_request",
      );
      assert.isTrue(
        approval.turnItem.type === "approval_request" &&
          approval.turnItem.prompt === "echo mock" &&
          approval.turnItem.options?.map((option) => option.decision).join(",") ===
            "accept,decline",
      );

      yield* runtime.respondToRuntimeRequest({
        requestId: pending.runtimeRequest.id,
        decision: "accept",
      });
      const terminal = yield* takeEvent(isTerminal);
      assert.strictEqual(terminal.status, "completed");
      assert.isTrue(
        seen.some(
          (event) =>
            event.type === "runtime_request.updated" &&
            event.runtimeRequest.status === "resolved" &&
            event.runtimeRequest.decision === "accept",
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("answers Droid questions from structured user input", () =>
    Effect.gen(function* () {
      const { runtime, takeEvent } = yield* openRuntime("ask-user");
      const providerThread = yield* ensureThread(runtime);
      yield* startTurn(runtime, providerThread);

      const pending = yield* takeEvent(isPendingRequest);
      assert.strictEqual(pending.runtimeRequest.kind, "user_input");
      const question = yield* takeEvent(
        (event): event is Extract<ProviderAdapterV2Event, { type: "turn_item.updated" }> =>
          event.type === "turn_item.updated" && event.turnItem.type === "user_input_request",
      );
      assert.isTrue(
        question.turnItem.type === "user_input_request" &&
          question.turnItem.questions[0]?.id === "1" &&
          question.turnItem.questions[0]?.options.map((option) => option.label).join(",") ===
            "workspace,session",
      );
      yield* runtime.respondToRuntimeRequest({
        requestId: pending.runtimeRequest.id,
        answers: { "1": "session" },
      });
      const terminal = yield* takeEvent(isTerminal);
      assert.strictEqual(terminal.status, "completed");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("captures a Spec Mode proposal as a plan and keeps Droid planning", () =>
    Effect.gen(function* () {
      const { runtime, takeEvent, seen } = yield* openRuntime("spec-autonomy-handoff");
      const providerThread = yield* ensureThread(runtime);
      yield* startTurn(runtime, providerThread, { interactionMode: "plan" });

      const terminal = yield* takeEvent(isTerminal);
      assert.strictEqual(terminal.status, "completed");
      const plan = turnItems(seen).findLast((item) => item.type === "proposed_plan");
      assert.isTrue(
        plan?.type === "proposed_plan" && plan.markdown.includes("Implement the approved plan."),
      );
      assert.isFalse(seen.some(isPendingRequest));
      assert.isFalse(
        turnItems(seen).some(
          (item) => item.type === "assistant_message" && item.text === "in-session implementation",
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("keeps a coalesced steer inside the running turn", () =>
    Effect.gen(function* () {
      const { runtime, takeEvent } = yield* openRuntime("steering-coalesced");
      const providerThread = yield* ensureThread(runtime);
      yield* startTurn(runtime, providerThread);
      const running = yield* takeEvent(isRunningTurn);

      yield* runtime.steerTurn({
        threadId: THREAD_ID,
        runId: RunId.make(`run:${THREAD_ID}:1`),
        providerThread,
        providerTurnId: running.providerTurn.id,
        message: {
          messageId: `message:${THREAD_ID}:steer` as never,
          text: "also do this",
          attachments: [],
          createdBy: "user",
          creationSource: "web",
        },
      });
      const terminal = yield* takeEvent(isTerminal);
      assert.strictEqual(terminal.status, "completed");
      assert.strictEqual(terminal.providerTurnId, running.providerTurn.id);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("stops a running turn when the user interrupts", () =>
    Effect.gen(function* () {
      const { runtime, takeEvent } = yield* openRuntime("hang-turn");
      const providerThread = yield* ensureThread(runtime);
      yield* startTurn(runtime, providerThread);
      const running = yield* takeEvent(isRunningTurn);

      yield* runtime.interruptTurn({ providerThread, providerTurnId: running.providerTurn.id });
      const terminal = yield* takeEvent(isTerminal);
      assert.strictEqual(terminal.status, "interrupted");

      // The session stays usable for the next turn.
      yield* startTurn(runtime, providerThread, { runOrdinal: 2 });
      yield* takeEvent(isRunningTurn);
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("rolls the conversation back by rewinding to the discarded turn", () =>
    Effect.gen(function* () {
      const requestLog = (yield* Path.Path).join(
        yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped(),
        "requests.ndjson",
      );
      const { runtime, takeEvent } = yield* openRuntime("default", {
        T3_DROID_MOCK_REQUEST_LOG: requestLog,
      });
      const providerThread = yield* ensureThread(runtime);
      yield* startTurn(runtime, providerThread);
      const settled = yield* takeEvent(
        (event): event is Extract<ProviderAdapterV2Event, { type: "provider_turn.updated" }> =>
          event.type === "provider_turn.updated" && event.providerTurn.status === "completed",
      );
      yield* takeEvent(isTerminal);
      const firstTurn: OrchestrationV2ProviderTurn = settled.providerTurn;

      const snapshot = yield* runtime.rollbackThread({
        providerThread,
        target: {
          type: "thread_start",
          checkpointId: CheckpointId.make("checkpoint:droid:start"),
          appRunOrdinal: 0,
        },
        providerThreadTurns: [firstTurn],
      });
      assert.strictEqual(snapshot.providerThread.nativeThreadRef?.nativeId, "mock-session-rewound");
      const log = (yield* (yield* FileSystem.FileSystem).readFileString(requestLog))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as { method: string; params: Record<string, unknown> });
      const rewind = log.find((entry) => entry.method === "droid.execute_rewind");
      assert.strictEqual(rewind?.params.messageId, firstTurn.nativeTurnRef?.nativeId);
      assert.isTrue(log.some((entry) => entry.method === "droid.load_session"));
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("reports Droid child sessions as subagents", () =>
    Effect.gen(function* () {
      const { runtime, takeEvent, seen } = yield* openRuntime("child-session");
      const providerThread = yield* ensureThread(runtime);
      yield* startTurn(runtime, providerThread);
      yield* takeEvent(isTerminal);

      const subagents = seen.flatMap((event) =>
        event.type === "subagent.updated" ? [event.subagent] : [],
      );
      assert.isTrue(subagents.some((subagent) => subagent.status === "running"));
      assert.strictEqual(subagents.at(-1)?.status, "completed");
      assert.isFalse(
        turnItems(seen).some(
          (item) => item.type === "assistant_message" && item.text.includes("child-only"),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );

  it.effect("fails the turn and the event stream when Droid exits mid-turn", () =>
    Effect.gen(function* () {
      const { runtime, takeEvent, streamEnd } = yield* openRuntime("exit-mid-turn");
      const providerThread = yield* ensureThread(runtime);
      yield* startTurn(runtime, providerThread);
      const terminal = yield* takeEvent(isTerminal);
      assert.isTrue(terminal.status === "failed" && terminal.failure.class === "transport_error");
      const exit = yield* Fiber.join(streamEnd);
      assert.isTrue(exit._tag === "Failure");
    }).pipe(Effect.scoped, Effect.provide(testLayer)),
  );
});
