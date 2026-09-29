/**
 * DroidTextGeneration — commit messages, PR content, branch names, and thread
 * titles generated through a short-lived `droid exec` JSON-RPC session. The
 * session has no tools and asks Droid for JSON-schema structured output.
 */
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { type DroidSettings, type ModelSelection, TextGenerationError } from "@t3tools/contracts";
import { formatGeneratedBranchName, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";

import { DROID_INHERIT_MODEL_SLUG } from "../provider/Layers/DroidProvider.ts";
import {
  DROID_SESSION_REQUEST_TIMEOUT_MS,
  makeDroidExecRpcClient,
} from "../provider/droid/DroidRpcClient.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
  toJsonSchemaObject,
} from "./TextGenerationUtils.ts";

const DROID_TIMEOUT_MS = 180_000;
const MAX_OUTPUT_BYTES = 256 * 1024;

const isTextGenerationError = Schema.is(TextGenerationError);
const encodeJson = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

type DroidTextGenerationOperation =
  | "generateCommitMessage"
  | "generatePrContent"
  | "generateBranchName"
  | "generateThreadTitle";

export const makeDroidTextGeneration = Effect.fn("makeDroidTextGeneration")(function* (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const { randomUUIDv4 } = yield* Crypto.Crypto;

  const runDroidJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchema,
    modelSelection,
  }: {
    operation: DroidTextGenerationOperation;
    cwd: string;
    prompt: string;
    outputSchema: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const failWith = (detail: string, cause?: unknown) =>
        new TextGenerationError({
          operation,
          detail,
          ...(cause !== undefined ? { cause } : {}),
        });
      const mapRpcError =
        (detail: string) =>
        <A, E, R>(effect: Effect.Effect<A, E, R>) =>
          effect.pipe(Effect.mapError((cause) => failWith(detail, cause)));
      const rpc = yield* makeDroidExecRpcClient({
        binaryPath: droidSettings.binaryPath,
        cwd,
        env: environment,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, commandSpawner),
        mapRpcError("Failed to start the Droid CLI."),
      );

      let structuredOutput: Record<string, unknown> | null | undefined;
      const turnDone = yield* Deferred.make<string | undefined, TextGenerationError>();
      const failTurn = (detail: string, cause?: unknown) =>
        Deferred.fail(turnDone, failWith(detail, cause)).pipe(Effect.asVoid);

      yield* Stream.runDrain(
        Stream.mapEffect(rpc.notifications, ({ notification }) => {
          switch (notification.type) {
            case "structured_output":
              return encodeJson(notification.structuredOutput).pipe(
                Effect.flatMap((encodedOutput) => {
                  if (Buffer.byteLength(encodedOutput, "utf8") > MAX_OUTPUT_BYTES)
                    return failTurn(`Droid output exceeded the ${MAX_OUTPUT_BYTES}-byte limit.`);
                  structuredOutput = notification.structuredOutput;
                  return Effect.void;
                }),
                Effect.catchTags({
                  SchemaError: (cause) =>
                    failTurn("Droid returned invalid structured output.", cause),
                }),
              );
            case "agent_turn_completed":
              return Deferred.succeed(turnDone, notification.reason).pipe(Effect.asVoid);
            default:
              return Effect.void;
          }
        }),
      ).pipe(Effect.forkScoped);

      const reasoningEffort = getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");
      const modelId =
        modelSelection.model && modelSelection.model !== DROID_INHERIT_MODEL_SLUG
          ? modelSelection.model
          : undefined;
      yield* rpc
        .request(
          "droid.initialize_session",
          {
            machineId: "default",
            cwd,
            autonomyLevel: "off",
            interactionMode: "auto",
            // Background helpers must never touch the workspace.
            restrictToolIds: ["t3_text_generation"],
            blockOnMcpLoad: false,
            ...(modelId ? { modelId } : {}),
            ...(modelId && reasoningEffort ? { reasoningEffort } : {}),
          },
          { timeoutMs: DROID_SESSION_REQUEST_TIMEOUT_MS },
        )
        .pipe(mapRpcError("Failed to initialize Droid session."));

      yield* rpc
        .request("droid.add_user_message", {
          messageId: yield* randomUUIDv4,
          text: prompt,
          outputFormat: {
            type: "json_schema",
            schema: toJsonSchemaObject(outputSchema),
          },
        })
        .pipe(mapRpcError("Droid rejected the prompt."));

      const completionReason = yield* Effect.raceFirst(
        Deferred.await(turnDone),
        Effect.flatMap(rpc.exits, (exit) =>
          Effect.fail(
            failWith(`Droid exited before completing the request (${exit.description}).`),
          ),
        ),
      ).pipe(
        Effect.timeoutOption(DROID_TIMEOUT_MS),
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(failWith("Droid request timed out.")),
            onSome: (value) => Effect.succeed(value),
          }),
        ),
      );

      if (completionReason !== "completed")
        return yield* failWith(
          completionReason === "cancelled"
            ? "Droid request was cancelled."
            : `Droid request failed (${completionReason ?? "unknown reason"}).`,
        );
      if (structuredOutput === undefined || structuredOutput === null)
        return yield* failWith("Droid returned no structured output.");

      return yield* Schema.decodeUnknownEffect(outputSchema)(structuredOutput).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(failWith("Droid returned invalid structured output.", cause)),
        }),
      );
    }).pipe(
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({
              operation,
              detail: "Droid text generation failed.",
              cause,
            }),
      ),
      Effect.scoped,
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("DroidTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });
      const generated = yield* runDroidJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("DroidTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });
      const generated = yield* runDroidJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("DroidTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
        naming: input.naming,
      });
      const generated = yield* runDroidJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        branch: formatGeneratedBranchName(generated.branch, input.naming),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("DroidTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        linkedContext: input.linkedContext,
        attachments: input.attachments,
      });
      const generated = yield* runDroidJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizeThreadTitle(generated.title),
        ...(generated.needsRefinement ? { needsRefinement: true } : {}),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
