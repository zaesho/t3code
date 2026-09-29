/**
 * DroidAdapterV2 — orchestrator-v2 adapter for Factory Droid, driving
 * `droid exec --input-format stream-jsonrpc --output-format stream-jsonrpc`
 * through `DroidRpcClient`. Droid's native protocol carries Spec Mode,
 * rewind, session settings, model discovery, and child-session progress,
 * which its ACP bridge does not expose.
 *
 * One process hosts one Droid session. The Droid session id is the durable
 * native thread ref, and each turn's opening user-message id is its native
 * turn ref: Droid keys `agent_turn_completed` by that id and rewinds to it.
 *
 * Steering: a message sent during a run either coalesces into the running
 * physical run (Droid persists it with `create_message` before that run's
 * terminal) or runs later with its own terminal. The T3 turn settles only when
 * every physical run it started has settled.
 *
 * Spec Mode: T3 plan mode maps onto Droid Spec Mode. When Droid proposes its
 * spec, the adapter captures it as a proposed plan and answers "keep
 * iterating", so implementation starts from T3's own plan flow in a later turn.
 */
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import {
  DroidSettings,
  ProviderDriverKind,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2ExecutionNode,
  type OrchestrationV2PlanArtifact,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderFailure,
  type OrchestrationV2ProviderRef,
  type OrchestrationV2ProviderSession,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2ProviderTurnTokenUsage,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2TurnItem,
  type OrchestrationV2UserInputQuestion,
  type ProviderApprovalDecision,
  type ProviderApprovalOption,
  type ProviderInstanceId,
  type ProviderRequestKind,
  type ProviderUserInputAnswers,
  type RuntimeMode,
  type ThreadId,
  type ThreadTokenUsageSnapshot,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { DROID_INHERIT_MODEL_SLUG } from "../../provider/Layers/DroidProvider.ts";
import {
  DroidExecuteRewindResult,
  DroidToolConfirmationDetails,
  type DroidLastCallTokenUsage,
  type DroidPermissionOption,
  type DroidPermissionRequest,
  type DroidSessionNotification,
  type DroidTokenUsage,
  type DroidToolUse,
} from "../../provider/droid/DroidProtocol.ts";
import {
  DROID_SERVER_REQUEST_CONCURRENCY,
  makeDroidRpcClient,
  type DroidAskUserServerRequest,
  type DroidPermissionServerRequest,
  type DroidRpcClient,
  type DroidServerRequest,
} from "../../provider/droid/DroidRpcClient.ts";
import { mergeProviderInstanceEnvironment } from "../../provider/ProviderInstanceEnvironment.ts";
import { buildRuntimeInstructions } from "../../provider/RuntimeInstructions.ts";
import { t3OrchestrationSystemPrompt } from "../../provider/T3OrchestrationInstructions.ts";
import { IdAllocatorV2 } from "../IdAllocator.ts";
import {
  ProviderAdapterEnsureThreadError,
  ProviderAdapterEventStreamError,
  ProviderAdapterForkThreadError,
  ProviderAdapterInterruptError,
  ProviderAdapterOpenSessionError,
  ProviderAdapterProtocolError,
  ProviderAdapterResumeThreadError,
  ProviderAdapterRollbackThreadError,
  ProviderAdapterRuntimeRequestResponseError,
  ProviderAdapterSteerRunError,
  ProviderAdapterTurnStartError,
  ProviderAdapterV2,
  type ProviderAdapterV2Error,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2EnsureThreadInput,
  type ProviderAdapterV2OpenSessionInput,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2Shape,
  type ProviderAdapterV2ThreadSnapshot,
  type ProviderAdapterV2TurnInput,
} from "../ProviderAdapter.ts";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "../ProviderAdapterDriver.ts";
import { makeProviderFailure, makeProviderRetryTurnItem } from "../ProviderFailure.ts";
import { turnScopedSelectionTransition } from "../ProviderSelectionTransition.ts";

export const DROID_PROVIDER = ProviderDriverKind.make("droid");
const DEFAULT_DROID_SETTINGS = Schema.decodeSync(DroidSettings)({});

const STREAM_FLUSH_MS = 50;
const DROID_REQUEST_TIMEOUT_MS = 15_000;
// Session lifecycle requests start MCP servers and load history.
const DROID_SESSION_TIMEOUT_MS = 75_000;
// After Stop, Droid normally settles the cancelled run at once. Past this the
// adapter settles the turn itself and restarts the process before new work.
const DROID_INTERRUPT_SETTLE_MS = 5_000;

const DroidProviderCapabilitiesV2 = {
  runtimePolicy: { enforcement: "native" },
  sessions: {
    supportsMultipleProviderThreadsPerSession: false,
    supportsModelSwitchInSession: true,
    supportsProviderSwitchingViaHandoff: true,
    // Autonomy level is a session setting Droid applies to its next run.
    supportsRuntimeModeSwitchInSession: true,
    pendingRequestsSurviveRestart: false,
  },
  threads: {
    canCreateEmptyThread: true,
    canReadThreadSnapshot: false,
    canRollbackThread: true,
    canForkThread: false,
    canForkFromTurn: false,
    canForkFromSubagentThread: false,
    exposesNativeThreadId: true,
  },
  turns: {
    exposesNativeTurnId: true,
    emitsTurnStarted: true,
    emitsTurnCompleted: true,
    supportsInterrupt: true,
    supportsActiveSteering: true,
    supportsSteeringByInterruptRestart: false,
    supportsQueuedMessages: true,
    terminalStatusQuality: "strong",
  },
  streaming: {
    streamsAssistantText: true,
    streamsReasoning: true,
    streamsToolOutput: false,
    streamsPlanText: false,
    emitsMessageCompleted: true,
  },
  tools: {
    exposesToolItemIds: true,
    emitsToolStarted: true,
    emitsToolCompleted: true,
    emitsToolOutput: true,
    supportsMcpTools: true,
    supportsDynamicToolCallbacks: false,
  },
  approvals: {
    supportsCommandApproval: true,
    supportsFileReadApproval: false,
    supportsFileChangeApproval: true,
    supportsApplyPatchApproval: true,
    approvalsHaveNativeRequestIds: true,
    approvalCallbacksAreLiveOnly: true,
    approvalsCanOriginateFromSubagents: false,
  },
  planning: {
    emitsPlanUpdated: true,
    emitsTodoList: false,
    emitsProposedPlan: true,
    supportsStructuredQuestions: true,
    planDeltasHaveItemIds: false,
  },
  subagents: {
    supportsSubagents: true,
    exposesSubagentThreadIds: false,
    emitsSubagentLifecycle: true,
    canWaitForSubagents: false,
    canCloseSubagents: false,
    canForkSubagentThread: false,
  },
  context: {
    acceptsSystemContext: false,
    acceptsDeveloperContext: false,
    acceptsSyntheticUserContext: true,
    canGenerateSummaries: false,
    canConsumeHandoffSummaries: true,
    supportsDeltaHandoff: true,
    supportsFullThreadHandoff: true,
    maxRecommendedHandoffChars: null,
  },
  checkpointing: {
    appCanCheckpointFilesystem: true,
    supportsNestedCheckpointScopes: false,
    providerCanRollbackConversation: true,
    providerRollbackReturnsSnapshot: true,
    providerCanReadConversationSnapshot: false,
  },
  identity: {
    nativeThreadIds: "strong",
    nativeTurnIds: "strong",
    nativeItemIds: "strong",
    nativeRequestIds: "strong",
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface DroidAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly settings: DroidSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly fileSystem: FileSystem.FileSystem;
  readonly crypto: Crypto.Crypto;
  readonly idAllocator: IdAllocatorV2["Service"];
  readonly serverConfig: ServerConfig["Service"];
}

// ── protocol mapping ──────────────────────────────────────────

type DroidAutonomyLevel = "off" | "low" | "medium" | "high";
type DroidInteractionMode = "auto" | "spec";

/** T3 runtime modes map 1:1 onto Droid autonomy levels. */
export function droidAutonomyLevelForRuntimeMode(runtimeMode: RuntimeMode): DroidAutonomyLevel {
  switch (runtimeMode) {
    case "approval-required":
      return "off";
    case "auto-accept-edits":
      return "low";
    case "auto":
      return "medium";
    case "full-access":
      return "high";
  }
}

type DroidApprovalDecision = Exclude<ProviderApprovalDecision, "cancel">;

/**
 * Pick the Droid outcome for a T3 decision. The reply must be one of the
 * offered outcomes, which Droid versions independently, so classification is
 * positive-only: an outcome this adapter does not recognize is never
 * selectable, because misbinding it could grant more than the user chose.
 */
export function selectDroidPermissionOutcome(
  options: ReadonlyArray<DroidPermissionOption>,
  decision: DroidApprovalDecision,
): string | undefined {
  const outcomes = options.map((option) => option.outcome).filter((outcome) => outcome.trim());
  switch (decision) {
    case "accept":
      return outcomes.find((outcome) => {
        const trimmed = outcome.trim();
        return trimmed.startsWith("proceed_") && !trimmed.startsWith("proceed_always");
      });
    case "acceptForSession":
      return outcomes.find((outcome) => outcome.trim().startsWith("proceed_always"));
    case "decline":
      return outcomes.find((outcome) => outcome.trim() === "cancel");
  }
}

const DROID_DECISION_FALLBACK_LABELS = {
  accept: "Approve",
  acceptForSession: "Always allow this session",
  decline: "Decline",
} as const;

/** Approval choices this request supports, labeled with the CLI's own labels. */
export function droidApprovalOptions(
  options: ReadonlyArray<DroidPermissionOption>,
): ReadonlyArray<ProviderApprovalOption> {
  const approvalOptions: Array<ProviderApprovalOption> = [];
  for (const decision of ["accept", "acceptForSession", "decline"] as const) {
    const outcome = selectDroidPermissionOutcome(options, decision);
    if (outcome === undefined) continue;
    const label = options.find((option) => option.outcome === outcome)?.label.trim();
    approvalOptions.push({ decision, label: label || DROID_DECISION_FALLBACK_LABELS[decision] });
  }
  return approvalOptions;
}

const decodeConfirmationDetails = Schema.decodeUnknownOption(DroidToolConfirmationDetails);
type DroidConfirmationDetails = DroidToolConfirmationDetails;

function droidRequestKind(details: DroidConfirmationDetails | undefined): ProviderRequestKind {
  switch (details?.type) {
    case "exec":
    case "sandbox_violation":
    case "droid_shield_violation":
      return "command";
    case "edit":
    case "create":
    case "apply_patch":
      return "file-change";
    default:
      return "permission";
  }
}

/** The summary shown next to the approval buttons. */
function droidPermissionPrompt(
  details: DroidConfirmationDetails | undefined,
  toolUse: DroidToolUse,
): string {
  switch (details?.type) {
    case "exec":
      return details.fullCommand.trim() || details.command;
    case "edit":
    case "create":
      return details.filePath;
    case "apply_patch": {
      const files = details.files?.map((file) => file.filePath);
      return files && files.length > 0 ? files.join("\n") : details.filePath;
    }
    case "exit_spec_mode":
      return details.title ? `${details.title}\n\n${details.plan}` : details.plan;
    case "propose_mission":
      return details.title ? `${details.title}\n\n${details.proposal}` : details.proposal;
    case "start_mission_run":
      return `Start a mission run (${details.runningMissionCount} already running).`;
    case "mcp_tool":
      return details.serverName
        ? `${details.serverName}: ${details.actualToolName ?? details.toolName}`
        : details.toolName;
    case "ask_user":
      return details.questionnaire;
    case "sandbox_violation":
      return `${details.violatingToolName} attempted a ${details.operationType} of ${details.target}: ${details.reason}`;
    case "droid_shield_violation":
      return `${details.command}\n${details.reason}`;
    case undefined:
      return `Allow ${toolUse.name}?`;
  }
}

type DroidTurnOutcome =
  | { readonly status: "completed" }
  | { readonly status: "cancelled" }
  | { readonly status: "failed"; readonly failure: OrchestrationV2ProviderFailure };

function droidTurnOutcome(reason: string, lastErrorMessage: string | null): DroidTurnOutcome {
  switch (reason) {
    case "completed":
    // A spec handoff finishes planning and continues in the implementation.
    case "spec_handoff":
    // The user declined an approval; Droid stopped and the thread is reusable.
    case "permission_rejected":
    case "prompt_rejected":
      return { status: "completed" };
    case "cancelled":
      return { status: "cancelled" };
    case "model_authentication_failed":
      return {
        status: "failed",
        failure: makeProviderFailure({
          message: "Droid is not signed in. Run `droid` in a terminal to sign in to Factory.",
          class: "permission_error",
          retryable: false,
        }),
      };
    case "model_usage_exhausted":
      return {
        status: "failed",
        failure: makeProviderFailure({
          message: lastErrorMessage ?? "Droid model usage is exhausted.",
          class: "usage_limit",
          retryable: false,
        }),
      };
    case "no_approver_available":
      return {
        status: "failed",
        failure: makeProviderFailure({
          message: "Droid required an approval that no client answered.",
          class: "provider_error",
        }),
      };
    default:
      return {
        status: "failed",
        failure: makeProviderFailure({
          message: lastErrorMessage ?? `Droid turn ended with reason '${reason}'.`,
          class: "provider_error",
        }),
      };
  }
}

/**
 * Cumulative spend is not the context meter: Droid compacts automatically and
 * reports the live context in `lastCallTokenUsage`.
 */
export function droidContextUsage(
  usage: DroidTokenUsage,
  lastCall: DroidLastCallTokenUsage | undefined,
): ThreadTokenUsageSnapshot {
  const inputTokens = usage.inputTokens;
  const cachedInputTokens = usage.cacheReadTokens;
  const outputTokens = usage.outputTokens;
  const cumulative = inputTokens + cachedInputTokens + outputTokens;
  const lastUsedTokens =
    lastCall === undefined
      ? undefined
      : lastCall.inputTokens + lastCall.cacheReadTokens + (lastCall.outputTokens ?? 0);
  return {
    usedTokens: lastUsedTokens ?? cumulative,
    totalProcessedTokens: cumulative + usage.cacheCreationTokens,
    inputTokens,
    cachedInputTokens,
    outputTokens,
    reasoningOutputTokens: usage.thinkingTokens,
    ...(lastCall !== undefined && lastUsedTokens !== undefined
      ? {
          lastUsedTokens,
          lastInputTokens: lastCall.inputTokens,
          lastCachedInputTokens: lastCall.cacheReadTokens,
          lastOutputTokens: lastCall.outputTokens ?? 0,
        }
      : {}),
    compactsAutomatically: true,
  };
}

function providerRef(
  nativeId: string,
  strength: "strong" | "weak" = "strong",
): OrchestrationV2ProviderRef {
  return { driver: DROID_PROVIDER, nativeId, strength };
}

function recordString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  const trimmed = typeof value === "string" ? value.trim() : "";
  return trimmed.length > 0 ? trimmed : undefined;
}

function toolResultText(content: unknown): string | undefined {
  if (typeof content === "string") return content.length > 0 ? content : undefined;
  if (Array.isArray(content)) {
    const text = content
      .map((block) =>
        typeof block === "object" && block !== null && (block as { type?: unknown }).type === "text"
          ? String((block as { text?: unknown }).text ?? "")
          : "",
      )
      .join("");
    return text.length > 0 ? text : undefined;
  }
  return undefined;
}

// ── per-session state ─────────────────────────────────────────

interface DroidStreamItem {
  readonly nativeItemId: string;
  readonly kind: "assistant_message" | "reasoning";
  text: string;
  completed: boolean;
  flushScheduled: boolean;
  readonly startedAt: DateTime.Utc;
}

interface DroidToolState {
  readonly toolUseId: string;
  name: string;
  input: Record<string, unknown>;
  readonly startedAt: DateTime.Utc;
  completed: boolean;
}

interface DroidSubagentState {
  readonly childSessionId: string;
  readonly toolUseId: string | undefined;
  readonly description: string;
  readonly startedAt: DateTime.Utc;
  progress: string | undefined;
  completed: boolean;
}

interface DroidRetryState {
  readonly startedAt: DateTime.Utc;
  readonly itemOrdinal: number;
  attempt: number;
}

interface ActiveDroidTurn {
  readonly turnInput: ProviderAdapterV2TurnInput;
  readonly providerTurn: OrchestrationV2ProviderTurn;
  readonly itemOrdinals: Map<string, number>;
  nextItemOrdinal: number;
  /** Open stream item per `${kind}:${messageId}`; a later block opens a new item. */
  readonly streamItems: Map<string, DroidStreamItem>;
  readonly streamItemCounts: Map<string, number>;
  readonly allStreamItems: Array<DroidStreamItem>;
  readonly tools: Map<string, DroidToolState>;
  readonly subagents: Map<string, DroidSubagentState>;
  readonly subagentByToolUseId: Map<string, string>;
  /** Physical runs still owed a terminal, keyed by their user-message id. */
  readonly pendingRunMessageIds: Set<string>;
  /** Pending steer ids Droid already persisted into a running physical run. */
  readonly persistedRunMessageIds: Set<string>;
  interrupted: boolean;
  lastErrorMessage: string | null;
  planCaptured: boolean;
  retry: DroidRetryState | null;
  tokenUsage: OrchestrationV2ProviderTurnTokenUsage | undefined;
}

type PendingDroidRequest =
  | {
      readonly type: "approval";
      readonly native: DroidPermissionServerRequest;
      readonly options: ReadonlyArray<DroidPermissionOption>;
      runtimeRequest: OrchestrationV2RuntimeRequest;
      readonly node: OrchestrationV2ExecutionNode;
      readonly turnItem: OrchestrationV2TurnItem;
    }
  | {
      readonly type: "user_input";
      readonly native: DroidAskUserServerRequest;
      runtimeRequest: OrchestrationV2RuntimeRequest;
      readonly node: OrchestrationV2ExecutionNode;
      readonly turnItem: OrchestrationV2TurnItem;
    };

interface DroidThreadState {
  providerThread: OrchestrationV2ProviderThread;
  activeTurn: ActiveDroidTurn | null;
}

interface AppliedDroidSettings {
  autonomyLevel: DroidAutonomyLevel | null;
  interactionMode: DroidInteractionMode | null;
  modelId: string | null;
  reasoningEffort: string | null;
}

const decodeRewindResult = Schema.decodeUnknownEffect(DroidExecuteRewindResult);

const DroidSessionSettingsResult = Schema.Struct({
  sessionId: Schema.optional(Schema.String),
  settings: Schema.optional(
    Schema.Struct({
      modelId: Schema.optional(Schema.String),
      reasoningEffort: Schema.optional(Schema.String),
    }),
  ),
  tokenUsage: Schema.optional(Schema.Unknown),
});
const decodeSessionSettingsResult = Schema.decodeUnknownEffect(DroidSessionSettingsResult);

/**
 * Droid takes MCP servers as named configs with `{name, value}` header pairs.
 * Recomputed per call so loads pick up the current T3 MCP credential.
 */
function droidMcpServersParam(threadId: ThreadId) {
  const mcpSession = McpProviderSession.readMcpProviderSession(threadId);
  return mcpSession
    ? {
        mcpServers: [
          {
            type: "http" as const,
            name: "t3-code",
            url: mcpSession.endpoint,
            headers: [{ name: "Authorization", value: mcpSession.authorizationHeader }],
          },
        ],
      }
    : {};
}

// ── adapter ───────────────────────────────────────────────────

export function makeDroidAdapterV2(options: DroidAdapterV2Options): ProviderAdapterV2Shape {
  const { idAllocator } = options;

  const protocolError = (detail: string, payload?: unknown) =>
    new ProviderAdapterProtocolError({
      driver: DROID_PROVIDER,
      detail,
      ...(payload === undefined ? {} : { payload }),
    });

  return ProviderAdapterV2.of({
    instanceId: options.instanceId,
    driver: DROID_PROVIDER,
    getCapabilities: () => Effect.succeed(DroidProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed(turnScopedSelectionTransition()),
    openSession: Effect.fn("DroidAdapterV2.openSession")(function* (
      input: ProviderAdapterV2OpenSessionInput,
    ) {
      const scope = yield* Effect.scope;
      const cwd = input.runtimePolicy.cwd ?? options.serverConfig.cwd;
      const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
      // Droid has a system-prompt channel, so T3 context never wraps user text.
      const systemPrompt = [
        buildRuntimeInstructions({ harness: "Factory Droid" }),
        t3OrchestrationSystemPrompt(mcpSession !== undefined)?.trim(),
      ]
        .filter((part): part is string => part !== undefined && part.length > 0)
        .join("\n\n");
      const rpc: DroidRpcClient = yield* makeDroidRpcClient({
        command: options.settings.binaryPath || "droid",
        args: [
          "exec",
          "--input-format",
          "stream-jsonrpc",
          "--output-format",
          "stream-jsonrpc",
          "--append-system-prompt",
          systemPrompt,
        ],
        cwd,
        env: options.environment,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, options.spawner),
        Effect.mapError(
          (cause) =>
            new ProviderAdapterOpenSessionError({
              driver: DROID_PROVIDER,
              providerSessionId: input.providerSessionId,
              cause,
            }),
        ),
      );

      const now = yield* DateTime.now;
      let sessionEntity: OrchestrationV2ProviderSession = {
        id: input.providerSessionId,
        driver: DROID_PROVIDER,
        providerInstanceId: options.instanceId,
        status: "ready",
        cwd,
        model: input.modelSelection.model,
        capabilities: DroidProviderCapabilitiesV2,
        createdAt: now,
        updatedAt: now,
        lastError: null,
      };
      const events = yield* Queue.unbounded<
        ProviderAdapterV2Event,
        ProviderAdapterV2Error | Cause.Done
      >();
      const pendingRequests = new Map<string, PendingDroidRequest>();
      // Notifications, server requests, and turn start/answer paths all
      // publish lifecycle events; serialize them so a terminal cannot
      // overtake the updates that precede it.
      const sessionEventPermit = yield* Semaphore.make(1);
      let threadState: DroidThreadState | null = null;
      let droidSessionId: string | null = null;
      let baselineModelId: string | null = null;
      const applied: AppliedDroidSettings = {
        autonomyLevel: null,
        interactionMode: null,
        modelId: null,
        reasoningEffort: null,
      };
      // Stop and runtime restarts end the process deliberately.
      let stopRequested = false;
      // Message ids of stopped runs whose terminal has not arrived yet.
      const interruptedRunMessageIds = new Set<string>();
      let interruptSettled: Deferred.Deferred<void> | null = null;

      const emit = (event: ProviderAdapterV2Event) =>
        Queue.offer(events, event).pipe(Effect.asVoid);

      const updateProviderSession = (
        status: OrchestrationV2ProviderSession["status"],
        lastError: string | null = sessionEntity.lastError,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, status, lastError, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: DROID_PROVIDER,
            providerSession: sessionEntity,
          });
        });

      const updateProviderThread = (
        state: DroidThreadState,
        patch: Partial<OrchestrationV2ProviderThread>,
      ) =>
        Effect.gen(function* () {
          const updatedAt = yield* DateTime.now;
          state.providerThread = { ...state.providerThread, ...patch, updatedAt };
          yield* emit({
            type: "provider_thread.updated",
            driver: DROID_PROVIDER,
            providerThread: state.providerThread,
          });
        });

      const request = (method: string, params: unknown, timeoutMs = DROID_REQUEST_TIMEOUT_MS) =>
        rpc.request(method, params, { timeoutMs });

      const itemOrdinal = (turn: ActiveDroidTurn, nativeItemId: string): number => {
        const existing = turn.itemOrdinals.get(nativeItemId);
        if (existing !== undefined) return existing;
        const ordinal = turn.nextItemOrdinal++;
        turn.itemOrdinals.set(nativeItemId, ordinal);
        return ordinal;
      };

      const baseItemFields = (
        turn: ActiveDroidTurn,
        nativeItemId: string,
        startedAt: DateTime.Utc,
        updatedAt: DateTime.Utc,
      ) => ({
        id: idAllocator.derive.turnItemFromProviderItem({
          driver: DROID_PROVIDER,
          nativeItemId,
        }),
        threadId: turn.turnInput.threadId,
        runId: turn.turnInput.runId,
        nodeId: idAllocator.derive.nodeFromProviderItem({
          driver: DROID_PROVIDER,
          nativeItemId,
        }),
        providerThreadId: turn.turnInput.providerThread.id,
        providerTurnId: turn.providerTurn.id,
        nativeItemRef: providerRef(nativeItemId),
        parentItemId: null,
        ordinal: itemOrdinal(turn, nativeItemId),
        startedAt,
        updatedAt,
      });

      const emitItemNode = (
        turn: ActiveDroidTurn,
        nativeItemId: string,
        kind: OrchestrationV2ExecutionNode["kind"],
        status: OrchestrationV2ExecutionNode["status"],
        startedAt: DateTime.Utc,
        completedAt: DateTime.Utc | null,
      ) =>
        emit({
          type: "node.updated",
          driver: DROID_PROVIDER,
          node: {
            id: idAllocator.derive.nodeFromProviderItem({
              driver: DROID_PROVIDER,
              nativeItemId,
            }),
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId: turn.turnInput.rootNodeId,
            rootNodeId: turn.turnInput.rootNodeId,
            kind,
            status,
            countsForRun: false,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            nativeItemRef: providerRef(nativeItemId),
            runtimeRequestId: null,
            checkpointScopeId: null,
            startedAt,
            completedAt,
          },
        });

      // ── streaming text / reasoning ────────────────────────

      const emitStreamItem = (turn: ActiveDroidTurn, item: DroidStreamItem, streaming: boolean) =>
        Effect.gen(function* () {
          const emittedAt = yield* DateTime.now;
          const base = baseItemFields(turn, item.nativeItemId, item.startedAt, emittedAt);
          yield* emitItemNode(
            turn,
            item.nativeItemId,
            item.kind,
            streaming ? "running" : "completed",
            item.startedAt,
            streaming ? null : emittedAt,
          );
          if (item.kind === "assistant_message") {
            const messageId = idAllocator.derive.messageFromProviderItem({
              driver: DROID_PROVIDER,
              nativeItemId: item.nativeItemId,
            });
            yield* emit({
              type: "turn_item.updated",
              driver: DROID_PROVIDER,
              turnItem: {
                ...base,
                status: streaming ? "running" : "completed",
                title: null,
                completedAt: streaming ? null : emittedAt,
                type: "assistant_message",
                messageId,
                text: item.text,
                streaming,
              },
            });
            yield* emit({
              type: "message.updated",
              driver: DROID_PROVIDER,
              message: {
                id: messageId,
                threadId: turn.turnInput.threadId,
                runId: turn.turnInput.runId,
                nodeId: idAllocator.derive.nodeFromProviderItem({
                  driver: DROID_PROVIDER,
                  nativeItemId: item.nativeItemId,
                }),
                role: "assistant",
                text: item.text,
                attachments: [],
                streaming,
                createdBy: "agent",
                creationSource: "provider",
                createdAt: item.startedAt,
                updatedAt: emittedAt,
              },
            });
            return;
          }
          yield* emit({
            type: "turn_item.updated",
            driver: DROID_PROVIDER,
            turnItem: {
              ...base,
              status: streaming ? "running" : "completed",
              title: null,
              completedAt: streaming ? null : emittedAt,
              type: "reasoning",
              text: item.text,
              streaming,
            },
          });
        });

      const scheduleStreamFlush = (turn: ActiveDroidTurn, item: DroidStreamItem) =>
        Effect.gen(function* () {
          if (item.flushScheduled || item.completed) return;
          item.flushScheduled = true;
          yield* Effect.sleep(Duration.millis(STREAM_FLUSH_MS)).pipe(
            Effect.andThen(
              sessionEventPermit.withPermits(1)(
                Effect.suspend(() => {
                  item.flushScheduled = false;
                  return item.completed ? Effect.void : emitStreamItem(turn, item, true);
                }),
              ),
            ),
            Effect.forkIn(scope),
          );
        });

      const streamItemFor = Effect.fnUntraced(function* (
        turn: ActiveDroidTurn,
        kind: DroidStreamItem["kind"],
        messageId: string,
      ) {
        const key = `${kind}:${messageId}`;
        const existing = turn.streamItems.get(key);
        if (existing !== undefined && !existing.completed) return existing;
        const index = turn.streamItemCounts.get(key) ?? 0;
        turn.streamItemCounts.set(key, index + 1);
        const prefix = kind === "assistant_message" ? "msg" : "reasoning";
        const startedAt = yield* DateTime.now;
        const item: DroidStreamItem = {
          nativeItemId: index === 0 ? `${prefix}:${messageId}` : `${prefix}:${messageId}:${index}`,
          kind,
          text: "",
          completed: false,
          flushScheduled: false,
          startedAt,
        };
        turn.streamItems.set(key, item);
        turn.allStreamItems.push(item);
        // Ordinal reserved on first delta so items appear in stream order.
        itemOrdinal(turn, item.nativeItemId);
        return item;
      });

      const completeStreamItem = (turn: ActiveDroidTurn, item: DroidStreamItem) =>
        Effect.suspend(() => {
          if (item.completed) return Effect.void;
          item.completed = true;
          return item.text.length === 0 ? Effect.void : emitStreamItem(turn, item, false);
        });

      const completeOpenStreamItems = (turn: ActiveDroidTurn) =>
        Effect.forEach(
          turn.allStreamItems.filter((item) => !item.completed),
          (item) => completeStreamItem(turn, item),
          { discard: true },
        );

      // ── tools ─────────────────────────────────────────────

      const emitToolItem = Effect.fnUntraced(function* (
        turn: ActiveDroidTurn,
        tool: DroidToolState,
        status: "running" | "completed" | "failed" | "interrupted",
        output?: string,
      ) {
        const emittedAt = yield* DateTime.now;
        const completedAt = status === "running" ? null : emittedAt;
        yield* emitItemNode(turn, tool.toolUseId, "tool_call", status, tool.startedAt, completedAt);
        const shared = {
          ...baseItemFields(turn, tool.toolUseId, tool.startedAt, emittedAt),
          status,
          title: tool.name,
          completedAt,
        } as const;
        const input = tool.input;
        switch (tool.name) {
          case "Execute": {
            yield* emit({
              type: "turn_item.updated",
              driver: DROID_PROVIDER,
              turnItem: {
                ...shared,
                type: "command_execution",
                input: recordString(input, "command") ?? "",
                ...(output === undefined ? {} : { output }),
                ...(status === "failed" ? { outputIndicatesFailure: true } : {}),
              },
            });
            return;
          }
          case "Create":
          case "Edit":
          case "MultiEdit":
          case "Write":
          case "ApplyPatch": {
            const fileName =
              recordString(input, "file_path") ??
              recordString(input, "filePath") ??
              recordString(input, "path");
            if (fileName !== undefined) {
              yield* emit({
                type: "turn_item.updated",
                driver: DROID_PROVIDER,
                turnItem: { ...shared, type: "file_change", fileName },
              });
              return;
            }
            break;
          }
          case "Grep":
          case "Glob": {
            const pattern = recordString(input, "pattern") ?? recordString(input, "patterns");
            yield* emit({
              type: "turn_item.updated",
              driver: DROID_PROVIDER,
              turnItem: {
                ...shared,
                type: "file_search",
                ...(pattern === undefined ? {} : { pattern }),
              },
            });
            return;
          }
          case "WebSearch": {
            const query = recordString(input, "query");
            yield* emit({
              type: "turn_item.updated",
              driver: DROID_PROVIDER,
              turnItem: {
                ...shared,
                type: "web_search",
                ...(query === undefined ? {} : { patterns: [query] }),
              },
            });
            return;
          }
        }
        yield* emit({
          type: "turn_item.updated",
          driver: DROID_PROVIDER,
          turnItem: {
            ...shared,
            type: "dynamic_tool",
            toolName: tool.name.trim() || "tool",
            input,
            ...(output === undefined ? {} : { output }),
          },
        });
      });

      // ── subagents ─────────────────────────────────────────

      const emitSubagent = Effect.fnUntraced(function* (
        turn: ActiveDroidTurn,
        subagent: DroidSubagentState,
        status: "running" | "completed" | "failed" | "interrupted",
      ) {
        const emittedAt = yield* DateTime.now;
        const nativeTaskId = `subagent:${subagent.childSessionId}`;
        const subagentId = idAllocator.derive.nodeFromProviderItem({
          driver: DROID_PROVIDER,
          nativeItemId: nativeTaskId,
        });
        const parentNodeId =
          subagent.toolUseId === undefined
            ? turn.turnInput.rootNodeId
            : idAllocator.derive.nodeFromProviderItem({
                driver: DROID_PROVIDER,
                nativeItemId: subagent.toolUseId,
              });
        const finished = status !== "running";
        const progress =
          !finished && subagent.progress !== undefined
            ? { progress: subagent.progress.slice(0, 200) }
            : {};
        yield* emit({
          type: "subagent.updated",
          driver: DROID_PROVIDER,
          subagent: {
            id: subagentId,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            parentNodeId,
            origin: "provider_native",
            createdBy: "agent",
            driver: DROID_PROVIDER,
            providerInstanceId: options.instanceId,
            providerThreadId: turn.turnInput.providerThread.id,
            childThreadId: null,
            nativeTaskRef: providerRef(subagent.childSessionId),
            prompt: subagent.description,
            title: subagent.description,
            model: null,
            status,
            ...progress,
            result: null,
            startedAt: subagent.startedAt,
            completedAt: finished ? emittedAt : null,
            updatedAt: emittedAt,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: DROID_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeTaskId, subagent.startedAt, emittedAt),
            status,
            title: subagent.description,
            completedAt: finished ? emittedAt : null,
            type: "subagent",
            subagentId,
            origin: "provider_native",
            driver: DROID_PROVIDER,
            providerInstanceId: options.instanceId,
            childThreadId: null,
            prompt: subagent.description,
            ...progress,
            result: null,
          },
        });
      });

      const settleSubagent = (
        turn: ActiveDroidTurn,
        childSessionId: string,
        status: "completed" | "failed" | "interrupted",
      ) =>
        Effect.suspend(() => {
          const subagent = turn.subagents.get(childSessionId);
          if (subagent === undefined || subagent.completed) return Effect.void;
          subagent.completed = true;
          return emitSubagent(turn, subagent, status);
        });

      // ── plan capture ──────────────────────────────────────

      const emitProposedPlan = Effect.fnUntraced(function* (
        turn: ActiveDroidTurn,
        toolUseId: string,
        markdown: string,
      ) {
        const nativeItemId = `plan:${toolUseId}`;
        const emittedAt = yield* DateTime.now;
        const nodeId = idAllocator.derive.nodeFromProviderItem({
          driver: DROID_PROVIDER,
          nativeItemId,
        });
        const planId = yield* idAllocator.allocate.plan({
          threadId: turn.turnInput.threadId,
          runId: turn.turnInput.runId,
          driver: DROID_PROVIDER,
        });
        const plan: OrchestrationV2PlanArtifact = {
          id: planId,
          threadId: turn.turnInput.threadId,
          runId: turn.turnInput.runId,
          nodeId,
          kind: "proposed_plan",
          status: "active",
          markdown,
        };
        yield* emitItemNode(turn, nativeItemId, "plan", "completed", emittedAt, emittedAt);
        yield* emit({ type: "plan.updated", driver: DROID_PROVIDER, plan });
        yield* emit({
          type: "turn_item.updated",
          driver: DROID_PROVIDER,
          turnItem: {
            ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
            status: "completed",
            title: null,
            completedAt: emittedAt,
            type: "proposed_plan",
            planId,
            markdown,
            streaming: false,
          },
        });
      });

      // ── runtime requests ──────────────────────────────────

      /** Droid streams a tool's input, so a repeated tool_call refines the known tool. */
      const rememberToolUse = (turn: ActiveDroidTurn, toolUse: DroidToolUse) => {
        const existing = turn.tools.get(toolUse.id);
        if (existing !== undefined) {
          existing.name = toolUse.name;
          if (Object.keys(toolUse.input).length > 0) existing.input = toolUse.input;
          return existing;
        }
        return undefined;
      };

      const resolvePendingRequest = Effect.fnUntraced(function* (
        pending: PendingDroidRequest,
        status: "resolved" | "cancelled",
        resolution: {
          readonly decision?: ProviderApprovalDecision;
          readonly answers?: ProviderUserInputAnswers;
        } = {},
      ) {
        const resolvedAt = yield* DateTime.now;
        pending.runtimeRequest = {
          ...pending.runtimeRequest,
          status,
          resolvedAt,
          ...(resolution.decision === undefined ? {} : { decision: resolution.decision }),
          ...(resolution.answers === undefined ? {} : { answers: resolution.answers }),
        };
        yield* emit({
          type: "runtime_request.updated",
          driver: DROID_PROVIDER,
          threadId: pending.node.threadId,
          runtimeRequest: pending.runtimeRequest,
        });
        yield* emit({
          type: "node.updated",
          driver: DROID_PROVIDER,
          node: {
            ...pending.node,
            status: status === "resolved" ? "completed" : "cancelled",
            completedAt: resolvedAt,
          },
        });
        yield* emit({
          type: "turn_item.updated",
          driver: DROID_PROVIDER,
          turnItem: {
            ...pending.turnItem,
            status: status === "resolved" ? "completed" : "cancelled",
            completedAt: resolvedAt,
            updatedAt: resolvedAt,
          },
        });
      });

      /** Retire every open request; Droid has already abandoned (or will) the run. */
      const cancelPendingRequests = Effect.gen(function* () {
        const pending = Array.from(pendingRequests.values());
        pendingRequests.clear();
        yield* Effect.forEach(
          pending,
          (entry) =>
            entry.native
              .fail(-32800, "The request was cancelled.")
              .pipe(Effect.ignore, Effect.andThen(resolvePendingRequest(entry, "cancelled"))),
          { discard: true },
        );
      });

      const openRuntimeRequest = Effect.fnUntraced(function* (
        turn: ActiveDroidTurn,
        native: DroidServerRequest,
        artifact:
          | {
              readonly type: "approval";
              readonly requestKind: ProviderRequestKind;
              readonly prompt: string;
              readonly title: string;
              readonly options: ReadonlyArray<DroidPermissionOption>;
            }
          | {
              readonly type: "user_input";
              readonly questions: ReadonlyArray<OrchestrationV2UserInputQuestion>;
            },
      ) {
        const createdAt = yield* DateTime.now;
        const nativeRequestId = `${native.method}:${native.id}`;
        const requestId = yield* idAllocator.allocate.runtimeRequest({
          driver: DROID_PROVIDER,
          providerTurnId: turn.providerTurn.id,
          nativeRequestId,
        });
        const nodeId = idAllocator.derive.approvalNode({ requestId });
        const runtimeRequest: OrchestrationV2RuntimeRequest = {
          id: requestId,
          nodeId,
          providerTurnId: turn.providerTurn.id,
          nativeRequestRef: providerRef(nativeRequestId),
          kind: artifact.type === "approval" ? artifact.requestKind : "user_input",
          status: "pending",
          responseCapability: { type: "live", providerSessionId: input.providerSessionId },
          createdAt,
          resolvedAt: null,
        };
        const node: OrchestrationV2ExecutionNode = {
          id: nodeId,
          threadId: turn.turnInput.threadId,
          runId: turn.turnInput.runId,
          parentNodeId: turn.turnInput.rootNodeId,
          rootNodeId: turn.turnInput.rootNodeId,
          kind: artifact.type === "approval" ? "approval_request" : "user_input_request",
          status: "waiting",
          countsForRun: false,
          providerThreadId: turn.turnInput.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: providerRef(nativeRequestId),
          runtimeRequestId: requestId,
          checkpointScopeId: null,
          startedAt: createdAt,
          completedAt: null,
        };
        const itemBase = {
          id: idAllocator.derive.approvalTurnItem({ requestId }),
          threadId: turn.turnInput.threadId,
          runId: turn.turnInput.runId,
          nodeId,
          providerThreadId: turn.turnInput.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          nativeItemRef: providerRef(nativeRequestId),
          parentItemId: null,
          ordinal: itemOrdinal(turn, nativeRequestId),
          status: "waiting" as const,
          startedAt: createdAt,
          completedAt: null,
          updatedAt: createdAt,
        };
        const turnItem: OrchestrationV2TurnItem =
          artifact.type === "approval"
            ? {
                ...itemBase,
                title: artifact.title,
                type: "approval_request",
                requestId,
                requestKind: artifact.requestKind,
                prompt: artifact.prompt,
                ...(artifact.options.length === 0
                  ? {}
                  : { options: [...droidApprovalOptions(artifact.options)] }),
              }
            : {
                ...itemBase,
                title: "Droid question",
                type: "user_input_request",
                requestId,
                questions: [...artifact.questions],
              };
        pendingRequests.set(
          String(requestId),
          native.method === "droid.request_permission"
            ? {
                type: "approval",
                native,
                options: artifact.type === "approval" ? artifact.options : [],
                runtimeRequest,
                node,
                turnItem,
              }
            : { type: "user_input", native, runtimeRequest, node, turnItem },
        );
        yield* emit({
          type: "runtime_request.updated",
          driver: DROID_PROVIDER,
          threadId: turn.turnInput.threadId,
          runtimeRequest,
        });
        yield* emit({ type: "node.updated", driver: DROID_PROVIDER, node });
        yield* emit({ type: "turn_item.updated", driver: DROID_PROVIDER, turnItem });
      });

      const handlePermissionRequest = Effect.fnUntraced(function* (
        turn: ActiveDroidTurn,
        native: DroidPermissionServerRequest,
      ) {
        const params: DroidPermissionRequest = native.params;
        const primary = params.toolUses[0]!;
        const details = Option.getOrUndefined(decodeConfirmationDetails(primary.details));
        rememberToolUse(turn, primary.toolUse);
        if (details?.type === "exit_spec_mode") {
          const keepIterating = params.options.find((option) => option.outcome.trim() === "cancel");
          if (keepIterating !== undefined) {
            // T3 owns plan approval: show the spec as a proposed plan and let
            // Droid finish the planning turn.
            const markdown = details.title ? `# ${details.title}\n\n${details.plan}` : details.plan;
            yield* emitProposedPlan(turn, primary.toolUse.id, markdown);
            turn.planCaptured = true;
            yield* native.respond({ selectedOption: keepIterating.outcome });
            return;
          }
        }
        if (droidApprovalOptions(params.options).length === 0) {
          yield* native.fail(-32602, "Droid permission request has no supported decisions");
          return;
        }
        const requestKind = droidRequestKind(details);
        yield* openRuntimeRequest(turn, native, {
          type: "approval",
          requestKind,
          prompt: droidPermissionPrompt(details, primary.toolUse),
          title: primary.toolUse.name,
          options: params.options,
        });
      });

      const handleAskUserRequest = Effect.fnUntraced(function* (
        turn: ActiveDroidTurn,
        native: DroidAskUserServerRequest,
      ) {
        const questions = native.params.questions.flatMap(
          (question): ReadonlyArray<OrchestrationV2UserInputQuestion> => {
            const header = question.topic.trim() || "Question";
            const text = question.question.trim() || header;
            const choices = question.options
              .map((option) => option.trim())
              .filter((option) => option.length > 0);
            return [
              {
                id: String(question.index),
                header,
                question: text,
                options: choices.map((option) => ({
                  label: option,
                  description: option,
                  value: option,
                })),
                ...(question.multiSelect === true ? { multiSelect: true } : {}),
                allowCustomAnswer: true,
              },
            ];
          },
        );
        if (questions.length === 0) {
          yield* native.respond({ cancelled: true, answers: [] });
          return;
        }
        yield* openRuntimeRequest(turn, native, { type: "user_input", questions });
      });

      const handleServerRequest = (native: DroidServerRequest) =>
        sessionEventPermit
          .withPermits(1)(
            Effect.gen(function* () {
              const turn = threadState?.activeTurn ?? null;
              const acceptsSession =
                native.sessionId === undefined || native.sessionId === droidSessionId;
              if (turn === null || turn.interrupted || !acceptsSession) {
                yield* native.fail(-32800, "The Droid request is no longer active.");
                return;
              }
              if (native.method === "droid.request_permission") {
                yield* handlePermissionRequest(turn, native);
                return;
              }
              yield* handleAskUserRequest(turn, native);
            }),
          )
          .pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Droid server request handling failed.", {
                method: native.method,
                cause: Cause.pretty(cause),
              }).pipe(
                Effect.andThen(
                  native.fail(-32603, "T3 Code failed to process the request.").pipe(Effect.ignore),
                ),
              ),
            ),
          );

      // ── turn lifecycle ────────────────────────────────────

      const emitContextUsage = (
        state: DroidThreadState,
        usage: DroidTokenUsage,
        lastCall: DroidLastCallTokenUsage | undefined,
      ) =>
        Effect.gen(function* () {
          const contextUsage = droidContextUsage(usage, lastCall);
          yield* updateProviderThread(state, { contextUsage });
          const turn = state.activeTurn;
          if (turn === null) return;
          const updatedAt = yield* DateTime.now;
          turn.tokenUsage = {
            usedTokens: contextUsage.usedTokens,
            ...(contextUsage.inputTokens === undefined
              ? {}
              : { inputTokens: contextUsage.inputTokens }),
            ...(contextUsage.cachedInputTokens === undefined
              ? {}
              : { cachedInputTokens: contextUsage.cachedInputTokens }),
            ...(contextUsage.outputTokens === undefined
              ? {}
              : { outputTokens: contextUsage.outputTokens }),
            ...(contextUsage.reasoningOutputTokens === undefined
              ? {}
              : { reasoningOutputTokens: contextUsage.reasoningOutputTokens }),
            updatedAt: DateTime.formatIso(updatedAt),
          };
          yield* emit({
            type: "provider_turn.updated",
            driver: DROID_PROVIDER,
            threadId: turn.turnInput.threadId,
            providerTurn: { ...turn.providerTurn, tokenUsage: turn.tokenUsage },
          });
        });

      const emitRetry = Effect.fnUntraced(function* (
        turn: ActiveDroidTurn,
        retry: DroidRetryState,
        status: "running" | "completed" | "failed" | "interrupted",
        failure: OrchestrationV2ProviderFailure,
      ) {
        const updatedAt = yield* DateTime.now;
        yield* emit({
          type: "turn_item.updated",
          driver: DROID_PROVIDER,
          turnItem: makeProviderRetryTurnItem({
            idAllocator,
            driver: DROID_PROVIDER,
            threadId: turn.turnInput.threadId,
            runId: turn.turnInput.runId,
            nodeId: turn.turnInput.rootNodeId,
            providerThreadId: turn.turnInput.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            itemOrdinal: retry.itemOrdinal,
            failure,
            retry: { attempt: retry.attempt, maxAttempts: null, retryDelayMs: null },
            status,
            startedAt: retry.startedAt,
            updatedAt,
          }),
        });
      });

      const retryFailure = (turn: ActiveDroidTurn) =>
        makeProviderFailure({
          message: turn.lastErrorMessage ?? "Droid is retrying the model request.",
          class: "provider_error",
          retryable: true,
        });

      const recoverRetry = (turn: ActiveDroidTurn) =>
        Effect.suspend(() => {
          const retry = turn.retry;
          if (retry === null) return Effect.void;
          turn.retry = null;
          return emitRetry(turn, retry, "completed", retryFailure(turn));
        });

      const finalizeTurn = Effect.fnUntraced(function* (
        state: DroidThreadState,
        outcome: DroidTurnOutcome | { readonly status: "interrupted" },
      ) {
        const turn = state.activeTurn;
        if (turn === null) return;
        state.activeTurn = null;
        const completedAt = yield* DateTime.now;
        yield* completeOpenStreamItems(turn);
        const openToolStatus =
          outcome.status === "completed"
            ? "completed"
            : outcome.status === "failed"
              ? "failed"
              : "interrupted";
        for (const tool of turn.tools.values()) {
          if (tool.completed) continue;
          tool.completed = true;
          yield* emitToolItem(turn, tool, openToolStatus);
        }
        for (const childSessionId of turn.subagents.keys()) {
          yield* settleSubagent(turn, childSessionId, openToolStatus);
        }
        yield* cancelPendingRequests;
        const failure = outcome.status === "failed" ? outcome.failure : null;
        const terminalStatus = outcome.status === "failed" ? null : outcome.status;
        if (turn.retry !== null && failure === null) {
          yield* emitRetry(
            turn,
            turn.retry,
            outcome.status === "completed" ? "completed" : "interrupted",
            retryFailure(turn),
          );
        }
        yield* emit({
          type: "provider_turn.updated",
          driver: DROID_PROVIDER,
          threadId: turn.turnInput.threadId,
          providerTurn: {
            ...turn.providerTurn,
            status: outcome.status,
            completedAt,
            ...(turn.tokenUsage === undefined ? {} : { tokenUsage: turn.tokenUsage }),
          },
        });
        yield* updateProviderThread(state, { status: "idle" });
        yield* updateProviderSession(
          failure !== null ? "error" : "ready",
          failure?.message ?? null,
        );
        if (failure !== null) {
          const failureItemId = `terminal-failure:${turn.providerTurn.id}`;
          const failureOrdinal = turn.retry?.itemOrdinal ?? itemOrdinal(turn, failureItemId);
          if (turn.retry !== null) {
            yield* emitRetry(turn, turn.retry, "failed", failure);
          } else {
            yield* emit({
              type: "turn_item.updated",
              driver: DROID_PROVIDER,
              turnItem: {
                ...baseItemFields(turn, failureItemId, completedAt, completedAt),
                status: "failed",
                title: null,
                completedAt,
                type: "error",
                failure,
              },
            });
          }
          yield* emit({
            type: "turn.terminal",
            driver: DROID_PROVIDER,
            providerThreadId: state.providerThread.id,
            providerTurnId: turn.providerTurn.id,
            runOrdinal: turn.turnInput.runOrdinal,
            failureItemOrdinal: failureOrdinal,
            status: "failed",
            failure,
            threadDisposition: "reusable",
          });
          return;
        }
        if (terminalStatus === null) return;
        yield* emit({
          type: "turn.terminal",
          driver: DROID_PROVIDER,
          providerThreadId: state.providerThread.id,
          providerTurnId: turn.providerTurn.id,
          runOrdinal: turn.turnInput.runOrdinal,
          status: terminalStatus,
          failure: null,
          threadDisposition: "reusable",
        });
      });

      const handleTurnCompleted = Effect.fnUntraced(function* (
        notification: Extract<DroidSessionNotification, { type: "agent_turn_completed" }>,
      ) {
        const runMessageId = notification.turnId;
        if (runMessageId === undefined) return;
        // Droid interrupts the whole session at once, so the first terminal
        // from a stopped run means the stopped work has settled.
        if (interruptedRunMessageIds.has(runMessageId)) {
          interruptedRunMessageIds.clear();
          const settled = interruptSettled;
          interruptSettled = null;
          if (settled !== null) yield* Deferred.succeed(settled, undefined);
        }
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (state === null || turn === null || !turn.pendingRunMessageIds.has(runMessageId)) {
          return;
        }
        if (turn.interrupted) {
          yield* finalizeTurn(state, { status: "interrupted" });
          return;
        }
        turn.pendingRunMessageIds.delete(runMessageId);
        turn.persistedRunMessageIds.delete(runMessageId);
        if (turn.pendingRunMessageIds.size > 0) {
          // Droid emits one terminal per physical run, keyed by its opening
          // message. Steers drained into that run were persisted before it
          // ended; steers still queued run later with their own terminal.
          const allCoalesced = Array.from(turn.pendingRunMessageIds).every((messageId) =>
            turn.persistedRunMessageIds.has(messageId),
          );
          if (!allCoalesced) return;
          turn.pendingRunMessageIds.clear();
          turn.persistedRunMessageIds.clear();
        }
        yield* finalizeTurn(state, droidTurnOutcome(notification.reason, turn.lastErrorMessage));
      });

      const handleChildNotification = Effect.fnUntraced(function* (
        turn: ActiveDroidTurn,
        childSessionId: string,
        notification: DroidSessionNotification,
      ) {
        if (notification.type !== "agent_turn_completed") return;
        const outcome = droidTurnOutcome(notification.reason, null);
        yield* settleSubagent(
          turn,
          childSessionId,
          outcome.status === "completed"
            ? "completed"
            : outcome.status === "cancelled"
              ? "interrupted"
              : "failed",
        );
      });

      const handleNotification = Effect.fnUntraced(function* (
        sessionId: string | undefined,
        notification: DroidSessionNotification,
      ) {
        const state = threadState;
        const turn = state?.activeTurn ?? null;
        if (sessionId !== undefined && sessionId !== droidSessionId) {
          // Known child sessions report subagent lifecycle; anything else is a
          // straggler from a session this process no longer hosts.
          if (turn !== null && turn.subagents.has(sessionId)) {
            yield* handleChildNotification(turn, sessionId, notification);
          }
          return;
        }
        if (notification.type === "agent_turn_completed") {
          yield* handleTurnCompleted(notification);
          return;
        }
        if (notification.type === "session_token_usage_changed") {
          if (state !== null) {
            yield* emitContextUsage(
              state,
              notification.inclusiveTokenUsage ?? notification.tokenUsage,
              notification.lastCallTokenUsage,
            );
          }
          return;
        }
        if (state === null || turn === null) return;
        switch (notification.type) {
          case "create_message": {
            const message = notification.message;
            if (
              typeof message === "object" &&
              message !== null &&
              typeof (message as { id?: unknown }).id === "string" &&
              (message as { role?: unknown }).role === "user"
            ) {
              const messageId = (message as { id: string }).id;
              if (turn.pendingRunMessageIds.has(messageId)) {
                turn.persistedRunMessageIds.add(messageId);
              }
            }
            return;
          }
          case "assistant_text_delta":
          case "thinking_text_delta": {
            if (turn.interrupted) return;
            yield* recoverRetry(turn);
            const item = yield* streamItemFor(
              turn,
              notification.type === "assistant_text_delta" ? "assistant_message" : "reasoning",
              notification.messageId,
            );
            item.text += notification.textDelta;
            yield* scheduleStreamFlush(turn, item);
            return;
          }
          case "assistant_text_complete":
          case "thinking_text_complete": {
            const kind =
              notification.type === "assistant_text_complete" ? "assistant_message" : "reasoning";
            const item = turn.streamItems.get(`${kind}:${notification.messageId}`);
            if (item !== undefined) yield* completeStreamItem(turn, item);
            return;
          }
          case "tool_call": {
            if (turn.interrupted) return;
            const toolUse = notification.toolUse;
            const existing = rememberToolUse(turn, toolUse);
            if (existing !== undefined) {
              if (!existing.completed) yield* emitToolItem(turn, existing, "running");
              return;
            }
            const tool: DroidToolState = {
              toolUseId: toolUse.id,
              name: toolUse.name,
              input: toolUse.input,
              startedAt: yield* DateTime.now,
              completed: false,
            };
            turn.tools.set(toolUse.id, tool);
            yield* emitToolItem(turn, tool, "running");
            return;
          }
          case "tool_result": {
            const tool = turn.tools.get(notification.toolUseId);
            if (tool !== undefined && !tool.completed) {
              tool.completed = true;
              const output = toolResultText(notification.content);
              yield* emitToolItem(
                turn,
                tool,
                notification.isError === true ? "failed" : "completed",
                output === undefined ? undefined : output.slice(0, 20_000),
              );
            }
            // Subagents run in daemon-hosted child processes, so the parent's
            // Task result is the closure signal for the child it spawned.
            const childSessionId = turn.subagentByToolUseId.get(notification.toolUseId);
            if (childSessionId !== undefined) {
              yield* settleSubagent(
                turn,
                childSessionId,
                notification.isError === true ? "failed" : "completed",
              );
            }
            return;
          }
          case "child_session_available": {
            if (turn.subagents.has(notification.childSessionId)) return;
            const subagent: DroidSubagentState = {
              childSessionId: notification.childSessionId,
              toolUseId: notification.toolUseId,
              description:
                notification.description?.trim() ||
                notification.subagentType?.trim() ||
                "Droid subagent",
              startedAt: yield* DateTime.now,
              progress: undefined,
              completed: false,
            };
            turn.subagents.set(subagent.childSessionId, subagent);
            if (subagent.toolUseId !== undefined) {
              turn.subagentByToolUseId.set(subagent.toolUseId, subagent.childSessionId);
            }
            yield* emitSubagent(turn, subagent, "running");
            return;
          }
          case "tool_progress_update": {
            const childSessionId = notification.update.subagentSessionId?.trim();
            if (!childSessionId) return;
            const subagent = turn.subagents.get(childSessionId);
            if (subagent === undefined || subagent.completed) return;
            const summary = [
              notification.update.text,
              notification.update.details,
              notification.update.status,
              notification.update.valueSnippet,
            ]
              .map((value) => value?.trim())
              .find((value): value is string => value !== undefined && value.length > 0);
            if (summary === undefined || summary === subagent.progress) return;
            subagent.progress = summary;
            yield* emitSubagent(turn, subagent, "running");
            return;
          }
          case "error":
            turn.lastErrorMessage = notification.message;
            return;
          case "llm_retry": {
            if (turn.retry === null) {
              turn.retry = {
                startedAt: yield* DateTime.now,
                itemOrdinal: itemOrdinal(turn, `terminal-failure:${turn.providerTurn.id}`),
                attempt: Math.max(1, Math.trunc(notification.attempt)),
              };
            } else {
              turn.retry.attempt = Math.max(1, Math.trunc(notification.attempt));
            }
            yield* emitRetry(turn, turn.retry, "running", retryFailure(turn));
            return;
          }
          case "session_compacted": {
            const emittedAt = yield* DateTime.now;
            const nativeItemId = `compaction:${notification.summaryId}`;
            yield* emitItemNode(turn, nativeItemId, "system", "completed", emittedAt, emittedAt);
            yield* emit({
              type: "turn_item.updated",
              driver: DROID_PROVIDER,
              turnItem: {
                ...baseItemFields(turn, nativeItemId, emittedAt, emittedAt),
                status: "completed",
                title: "Context compacted",
                completedAt: emittedAt,
                type: "compaction",
                driver: DROID_PROVIDER,
              },
            });
            return;
          }
          default:
            return;
        }
      });

      // ── event pumps ───────────────────────────────────────

      yield* rpc.notifications.pipe(
        Stream.runForEach((envelope) =>
          sessionEventPermit.withPermits(1)(
            handleNotification(envelope.sessionId, envelope.notification),
          ),
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning("Droid notification handling failed.", {
            cause: Cause.pretty(cause),
          }),
        ),
        Effect.forkIn(scope),
      );

      // Requests only register the runtime request and return; the answer is
      // written back from respondToRuntimeRequest, so a parked approval never
      // holds a stream slot.
      yield* rpc.serverRequests.pipe(
        Stream.mapEffect(handleServerRequest, {
          concurrency: DROID_SERVER_REQUEST_CONCURRENCY,
          unordered: true,
        }),
        Stream.runDrain,
        Effect.forkIn(scope),
      );

      yield* rpc.exits.pipe(
        Effect.flatMap((exit) =>
          sessionEventPermit.withPermits(1)(
            Effect.gen(function* () {
              const settled = interruptSettled;
              interruptSettled = null;
              interruptedRunMessageIds.clear();
              if (settled !== null) yield* Deferred.succeed(settled, undefined);
              const state = threadState;
              const turn = state?.activeTurn ?? null;
              if (state !== null && turn !== null) {
                yield* finalizeTurn(
                  state,
                  turn.interrupted || stopRequested
                    ? { status: "interrupted" }
                    : {
                        status: "failed",
                        failure: makeProviderFailure({
                          message: `Droid exited unexpectedly (${exit.description}).`,
                          class: "transport_error",
                        }),
                      },
                );
              }
              if (stopRequested) {
                yield* updateProviderSession("stopped", null);
                yield* Queue.end(events);
                return;
              }
              yield* updateProviderSession("error", `Droid exited (${exit.description}).`);
              yield* Queue.fail(
                events,
                new ProviderAdapterEventStreamError({
                  driver: DROID_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  cause: exit,
                }),
              );
            }),
          ),
        ),
        Effect.forkIn(scope),
      );

      yield* Scope.addFinalizer(
        scope,
        Effect.sync(() => {
          stopRequested = true;
        }),
      );

      // ── session settings ──────────────────────────────────

      const desiredSettings = (
        modelSelection: ModelSelection,
        runtimeMode: RuntimeMode,
        interactionMode: "default" | "plan",
      ) => {
        const requestedModel =
          modelSelection.instanceId === options.instanceId ? modelSelection.model : undefined;
        const modelId =
          requestedModel === undefined || requestedModel === DROID_INHERIT_MODEL_SLUG
            ? baselineModelId
            : requestedModel;
        const reasoningEffort =
          requestedModel === undefined || requestedModel === DROID_INHERIT_MODEL_SLUG
            ? undefined
            : getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");
        return {
          autonomyLevel: droidAutonomyLevelForRuntimeMode(runtimeMode),
          interactionMode: (interactionMode === "plan" ? "spec" : "auto") as DroidInteractionMode,
          modelId,
          reasoningEffort: reasoningEffort ?? null,
        };
      };

      const initialSettingsParams = (desired: ReturnType<typeof desiredSettings>) => ({
        autonomyLevel: desired.autonomyLevel,
        interactionMode: desired.interactionMode,
        ...(desired.modelId === null
          ? {}
          : { modelId: desired.modelId, specModeModelId: desired.modelId }),
        ...(desired.reasoningEffort === null
          ? {}
          : {
              reasoningEffort: desired.reasoningEffort,
              specModeReasoningEffort: desired.reasoningEffort,
            }),
      });

      const recordApplied = (desired: ReturnType<typeof desiredSettings>) => {
        applied.autonomyLevel = desired.autonomyLevel;
        applied.interactionMode = desired.interactionMode;
        applied.modelId = desired.modelId;
        applied.reasoningEffort = desired.reasoningEffort;
      };

      const applySettings = Effect.fnUntraced(function* (
        desired: ReturnType<typeof desiredSettings>,
      ) {
        const patch: Record<string, unknown> = {};
        if (desired.autonomyLevel !== applied.autonomyLevel) {
          patch.autonomyLevel = desired.autonomyLevel;
        }
        if (desired.interactionMode !== applied.interactionMode) {
          patch.interactionMode = desired.interactionMode;
        }
        if (desired.modelId !== null && desired.modelId !== applied.modelId) {
          patch.modelId = desired.modelId;
          patch.specModeModelId = desired.modelId;
        }
        if (
          desired.reasoningEffort !== null &&
          desired.reasoningEffort !== applied.reasoningEffort
        ) {
          patch.reasoningEffort = desired.reasoningEffort;
          patch.specModeReasoningEffort = desired.reasoningEffort;
        }
        if (Object.keys(patch).length === 0) return;
        yield* request("droid.update_session_settings", patch);
        recordApplied({
          ...desired,
          modelId: desired.modelId ?? applied.modelId,
          reasoningEffort: desired.reasoningEffort ?? applied.reasoningEffort,
        });
        const model = desired.modelId ?? sessionEntity.model;
        if (model !== sessionEntity.model) {
          const updatedAt = yield* DateTime.now;
          sessionEntity = { ...sessionEntity, model, updatedAt };
          yield* emit({
            type: "provider_session.updated",
            driver: DROID_PROVIDER,
            providerSession: sessionEntity,
          });
        }
      });

      const rememberBaseline = (result: typeof DroidSessionSettingsResult.Type) => {
        const modelId = result.settings?.modelId?.trim();
        if (baselineModelId === null && modelId) baselineModelId = modelId;
      };

      // ── thread registration ───────────────────────────────

      const makeProviderThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapterV2EnsureThreadInput,
        nativeId: string,
      ) {
        const createdAt = yield* DateTime.now;
        const existing = threadInput.existingProviderThread;
        return existing !== undefined
          ? ({
              ...existing,
              providerSessionId: input.providerSessionId,
              nativeThreadRef: providerRef(nativeId),
              status: "idle",
              updatedAt: createdAt,
            } satisfies OrchestrationV2ProviderThread)
          : ({
              id: idAllocator.derive.providerThread({
                driver: DROID_PROVIDER,
                nativeThreadId: nativeId,
              }),
              driver: DROID_PROVIDER,
              providerInstanceId: options.instanceId,
              providerSessionId: input.providerSessionId,
              appThreadId: threadInput.threadId,
              ownerNodeId: null,
              nativeThreadRef: providerRef(nativeId),
              nativeConversationHeadRef: null,
              status: "idle",
              firstRunOrdinal: null,
              lastRunOrdinal: null,
              handoffIds: [],
              forkedFrom: null,
              pendingBackgroundTasks: [],
              createdAt,
              updatedAt: createdAt,
            } satisfies OrchestrationV2ProviderThread);
      });

      const loadSession = Effect.fnUntraced(function* (
        threadId: ThreadId,
        sessionId: string,
        desired: ReturnType<typeof desiredSettings>,
      ) {
        const loaded = yield* request(
          "droid.load_session",
          { sessionId, messageLimit: 1, ...droidMcpServersParam(threadId) },
          DROID_SESSION_TIMEOUT_MS,
        ).pipe(Effect.flatMap(decodeSessionSettingsResult));
        rememberBaseline(loaded);
        droidSessionId = sessionId;
        applied.autonomyLevel = null;
        applied.interactionMode = null;
        applied.modelId = null;
        applied.reasoningEffort = null;
        yield* request("droid.update_session_settings", initialSettingsParams(desired));
        recordApplied(desired);
      });

      const registerThread = Effect.fnUntraced(function* (
        threadInput: ProviderAdapterV2EnsureThreadInput,
      ) {
        if (threadState !== null && threadState.activeTurn !== null) {
          return yield* protocolError("Cannot register a Droid thread while a turn is active");
        }
        const desired = desiredSettings(
          threadInput.modelSelection,
          threadInput.runtimePolicy.runtimeMode,
          threadInput.runtimePolicy.interactionMode,
        );
        const resumeId = threadInput.existingProviderThread?.nativeThreadRef?.nativeId;
        let nativeId: string;
        if (resumeId != null) {
          if (resumeId !== droidSessionId) {
            yield* loadSession(threadInput.threadId, resumeId, desired);
          }
          nativeId = resumeId;
        } else {
          const initialized = yield* request(
            "droid.initialize_session",
            {
              machineId: "default",
              cwd,
              ...initialSettingsParams(desired),
              ...droidMcpServersParam(threadInput.threadId),
            },
            DROID_SESSION_TIMEOUT_MS,
          ).pipe(Effect.flatMap(decodeSessionSettingsResult));
          rememberBaseline(initialized);
          if (initialized.sessionId === undefined) {
            return yield* protocolError("droid.initialize_session returned no session id");
          }
          nativeId = initialized.sessionId;
          droidSessionId = nativeId;
          recordApplied({
            ...desired,
            modelId: desired.modelId ?? baselineModelId,
          });
        }
        const providerThread = yield* makeProviderThread(threadInput, nativeId);
        threadState = { providerThread, activeTurn: null };
        yield* emit({
          type: "provider_thread.updated",
          driver: DROID_PROVIDER,
          providerThread,
        });
        return providerThread;
      });

      // ── prompts ───────────────────────────────────────────

      const resolvePromptPayload = Effect.fnUntraced(function* (
        text: string,
        attachments: ReadonlyArray<ChatAttachment>,
      ) {
        const images: Array<{ type: "base64"; data: string; mediaType: string }> = [];
        const extraLines: Array<string> = [];
        for (const attachment of attachments) {
          const path = resolveAttachmentPath({
            attachmentsDir: options.serverConfig.attachmentsDir,
            attachment,
          });
          if (path === null) continue;
          if (attachment.mimeType.startsWith("image/")) {
            const bytes = yield* options.fileSystem.readFile(path);
            images.push({
              type: "base64",
              data: Buffer.from(bytes).toString("base64"),
              mediaType: attachment.mimeType,
            });
          } else {
            extraLines.push(`[Attachment saved at ${path}]`);
          }
        }
        const message = extraLines.length === 0 ? text : `${text}\n\n${extraLines.join("\n")}`;
        return { text: message, images };
      });

      const sendUserMessage = (
        messageId: string,
        payload: { readonly text: string; readonly images: ReadonlyArray<unknown> },
      ) =>
        request("droid.add_user_message", {
          messageId,
          text: payload.text,
          ...(payload.images.length === 0 ? {} : { images: payload.images }),
        });

      /** After Stop, wait for Droid to settle the cancelled run before new work. */
      const awaitInterruptSettled = Effect.gen(function* () {
        const settled = interruptSettled;
        if (settled === null) return;
        const done = yield* Deferred.await(settled).pipe(
          Effect.timeoutOption(Duration.millis(DROID_INTERRUPT_SETTLE_MS)),
        );
        if (Option.isSome(done)) return;
        // Droid never settled the stopped run. Restart so the next turn cannot
        // interleave with it; the session manager reopens the runtime.
        stopRequested = true;
        yield* rpc.shutdown;
        return yield* protocolError("Droid did not settle the stopped turn; restarting Droid");
      });

      // ── session runtime ───────────────────────────────────

      const runtime: ProviderAdapterV2SessionRuntime = {
        instanceId: options.instanceId,
        driver: DROID_PROVIDER,
        providerSessionId: input.providerSessionId,
        get providerSession() {
          return sessionEntity;
        },
        events: Stream.fromQueue(events),
        ensureThread: (threadInput) =>
          registerThread(threadInput).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterEnsureThreadError({
                  driver: DROID_PROVIDER,
                  threadId: threadInput.threadId,
                  cause,
                }),
            ),
          ),
        resumeThread: (threadInput) =>
          registerThread({
            threadId:
              threadInput.threadId ?? threadInput.providerThread.appThreadId ?? input.threadId,
            modelSelection: threadInput.modelSelection ?? input.modelSelection,
            runtimePolicy: threadInput.runtimePolicy ?? input.runtimePolicy,
            existingProviderThread: threadInput.providerThread,
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterResumeThreadError({
                  driver: DROID_PROVIDER,
                  providerSessionId: input.providerSessionId,
                  providerThreadId: threadInput.providerThread.id,
                  cause,
                }),
            ),
          ),
        startTurn: (turnInput) =>
          Effect.gen(function* () {
            yield* awaitInterruptSettled;
            const state = threadState;
            if (state === null) {
              return yield* protocolError("Droid session has no registered thread");
            }
            if (state.activeTurn !== null) {
              return yield* protocolError(
                `Droid provider thread ${turnInput.providerThread.id} already has an active turn`,
              );
            }
            if (
              state.providerThread.nativeThreadRef?.nativeId !==
              turnInput.providerThread.nativeThreadRef?.nativeId
            ) {
              return yield* protocolError("Droid turn requested for a different native session");
            }
            state.providerThread = turnInput.providerThread;
            yield* applySettings(
              desiredSettings(
                turnInput.modelSelection,
                turnInput.runtimePolicy.runtimeMode,
                turnInput.runtimePolicy.interactionMode,
              ),
            );
            // Resolved before the turn is installed so an unreadable attachment
            // cannot leave a turn active that never started.
            const payload = yield* resolvePromptPayload(
              turnInput.message.text,
              turnInput.message.attachments,
            );
            if (payload.text.trim().length === 0 && payload.images.length === 0) {
              return yield* protocolError("Droid turn requires non-empty text or attachments");
            }
            const messageId = yield* options.crypto.randomUUIDv4;
            const startedAt = yield* DateTime.now;
            const providerTurn: OrchestrationV2ProviderTurn = {
              id: idAllocator.derive.providerTurn({
                driver: DROID_PROVIDER,
                nativeTurnId: messageId,
              }),
              providerThreadId: turnInput.providerThread.id,
              nodeId: turnInput.rootNodeId,
              runAttemptId: turnInput.attemptId,
              nativeTurnRef: providerRef(messageId),
              ordinal: turnInput.providerTurnOrdinal,
              status: "running",
              startedAt,
              completedAt: null,
            };
            const activeTurn: ActiveDroidTurn = {
              turnInput,
              providerTurn,
              itemOrdinals: new Map(),
              nextItemOrdinal: turnInput.providerTurnOrdinal * 100 + 1,
              streamItems: new Map(),
              streamItemCounts: new Map(),
              allStreamItems: [],
              tools: new Map(),
              subagents: new Map(),
              subagentByToolUseId: new Map(),
              pendingRunMessageIds: new Set([messageId]),
              persistedRunMessageIds: new Set(),
              interrupted: false,
              lastErrorMessage: null,
              planCaptured: false,
              retry: null,
              tokenUsage: undefined,
            };
            yield* Effect.gen(function* () {
              state.activeTurn = activeTurn;
              yield* emit({
                type: "provider_turn.updated",
                driver: DROID_PROVIDER,
                threadId: turnInput.threadId,
                providerTurn,
              });
              yield* updateProviderThread(state, {
                status: "active",
                firstRunOrdinal: state.providerThread.firstRunOrdinal ?? turnInput.runOrdinal,
                lastRunOrdinal: turnInput.runOrdinal,
              });
              yield* updateProviderSession("running", null);
            }).pipe(sessionEventPermit.withPermits(1));
            // Sent outside the permit: Droid may raise a request or stream
            // before it acknowledges the message.
            yield* sendUserMessage(messageId, payload).pipe(
              Effect.tapError(() =>
                sessionEventPermit.withPermits(1)(
                  Effect.suspend(() =>
                    state.activeTurn === activeTurn
                      ? finalizeTurn(state, {
                          status: "failed",
                          failure: makeProviderFailure({
                            message: "Droid rejected the message.",
                            class: "provider_error",
                          }),
                        })
                      : Effect.void,
                  ),
                ),
              ),
            );
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterTurnStartError({
                  driver: DROID_PROVIDER,
                  threadId: turnInput.threadId,
                  providerThreadId: turnInput.providerThread.id,
                  runId: turnInput.runId,
                  cause,
                }),
            ),
          ),
        steerTurn: (steerInput) =>
          Effect.gen(function* () {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null || turn.providerTurn.id !== steerInput.providerTurnId) {
              return yield* protocolError(`Droid turn ${steerInput.providerTurnId} is not active`);
            }
            const payload = yield* resolvePromptPayload(
              steerInput.message.text,
              steerInput.message.attachments,
            );
            const messageId = yield* options.crypto.randomUUIDv4;
            yield* sessionEventPermit.withPermits(1)(
              Effect.suspend(() => {
                if (threadState?.activeTurn !== turn || turn.interrupted) {
                  return Effect.fail(
                    protocolError(`Droid turn ${steerInput.providerTurnId} is not active`),
                  );
                }
                turn.pendingRunMessageIds.add(messageId);
                return Effect.void;
              }),
            );
            yield* sendUserMessage(messageId, payload).pipe(
              Effect.tapError(() =>
                Effect.sync(() => {
                  turn.pendingRunMessageIds.delete(messageId);
                  turn.persistedRunMessageIds.delete(messageId);
                }),
              ),
            );
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterSteerRunError({
                  driver: DROID_PROVIDER,
                  providerThreadId: steerInput.providerThread.id,
                  providerTurnId: steerInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        interruptTurn: (interruptInput) =>
          Effect.gen(function* () {
            const turn = threadState?.activeTurn ?? null;
            if (turn === null || turn.providerTurn.id !== interruptInput.providerTurnId) {
              return yield* protocolError(
                `Droid turn ${interruptInput.providerTurnId} is not active`,
              );
            }
            const settled = yield* sessionEventPermit.withPermits(1)(
              Effect.gen(function* () {
                turn.interrupted = true;
                for (const messageId of turn.pendingRunMessageIds) {
                  interruptedRunMessageIds.add(messageId);
                }
                const deferred = yield* Deferred.make<void>();
                interruptSettled = deferred;
                yield* cancelPendingRequests;
                return deferred;
              }),
            );
            yield* request("droid.interrupt_session", {}).pipe(
              Effect.tapError(() =>
                sessionEventPermit.withPermits(1)(
                  Effect.sync(() => {
                    turn.interrupted = false;
                    interruptedRunMessageIds.clear();
                    if (interruptSettled === settled) interruptSettled = null;
                  }),
                ),
              ),
            );
            // Droid settles the stopped run with a cancelled terminal. If it
            // does not, settle the turn here so Stop never leaves it running.
            const done = yield* Deferred.await(settled).pipe(
              Effect.timeoutOption(Duration.millis(DROID_INTERRUPT_SETTLE_MS)),
            );
            if (Option.isNone(done) || interruptInput.requestRuntimeRestart === true) {
              yield* sessionEventPermit.withPermits(1)(
                Effect.suspend(() => {
                  const state = threadState;
                  return state !== null && state.activeTurn === turn
                    ? finalizeTurn(state, { status: "interrupted" })
                    : Effect.void;
                }),
              );
            }
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterInterruptError({
                  driver: DROID_PROVIDER,
                  providerThreadId: interruptInput.providerThread.id,
                  providerTurnId: interruptInput.providerTurnId,
                  cause,
                }),
            ),
          ),
        respondToRuntimeRequest: (requestInput) =>
          Effect.gen(function* () {
            const pending = pendingRequests.get(String(requestInput.requestId));
            if (pending === undefined) {
              return yield* protocolError(`No pending Droid request ${requestInput.requestId}`);
            }
            if (pending.type === "approval") {
              const decision = requestInput.decision ?? "decline";
              const outcome =
                decision === "cancel"
                  ? pending.options.find((option) => option.outcome.trim() === "cancel")?.outcome
                  : selectDroidPermissionOutcome(pending.options, decision);
              if (outcome === undefined) {
                return yield* protocolError(
                  `Droid request ${requestInput.requestId} does not support '${decision}'`,
                );
              }
              yield* pending.native.respond({ selectedOption: outcome });
              pendingRequests.delete(String(requestInput.requestId));
              yield* resolvePendingRequest(pending, "resolved", { decision });
              return;
            }
            const answers = requestInput.answers;
            if (answers === undefined) {
              yield* pending.native.respond({ cancelled: true, answers: [] });
              pendingRequests.delete(String(requestInput.requestId));
              yield* resolvePendingRequest(pending, "cancelled");
              return;
            }
            yield* pending.native.respond({
              answers: pending.native.params.questions.map((question) => {
                const raw = answers[String(question.index)];
                const answer = Array.isArray(raw) ? raw.map(String).join(", ") : String(raw ?? "");
                return { index: question.index, question: question.question, answer };
              }),
            });
            pendingRequests.delete(String(requestInput.requestId));
            yield* resolvePendingRequest(pending, "resolved", { answers });
          }).pipe(
            sessionEventPermit.withPermits(1),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRuntimeRequestResponseError({
                  driver: DROID_PROVIDER,
                  requestId: requestInput.requestId,
                  cause,
                }),
            ),
          ),
        readThreadSnapshot: (snapshotInput) =>
          Effect.succeed(
            droidThreadSnapshot(threadState?.providerThread ?? snapshotInput.providerThread),
          ),
        rollbackThread: (rollbackInput) =>
          Effect.gen(function* () {
            const state = threadState;
            if (state === null || state.providerThread.id !== rollbackInput.providerThread.id) {
              return yield* protocolError(
                "Droid rollback requested for a thread this session does not host",
              );
            }
            if (state.activeTurn !== null) {
              return yield* protocolError("Cannot roll back while a Droid turn is active");
            }
            const boundaryOrdinal =
              rollbackInput.target.type === "thread_start"
                ? 0
                : rollbackInput.target.providerTurn.ordinal;
            const boundary = rollbackInput.providerThreadTurns
              .filter((turn) => turn.ordinal > boundaryOrdinal)
              .toSorted((left, right) => left.ordinal - right.ordinal)[0];
            if (boundary === undefined) return droidThreadSnapshot(state.providerThread);
            const anchor = boundary.nativeTurnRef;
            if (anchor === null || anchor.strength !== "strong" || anchor.nativeId === null) {
              return yield* protocolError("Droid rollback target has no native message id");
            }
            const currentSessionId = state.providerThread.nativeThreadRef?.nativeId;
            if (currentSessionId == null) {
              return yield* protocolError("Droid thread has no native session id");
            }
            // Rewind forks the conversation before the first discarded user
            // message. T3's checkpoint refs own file restoration.
            const rewound = yield* request(
              "droid.execute_rewind",
              {
                sessionId: currentSessionId,
                messageId: anchor.nativeId,
                filesToRestore: [],
                filesToDelete: [],
                forkTitle: "T3 Code checkpoint revert",
              },
              DROID_SESSION_TIMEOUT_MS,
            ).pipe(Effect.flatMap(decodeRewindResult));
            const desired = {
              autonomyLevel: applied.autonomyLevel ?? "off",
              interactionMode: applied.interactionMode ?? "auto",
              modelId: applied.modelId,
              reasoningEffort: applied.reasoningEffort,
            } satisfies ReturnType<typeof desiredSettings>;
            yield* loadSession(
              state.providerThread.appThreadId ?? input.threadId,
              rewound.newSessionId,
              desired,
            );
            yield* updateProviderThread(state, {
              nativeThreadRef: providerRef(rewound.newSessionId),
              contextUsage: null,
            });
            return droidThreadSnapshot(state.providerThread);
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRollbackThreadError({
                  driver: DROID_PROVIDER,
                  providerThreadId: rollbackInput.providerThread.id,
                  checkpointId: rollbackInput.target.checkpointId,
                  cause,
                }),
            ),
          ),
        forkThread: (forkInput) =>
          Effect.fail(
            new ProviderAdapterForkThreadError({
              driver: DROID_PROVIDER,
              providerThreadId: forkInput.sourceProviderThread.id,
              cause: protocolError("Droid threads fork through a context handoff"),
            }),
          ),
      };
      return runtime;
    }),
  });
}

function droidThreadSnapshot(
  providerThread: OrchestrationV2ProviderThread,
): ProviderAdapterV2ThreadSnapshot {
  return { providerThread, providerTurns: [], messages: [], runtimeRequests: [] };
}

// ── driver ────────────────────────────────────────────────────

export type DroidAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | IdAllocatorV2
  | ServerConfig;

export const DroidAdapterV2Driver: ProviderAdapterDriver<DroidSettings, DroidAdapterV2DriverEnv> = {
  driverKind: DROID_PROVIDER,
  configSchema: DroidSettings,
  defaultConfig: (): DroidSettings => DEFAULT_DROID_SETTINGS,
  create: Effect.fn("DroidAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<DroidSettings>) {
      const hostEnvironment = yield* HostProcessEnvironment;
      return makeDroidAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        spawner: yield* ChildProcessSpawner.ChildProcessSpawner,
        fileSystem: yield* FileSystem.FileSystem,
        crypto: yield* Crypto.Crypto,
        idAllocator: yield* IdAllocatorV2,
        serverConfig: yield* ServerConfig,
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: DROID_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create Droid adapter.",
              cause,
            }),
        ),
      ),
  ),
};
