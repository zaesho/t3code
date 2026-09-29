/**
 * DroidProvider — snapshot/probe layer for Factory Droid.
 *
 * Health is probed with `droid --version`. Models, slash commands, and skills
 * come from a short-lived `droid exec` JSON-RPC process, which lists them
 * without opening a session, so probing never starts MCP servers or hooks.
 */
import {
  type CustomModelSetting,
  type DroidSettings,
  type ModelCapabilities,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { causeErrorTag } from "@t3tools/shared/observability";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess } from "effect/unstable/process";

import {
  type DroidCommandInfo,
  DroidListCommandsResult,
  DroidListModelsResult,
  DroidListSkillsResult,
  type DroidModelInfo,
  type DroidSkillInfo,
} from "../droid/DroidProtocol.ts";
import { logDroidWarning } from "../droid/DroidDiagnostics.ts";
import { makeDroidExecRpcClient } from "../droid/DroidRpcClient.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  type ProviderMaintenanceCapabilities,
} from "../providerMaintenance.ts";
import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const DROID_PRESENTATION = {
  displayName: "Droid",
  badgeLabel: "Early Access",
  // Spec Mode maps onto the plan/build toggle, and models switch in-session
  // through droid.update_session_settings.
  showInteractionModeToggle: true,
  requiresNewThreadForModelChange: false,
} as const;

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const VERSION_PROBE_TIMEOUT_MS = 4_000;
const INVENTORY_DISCOVERY_TIMEOUT_MS = 15_000;
const UNKNOWN_AUTH = { status: "unknown" } as const;

type DroidSnapshotInput = Omit<
  Parameters<typeof buildServerProvider>[0],
  "presentation" | "enabled"
>;

export const DROID_LOGIN_MESSAGE = "Run `droid` in a terminal to sign in to Factory.";

/** Sentinel slug: leave the session on the model chosen in the user's Factory settings. */
export const DROID_INHERIT_MODEL_SLUG = "default";

const DROID_DEFAULT_MODEL: ServerProviderModel = {
  slug: DROID_INHERIT_MODEL_SLUG,
  name: "Droid default",
  isCustom: false,
  isDefault: true,
  capabilities: EMPTY_CAPABILITIES,
};

function droidModelsFromSettings(
  customModels: ReadonlyArray<CustomModelSetting> | undefined,
  discovered: ReadonlyArray<ServerProviderModel> = [],
) {
  return providerModelsFromSettings(
    [DROID_DEFAULT_MODEL, ...discovered],
    customModels ?? [],
    EMPTY_CAPABILITIES,
  );
}

const buildDroidSnapshot = (settings: DroidSettings, input: DroidSnapshotInput) =>
  buildServerProvider({
    presentation: DROID_PRESENTATION,
    enabled: settings.enabled,
    ...input,
  });

const buildDroidProbe = (
  status: DroidSnapshotInput["probe"]["status"],
  message: string | undefined,
  overrides: Partial<DroidSnapshotInput["probe"]> = {},
): DroidSnapshotInput["probe"] => ({
  installed: true,
  version: null,
  auth: UNKNOWN_AUTH,
  ...overrides,
  status,
  ...(message ? { message } : {}),
});

function mapUnique<I, O>(
  inputs: ReadonlyArray<I>,
  map: (input: I) => readonly [key: string, value: O] | undefined,
): O[] {
  const seen = new Set<string>();
  return inputs.flatMap((input) => {
    const entry = map(input);
    if (!entry || seen.has(entry[0])) return [];
    seen.add(entry[0]);
    return [entry[1]];
  });
}

function reasoningEffortCapabilities(model: DroidModelInfo): ModelCapabilities {
  // Routers such as "auto" advertise only "none", which is not a real choice.
  const efforts = model.supportedReasoningEfforts.filter((effort) => effort !== "none");
  if (efforts.length === 0) return EMPTY_CAPABILITIES;
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: "reasoningEffort",
        label: "Reasoning effort",
        options: efforts.map((effort) => ({
          value: effort,
          label: effort,
          ...(model.defaultReasoningEffort === effort ? { isDefault: true } : {}),
        })),
      }),
    ],
  });
}

export function buildDroidDiscoveredModels(
  models: ReadonlyArray<DroidModelInfo>,
): ReadonlyArray<ServerProviderModel> {
  return mapUnique(models, (model) => {
    const slug = model.id.trim();
    // Legacy models have a named replacement in the same list.
    if (model.disabled === true || model.legacy === true || !slug) return undefined;
    if (slug === DROID_INHERIT_MODEL_SLUG) return undefined;
    const shortName = model.shortDisplayName.trim();
    return [
      slug,
      {
        slug,
        name: model.displayName.trim() || slug,
        ...(shortName ? { shortName } : {}),
        isCustom: false,
        capabilities: reasoningEffortCapabilities(model),
      },
    ];
  });
}

export function buildDroidSlashCommands(commands: ReadonlyArray<DroidCommandInfo>) {
  return mapUnique(commands, (command) => {
    const name = command.name.trim();
    if (!name) return undefined;
    const description = command.description.trim();
    const hint = command.argumentHint?.trim();
    return [
      name,
      {
        name,
        ...(description ? { description } : {}),
        ...(hint ? { input: { hint } } : {}),
      },
    ];
  }).toSorted((left, right) => left.name.localeCompare(right.name));
}

export function buildDroidSkills(skills: ReadonlyArray<DroidSkillInfo>) {
  return mapUnique(skills, (skill) => {
    const name = skill.name.trim();
    const path = skill.filePath.trim();
    if (skill.userInvocable === false || !name || !path) return undefined;
    const description = skill.description?.trim();
    const scope =
      skill.location === "builtin"
        ? "system"
        : skill.location === "automation"
          ? "app"
          : skill.location;
    return [
      name,
      {
        name,
        path,
        enabled: skill.enabled !== false,
        scope,
        ...(description ? { description, shortDescription: description } : {}),
      },
    ];
  }).toSorted((left, right) => left.name.localeCompare(right.name));
}

/**
 * Droid keeps its credentials in ~/.factory. A stored login or API key only
 * shows that sign-in happened, not that it is still valid, so status stays
 * unknown and the first turn reports an expired login.
 */
export const detectDroidAuth = Effect.fn("detectDroidAuth")(function* (
  environment: NodeJS.ProcessEnv,
) {
  if (environment.FACTORY_API_KEY?.trim()) {
    return { status: "unknown", type: "api-key", label: "API key" } as const;
  }
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home =
    environment.FACTORY_HOME_OVERRIDE?.trim() ||
    environment.HOME?.trim() ||
    environment.USERPROFILE?.trim();
  const factoryHome = home ? path.join(home, ".factory") : undefined;
  if (!factoryHome) {
    return { status: "unknown" } as const;
  }
  for (const candidate of ["auth.v2.keyring", "auth.v2.loginkeychain", "auth.v2.file"]) {
    const exists = yield* fileSystem
      .exists(path.join(factoryHome, candidate))
      .pipe(Effect.orElseSucceed(() => false));
    if (exists) {
      return { status: "unknown", type: "oauth", label: "Factory account" } as const;
    }
  }
  return { status: "unknown" } as const;
});

const decodeListModelsResult = Schema.decodeUnknownEffect(DroidListModelsResult);
const decodeListCommandsResult = Schema.decodeUnknownEffect(DroidListCommandsResult);
const decodeListSkillsResult = Schema.decodeUnknownEffect(DroidListSkillsResult);

const discoverDroidInventory = (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
) =>
  Effect.gen(function* () {
    const rpc = yield* makeDroidExecRpcClient({
      binaryPath: droidSettings.binaryPath,
      cwd,
      env: environment,
    });

    const capture = Effect.fn("discoverDroidInventory.capture")(function* <A, E>(
      label: string,
      warning: string,
      request: Effect.Effect<A, E>,
    ) {
      const exit = yield* request.pipe(
        Effect.timeoutOption(INVENTORY_DISCOVERY_TIMEOUT_MS),
        Effect.exit,
      );
      if (Exit.isFailure(exit)) {
        yield* logDroidWarning(`Droid ${label} inventory discovery failed.`, {
          cause: exit.cause,
        });
        return { value: undefined, warning };
      }
      if (Option.isNone(exit.value)) {
        yield* Effect.logWarning(
          `Droid ${label} inventory discovery timed out after ${INVENTORY_DISCOVERY_TIMEOUT_MS}ms.`,
        );
        return { value: undefined, warning };
      }
      return { value: exit.value.value, warning: undefined };
    });

    const [modelResult, commandResult, skillResult] = yield* Effect.all(
      [
        capture(
          "model",
          "Droid model inventory failed; only the default model is available.",
          rpc.request("droid.list_models", {}).pipe(Effect.flatMap(decodeListModelsResult)),
        ),
        capture(
          "command",
          "Droid command inventory failed; slash commands are unavailable.",
          rpc.request("droid.list_commands", {}).pipe(Effect.flatMap(decodeListCommandsResult)),
        ),
        capture(
          "skill",
          "Droid skill inventory failed; skills are unavailable.",
          rpc.request("droid.list_skills", {}).pipe(Effect.flatMap(decodeListSkillsResult)),
        ),
      ],
      { concurrency: "unbounded" },
    );

    return {
      models: modelResult.value ? buildDroidDiscoveredModels(modelResult.value.models) : undefined,
      slashCommands: commandResult.value
        ? buildDroidSlashCommands(commandResult.value.commands)
        : undefined,
      skills: skillResult.value ? buildDroidSkills(skillResult.value.skills) : undefined,
      warnings: [modelResult.warning, commandResult.warning, skillResult.warning].filter(
        (warning): warning is string => warning !== undefined,
      ),
    };
  }).pipe(Effect.scoped);

const droidCliCommandMissingMessage = (droidSettings: DroidSettings) => {
  const command = droidSettings.binaryPath || "droid";
  return [
    `Droid CLI command \`${command}\` was not found.`,
    `Install the Droid CLI, make sure \`${command}\` is on PATH, then restart T3 Code.`,
    "See https://docs.factory.ai/cli/getting-started/quickstart.",
  ].join(" ");
};

const runDroidVersionCommand = (droidSettings: DroidSettings, environment: NodeJS.ProcessEnv) =>
  Effect.gen(function* () {
    const command = droidSettings.binaryPath || "droid";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export function buildInitialDroidProviderSnapshot(
  droidSettings: DroidSettings,
): Effect.Effect<ServerProviderDraft> {
  return DateTime.now.pipe(
    Effect.map(DateTime.formatIso),
    Effect.map((checkedAt) =>
      buildDroidSnapshot(droidSettings, {
        checkedAt,
        models: droidModelsFromSettings(droidSettings.customModels),
        probe: buildDroidProbe(
          "warning",
          droidSettings.enabled
            ? "Checking Droid CLI availability..."
            : "Droid is disabled in T3 Code settings.",
          { installed: droidSettings.enabled },
        ),
      }),
    ),
  );
}

export const checkDroidProviderStatus = Effect.fn("checkDroidProviderStatus")(function* (
  droidSettings: DroidSettings,
  environment: NodeJS.ProcessEnv,
  cwd: string,
) {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = droidModelsFromSettings(droidSettings.customModels);
  const snapshot = (
    probe: DroidSnapshotInput["probe"],
    inventory: Pick<DroidSnapshotInput, "slashCommands" | "skills"> = {},
    models = fallbackModels,
  ) =>
    buildDroidSnapshot(droidSettings, {
      checkedAt,
      models,
      ...inventory,
      probe,
    });
  const errorSnapshot = (message: string, overrides?: Partial<DroidSnapshotInput["probe"]>) =>
    snapshot(buildDroidProbe("error", message, overrides));

  if (!droidSettings.enabled) {
    return snapshot(
      buildDroidProbe("warning", "Droid is disabled in T3 Code settings.", { installed: false }),
    );
  }

  const versionResult = yield* runDroidVersionCommand(droidSettings, environment).pipe(
    Effect.timeoutOption(VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionResult)) {
    const error = versionResult.failure;
    yield* logDroidWarning("Droid CLI health check failed.", { error });
    const missing = isCommandMissingCause(error);
    return errorSnapshot(
      missing
        ? droidCliCommandMissingMessage(droidSettings)
        : "Failed to execute Droid CLI health check.",
      { installed: !missing },
    );
  }

  if (Option.isNone(versionResult.success)) {
    return errorSnapshot("Droid CLI is installed but timed out while running `droid --version`.");
  }

  const versionOutput = versionResult.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("Droid CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
      stdoutLength: versionOutput.stdout.length,
      stderrLength: versionOutput.stderr.length,
    });
    return errorSnapshot("Droid CLI is installed but failed to run.", { version });
  }

  const auth = yield* detectDroidAuth(environment);
  const discoveryExit = yield* discoverDroidInventory(droidSettings, environment, cwd).pipe(
    Effect.exit,
  );
  const inventory = Exit.isSuccess(discoveryExit) ? discoveryExit.value : undefined;
  let inventoryWarnings: ReadonlyArray<string> = inventory?.warnings ?? [];
  if (Exit.isFailure(discoveryExit)) {
    yield* logDroidWarning("Droid inventory discovery failed.", {
      cause: discoveryExit.cause,
    });
    inventoryWarnings = [
      "Droid inventory discovery failed. Only the default model is available; slash commands and skills are unavailable.",
    ];
  }
  const models =
    inventory?.models !== undefined && inventory.models.length > 0
      ? droidModelsFromSettings(droidSettings.customModels, inventory.models)
      : fallbackModels;
  let message = inventoryWarnings.length > 0 ? inventoryWarnings.join(" ") : undefined;
  if (auth.status === "unknown" && auth.type === undefined) {
    message = message ? `${message} ${DROID_LOGIN_MESSAGE}` : DROID_LOGIN_MESSAGE;
  }

  return snapshot(
    buildDroidProbe(inventoryWarnings.length > 0 ? "warning" : "ready", message, {
      version,
      auth,
    }),
    {
      ...(inventory?.slashCommands ? { slashCommands: inventory.slashCommands } : {}),
      ...(inventory?.skills ? { skills: inventory.skills } : {}),
    },
    models,
  );
});

export const enrichDroidSnapshot = (input: {
  readonly snapshot: ServerProvider;
  readonly maintenanceCapabilities: ProviderMaintenanceCapabilities;
  readonly enableProviderUpdateChecks?: boolean;
  readonly publishSnapshot: (snapshot: ServerProvider) => Effect.Effect<void>;
  readonly httpClient: HttpClient.HttpClient;
}): Effect.Effect<void> =>
  enrichProviderSnapshotWithVersionAdvisory(input.snapshot, input.maintenanceCapabilities, {
    enableProviderUpdateChecks: input.enableProviderUpdateChecks,
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, input.httpClient),
    Effect.flatMap((enrichedSnapshot) => input.publishSnapshot(enrichedSnapshot)),
    Effect.catchCause((cause) =>
      Effect.logWarning("Droid version advisory enrichment failed", {
        errorTag: causeErrorTag(cause),
      }),
    ),
    Effect.asVoid,
  );
