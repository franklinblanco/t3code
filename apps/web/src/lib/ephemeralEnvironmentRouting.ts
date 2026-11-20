import {
  BearerConnectionCredential,
  BearerConnectionProfile,
  BearerConnectionRegistration,
  BearerConnectionTarget,
  ConnectionOnboarding,
  connectionRoutes,
  EnvironmentRegistry,
} from "@t3tools/client-runtime/connection";
import {
  fetchRemoteEnvironmentDescriptor,
  normalizeHttpBaseUrl,
  scopeProjectRef,
} from "@t3tools/client-runtime/environment";
import { isRoutingProviderAvailable } from "@t3tools/client-runtime/load-balancing";
import {
  getAddProjectInitialQuery,
  getCloneDestinationPath,
  getCloneDirectoryPath,
} from "@t3tools/client-runtime/operations/projects";
import {
  createAtomCommandScheduler,
  createRuntimeCommand,
  runAtomCommand,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/shell";
import {
  AuthOrchestrationOperateScope,
  CommandId,
  EphemeralEnvironmentError,
  type EphemeralEnvironmentLease,
  type ProviderDriverKind,
  type ProviderInstanceId,
  repositoryGroupingKeyOf,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { msg } from "gt-react";

import { connectionAtomRuntime } from "../connection/runtime";
import { m } from "../i18n/messages";
import { appAtomRegistry } from "../rpc/atomRegistry";
import { readProjects } from "../state/entities";
import { environmentProjectClonesAtom } from "../state/projectClones";
import { environmentProjects, projectEnvironment } from "../state/projects";
import { EPHEMERAL_CONNECTION_ID_PREFIX } from "../state/ephemeralEnvironments";
import { serverEnvironment } from "../state/server";
import { environmentSession, sessionHasScope } from "../state/session";
import { environmentShell } from "../state/shell";
import { waitForAtomValue } from "../state/waitForAtomValue";
import { addProjectToMachine } from "./addProjectToMachine";
import { newProjectId } from "./utils";

const connectEphemeralEnvironmentCommand = createRuntimeCommand(connectionAtomRuntime, {
  label: "web:connection:connect-ephemeral-environment",
  scheduler: createAtomCommandScheduler(),
  concurrency: {
    mode: "serial",
    key: ({ lease }: { lease: EphemeralEnvironmentLease; label: string }) =>
      lease.environmentId ?? lease.httpUrl ?? lease.leaseId,
  },
  execute: Effect.fn("web.connection.connectEphemeralEnvironment")(function* ({ lease, label }) {
    const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
    let environmentId = lease.environmentId;
    const isRegistered = (id: NonNullable<typeof environmentId>) =>
      SubscriptionRef.get(registry.entries).pipe(Effect.map((entries) => entries.has(id)));
    const isConnected = Effect.fn("web.connection.isEphemeralEnvironmentConnected")(function* (
      id: NonNullable<typeof environmentId>,
    ) {
      return (yield* isRegistered(id)) && (yield* registry.state(id)).phase === "connected";
    });
    if (environmentId !== null && (yield* isConnected(environmentId))) return environmentId;

    if (!lease.httpUrl || !lease.wsUrl || (!lease.bearerToken && !lease.pairingToken)) {
      return yield* new EphemeralEnvironmentError({
        message: m(msg("The lease did not provide a usable connection.")),
      });
    }
    const { httpBaseUrl, wsBaseUrl } = yield* Effect.try({
      try: () => {
        const httpBaseUrl = normalizeHttpBaseUrl(lease.httpUrl!);
        const wsBaseUrl = new URL(lease.wsUrl!);
        return { httpBaseUrl, wsBaseUrl };
      },
      catch: () =>
        new EphemeralEnvironmentError({
          message: m(msg("The lease did not provide a usable connection.")),
        }),
    });
    if (wsBaseUrl.protocol !== "ws:" && wsBaseUrl.protocol !== "wss:") {
      return yield* new EphemeralEnvironmentError({
        message: m(msg("The lease did not provide a usable connection.")),
      });
    }

    const descriptor = yield* fetchRemoteEnvironmentDescriptor({ httpBaseUrl });
    if (environmentId !== null && descriptor.environmentId !== environmentId) {
      return yield* new EphemeralEnvironmentError({
        message: m(msg("The lease address reaches a different machine.")),
      });
    }
    environmentId = descriptor.environmentId;
    if (yield* isConnected(environmentId)) return environmentId;

    const address = new URL(httpBaseUrl).origin;
    const previous = (yield* SubscriptionRef.get(registry.entries)).get(environmentId);
    const previousRoute =
      previous &&
      connectionRoutes(previous).find(
        ({ profile }) =>
          Option.isSome(profile) &&
          profile.value._tag === "BearerConnectionProfile" &&
          new URL(profile.value.httpBaseUrl).origin === address,
      );
    const connectionId =
      previousRoute?.target._tag === "BearerConnectionTarget"
        ? previousRoute.target.connectionId
        : `${EPHEMERAL_CONNECTION_ID_PREFIX}${environmentId}:${address}`;
    const credential = lease.bearerToken
      ? new BearerConnectionCredential({ token: lease.bearerToken })
      : (yield* ConnectionOnboarding.preparePairingRegistration({
          host: httpBaseUrl,
          pairingCode: lease.pairingToken!,
          expectedEnvironmentId: environmentId,
        })).credential;
    const registered = yield* registry.register(
      new BearerConnectionRegistration({
        target: new BearerConnectionTarget({ environmentId, label, connectionId }),
        profile: new BearerConnectionProfile({
          environmentId,
          label,
          connectionId,
          httpBaseUrl,
          wsBaseUrl: wsBaseUrl.toString(),
        }),
        credential,
      }),
    );
    if (registered) {
      yield* registry.setCompatibility(environmentId, null);
      yield* registry.setEnabled(environmentId, true);
      yield* registry.retryNow(environmentId);
    }
    const connection = yield* registry.stateChanges(environmentId).pipe(
      Stream.filter(
        ({ phase }) => phase === "connected" || phase === "blocked" || phase === "offline",
      ),
      Stream.runHead,
    );
    if (Option.isNone(connection) || connection.value.phase !== "connected") {
      return yield* (
        Option.getOrNull(connection)?.lastFailure ??
          new EphemeralEnvironmentError({
            message: m(msg("Could not connect to the ephemeral environment.")),
          })
      );
    }
    return environmentId;
  }, Effect.timeout(30_000)),
});

export async function connectEphemeralEnvironment(input: {
  lease: EphemeralEnvironmentLease;
  label: string;
  signal?: AbortSignal;
}) {
  const { lease, label, signal } = input;
  const connection = await runAtomCommand(
    appAtomRegistry,
    connectEphemeralEnvironmentCommand,
    { lease, label },
    { signal, reportFailure: false },
  );
  if (connection._tag === "Failure") throw squashAtomCommandFailure(connection);
  return connection.value;
}

export function ephemeralCloneRemoteUrl(remoteUrl: string) {
  if (!/^\s*https?:/i.test(remoteUrl)) return remoteUrl;
  const parsed = new URL(remoteUrl);
  if (!parsed.username && !parsed.password) return remoteUrl;
  parsed.username = "";
  parsed.password = "";
  return parsed.toString();
}

export async function prepareEphemeralDestination(input: {
  lease: EphemeralEnvironmentLease;
  label: string;
  project: EnvironmentProject;
  repositoryKey: string | null;
  driver: ProviderDriverKind;
  instanceId: ProviderInstanceId | null;
  signal: AbortSignal;
}) {
  const { lease, label, project, repositoryKey, driver, instanceId, signal } = input;
  const environmentId = await connectEphemeralEnvironment({ lease, label, signal });
  signal.throwIfAborted();
  const configAtom = serverEnvironment.configValueAtom(environmentId);
  const sessionAtom = environmentSession.sessionStateAtom(environmentId);
  const shellAtom = environmentShell.statusAtom(environmentId);
  await Promise.all([
    waitForAtomValue({
      registry: appAtomRegistry,
      atom: configAtom,
      predicate: (config) => config !== null,
      timeoutMs: 10_000,
      signal,
    }),
    waitForAtomValue({
      registry: appAtomRegistry,
      atom: sessionAtom,
      predicate: (session) => session._tag === "Success",
      timeoutMs: 10_000,
      signal,
    }),
    waitForAtomValue({
      registry: appAtomRegistry,
      atom: shellAtom,
      predicate: (status) => status === "live",
      timeoutMs: 10_000,
      signal,
    }),
  ]);
  signal.throwIfAborted();
  const config = appAtomRegistry.get(configAtom);
  if (
    !config ||
    appAtomRegistry.get(shellAtom) !== "live" ||
    !sessionHasScope(appAtomRegistry.get(sessionAtom), AuthOrchestrationOperateScope)
  ) {
    throw new Error(m(msg("Could not connect to the ephemeral environment.")));
  }
  const projects = readProjects().filter((candidate) => candidate.environmentId === environmentId);
  const matchedByRoot = projects.find(
    (candidate) => candidate.workspaceRoot === lease.workspaceRoot,
  );
  const matchedByRepository =
    repositoryKey === null
      ? undefined
      : projects.find(
          (candidate) =>
            candidate.repositoryIdentity &&
            repositoryGroupingKeyOf(candidate.repositoryIdentity) === repositoryKey,
        );
  let projectId = (matchedByRoot ?? matchedByRepository)?.id;
  if (!projectId && lease.workspaceRoot) {
    projectId = newProjectId();
    const created = await runAtomCommand(
      appAtomRegistry,
      projectEnvironment.create,
      {
        environmentId,
        input: {
          projectId,
          commandId: CommandId.make(`ephemeral:project:create:${projectId}`),
          title: project.title,
          workspaceRoot: lease.workspaceRoot,
          createWorkspaceRootIfMissing: false,
          defaultModelSelection: null,
        },
      },
      { signal, reportFailure: false },
    );
    if (created._tag === "Failure") throw squashAtomCommandFailure(created);
  } else if (!projectId) {
    const sourceRemoteUrl =
      project.repositoryIdentity?.origin?.remoteUrl ??
      project.repositoryIdentity?.locator.remoteUrl;
    if (!sourceRemoteUrl)
      throw new Error(m(msg("The lease has no workspace and this project has no clone URL.")));
    const remoteUrl = ephemeralCloneRemoteUrl(sourceRemoteUrl);
    projectId = await addProjectToMachine({
      source: { environmentId: project.environmentId, cwd: project.workspaceRoot },
      subdirectory: "",
      target: { environmentId, label },
      projectId: newProjectId(),
      title: project.title,
      remoteUrl,
      destinationPath: getCloneDestinationPath(
        getAddProjectInitialQuery(config.settings.addProjectBaseDirectory),
        getCloneDirectoryPath(remoteUrl, config.settings.ghqCloneLayout === true),
      ),
      files: [],
    });
    const cloneAtom = environmentProjectClonesAtom(environmentId);
    const finished = await waitForAtomValue({
      registry: appAtomRegistry,
      atom: cloneAtom,
      predicate: (clones) =>
        clones.some((clone) => clone.projectId === projectId && clone.phase !== "running"),
      timeoutMs: 120_000,
      signal,
    });
    signal.throwIfAborted();
    const clone = appAtomRegistry.get(cloneAtom).find((entry) => entry.projectId === projectId);
    if (!finished || clone?.phase !== "done")
      throw new Error(m(msg("Could not finish cloning the project.")));
  }
  const projectRef = scopeProjectRef(environmentId, projectId);
  const projectReady = await waitForAtomValue({
    registry: appAtomRegistry,
    atom: environmentProjects.projectAtom(projectRef),
    predicate: (project) => project !== null,
    timeoutMs: 10_000,
    signal,
  });
  signal.throwIfAborted();
  if (!projectReady) throw new Error(m(msg("The project did not appear in the desktop app.")));
  const providersAtom = serverEnvironment.providersValueAtom(environmentId);
  const providersReady = await waitForAtomValue({
    registry: appAtomRegistry,
    atom: providersAtom,
    predicate: (providers) => providers !== null,
    timeoutMs: 10_000,
    signal,
  });
  signal.throwIfAborted();
  const providers = appAtomRegistry.get(providersAtom) ?? [];
  if (
    !providersReady ||
    !providers.some((provider) =>
      isRoutingProviderAvailable({
        provider,
        driver,
        instanceId,
        projectId,
        settings: appAtomRegistry.get(configAtom)?.settings,
      }),
    )
  ) {
    throw new Error(m(msg("The selected provider is unavailable on the ephemeral environment.")));
  }
  return projectRef;
}
