import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, describe, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentId,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  EphemeralEnvironmentError,
  EphemeralEnvironmentLeaseId,
  EphemeralEnvironmentTemplateId,
  type EphemeralEnvironmentTemplate,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Manager from "./EphemeralEnvironmentManager.ts";
import * as Provisioner from "./EphemeralEnvironmentProvisioner.ts";

const template: EphemeralEnvironmentTemplate = {
  id: EphemeralEnvironmentTemplateId.make("sandbox"),
  label: "Sandbox",
  enabled: true,
  weight: 50,
  provisioner: {
    kind: "command",
    provisionCommand: "create-sandbox",
    teardownCommand: "remove-sandbox",
    provisionTimeoutSeconds: 10,
  },
  idleTimeoutMinutes: 1,
  maxConcurrentLeases: 1,
  eligibleRepositoryKeys: ["example/repo"],
};

const callerId = AuthSessionId.make("owner-session");
const request = {
  templateId: template.id,
  repositoryKey: "example/repo",
  clientRequestId: "start-thread",
};
const environmentId = EnvironmentId.make("sandbox-environment");
const allocation = {
  environmentId,
  allocationHandle: "allocation-1",
  httpUrl: "https://sandbox.example",
  wsUrl: "wss://sandbox.example/ws",
  bearerToken: "test-bearer",
  pairingToken: "test-pairing-code",
  workspaceRoot: "/repo",
};

const output = (stdout = "", code = 0): ProcessRunner.ProcessRunOutput => ({
  stdout,
  stderr: "",
  code: ChildProcessSpawner.ExitCode(code),
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
});

const layerManager = (
  run: ProcessRunner.ProcessRunner["Service"]["run"],
  describe: Provisioner.EphemeralEnvironmentProbe["Service"]["describe"] = () =>
    Effect.succeed(environmentId),
  checkActiveWork: Provisioner.EphemeralEnvironmentProbe["Service"]["checkActiveWork"] = () =>
    Effect.succeed(false),
) => {
  const credentials = new Map<string, Uint8Array>();
  return Manager.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        ServerSettings.layerTest(),
        NodeCrypto.layer,
        Layer.succeed(ProcessRunner.ProcessRunner, { run }),
        Layer.succeed(Provisioner.EphemeralEnvironmentProbe, { describe, checkActiveWork }),
        Layer.mock(ServerSecretStore.ServerSecretStore)({
          get: (key) => Effect.sync(() => Option.fromNullishOr(credentials.get(key))),
          set: (key, value) =>
            Effect.sync(() => {
              credentials.set(key, value);
            }),
          remove: (key) =>
            Effect.sync(() => {
              credentials.delete(key);
            }),
        }),
      ),
    ),
  );
};

const provisioned = output(
  [
    '{"allocationHandle":"earlier-handle"}',
    "provisioner log message",
    JSON.stringify(allocation, null, 2),
    "finished",
  ].join("\n"),
);

describe("EphemeralEnvironmentManager", () => {
  it.effect("upserts, lists and deletes templates, and protects templates with active leases", () =>
    Effect.gen(function* () {
      const manager = yield* Manager.EphemeralEnvironmentManager;
      assert.deepStrictEqual((yield* manager.list).templates, []);
      assert.deepStrictEqual(yield* manager.upsertTemplate({ template }), template);

      const renamed = { ...template, label: "Renamed sandbox" };
      assert.deepStrictEqual(yield* manager.upsertTemplate({ template: renamed }), renamed);
      assert.deepStrictEqual((yield* manager.list).templates, [renamed]);

      const { lease } = yield* manager.acquire(request, callerId);
      const blocked = yield* Effect.flip(manager.deleteTemplate({ templateId: template.id }));
      assert.include(blocked.message, "active leases");
      yield* manager.release({ leaseId: lease.leaseId });
      yield* manager.deleteTemplate({ templateId: template.id });
      assert.deepStrictEqual((yield* manager.list).templates, []);
      assert.strictEqual((yield* manager.list).leases[0]?.state, "released");
    }).pipe(
      Effect.provide(
        layerManager((input) =>
          Effect.succeed(input.env?.T3_EPHEMERAL_ALLOCATION_HANDLE ? output() : provisioned),
        ).pipe(Layer.provideMerge(Sqlite.layerMemory)),
      ),
    ),
  );

  it.effect(
    "coalesces concurrent requests and enforces repository eligibility and lease capacity",
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<ProcessRunner.ProcessRunInput>();
        const ready = yield* Deferred.make<void>();
        const calls: ProcessRunner.ProcessRunInput[] = [];

        yield* Effect.gen(function* () {
          const manager = yield* Manager.EphemeralEnvironmentManager;
          const sql = yield* SqlClient.SqlClient;
          yield* manager.upsertTemplate({ template: { ...template, enabled: false } });
          assert.include(
            (yield* Effect.flip(manager.acquire(request, callerId))).message,
            "disabled",
          );
          yield* manager.upsertTemplate({ template });
          const ineligible = yield* Effect.flip(
            manager.acquire({ ...request, repositoryKey: "other/repo" }, callerId),
          );
          assert.include(ineligible.message, "not eligible");

          const first = yield* manager.acquire(request, callerId).pipe(Effect.forkChild);
          const command = yield* Deferred.await(started);
          const duplicate = yield* manager
            .acquire(request, callerId)
            .pipe(Effect.forkChild({ startImmediately: true }));
          assert.strictEqual((yield* manager.list).leases[0]?.state, "provisioning");
          const limited = yield* Effect.flip(
            manager.acquire({ ...request, clientRequestId: "another" }, callerId),
          );
          assert.include(limited.message, "lease limit");

          yield* Deferred.succeed(ready, undefined);
          const acquired = yield* Fiber.join(first);
          assert.isFalse(acquired.reused);
          assert.strictEqual(acquired.lease.state, "ready");
          assert.strictEqual(acquired.lease.environmentId, environmentId);
          assert.strictEqual(acquired.lease.bearerToken, allocation.bearerToken);
          assert.strictEqual(acquired.lease.pairingToken, allocation.pairingToken);
          assert.deepStrictEqual(yield* Fiber.join(duplicate), { ...acquired, reused: true });
          assert.deepStrictEqual(yield* manager.acquire(request, callerId), {
            ...acquired,
            reused: true,
          });
          const listed = (yield* manager.list).leases[0];
          assert.isNull(listed?.bearerToken);
          assert.isNull(listed?.pairingToken);
          assert.isNull(listed?.clientRequestId);
          const persisted = yield* sql`SELECT * FROM ephemeral_environment_leases`;
          assert.include(persisted[0], {
            caller_id: callerId,
            repository_key: request.repositoryKey,
          });
          assert.notInclude(JSON.stringify(persisted), allocation.bearerToken);
          assert.notInclude(JSON.stringify(persisted), allocation.pairingToken);
          assert.deepStrictEqual(command.env, {
            T3_EPHEMERAL_TEMPLATE_ID: template.id,
            T3_EPHEMERAL_LEASE_ID: acquired.lease.leaseId,
            T3_EPHEMERAL_REPOSITORY_KEY: request.repositoryKey,
          });
          assert.lengthOf(calls, 1);

          const otherCaller = AuthSessionId.make("other-session");
          const otherRepository = { ...request, repositoryKey: "other/repo" };
          yield* manager.upsertTemplate({
            template: { ...template, eligibleRepositoryKeys: [], maxConcurrentLeases: 3 },
          });
          const foreign = yield* manager.acquire(request, otherCaller);
          const otherRepo = yield* manager.acquire(otherRepository, callerId);
          assert.isFalse(foreign.reused);
          assert.isFalse(otherRepo.reused);
          assert.lengthOf(
            new Set([acquired.lease.leaseId, foreign.lease.leaseId, otherRepo.lease.leaseId]),
            3,
          );
          assert.deepStrictEqual(yield* manager.acquire(request, callerId), {
            ...acquired,
            reused: true,
          });
        }).pipe(
          Effect.provide(
            layerManager((input) =>
              Effect.sync(() => calls.push(input)).pipe(
                Effect.andThen(Deferred.succeed(started, input)),
                Effect.andThen(Deferred.await(ready)),
                Effect.as(provisioned),
              ),
            ),
          ),
        );
      }).pipe(Effect.provide(Sqlite.layerMemory)),
  );

  it.effect.each([
    {
      kind: "fails before emitting a handle",
      provision: output("", 2),
      expected: "exit code 2",
      handle: "",
    },
    {
      kind: "exits unsuccessfully",
      provision: output(`${JSON.stringify(allocation)}\n{"status":"failed"}`, 2),
      expected: "exit code 2",
    },
    {
      kind: "times out after announcing an allocation",
      provision: { ...output(), timedOut: true },
      expected: "timed out",
    },
    {
      kind: "reports an invalid address",
      provision: output(JSON.stringify({ ...allocation, wsUrl: "https://sandbox.example" })),
      expected: "valid environment",
    },
    ...[
      { wsUrl: "wss://unverified.example/ws" },
      { wsUrl: "wss://sandbox.example:8443/ws" },
      { wsUrl: "ws://sandbox.example/ws" },
      { wsUrl: "wss://user@sandbox.example/ws" },
      { wsUrl: "wss://:password@sandbox.example/ws" },
      { wsUrl: "wss://sandbox.example/ws?token=test" },
      { wsUrl: "wss://sandbox.example/ws#token" },
      { httpUrl: "https://user@sandbox.example" },
      { httpUrl: "https://:password@sandbox.example" },
      { httpUrl: "https://sandbox.example?token=test" },
      { httpUrl: "https://sandbox.example#token" },
      { httpUrl: "https://sandbox.example?" },
      { wsUrl: "wss://sandbox.example/ws#" },
    ].map((endpoint) => ({
      kind: `reports ${Object.values(endpoint)[0]}`,
      provision: output(JSON.stringify({ ...allocation, ...endpoint })),
      expected: "valid environment",
    })),
    {
      kind: "has an identity mismatch",
      provision: provisioned,
      expected: "different environment ID",
    },
    { kind: "cannot be probed", provision: provisioned, expected: "could not be reached" },
  ])(
    "cleans up the allocation and persists a failure when provisioning $kind",
    ({ kind, provision, expected, handle = allocation.allocationHandle }) =>
      Effect.gen(function* () {
        const teardowns: ProcessRunner.ProcessRunInput[] = [];
        yield* Effect.gen(function* () {
          const manager = yield* Manager.EphemeralEnvironmentManager;
          yield* manager.upsertTemplate({ template });
          const failed = yield* Effect.flip(manager.acquire(request, callerId));
          assert.include(failed.message, expected);
          const [lease] = (yield* manager.list).leases;
          assert.strictEqual(lease?.state, "failed");
          assert.strictEqual(lease?.lastError, failed.message);
          assert.deepStrictEqual(
            teardowns.map((call) => call.env),
            [
              {
                T3_EPHEMERAL_TEMPLATE_ID: template.id,
                T3_EPHEMERAL_LEASE_ID: lease?.leaseId,
                T3_EPHEMERAL_ALLOCATION_HANDLE: handle,
              },
            ],
          );
        }).pipe(
          Effect.provide(
            layerManager(
              (input) =>
                Effect.sync(() => {
                  if (input.env?.T3_EPHEMERAL_ALLOCATION_HANDLE === undefined) {
                    if (provision.timedOut) {
                      input.onStdoutChunk?.(new TextEncoder().encode(JSON.stringify(allocation)));
                    }
                    return provision;
                  }
                  teardowns.push(input);
                  return output();
                }),
              () =>
                kind === "cannot be probed"
                  ? Effect.fail(
                      new EphemeralEnvironmentError({
                        message: "The environment could not be reached.",
                      }),
                    )
                  : Effect.succeed(EnvironmentId.make("different-environment")),
            ),
          ),
        );
      }).pipe(Effect.provide(Sqlite.layerMemory)),
  );

  it.effect("counts failed teardown against capacity and retries the retained command", () =>
    Effect.gen(function* () {
      const teardowns: ProcessRunner.ProcessRunInput[] = [];
      yield* Effect.gen(function* () {
        const manager = yield* Manager.EphemeralEnvironmentManager;
        yield* manager.upsertTemplate({ template });
        const { lease } = yield* manager.acquire(request, callerId);

        const failure = yield* Effect.flip(manager.release({ leaseId: lease.leaseId }));
        assert.include(failure.message, "retry cleanup");
        const failed = (yield* manager.list).leases[0];
        assert.strictEqual(failed?.state, "failed");
        assert.strictEqual(failed?.lastError, failure.message);
        assert.include(
          (yield* Effect.flip(manager.acquire(request, callerId))).message,
          "lease limit",
        );
        assert.include(
          (yield* Effect.flip(manager.deleteTemplate({ templateId: template.id }))).message,
          "active leases",
        );

        yield* manager.upsertTemplate({
          template: {
            ...template,
            provisioner: { ...template.provisioner, teardownCommand: "new-cleanup" },
          },
        });
        yield* TestClock.adjust("30 seconds");
        assert.strictEqual((yield* manager.list).leases[0]?.state, "released");
        yield* manager.deleteTemplate({ templateId: template.id });
        const released = yield* manager.release({ leaseId: lease.leaseId });
        assert.strictEqual(released.lease.state, "released");
        assert.strictEqual(released.lease.generation, lease.generation + 2);
        assert.isNull(released.lease.lastError);
        assert.deepStrictEqual(yield* manager.release({ leaseId: lease.leaseId }), released);
        assert.deepStrictEqual(
          teardowns.map(({ args }) => args),
          [
            ["-c", "remove-sandbox"],
            ["-c", "remove-sandbox"],
          ],
        );
      }).pipe(
        Effect.provide(
          layerManager((input) =>
            Effect.sync(() =>
              input.env?.T3_EPHEMERAL_ALLOCATION_HANDLE === undefined
                ? provisioned
                : output("", teardowns.push(input) === 1 ? 1 : 0),
            ),
          ),
        ),
      );
    }).pipe(Effect.provide(Sqlite.layerMemory)),
  );

  it.effect("retains cleanup after a handleless provisioning failure", () =>
    Effect.gen(function* () {
      let cleanupAttempts = 0;
      yield* Effect.gen(function* () {
        const manager = yield* Manager.EphemeralEnvironmentManager;
        yield* manager.upsertTemplate({ template });
        const failed = yield* Effect.flip(manager.acquire(request, callerId));
        assert.include(failed.message, "retry cleanup");
        assert.include(
          (yield* Effect.flip(manager.acquire(request, callerId))).message,
          "lease limit",
        );
        yield* manager.reconcileIdleLeases;
        assert.strictEqual(cleanupAttempts, 2);
        yield* manager.deleteTemplate({ templateId: template.id });
      }).pipe(
        Effect.provide(
          layerManager((input) =>
            Effect.sync(() =>
              input.env?.T3_EPHEMERAL_ALLOCATION_HANDLE === undefined
                ? output("", 2)
                : output("", ++cleanupAttempts === 1 ? 1 : 0),
            ),
          ),
        ),
      );
    }).pipe(Effect.provide(Sqlite.layerMemory)),
  );

  it.effect("fences a release during provisioning and hands the late allocation to teardown", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const allocated = yield* Deferred.make<void>();
      const releases: string[] = [];

      yield* Effect.gen(function* () {
        const manager = yield* Manager.EphemeralEnvironmentManager;
        yield* manager.upsertTemplate({ template });
        const acquire = yield* Effect.flip(manager.acquire(request, callerId)).pipe(
          Effect.forkChild,
        );
        yield* Deferred.await(started);
        const [provisioning] = (yield* manager.list).leases;
        assert.strictEqual(provisioning?.state, "provisioning");
        const release = yield* manager
          .release({ leaseId: provisioning!.leaseId })
          .pipe(Effect.forkChild({ startImmediately: true }));

        const [draining] = (yield* manager.list).leases;
        assert.strictEqual(draining?.state, "draining");
        assert.strictEqual(draining?.generation, provisioning!.generation + 1);
        yield* Deferred.succeed(allocated, undefined);
        assert.include((yield* Fiber.join(acquire)).message, "lease changed");
        const { lease } = yield* Fiber.join(release);
        assert.strictEqual(lease.state, "released");
        assert.strictEqual(lease.generation, draining?.generation);
        assert.isNull(lease.environmentId);
        assert.deepStrictEqual(releases, [allocation.allocationHandle]);
      }).pipe(
        Effect.provide(
          layerManager((input) => {
            const handle = input.env?.T3_EPHEMERAL_ALLOCATION_HANDLE;
            return handle === undefined
              ? Deferred.succeed(started, undefined).pipe(
                  Effect.andThen(Deferred.await(allocated)),
                  Effect.as(provisioned),
                )
              : Effect.sync(() => {
                  releases.push(handle);
                  return output();
                });
          }),
        ),
      );
    }).pipe(Effect.provide(Sqlite.layerMemory)),
  );

  it.effect("reconciles at the idle deadline both explicitly and in the background", () =>
    Effect.gen(function* () {
      const manager = yield* Manager.EphemeralEnvironmentManager;
      yield* manager.upsertTemplate({ template });
      yield* TestClock.adjust("1 second");
      const { lease } = yield* manager.acquire(request, callerId);
      assert.strictEqual(Date.parse(lease.idleDeadlineAt!) - Date.parse(lease.updatedAt), 60_000);

      yield* TestClock.adjust("59 seconds");
      yield* manager.reconcileIdleLeases;
      assert.strictEqual((yield* manager.list).leases[0]?.state, "ready");
      yield* TestClock.adjust("2 seconds");
      yield* manager.reconcileIdleLeases;
      const released = (yield* manager.list).leases[0];
      assert.strictEqual(released?.state, "released");
      assert.isNull(released?.idleDeadlineAt);

      const second = yield* manager.acquire(request, callerId);
      yield* TestClock.adjust("90 seconds");
      assert.strictEqual(
        (yield* manager.list).leases.find((entry) => entry.leaseId === second.lease.leaseId)?.state,
        "released",
      );
    }).pipe(
      Effect.provide(
        layerManager((input) =>
          Effect.succeed(input.env?.T3_EPHEMERAL_ALLOCATION_HANDLE ? output() : provisioned),
        ).pipe(Layer.provideMerge(Sqlite.layerMemory)),
      ),
    ),
  );

  it.effect("postpones idle teardown for active work and when the probe fails", () =>
    Effect.gen(function* () {
      const activities = ["busy", "unreachable", "idle"] as const;
      let checks = 0;
      let teardownCount = 0;
      const http = HttpClient.make((request) =>
        Effect.sync(() => {
          assert.equal(request.url, "https://sandbox.example/api/orchestration/shell");
          assert.equal(request.headers.authorization, `Bearer ${allocation.bearerToken}`);
          assert.equal(
            request.headers[ORCHESTRATION_PROTOCOL_HEADER],
            ORCHESTRATION_PROTOCOL_VERSION_TEXT,
          );
          const activity = activities[checks++];
          const response =
            activity === "unreachable"
              ? new Response(null, { status: 503 })
              : Response.json({
                  threads: [],
                  archivedThreads: [
                    {
                      status: "idle",
                      activeRunId: activity === "busy" ? "active-run" : null,
                      pendingRuntimeRequest: null,
                    },
                  ],
                });
          return HttpClientResponse.fromWeb(request, response);
        }),
      );
      const { checkActiveWork } = yield* Provisioner.EphemeralEnvironmentProbe.pipe(
        Effect.provide(
          Provisioner.layerProbe.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http))),
        ),
      );
      yield* Effect.gen(function* () {
        const manager = yield* Manager.EphemeralEnvironmentManager;
        yield* manager.upsertTemplate({ template });
        yield* TestClock.adjust("1 second");
        const { lease } = yield* manager.acquire(request, callerId);
        assert.isTrue(
          yield* checkActiveWork(lease, {
            bearerToken: null,
            pairingToken: allocation.pairingToken,
          }),
        );
        assert.strictEqual(checks, 0);

        for (const minute of [1, 2]) {
          yield* TestClock.adjust("1 minute");
          yield* manager.reconcileIdleLeases;
          const [postponed] = (yield* manager.list).leases;
          assert.strictEqual(postponed?.state, "ready");
          assert.equal(
            Date.parse(postponed!.idleDeadlineAt!) - Date.parse(lease.idleDeadlineAt!),
            minute * 60_000,
          );
          assert.strictEqual(teardownCount, 0);
        }

        yield* TestClock.adjust("1 minute");
        yield* manager.reconcileIdleLeases;
        assert.strictEqual((yield* manager.list).leases[0]?.state, "released");
        assert.strictEqual(checks, 3);
        assert.strictEqual(teardownCount, 1);
      }).pipe(
        Effect.provide(
          layerManager(
            (input) =>
              Effect.sync(() => {
                if (input.env?.T3_EPHEMERAL_ALLOCATION_HANDLE === undefined) return provisioned;
                teardownCount++;
                return output();
              }),
            undefined,
            checkActiveWork,
          ),
        ),
      );
    }).pipe(Effect.provide(Sqlite.layerMemory)),
  );

  it.effect("resumes interrupted provision and release cleanup on startup", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const interruptedProvision = EphemeralEnvironmentLeaseId.make("interrupted-provision");
      const interruptedRelease = EphemeralEnvironmentLeaseId.make("interrupted-release");
      const failedRelease = EphemeralEnvironmentLeaseId.make("failed-release");
      yield* sql`
        INSERT INTO ephemeral_environment_leases (
          lease_id, template_id, generation, state, allocation_handle, created_at, updated_at,
          caller_id, idle_timeout_minutes, teardown_command, teardown_timeout_seconds, teardown_pending
        ) VALUES
          (${interruptedProvision}, ${template.id}, 2, 'provisioning', NULL, '2026-01-01', '2026-01-01', ${callerId}, 1, 'remove-sandbox', 10, 1),
          (${interruptedRelease}, ${template.id}, 4, 'releasing', 'saved-handle', '2026-01-01', '2026-01-01', ${callerId}, 1, 'remove-sandbox', 10, 1),
          (${failedRelease}, ${template.id}, 5, 'failed', NULL, '2026-01-01', '2026-01-01', ${callerId}, 1, 'old-cleanup', 10, 1)
      `;
      const teardowns: ProcessRunner.ProcessRunInput[] = [];
      const commandsStarted = yield* Deferred.make<void>();
      const done = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const manager = yield* Manager.EphemeralEnvironmentManager;
        yield* Deferred.await(commandsStarted);
        yield* Deferred.succeed(done, undefined);
        const [provision, release, failed] = yield* Effect.all(
          [
            manager.release({ leaseId: interruptedProvision }),
            manager.release({ leaseId: interruptedRelease }),
            manager.release({ leaseId: failedRelease }),
          ],
          { concurrency: 3 },
        );
        assert.strictEqual(provision.lease.state, "released");
        assert.strictEqual(release.lease.state, "released");
        assert.strictEqual(failed.lease.state, "released");
        assert.deepStrictEqual(
          teardowns
            .map(({ env }) => [env?.T3_EPHEMERAL_LEASE_ID, env?.T3_EPHEMERAL_ALLOCATION_HANDLE])
            .sort(),
          [
            [failedRelease, ""],
            [interruptedProvision, ""],
            [interruptedRelease, "saved-handle"],
          ],
        );
      }).pipe(
        Effect.provide(
          layerManager((input) =>
            Effect.sync(() => teardowns.push(input)).pipe(
              Effect.flatMap((count) =>
                count === 3 ? Deferred.succeed(commandsStarted, undefined) : Effect.void,
              ),
              Effect.andThen(Deferred.await(done)),
              Effect.as(output()),
            ),
          ),
        ),
      );
    }).pipe(Effect.provide(Sqlite.layerMemory)),
  );
});
