import {
  EphemeralEnvironmentError,
  EphemeralEnvironmentLeaseId,
  EphemeralEnvironmentTemplateSchema,
  type AuthSessionId,
  type EnvironmentId,
  type EphemeralEnvironmentAcquireInput,
  type EphemeralEnvironmentAcquireResult,
  type EphemeralEnvironmentDeleteTemplateInput,
  type EphemeralEnvironmentLease,
  type EphemeralEnvironmentListResult,
  type EphemeralEnvironmentReleaseInput,
  type EphemeralEnvironmentReleaseResult,
  type EphemeralEnvironmentTemplate,
  type EphemeralEnvironmentTemplateId,
  type EphemeralEnvironmentUpsertTemplateInput,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Provisioner from "./EphemeralEnvironmentProvisioner.ts";

export class EphemeralEnvironmentManager extends Context.Service<
  EphemeralEnvironmentManager,
  {
    readonly list: Effect.Effect<EphemeralEnvironmentListResult, EphemeralEnvironmentError>;
    readonly upsertTemplate: (
      input: EphemeralEnvironmentUpsertTemplateInput,
    ) => Effect.Effect<EphemeralEnvironmentTemplate, EphemeralEnvironmentError>;
    readonly deleteTemplate: (
      input: EphemeralEnvironmentDeleteTemplateInput,
    ) => Effect.Effect<void, EphemeralEnvironmentError>;
    readonly acquire: (
      input: EphemeralEnvironmentAcquireInput,
      callerId: AuthSessionId,
    ) => Effect.Effect<EphemeralEnvironmentAcquireResult, EphemeralEnvironmentError>;
    readonly release: (
      input: EphemeralEnvironmentReleaseInput,
    ) => Effect.Effect<EphemeralEnvironmentReleaseResult, EphemeralEnvironmentError>;
    readonly reconcileIdleLeases: Effect.Effect<void, EphemeralEnvironmentError>;
  }
>()("t3/environment/EphemeralEnvironmentManager") {}

interface LeaseRow {
  readonly lease_id: EphemeralEnvironmentLeaseId;
  readonly template_id: EphemeralEnvironmentTemplateId;
  readonly generation: number;
  readonly state: EphemeralEnvironmentLease["state"];
  readonly allocation_handle: string | null;
  readonly environment_id: EnvironmentId | null;
  readonly http_url: string | null;
  readonly ws_url: string | null;
  readonly workspace_root: string | null;
  readonly client_request_id: string | null;
  readonly caller_id: AuthSessionId;
  readonly repository_key: string | null;
  readonly last_error: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly idle_deadline_at: string | null;
  readonly idle_timeout_minutes: number;
  readonly teardown_command: string | null;
  readonly teardown_timeout_seconds: number;
  readonly teardown_pending: number;
}

const decodeTemplate = Schema.decodeEffect(EphemeralEnvironmentTemplateSchema);
const interruptedProvisionMessage = "The server restarted during provisioning.";

const LeaseCredentials = Schema.Struct({
  bearerToken: Schema.NullOr(Schema.String),
  pairingToken: Schema.NullOr(Schema.String),
});
const CredentialsJson = Schema.fromJsonString(LeaseCredentials);
const decodeCredentials = Schema.decodeEffect(CredentialsJson);
const encodeCredentials = Schema.encodeEffect(CredentialsJson);
const noCredentials: Provisioner.EphemeralEnvironmentCredentials = {
  bearerToken: null,
  pairingToken: null,
};

const toLease = (row: LeaseRow, credentials = noCredentials): EphemeralEnvironmentLease => ({
  leaseId: row.lease_id,
  templateId: row.template_id,
  generation: row.generation,
  state: row.state,
  allocationHandle: row.allocation_handle,
  environmentId: row.environment_id,
  httpUrl: row.http_url,
  wsUrl: row.ws_url,
  workspaceRoot: row.workspace_root,
  clientRequestId: row.client_request_id,
  lastError: row.last_error,
  createdAt: row.created_at,
  updatedAt: row.updated_at,
  idleDeadlineAt: row.idle_deadline_at,
  ...credentials,
});

const secretName = (leaseId: EphemeralEnvironmentLeaseId) => `ephemeral-environment-${leaseId}`;
const isoTime = (ms: number) => DateTime.formatIso(DateTime.makeUnsafe(ms));
const now = Clock.currentTimeMillis.pipe(Effect.map(isoTime));
const leaseStorageError = () =>
  new EphemeralEnvironmentError({
    message: "Could not read or save ephemeral environment leases.",
  });
const tokenStorageError = () =>
  new EphemeralEnvironmentError({
    message: "Could not read or save ephemeral environment credentials.",
  });
const settingsError = () =>
  new EphemeralEnvironmentError({
    message: "Could not read or save ephemeral environment templates.",
  });
const cancelled = () =>
  new EphemeralEnvironmentError({ message: "The lease changed while the operation was running." });

const sameVersion = (left: LeaseRow, right: LeaseRow) =>
  left.generation === right.generation && left.state === right.state;

type LeaseFiber = Fiber.Fiber<EphemeralEnvironmentLease, EphemeralEnvironmentError>;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const settings = yield* ServerSettings.ServerSettingsService;
  const updateTemplates = yield* ServerSettings.EphemeralEnvironmentTemplateSettings;
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const random = yield* SecureRandom.SecureRandom;
  const probe = yield* Provisioner.EphemeralEnvironmentProbe;
  const commands = yield* Provisioner.makeCommands;
  const scope = yield* Scope.Scope;
  const gate = yield* Semaphore.make(1);
  const provisions = new Map<EphemeralEnvironmentLeaseId, LeaseFiber>();
  const releases = new Map<EphemeralEnvironmentLeaseId, LeaseFiber>();

  const serialize = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    gate.withPermits(1)(Effect.uninterruptible(effect));

  const startJob = Effect.fn("EphemeralEnvironmentManager.startJob")(function* (
    jobs: Map<EphemeralEnvironmentLeaseId, LeaseFiber>,
    leaseId: EphemeralEnvironmentLeaseId,
    effect: Effect.Effect<EphemeralEnvironmentLease, EphemeralEnvironmentError>,
  ) {
    // Socket cancellation only interrupts the waiter. The server scope owns the allocation.
    const job = yield* effect.pipe(
      Effect.ensuring(serialize(Effect.sync(() => jobs.delete(leaseId)))),
      Effect.forkIn(scope),
    );
    jobs.set(leaseId, job);
    return job;
  });

  const getRow = Effect.fn("EphemeralEnvironmentManager.getRow")(function* (
    leaseId: EphemeralEnvironmentLeaseId,
  ) {
    const [row] = yield* sql<LeaseRow>`
      SELECT * FROM ephemeral_environment_leases WHERE lease_id = ${leaseId}
    `.pipe(Effect.mapError(leaseStorageError));
    if (!row) {
      return yield* new EphemeralEnvironmentError({
        message: "The ephemeral lease does not exist.",
      });
    }
    return row;
  });

  const updateRow = Effect.fn("EphemeralEnvironmentManager.updateRow")(function* (
    row: LeaseRow,
    changes: Partial<Omit<LeaseRow, "lease_id" | "template_id" | "created_at">>,
  ) {
    const updatedAt = yield* now;
    const [updated] = yield* sql<LeaseRow>`
      UPDATE ephemeral_environment_leases
      SET ${sql.update({ ...changes, updated_at: updatedAt })}
      WHERE lease_id = ${row.lease_id} AND generation = ${row.generation} AND state = ${row.state}
      RETURNING *
    `.pipe(Effect.mapError(leaseStorageError));
    return updated ?? (yield* cancelled());
  });

  const readCredentials = Effect.fn("EphemeralEnvironmentManager.readCredentials")(function* (
    leaseId: EphemeralEnvironmentLeaseId,
  ) {
    const stored = yield* secrets.get(secretName(leaseId));
    if (Option.isNone(stored)) return noCredentials;
    return yield* decodeCredentials(new TextDecoder().decode(stored.value));
  }, Effect.mapError(tokenStorageError));

  const getTemplates = settings.getSettings.pipe(
    Effect.map((current) => current.ephemeralEnvironmentTemplates),
    Effect.mapError(settingsError),
  );
  const activeCount = Effect.fn("EphemeralEnvironmentManager.activeCount")(function* (
    templateId: EphemeralEnvironmentTemplateId,
  ) {
    const [row] = yield* sql<{ readonly count: number }>`
      SELECT COUNT(*) AS count FROM ephemeral_environment_leases
      WHERE template_id = ${templateId}
        AND (state IN ('provisioning', 'ready', 'draining', 'releasing') OR teardown_pending = 1)
    `.pipe(Effect.mapError(leaseStorageError));
    return row!.count;
  });

  const list = Effect.gen(function* () {
    const templates = yield* getTemplates;
    const rows = yield* sql<LeaseRow>`
      SELECT * FROM ephemeral_environment_leases ORDER BY created_at DESC, lease_id
    `.pipe(Effect.mapError(leaseStorageError));
    return { templates, leases: rows.map((row) => ({ ...toLease(row), clientRequestId: null })) };
  });

  const upsertTemplate = Effect.fn("EphemeralEnvironmentManager.upsertTemplate")(function* (
    input: EphemeralEnvironmentUpsertTemplateInput,
  ) {
    const template = yield* decodeTemplate(input.template).pipe(
      Effect.mapError(
        () => new EphemeralEnvironmentError({ message: "The ephemeral template is invalid." }),
      ),
    );
    const saved = yield* updateTemplates((templates) => {
      const index = templates.findIndex((entry) => entry.id === template.id);
      return index < 0 ? [...templates, template] : templates.with(index, template);
    }).pipe(Effect.mapError(settingsError));
    return (
      saved.ephemeralEnvironmentTemplates.find((entry) => entry.id === template.id) ??
      (yield* settingsError())
    );
  }, serialize);

  const deleteTemplate = Effect.fn("EphemeralEnvironmentManager.deleteTemplate")(function* ({
    templateId,
  }: EphemeralEnvironmentDeleteTemplateInput) {
    if ((yield* activeCount(templateId)) > 0) {
      return yield* new EphemeralEnvironmentError({
        message: "Release this template's active leases before deleting it.",
      });
    }
    if ((yield* getTemplates).some((template) => template.id === templateId)) {
      yield* updateTemplates((templates) =>
        templates.filter((template) => template.id !== templateId),
      ).pipe(Effect.mapError(settingsError));
    }
  }, serialize);

  const runTeardown = Effect.fn("EphemeralEnvironmentManager.runTeardown")(function* (
    row: LeaseRow,
  ) {
    if (row.teardown_pending && row.teardown_command) {
      yield* commands.teardown({
        command: row.teardown_command,
        timeoutSeconds: row.teardown_timeout_seconds,
        templateId: row.template_id,
        leaseId: row.lease_id,
        allocationHandle: row.allocation_handle,
      });
    }
  });

  const provisionOwnsAllocation = (reservation: LeaseRow, current: LeaseRow) =>
    sameVersion(reservation, current) ||
    (current.state === "draining" && current.generation === reservation.generation + 1);

  const failProvision = Effect.fn("EphemeralEnvironmentManager.failProvision")(function* (
    reservation: LeaseRow,
    handle: string | null,
    error: EphemeralEnvironmentError,
  ) {
    const current = yield* getRow(reservation.lease_id);
    if (!sameVersion(reservation, current)) return yield* cancelled();

    const cleanup = yield* Effect.result(runTeardown({ ...current, allocation_handle: handle }));
    const message = Result.isFailure(cleanup)
      ? `${error.message} ${cleanup.failure.message} Release the lease to retry cleanup.`
      : error.message;

    yield* serialize(
      Effect.gen(function* () {
        const row = yield* getRow(reservation.lease_id);
        if (!provisionOwnsAllocation(reservation, row)) return;
        const pending = Result.isFailure(cleanup) ? row.teardown_pending : 0;
        yield* updateRow(
          row,
          sameVersion(reservation, row)
            ? {
                state: "failed",
                last_error: message,
                allocation_handle: handle,
                teardown_pending: pending,
              }
            : { teardown_pending: pending },
        );
        yield* secrets.remove(secretName(row.lease_id)).pipe(Effect.ignore);
      }),
    );
    return yield* new EphemeralEnvironmentError({ message });
  });

  const provisionLease = Effect.fn("EphemeralEnvironmentManager.provisionLease")(function* (
    reservation: LeaseRow,
    template: EphemeralEnvironmentTemplate,
  ) {
    const result = yield* commands.provision({
      command: template.provisioner.provisionCommand,
      timeoutSeconds: template.provisioner.provisionTimeoutSeconds,
      templateId: template.id,
      leaseId: reservation.lease_id,
      repositoryKey: reservation.repository_key ?? undefined,
    });
    const handle = result.error ? result.allocationHandle : result.output.allocationHandle;

    return yield* Effect.gen(function* () {
      yield* serialize(
        Effect.gen(function* () {
          const row = yield* getRow(reservation.lease_id);
          // A draining lease waits for the provision worker to hand it the emitted handle.
          if (handle !== null && provisionOwnsAllocation(reservation, row)) {
            yield* updateRow(row, { allocation_handle: handle });
          }
          if (!sameVersion(reservation, row)) return yield* cancelled();
        }),
      );
      if (result.error) return yield* result.error;

      const output = result.output;
      const environmentId = yield* probe.describe(output.httpUrl);
      if (output.environmentId !== undefined && output.environmentId !== environmentId) {
        return yield* new EphemeralEnvironmentError({
          message: "The provisioned environment reported a different environment ID.",
        });
      }

      return yield* serialize(
        Effect.gen(function* () {
          const row = yield* getRow(reservation.lease_id);
          if (!sameVersion(reservation, row)) return yield* cancelled();
          const credentials = {
            bearerToken: output.bearerToken ?? null,
            pairingToken: output.pairingToken ?? null,
          };
          if (credentials.bearerToken !== null || credentials.pairingToken !== null) {
            yield* encodeCredentials(credentials).pipe(
              Effect.flatMap((encoded) =>
                secrets.set(secretName(row.lease_id), new TextEncoder().encode(encoded)),
              ),
              Effect.mapError(tokenStorageError),
            );
          }
          const ready = yield* updateRow(row, {
            state: "ready",
            environment_id: environmentId,
            http_url: output.httpUrl,
            ws_url: output.wsUrl,
            workspace_root: output.workspaceRoot ?? null,
            idle_deadline_at: isoTime(
              (yield* Clock.currentTimeMillis) + row.idle_timeout_minutes * 60_000,
            ),
          });
          return toLease(ready, credentials);
        }),
      );
    }).pipe(Effect.catch((error) => failProvision(reservation, handle, error)));
  });

  const reserve = Effect.fn("EphemeralEnvironmentManager.reserve")(function* (
    input: EphemeralEnvironmentAcquireInput,
    callerId: AuthSessionId,
  ) {
    const template = (yield* getTemplates).find((entry) => entry.id === input.templateId);
    if (template?.enabled !== true) {
      return yield* new EphemeralEnvironmentError({
        message: "The ephemeral template does not exist or is disabled.",
      });
    }
    if (
      template.eligibleRepositoryKeys.length > 0 &&
      !template.eligibleRepositoryKeys.some((key) => key === input.repositoryKey)
    ) {
      return yield* new EphemeralEnvironmentError({
        message: "This repository is not eligible for the ephemeral template.",
      });
    }

    if (input.clientRequestId !== undefined) {
      const [existing] = yield* sql<LeaseRow>`
        SELECT * FROM ephemeral_environment_leases
        WHERE template_id = ${template.id} AND client_request_id = ${input.clientRequestId}
          AND caller_id = ${callerId} AND repository_key IS ${input.repositoryKey ?? null}
          AND state IN ('provisioning', 'ready')
      `.pipe(Effect.mapError(leaseStorageError));
      if (existing) {
        if (existing.state === "ready") {
          const credentials = yield* readCredentials(existing.lease_id);
          return { lease: Effect.succeed(toLease(existing, credentials)), reused: true };
        }
        const job = provisions.get(existing.lease_id);
        return { lease: job ? Fiber.join(job) : Effect.succeed(toLease(existing)), reused: true };
      }
    }

    if ((yield* activeCount(template.id)) >= template.maxConcurrentLeases) {
      return yield* new EphemeralEnvironmentError({
        message: "The ephemeral template has reached its lease limit.",
      });
    }
    const leaseId = EphemeralEnvironmentLeaseId.make(
      yield* random.uuidV4().pipe(Effect.mapError(leaseStorageError)),
    );
    const createdAt = yield* now;
    const teardownCommand = template.provisioner.teardownCommand || null;
    const reservation: LeaseRow = {
      lease_id: leaseId,
      template_id: template.id,
      generation: 0,
      state: "provisioning",
      allocation_handle: null,
      environment_id: null,
      http_url: null,
      ws_url: null,
      workspace_root: null,
      client_request_id: input.clientRequestId ?? null,
      caller_id: callerId,
      repository_key: input.repositoryKey ?? null,
      last_error: null,
      created_at: createdAt,
      updated_at: createdAt,
      idle_deadline_at: null,
      idle_timeout_minutes: template.idleTimeoutMinutes,
      teardown_command: teardownCommand,
      teardown_timeout_seconds: template.provisioner.provisionTimeoutSeconds,
      teardown_pending: teardownCommand === null ? 0 : 1,
    };
    yield* sql`INSERT INTO ephemeral_environment_leases ${sql.insert({ ...reservation })}`.pipe(
      Effect.mapError(leaseStorageError),
    );
    const job = yield* startJob(provisions, leaseId, provisionLease(reservation, template));
    return { lease: Fiber.join(job), reused: false };
  }, serialize);

  const acquire = Effect.fn("EphemeralEnvironmentManager.acquire")(function* (
    input: EphemeralEnvironmentAcquireInput,
    callerId: AuthSessionId,
  ) {
    const { lease, reused } = yield* reserve(input, callerId);
    return { lease: yield* lease, reused };
  });

  const releaseLease = Effect.fn("EphemeralEnvironmentManager.releaseLease")(function* (
    draining: LeaseRow,
    provision: LeaseFiber | undefined,
    interruptedProvision: string | undefined,
  ) {
    if (provision) yield* Fiber.await(provision);
    const row = yield* getRow(draining.lease_id);
    if (!sameVersion(draining, row)) return yield* cancelled();
    const releasing = yield* updateRow(row, { state: "releasing" });
    const teardown = yield* Effect.result(runTeardown(releasing));
    if (Result.isFailure(teardown)) {
      const detail = `${teardown.failure.message} Release the lease to retry cleanup.`;
      const message = interruptedProvision ? `${interruptedProvision} ${detail}` : detail;
      yield* updateRow(releasing, { state: "failed", last_error: message, teardown_pending: 1 });
      return yield* new EphemeralEnvironmentError({ message });
    }

    const cleaned = yield* updateRow(releasing, { teardown_pending: 0 });
    const removed = yield* Effect.result(
      secrets.remove(secretName(row.lease_id)).pipe(Effect.mapError(tokenStorageError)),
    );
    if (Result.isFailure(removed)) {
      yield* updateRow(cleaned, { state: "failed", last_error: removed.failure.message });
      return yield* removed.failure;
    }

    const finished = yield* updateRow(cleaned, {
      state: interruptedProvision ? "failed" : "released",
      last_error: interruptedProvision ?? null,
    });
    return toLease(finished);
  });

  const beginRelease = Effect.fn("EphemeralEnvironmentManager.beginRelease")(function* (
    leaseId: EphemeralEnvironmentLeaseId,
    expected?: LeaseRow,
    interruptedProvision?: string,
  ) {
    const running = releases.get(leaseId);
    if (running) return Fiber.join(running);

    const row = yield* getRow(leaseId);
    if (
      row.state === "released" ||
      (expected &&
        (!sameVersion(row, expected) || row.idle_deadline_at !== expected.idle_deadline_at))
    ) {
      return Effect.succeed(toLease(row));
    }
    const draining = yield* updateRow(row, {
      generation: row.generation + 1,
      state: "draining",
      last_error: interruptedProvision ?? null,
      idle_deadline_at: null,
    });
    const job = yield* startJob(
      releases,
      leaseId,
      releaseLease(draining, provisions.get(leaseId), interruptedProvision),
    );
    return Fiber.join(job);
  }, serialize);

  const release = Effect.fn("EphemeralEnvironmentManager.release")(function* (
    input: EphemeralEnvironmentReleaseInput,
  ) {
    const completion = yield* beginRelease(input.leaseId);
    const lease = yield* completion;
    if (lease.state === "released") return { lease };
    // A successful startup cleanup preserves the interrupted provision failure until explicitly released.
    const releaseRecovered = yield* beginRelease(input.leaseId);
    return { lease: yield* releaseRecovered };
  });

  const noteCleanupFailure = (error: EphemeralEnvironmentError) =>
    Effect.logWarning("Ephemeral environment cleanup failed", error.message);

  const interruptedProvisionFor = (row: LeaseRow) =>
    row.state === "provisioning" || row.last_error?.startsWith(interruptedProvisionMessage)
      ? interruptedProvisionMessage
      : undefined;

  const reconcileLease = Effect.fn("EphemeralEnvironmentManager.reconcileLease")(function* (
    row: LeaseRow,
  ) {
    if (row.state === "ready") {
      const busy = yield* readCredentials(row.lease_id).pipe(
        Effect.flatMap((credentials) => probe.checkActiveWork(toLease(row), credentials)),
        Effect.orElseSucceed(() => true),
      );
      if (busy) {
        yield* serialize(
          Effect.gen(function* () {
            const current = yield* getRow(row.lease_id);
            if (!sameVersion(current, row) || current.idle_deadline_at !== row.idle_deadline_at)
              return;
            yield* updateRow(current, {
              idle_deadline_at: isoTime(
                (yield* Clock.currentTimeMillis) + current.idle_timeout_minutes * 60_000,
              ),
            });
          }),
        );
        return;
      }
    }
    const completion = yield* beginRelease(row.lease_id, row, interruptedProvisionFor(row));
    yield* completion;
  });

  const reconcileGate = yield* Semaphore.make(1);
  const reconcileIdleLeases = reconcileGate.withPermits(1)(
    Effect.gen(function* () {
      const deadline = yield* now;
      const due = yield* sql<LeaseRow>`
      SELECT * FROM ephemeral_environment_leases
      WHERE (state = 'ready' AND idle_deadline_at <= ${deadline})
        OR (state = 'failed' AND teardown_pending = 1)
    `.pipe(Effect.mapError(leaseStorageError));
      yield* Effect.forEach(
        due,
        (row) => reconcileLease(row).pipe(Effect.catch(noteCleanupFailure)),
        { concurrency: 4, discard: true },
      );
    }),
  );

  const stale = yield* sql<LeaseRow>`
    SELECT * FROM ephemeral_environment_leases WHERE state IN ('provisioning', 'draining', 'releasing')
  `.pipe(Effect.mapError(leaseStorageError));
  for (const row of stale) {
    const completion = yield* beginRelease(row.lease_id, row, interruptedProvisionFor(row));
    yield* completion.pipe(Effect.catch(noteCleanupFailure), Effect.forkScoped);
  }
  yield* reconcileIdleLeases.pipe(
    Effect.catch(noteCleanupFailure),
    Effect.repeat(Schedule.spaced("30 seconds")),
    Effect.forkScoped,
  );

  return EphemeralEnvironmentManager.of({
    list,
    upsertTemplate,
    deleteTemplate,
    acquire,
    release,
    reconcileIdleLeases,
  });
});

export const layer = Layer.effect(EphemeralEnvironmentManager, make);
