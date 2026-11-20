import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  EnvironmentId,
  IsoDateTime,
  NonNegativeInt,
  TrimmedNonEmptyString,
  TrimmedString,
} from "./baseSchemas.ts";

export const EphemeralEnvironmentTemplateId = TrimmedNonEmptyString.pipe(
  Schema.brand("EphemeralEnvironmentTemplateId"),
);
export type EphemeralEnvironmentTemplateId = typeof EphemeralEnvironmentTemplateId.Type;

export const EphemeralEnvironmentLeaseId = TrimmedNonEmptyString.pipe(
  Schema.brand("EphemeralEnvironmentLeaseId"),
);
export type EphemeralEnvironmentLeaseId = typeof EphemeralEnvironmentLeaseId.Type;

export const EphemeralEnvironmentColabHardwareSchema = Schema.Literals([
  "cpu-high-mem",
  "cpu",
  "gpu-t4",
  "gpu-l4",
  "gpu-g4",
  "gpu-a100",
  "gpu-h100",
  "tpu-v5e1",
  "tpu-v6e1",
]);
export type EphemeralEnvironmentColabHardware = typeof EphemeralEnvironmentColabHardwareSchema.Type;

export const EphemeralEnvironmentProvisionerColabSchema = Schema.Struct({
  kind: Schema.Literal("colab"),
  hardware: EphemeralEnvironmentColabHardwareSchema.pipe(
    Schema.withDecodingDefault(Effect.succeed("cpu-high-mem" as const)),
  ),
  syncLocalCredentials: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
  provisionTimeoutSeconds: Schema.Int.check(Schema.isBetween({ minimum: 30, maximum: 900 })).pipe(
    Schema.withDecodingDefault(Effect.succeed(240)),
  ),
});
export type EphemeralEnvironmentProvisionerColab =
  typeof EphemeralEnvironmentProvisionerColabSchema.Type;

export const EphemeralEnvironmentProvisionerCommandSchema = Schema.Struct({
  kind: Schema.Literal("command"),
  /** Runs on the controller host. Stdout is JSON with httpUrl, wsUrl, allocationHandle,
   * and optional environmentId, pairingToken, bearerToken, and workspaceRoot. */
  provisionCommand: TrimmedNonEmptyString,
  /** Runs with T3_EPHEMERAL_ALLOCATION_HANDLE and T3_EPHEMERAL_LEASE_ID. */
  teardownCommand: Schema.optionalKey(Schema.NullOr(TrimmedString)),
  provisionTimeoutSeconds: Schema.Int.check(Schema.isBetween({ minimum: 10, maximum: 900 })).pipe(
    Schema.withDecodingDefault(Effect.succeed(180)),
  ),
});
export type EphemeralEnvironmentProvisionerCommand =
  typeof EphemeralEnvironmentProvisionerCommandSchema.Type;

export const EphemeralEnvironmentProvisionerSchema = Schema.Union([
  EphemeralEnvironmentProvisionerColabSchema,
  EphemeralEnvironmentProvisionerCommandSchema,
]);
export type EphemeralEnvironmentProvisioner = typeof EphemeralEnvironmentProvisionerSchema.Type;

export const EphemeralEnvironmentTemplateSchema = Schema.Struct({
  id: EphemeralEnvironmentTemplateId,
  label: TrimmedNonEmptyString.check(Schema.isMaxLength(80)),
  enabled: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
  /** Matches LoadBalancingWeights. Zero is manual only. */
  weight: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 100 })).pipe(
    Schema.withDecodingDefault(Effect.succeed(50)),
  ),
  provisioner: EphemeralEnvironmentProvisionerSchema,
  idleTimeoutMinutes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1440 })).pipe(
    Schema.withDecodingDefault(Effect.succeed(15)),
  ),
  maxConcurrentLeases: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 16 })).pipe(
    Schema.withDecodingDefault(Effect.succeed(1)),
  ),
  /** An empty list allows any repository. */
  eligibleRepositoryKeys: Schema.Array(TrimmedNonEmptyString).pipe(
    Schema.withDecodingDefault(Effect.succeed([])),
  ),
});
export type EphemeralEnvironmentTemplate = typeof EphemeralEnvironmentTemplateSchema.Type;

export const EphemeralEnvironmentLeaseState = Schema.Literals([
  "provisioning",
  "ready",
  "draining",
  "suspended",
  "releasing",
  "released",
  "failed",
]);
export type EphemeralEnvironmentLeaseState = typeof EphemeralEnvironmentLeaseState.Type;

export const EphemeralEnvironmentLeaseSchema = Schema.Struct({
  leaseId: EphemeralEnvironmentLeaseId,
  templateId: EphemeralEnvironmentTemplateId,
  generation: NonNegativeInt,
  state: EphemeralEnvironmentLeaseState,
  allocationHandle: Schema.NullOr(Schema.String),
  environmentId: Schema.NullOr(EnvironmentId),
  httpUrl: Schema.NullOr(Schema.String),
  wsUrl: Schema.NullOr(Schema.String),
  bearerToken: Schema.NullOr(Schema.String),
  pairingToken: Schema.NullOr(Schema.String),
  workspaceRoot: Schema.NullOr(Schema.String),
  clientRequestId: Schema.NullOr(Schema.String),
  lastError: Schema.NullOr(Schema.String),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  idleDeadlineAt: Schema.NullOr(IsoDateTime),
});
export type EphemeralEnvironmentLease = typeof EphemeralEnvironmentLeaseSchema.Type;

export const EphemeralEnvironmentListResult = Schema.Struct({
  templates: Schema.Array(EphemeralEnvironmentTemplateSchema),
  leases: Schema.Array(EphemeralEnvironmentLeaseSchema),
});
export type EphemeralEnvironmentListResult = typeof EphemeralEnvironmentListResult.Type;

export const EphemeralEnvironmentUpsertTemplateInput = Schema.Struct({
  template: EphemeralEnvironmentTemplateSchema,
});
export type EphemeralEnvironmentUpsertTemplateInput =
  typeof EphemeralEnvironmentUpsertTemplateInput.Type;

export const EphemeralEnvironmentDeleteTemplateInput = Schema.Struct({
  templateId: EphemeralEnvironmentTemplateId,
});
export type EphemeralEnvironmentDeleteTemplateInput =
  typeof EphemeralEnvironmentDeleteTemplateInput.Type;

export const EphemeralEnvironmentAcquireInput = Schema.Struct({
  templateId: EphemeralEnvironmentTemplateId,
  clientRequestId: Schema.optionalKey(Schema.String),
  repositoryKey: Schema.optionalKey(Schema.String),
});
export type EphemeralEnvironmentAcquireInput = typeof EphemeralEnvironmentAcquireInput.Type;

export const EphemeralEnvironmentAcquireResult = Schema.Struct({
  lease: EphemeralEnvironmentLeaseSchema,
  reused: Schema.Boolean,
});
export type EphemeralEnvironmentAcquireResult = typeof EphemeralEnvironmentAcquireResult.Type;

export const EphemeralEnvironmentReleaseInput = Schema.Struct({
  leaseId: EphemeralEnvironmentLeaseId,
});
export type EphemeralEnvironmentReleaseInput = typeof EphemeralEnvironmentReleaseInput.Type;

export const EphemeralEnvironmentReleaseResult = Schema.Struct({
  lease: EphemeralEnvironmentLeaseSchema,
});
export type EphemeralEnvironmentReleaseResult = typeof EphemeralEnvironmentReleaseResult.Type;

export class EphemeralEnvironmentError extends Schema.TaggedError<EphemeralEnvironmentError>()(
  "EphemeralEnvironmentError",
  { message: Schema.String },
) {}
