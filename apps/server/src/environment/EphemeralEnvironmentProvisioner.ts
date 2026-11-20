import {
  EnvironmentId,
  ExecutionEnvironmentDescriptor,
  EphemeralEnvironmentError,
  isLiveSubagentShell,
  ORCHESTRATION_PROTOCOL_HEADER,
  ORCHESTRATION_PROTOCOL_VERSION_TEXT,
  OrchestrationV2ThreadShellJson,
  TrimmedNonEmptyString,
  type EphemeralEnvironmentLease,
  type EphemeralEnvironmentLeaseId,
  type EphemeralEnvironmentTemplateId,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";

import * as ProcessRunner from "../processRunner.ts";

const AllocationHandle = Schema.Struct({ allocationHandle: TrimmedNonEmptyString });
const ProvisionOutput = Schema.Struct({
  ...AllocationHandle.fields,
  httpUrl: TrimmedNonEmptyString,
  wsUrl: TrimmedNonEmptyString,
  environmentId: Schema.optionalKey(EnvironmentId),
  pairingToken: Schema.optionalKey(TrimmedNonEmptyString),
  bearerToken: Schema.optionalKey(TrimmedNonEmptyString),
  workspaceRoot: Schema.optionalKey(TrimmedNonEmptyString),
});

const decodeHandle = Schema.decodeUnknownOption(AllocationHandle);
const decodeProvisionOutput = Schema.decodeUnknownResult(ProvisionOutput);
const MAX_OUTPUT_BYTES = 256 * 1024;

export interface EphemeralEnvironmentCredentials {
  readonly bearerToken: string | null;
  readonly pairingToken: string | null;
}

const ThreadActivity = OrchestrationV2ThreadShellJson.mapFields(
  ({ status, activeRunId, activityRunStatus, pendingRuntimeRequest, pendingBackgroundTasks }) => ({
    status,
    activeRunId,
    activityRunStatus,
    pendingRuntimeRequest,
    pendingBackgroundTasks,
  }),
);
const ShellActivity = Schema.Struct({
  threads: Schema.Array(ThreadActivity),
  archivedThreads: Schema.Array(ThreadActivity),
});

export class EphemeralEnvironmentProbe extends Context.Service<
  EphemeralEnvironmentProbe,
  {
    readonly describe: (httpUrl: string) => Effect.Effect<EnvironmentId, EphemeralEnvironmentError>;
    readonly checkActiveWork: (
      lease: EphemeralEnvironmentLease,
      credentials: EphemeralEnvironmentCredentials,
    ) => Effect.Effect<boolean, EphemeralEnvironmentError>;
  }
>()("t3/environment/EphemeralEnvironmentProvisioner/EphemeralEnvironmentProbe") {}

interface CommandInput {
  readonly command: string;
  readonly timeoutSeconds: number;
  readonly templateId: EphemeralEnvironmentTemplateId;
  readonly leaseId: EphemeralEnvironmentLeaseId;
}

type ProvisionResult =
  | { readonly output: typeof ProvisionOutput.Type; readonly error?: never }
  | {
      readonly output?: never;
      readonly allocationHandle: string | null;
      readonly error: EphemeralEnvironmentError;
    };

const parseCommandOutput = (stdout: string) => {
  let last: unknown;
  let allocationHandle: string | null = null;
  for (const match of stdout
    .trim()
    .matchAll(/^[\t ]*(\{[^\r\n]*\}|\{[^\r\n]*\r?\n[\s\S]*?^[^\r\n]*\})[\t ]*\r?$/gm)) {
    try {
      last = JSON.parse(match[1]!);
    } catch {
      continue;
    }
    const decoded = decodeHandle(last);
    if (decoded._tag === "Some") allocationHandle = decoded.value.allocationHandle;
  }
  return { last, allocationHandle };
};

const parseEndpoint = (address: string, protocols: readonly string[]) => {
  const url = URL.parse(address);
  return url !== null &&
    protocols.includes(url.protocol) &&
    !url.username &&
    !url.password &&
    !address.includes("?") &&
    !address.includes("#")
    ? url
    : null;
};

export const makeCommands = Effect.gen(function* () {
  const runner = yield* ProcessRunner.ProcessRunner;
  const platform = yield* HostProcess.Platform;

  const run = Effect.fn("EphemeralEnvironmentProvisioner.run")(function* (
    input: CommandInput,
    extraEnv: Readonly<Record<string, string>>,
    onStdoutChunk?: (chunk: Uint8Array) => void,
  ) {
    const shell =
      platform === "win32"
        ? {
            command: "powershell.exe",
            args: [
              "-NoProfile",
              "-NonInteractive",
              "-EncodedCommand",
              Buffer.from(input.command, "utf16le").toString("base64"),
            ],
          }
        : { command: "/bin/sh", args: ["-c", input.command] };
    return yield* runner.run({
      ...shell,
      env: {
        T3_EPHEMERAL_TEMPLATE_ID: input.templateId,
        T3_EPHEMERAL_LEASE_ID: input.leaseId,
        ...extraEnv,
      },
      timeout: Duration.seconds(input.timeoutSeconds),
      timeoutBehavior: "timedOutResult",
      maxOutputBytes: MAX_OUTPUT_BYTES,
      outputMode: "truncate",
      onStdoutChunk,
    });
  });

  const provision = Effect.fn("EphemeralEnvironmentProvisioner.provision")(function* (
    input: CommandInput & { readonly repositoryKey: string | undefined },
  ): Effect.fn.Return<ProvisionResult> {
    // ProcessRunner discards buffered output on timeout. Keep the tail to recover an emitted handle.
    const decoder = new TextDecoder();
    let tail = "";
    const result = yield* Effect.result(
      run(input, { T3_EPHEMERAL_REPOSITORY_KEY: input.repositoryKey ?? "" }, (chunk) => {
        tail = (tail + decoder.decode(chunk, { stream: true })).slice(-MAX_OUTPUT_BYTES);
      }),
    );
    const recent = parseCommandOutput(tail + decoder.decode());
    const buffered =
      Result.isSuccess(result) && (recent.last === undefined || recent.allocationHandle === null)
        ? parseCommandOutput(result.success.stdout)
        : undefined;
    const fail = (message: string): ProvisionResult => ({
      allocationHandle: recent.allocationHandle ?? buffered?.allocationHandle ?? null,
      error: new EphemeralEnvironmentError({ message }),
    });

    if (Result.isFailure(result)) return fail("The provision command could not run.");
    if (result.success.timedOut) return fail("The provision command timed out.");
    if (result.success.code !== 0) {
      return fail(`The provision command failed (exit code ${result.success.code ?? "unknown"}).`);
    }
    const output = decodeProvisionOutput(recent.last ?? buffered?.last);
    const invalidOutput = () => fail("The provision command did not return a valid environment.");
    if (Result.isFailure(output)) return invalidOutput();
    const http = parseEndpoint(output.success.httpUrl, ["http:", "https:"]);
    const ws = parseEndpoint(output.success.wsUrl, ["ws:", "wss:"]);
    if (
      !http ||
      !ws ||
      ws.host !== http.host ||
      (http.protocol === "https:" && ws.protocol !== "wss:")
    ) {
      return invalidOutput();
    }
    return { output: output.success };
  });

  const teardown = Effect.fn("EphemeralEnvironmentProvisioner.teardown")(function* (
    input: CommandInput & { readonly allocationHandle: string | null },
  ) {
    const result = yield* run(input, {
      T3_EPHEMERAL_ALLOCATION_HANDLE: input.allocationHandle ?? "",
    }).pipe(
      Effect.mapError(
        () => new EphemeralEnvironmentError({ message: "The teardown command could not run." }),
      ),
    );
    if (result.timedOut || result.code !== 0) {
      return yield* new EphemeralEnvironmentError({
        message: result.timedOut
          ? "The teardown command timed out."
          : `The teardown command failed (exit code ${result.code ?? "unknown"}).`,
      });
    }
  });

  return { provision, teardown };
});

const makeProbe = Effect.gen(function* () {
  const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk);
  const describe = Effect.fn("EphemeralEnvironmentProbe.describe")(
    function* (httpUrl: string) {
      const response = yield* client.get(new URL("/.well-known/t3/environment", httpUrl));
      const descriptor = yield* HttpClientResponse.schemaBodyJson(ExecutionEnvironmentDescriptor)(
        response,
      );
      return descriptor.environmentId;
    },
    (effect) =>
      effect.pipe(
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
        Effect.timeout("10 seconds"),
        Effect.mapError(
          () =>
            new EphemeralEnvironmentError({
              message: "The provisioned environment did not respond with a valid identity.",
            }),
        ),
      ),
  );
  const checkActiveWork = Effect.fn("EphemeralEnvironmentProbe.checkActiveWork")(
    function* (lease: EphemeralEnvironmentLease, credentials: EphemeralEnvironmentCredentials) {
      if (lease.httpUrl === null || credentials.bearerToken === null) return true;
      const request = HttpClientRequest.get(
        new URL("/api/orchestration/shell", lease.httpUrl),
      ).pipe(
        HttpClientRequest.setHeader(
          ORCHESTRATION_PROTOCOL_HEADER,
          ORCHESTRATION_PROTOCOL_VERSION_TEXT,
        ),
        HttpClientRequest.bearerToken(credentials.bearerToken),
      );
      const response = yield* client.execute(request);
      const { threads, archivedThreads } =
        yield* HttpClientResponse.schemaBodyJson(ShellActivity)(response);
      const isActive = (thread: typeof ThreadActivity.Type) =>
        thread.activeRunId !== null || isLiveSubagentShell(thread);
      return threads.some(isActive) || archivedThreads.some(isActive);
    },
    (effect) =>
      effect.pipe(
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
        Effect.timeout("10 seconds"),
        Effect.mapError(
          () =>
            new EphemeralEnvironmentError({
              message: "Could not check the ephemeral environment for active work.",
            }),
        ),
      ),
  );
  return EphemeralEnvironmentProbe.of({ describe, checkActiveWork });
});

export const layerProbe = Layer.effect(EphemeralEnvironmentProbe, makeProbe);
