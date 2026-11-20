import { type ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Config from "effect/Config";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";

import { makeTransport } from "./ColabComputeTransport.ts";

const ColabHardware = Schema.Literals([
  "cpu",
  "cpu-high-mem",
  "gpu-t4",
  "gpu-l4",
  "gpu-g4",
  "gpu-a100",
  "gpu-h100",
  "tpu-v5e1",
  "tpu-v6e1",
]);
type Hardware = typeof ColabHardware.Type;

const profiles: Record<
  Hardware,
  { readonly flags: ReadonlyArray<string>; readonly maxJobs: number }
> = {
  cpu: { flags: [], maxJobs: 2 },
  "cpu-high-mem": { flags: ["--high-mem"], maxJobs: 4 },
  "gpu-t4": { flags: ["--gpu", "T4", "--high-mem"], maxJobs: 2 },
  "gpu-l4": { flags: ["--gpu", "L4"], maxJobs: 2 },
  "gpu-g4": { flags: ["--gpu", "G4", "--high-mem"], maxJobs: 2 },
  "gpu-a100": { flags: ["--gpu", "A100", "--high-mem"], maxJobs: 2 },
  "gpu-h100": { flags: ["--gpu", "H100", "--high-mem"], maxJobs: 2 },
  "tpu-v5e1": { flags: ["--tpu", "v5e1"], maxJobs: 1 },
  "tpu-v6e1": { flags: ["--tpu", "v6e1"], maxJobs: 1 },
};

export const ColabJobOptions = Schema.Struct({
  command: Schema.String,
  hardware: Schema.optional(ColabHardware),
  syncWorkspace: Schema.optional(Schema.Boolean),
  cwd: Schema.optional(Schema.String),
  env: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  artifactPaths: Schema.optional(Schema.Array(Schema.String)),
  mode: Schema.optional(Schema.Literals(["wait", "async"])),
  timeoutSeconds: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1800 })),
  ),
});

const JobState = Schema.Literals([
  "queued",
  "running",
  "completed",
  "failed",
  "cancelled",
  "timed_out",
]);
const VmState = Schema.Literals(["provisioning", "ready", "draining", "stopping"]);

export const ColabJobStatus = Schema.Struct({
  jobId: Schema.String,
  hardware: ColabHardware,
  status: JobState,
  sessionName: Schema.optional(Schema.String),
  exitCode: Schema.optional(Schema.Int),
  stdout: Schema.optional(Schema.String),
  stderr: Schema.optional(Schema.String),
  queuedMs: Schema.Number,
  durationMs: Schema.Number,
  artifacts: Schema.Array(Schema.String),
  error: Schema.optional(Schema.String),
});
type JobStatus = typeof ColabJobStatus.Type;

export const ColabPoolStatus = Schema.Struct({
  vms: Schema.Array(
    Schema.Struct({
      sessionName: Schema.String,
      hardware: ColabHardware,
      state: VmState,
      activeJobs: Schema.Number,
      maxJobs: Schema.Number,
      queuedJobs: Schema.Number,
    }),
  ),
  maxVms: Schema.Number,
  queuedJobs: Schema.Number,
  computeUnitBalance: Schema.NullOr(Schema.Number),
});
type PoolStatus = typeof ColabPoolStatus.Type;

export class ColabPoolError extends Schema.TaggedError<
  ColabPoolError,
  { readonly brand: unique symbol }
>()("ColabPoolError", {
  message: Schema.String,
}) {}

type JobInput = typeof ColabJobOptions.Type & {
  readonly threadId: ThreadId;
  readonly workspace: string;
};

export class ColabComputePool extends Context.Service<
  ColabComputePool,
  {
    readonly isAvailable: () => Effect.Effect<boolean>;
    readonly runJob: (input: JobInput) => Effect.Effect<JobStatus, ColabPoolError>;
    readonly jobStatus: (
      threadId: ThreadId,
      jobId: string,
    ) => Effect.Effect<JobStatus, ColabPoolError>;
    readonly cancelJob: (
      threadId: ThreadId,
      jobId: string,
    ) => Effect.Effect<JobStatus, ColabPoolError>;
    readonly poolStatus: () => Effect.Effect<PoolStatus>;
    readonly stop: (input: {
      readonly threadId?: ThreadId | undefined;
      readonly sessionName?: string | undefined;
      readonly force?: boolean | undefined;
    }) => Effect.Effect<ReadonlyArray<string>, ColabPoolError>;
  }
>()("t3/orchestration-v2/ColabComputePool") {}

export interface ColabTransport {
  readonly isAvailable: Effect.Effect<boolean>;
  readonly start: (
    sessionName: string,
    flags: ReadonlyArray<string>,
  ) => Effect.Effect<void, ColabPoolError>;
  readonly probe: (
    sessionName: string,
  ) => Effect.Effect<{ readonly cpu: number; readonly memoryAvailable: number }, ColabPoolError>;
  readonly run: (
    sessionName: string,
    jobId: string,
    input: JobInput,
  ) => Effect.Effect<
    {
      readonly exitCode: number;
      readonly stdout: string;
      readonly stderr: string;
      readonly artifacts: ReadonlyArray<string>;
      readonly error?: string;
    },
    ColabPoolError
  >;
  readonly clean: (sessionName: string, jobId: string) => Effect.Effect<void, ColabPoolError>;
  readonly stop: (sessionName: string) => Effect.Effect<void, ColabPoolError>;
  readonly reconcileOrphans: (ownsSession: (sessionName: string) => boolean) => Effect.Effect<void>;
  readonly balance: Effect.Effect<number | null>;
}

interface Vm {
  readonly sessionName: string;
  readonly hardware: Hardware;
  readonly maxJobs: number;
  readonly createdAt: number;
  readonly activeJobIds: Set<string>;
  readonly booted: Deferred.Deferred<void>;
  state: typeof VmState.Type;
  lastIdleAt: number;
  stopping?: Deferred.Deferred<void, ColabPoolError> | undefined;
}

interface Job {
  readonly input: JobInput;
  readonly hardware: Hardware;
  readonly jobId: string;
  readonly queuedAt: number;
  readonly assigned: Deferred.Deferred<Vm, ColabPoolError>;
  readonly done: Deferred.Deferred<JobStatus>;
  status: typeof JobState.Type;
  vm?: Vm;
  startedAt?: number;
  result?: JobStatus;
  fiber?: Fiber.Fiber<void> | undefined;
}

const safeRelative = (path: string) =>
  path.length > 0 &&
  !path.startsWith("/") &&
  !/^[A-Za-z]:/u.test(path) &&
  !path.includes("\\") &&
  !path.includes("\0") &&
  path.split("/").every((part) => part !== ".." && part.toLowerCase() !== ".git");

const safeEnvKey = (key: string) =>
  /^[A-Za-z_][A-Za-z0-9_]*$/u.test(key) &&
  !/^(?:LD_|DYLD_|BASH_FUNC_)/u.test(key) &&
  !["BASH_ENV", "ENV", "HOME", "USER", "LOGNAME", "PWD", "SHELL"].includes(key);

const validate = (input: JobInput): string | undefined => {
  if (!input.command.trim() || input.command.includes("\0")) return "Provide a nonempty command.";
  if (input.cwd !== undefined && !safeRelative(input.cwd))
    return "cwd must be inside the job directory.";
  if (
    input.artifactPaths?.some(
      (path) => !safeRelative(path) || path.split("/").every((part) => part === "." || !part),
    )
  )
    return "Artifact paths must be relative to and inside the job directory.";
  if (
    Object.entries(input.env ?? {}).some(([key, value]) => !safeEnvKey(key) || value.includes("\0"))
  )
    return "env must contain valid environment variable names and values.";
  const seconds = input.timeoutSeconds ?? 300;
  if (!Number.isInteger(seconds) || seconds < 1 || seconds > 1800)
    return "timeoutSeconds must be between 1 and 1800.";
  return undefined;
};

const MAX_RETAINED_RESULTS = 128;
const MAX_LOG_BYTES = 64 * 1024;

const tail = (value: string) =>
  Buffer.byteLength(value) <= MAX_LOG_BYTES
    ? value
    : Buffer.from(value).subarray(-MAX_LOG_BYTES).toString("utf8");

const snapshot = (job: Job, now: number): JobStatus =>
  job.result ?? {
    jobId: job.jobId,
    hardware: job.hardware,
    status: job.status,
    ...(job.vm === undefined ? {} : { sessionName: job.vm.sessionName }),
    queuedMs: (job.startedAt ?? now) - job.queuedAt,
    durationMs: job.startedAt === undefined ? 0 : now - job.startedAt,
    artifacts: [],
  };

const positiveInt = (name: string, fallback: number) =>
  Config.Int(name).pipe(
    Effect.filterOrElse(
      (value) => Number.isSafeInteger(value) && value > 0,
      () => Effect.succeed(fallback),
    ),
    Effect.orElseSucceed(() => fallback),
  );

interface Options {
  readonly transport: ColabTransport;
  readonly newId: Effect.Effect<string>;
  readonly maxVms?: number;
  readonly idleTimeoutSeconds?: number;
  readonly maxCpuJobs?: number;
  readonly maxHighMemJobs?: number;
}

export const make = Effect.fn("ColabComputePool.make")(function* ({
  transport,
  newId,
  maxVms = 2,
  idleTimeoutSeconds = 600,
  maxCpuJobs = profiles.cpu.maxJobs,
  maxHighMemJobs = profiles["cpu-high-mem"].maxJobs,
}: Options) {
  const work = yield* Scope.make("parallel");
  const signal = yield* Queue.sliding<void>(1);
  const wake = Queue.offer(signal, undefined);
  const vms = new Map<string, Vm>();
  const jobs = new Map<string, Job>();
  const waiting = new Set<Job>();
  const finished = new Set<string>();
  let closing = false;

  const startStop = Effect.fn("ColabComputePool.startStop")(function* (vm: Vm) {
    if (vm.stopping !== undefined) return vm.stopping;
    const stopped = Deferred.makeUnsafe<void, ColabPoolError>();
    vm.stopping = stopped;
    vm.state = "stopping";
    yield* Effect.gen(function* () {
      yield* Deferred.await(vm.booted);
      const result = yield* Effect.exit(transport.stop(vm.sessionName));
      if (Exit.isSuccess(result)) vms.delete(vm.sessionName);
      else {
        vm.stopping = undefined;
        vm.state = "draining";
      }
      yield* Deferred.done(stopped, result);
      yield* wake;
    }).pipe(Effect.forkIn(work, { uninterruptible: true }));
    return stopped;
  }, Effect.uninterruptible);

  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      closing = true;
      yield* Scope.close(work, Exit.void);
      yield* Effect.forEach(
        vms.values(),
        (vm) => transport.stop(vm.sessionName).pipe(Effect.ignore({ log: true })),
        { concurrency: "unbounded", discard: true },
      );
    }),
  );

  const run = Effect.fn("ColabComputePool.run")(function* (job: Job) {
    yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const exit = yield* restore(
          Deferred.await(job.assigned).pipe(
            Effect.flatMap((vm) => transport.run(vm.sessionName, job.jobId, job.input)),
            Effect.timeoutOption(`${job.input.timeoutSeconds ?? 300} seconds`),
          ),
        ).pipe(Effect.exit);
        waiting.delete(job);
        const vm = job.vm;
        if (vm !== undefined) {
          const cleaned = yield* Effect.exit(transport.clean(vm.sessionName, job.jobId));
          vm.activeJobIds.delete(job.jobId);
          vm.lastIdleAt = yield* Clock.currentTimeMillis;
          if (Exit.isFailure(cleaned) && vm.state === "ready") vm.state = "draining";
          if (vm.state === "draining" && vm.activeJobIds.size === 0 && !closing)
            yield* startStop(vm);
        }
        const now = yield* Clock.currentTimeMillis;
        const base = snapshot(job, now);
        if (Exit.isFailure(exit)) {
          job.result = Exit.hasInterrupts(exit)
            ? { ...base, status: "cancelled" }
            : {
                ...base,
                status: "failed",
                error: Exit.findErrorOption(exit).pipe(
                  Option.map((error) => error.message),
                  Option.getOrElse(() => "Colab job failed."),
                ),
              };
        } else if (Option.isNone(exit.value)) {
          job.result = { ...base, status: "timed_out" };
        } else {
          const { stdout, stderr, ...result } = exit.value.value;
          job.result = {
            ...base,
            ...result,
            stdout: tail(stdout),
            stderr: tail(stderr),
            status: result.exitCode === 0 && result.error === undefined ? "completed" : "failed",
          };
        }
        job.status = job.result.status;
        job.fiber = undefined;
        finished.add(job.jobId);
        if (finished.size > MAX_RETAINED_RESULTS) {
          const oldest = finished.values().next().value;
          if (oldest !== undefined) {
            finished.delete(oldest);
            jobs.delete(oldest);
          }
        }
        yield* Deferred.succeed(job.done, job.result);
        if (!closing) yield* wake;
      }),
    );
  });

  const provision = Effect.fn("ColabComputePool.provision")(function* (hardware: Hardware) {
    const now = yield* Clock.currentTimeMillis;
    if (closing || vms.size >= maxVms) return;
    const id = yield* newId;
    const vm: Vm = {
      sessionName: `t3-${id.slice(0, 12)}`,
      hardware,
      state: "provisioning",
      activeJobIds: new Set(),
      booted: Deferred.makeUnsafe<void>(),
      maxJobs:
        hardware === "cpu"
          ? maxCpuJobs
          : hardware === "cpu-high-mem"
            ? maxHighMemJobs
            : profiles[hardware].maxJobs,
      createdAt: now,
      lastIdleAt: now,
    };
    vms.set(vm.sessionName, vm);
    yield* Effect.gen(function* () {
      const started = yield* Effect.exit(transport.start(vm.sessionName, profiles[hardware].flags));
      if (Exit.isSuccess(started)) {
        vm.lastIdleAt = yield* Clock.currentTimeMillis;
        if (vm.state === "provisioning") vm.state = "ready";
      } else if (vm.state === "provisioning") {
        vm.state = "draining";
        const next = Array.from(waiting).find((job) => job.hardware === hardware);
        if (next !== undefined) {
          waiting.delete(next);
          yield* Deferred.fail(
            next.assigned,
            Exit.findErrorOption(started).pipe(
              Option.getOrElse(
                () => new ColabPoolError({ message: "Could not provision a Colab VM." }),
              ),
            ),
          );
        }
      }
      yield* Deferred.succeed(vm.booted, undefined);
      if (Exit.isFailure(started) && !closing) yield* startStop(vm);
      if (!closing) yield* wake;
    }).pipe(Effect.forkIn(work, { uninterruptible: true }));
    return vm;
  }, Effect.uninterruptible);

  const chooseVm = Effect.fn("ColabComputePool.chooseVm")(function* (hardware: Hardware) {
    const candidates = Array.from(vms.values()).filter(
      (vm) => vm.hardware === hardware && vm.state === "ready" && vm.activeJobIds.size < vm.maxJobs,
    );
    const idle = candidates.find((vm) => vm.activeJobIds.size === 0);
    if (idle !== undefined) return idle;
    const checked = yield* Effect.forEach(
      candidates,
      (vm) =>
        transport.probe(vm.sessionName).pipe(
          Effect.map((load) => ({ vm, ...load })),
          Effect.option,
        ),
      { concurrency: "unbounded" },
    );
    return checked
      .flatMap(Option.toArray)
      .filter(({ cpu, memoryAvailable }) => cpu < 0.9 && memoryAvailable > 0.1)
      .sort(
        (a, b) =>
          a.cpu - b.cpu ||
          b.memoryAvailable - a.memoryAvailable ||
          a.vm.activeJobIds.size - b.vm.activeJobIds.size,
      )[0]?.vm;
  });

  const dispatch = Effect.fn("ColabComputePool.dispatch")(function* () {
    const blocked = new Set<Hardware>();
    for (const job of Array.from(waiting)) {
      if (closing) return;
      if (!waiting.has(job) || blocked.has(job.hardware)) continue;
      const vm = yield* chooseVm(job.hardware);
      const now = yield* Clock.currentTimeMillis;
      if (!waiting.has(job) || closing) continue;
      if (vm !== undefined && vm.state === "ready" && vm.activeJobIds.size < vm.maxJobs) {
        yield* Effect.uninterruptible(
          Effect.gen(function* () {
            waiting.delete(job);
            job.vm = vm;
            job.startedAt = now;
            job.status = "running";
            vm.activeJobIds.add(job.jobId);
            yield* Deferred.succeed(job.assigned, vm);
          }),
        );
      } else {
        blocked.add(job.hardware);
      }
    }

    const queued = new Map<Hardware, number>();
    for (const job of waiting) queued.set(job.hardware, (queued.get(job.hardware) ?? 0) + 1);
    for (const [hardware, demand] of queued) {
      if (!blocked.has(hardware)) continue;
      // Waiting jobs reserve committed slots even while a VM boots or is under load.
      let capacity = 0;
      for (const vm of vms.values()) {
        if (vm.hardware === hardware && (vm.state === "provisioning" || vm.state === "ready")) {
          capacity += vm.maxJobs - vm.activeJobIds.size;
        }
      }
      while (capacity < demand && vms.size < maxVms) {
        const vm = yield* provision(hardware);
        if (vm === undefined) return;
        capacity += vm.maxJobs;
      }
      if (capacity >= demand || Array.from(vms.values()).some((vm) => vm.state === "stopping"))
        continue;
      const idle = Array.from(vms.values()).find(
        (vm) =>
          vm.hardware !== hardware &&
          vm.state === "ready" &&
          vm.activeJobIds.size === 0 &&
          !Array.from(waiting).some((job) => job.hardware === vm.hardware),
      );
      if (idle !== undefined) yield* startStop(idle);
    }
  });

  const reap = Effect.fn("ColabComputePool.reap")(function* () {
    const now = yield* Clock.currentTimeMillis;
    for (const vm of vms.values()) {
      if (vm.activeJobIds.size !== 0) continue;
      if (
        vm.state === "draining" ||
        (vm.state === "ready" &&
          now >= vm.lastIdleAt + idleTimeoutSeconds * 1000 &&
          !Array.from(waiting).some((job) => job.hardware === vm.hardware))
      ) {
        yield* startStop(vm);
      }
    }
    if (waiting.size > 0) yield* wake;
  });

  yield* Effect.forever(Queue.take(signal).pipe(Effect.andThen(dispatch()))).pipe(
    Effect.forkIn(work),
  );
  yield* Effect.forever(
    Effect.sleep(`${Math.min(idleTimeoutSeconds, 5)} seconds`).pipe(Effect.andThen(reap())),
  ).pipe(Effect.forkIn(work));
  yield* transport.reconcileOrphans((session) => vms.has(session)).pipe(Effect.forkIn(work));

  const submit = Effect.fn("ColabComputePool.submit")(function* (input: JobInput) {
    const assigned = yield* Deferred.make<Vm, ColabPoolError>();
    const done = yield* Deferred.make<JobStatus>();
    const queuedAt = yield* Clock.currentTimeMillis;
    if (closing) return yield* new ColabPoolError({ message: "The Colab pool is shutting down." });
    const job: Job = {
      input,
      jobId: yield* newId,
      hardware: input.hardware ?? "cpu",
      status: "queued",
      queuedAt,
      assigned,
      done,
    };
    jobs.set(job.jobId, job);
    waiting.add(job);
    job.fiber = yield* run(job).pipe(
      Effect.forkIn(work, { startImmediately: true, uninterruptible: false }),
    );
    yield* wake;
    return job;
  }, Effect.uninterruptible);

  const cancel = (job: Job) => (job.fiber === undefined ? Effect.void : Fiber.interrupt(job.fiber));

  const findJob = (threadId: ThreadId, jobId: string) => {
    const job = jobs.get(jobId);
    return job?.input.threadId === threadId
      ? Effect.succeed(job)
      : Effect.fail(new ColabPoolError({ message: "Colab job not found for this thread." }));
  };

  return ColabComputePool.of({
    isAvailable: Effect.fn("ColabComputePool.isAvailable")(function* () {
      return yield* transport.isAvailable;
    }),
    runJob: Effect.fn("ColabComputePool.runJob")(function* (input) {
      const problem = validate(input);
      if (problem !== undefined) return yield* new ColabPoolError({ message: problem });
      if (!(yield* transport.isAvailable))
        return yield* new ColabPoolError({ message: "The Colab CLI is unavailable." });
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const job = yield* submit(input);
          return input.mode === "async"
            ? snapshot(job, yield* Clock.currentTimeMillis)
            : yield* restore(Deferred.await(job.done)).pipe(Effect.onInterrupt(() => cancel(job)));
        }),
      );
    }),
    jobStatus: Effect.fn("ColabComputePool.jobStatus")(function* (threadId, jobId) {
      return snapshot(yield* findJob(threadId, jobId), yield* Clock.currentTimeMillis);
    }),
    cancelJob: Effect.fn("ColabComputePool.cancelJob")(function* (threadId, jobId) {
      const job = yield* findJob(threadId, jobId);
      yield* cancel(job);
      return yield* Deferred.await(job.done);
    }),
    poolStatus: Effect.fn("ColabComputePool.poolStatus")(function* () {
      const computeUnitBalance = yield* transport.balance;
      return {
        vms: Array.from(vms.values(), (vm) => ({
          sessionName: vm.sessionName,
          hardware: vm.hardware,
          state: vm.state,
          activeJobs: vm.activeJobIds.size,
          maxJobs: vm.maxJobs,
          queuedJobs: Array.from(waiting).filter((job) => job.hardware === vm.hardware).length,
        })),
        maxVms,
        queuedJobs: waiting.size,
        computeUnitBalance,
      };
    }),
    stop: Effect.fn("ColabComputePool.stop")(function* ({ threadId, sessionName, force }) {
      const selected = Array.from(vms.values()).filter(
        (vm) =>
          (sessionName === undefined || vm.sessionName === sessionName) &&
          (force === true || (vm.state !== "provisioning" && vm.activeJobIds.size === 0)),
      );
      if (
        threadId !== undefined &&
        selected.some((vm) =>
          Array.from(vm.activeJobIds).some((id) => jobs.get(id)?.input.threadId !== threadId),
        )
      )
        return yield* new ColabPoolError({
          message: "A selected Colab VM has jobs from another thread.",
        });
      for (const vm of selected) if (vm.state !== "stopping") vm.state = "draining";
      yield* Effect.forEach(
        selected,
        (vm) =>
          Effect.forEach(
            Array.from(vm.activeJobIds),
            (id) => {
              const job = jobs.get(id);
              return job === undefined ? Effect.void : cancel(job);
            },
            { concurrency: "unbounded", discard: true },
          ).pipe(Effect.andThen(startStop(vm)), Effect.flatMap(Deferred.await)),
        { concurrency: "unbounded", discard: true },
      );
      return selected.map((vm) => vm.sessionName);
    }, Effect.uninterruptible),
  });
});

export const layer = Layer.effect(
  ColabComputePool,
  Effect.gen(function* () {
    return yield* make({
      transport: yield* makeTransport,
      newId: (yield* SecureRandom.SecureRandom).uuidV4(),
      maxVms: yield* positiveInt("T3CODE_COLAB_MAX_VMS", 2),
      idleTimeoutSeconds: yield* positiveInt("T3CODE_COLAB_IDLE_TIMEOUT_SECONDS", 600),
      maxCpuJobs: yield* positiveInt("T3CODE_COLAB_MAX_JOBS_PER_CPU_VM", profiles.cpu.maxJobs),
      maxHighMemJobs: yield* positiveInt(
        "T3CODE_COLAB_MAX_JOBS_PER_HIGH_MEM_VM",
        profiles["cpu-high-mem"].maxJobs,
      ),
    });
  }),
);
