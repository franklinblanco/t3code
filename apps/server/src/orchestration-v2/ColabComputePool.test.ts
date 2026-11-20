import { expect, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";
import * as Redacted from "effect/Redacted";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { TestClock } from "effect/testing";

import { ColabPoolError, make, type ColabTransport } from "./ColabComputePool.ts";
import { supervisor } from "./ColabComputeRemote.ts";
import { makeTransport } from "./ColabComputeTransport.ts";

const thread = ThreadId.make("first");
const other = ThreadId.make("second");
const job = {
  threadId: thread,
  workspace: "/workspace",
  command: "npm test",
  mode: "async" as const,
};
const successful = { exitCode: 0, stdout: "passed", stderr: "", artifacts: [] };

const fakeTransport = (holdBoot = false) =>
  Effect.gen(function* () {
    const started = yield* Queue.unbounded<{ session: string; jobId: string }>();
    const booting = yield* Queue.unbounded<string>();
    const stopped = yield* Queue.unbounded<string>();
    const cleaned = yield* Queue.unbounded<string>();
    const boots: Array<{ session: string; flags: ReadonlyArray<string> }> = [];
    const gates = new Map<string, Deferred.Deferred<void>>();
    const load = new Map<string, { cpu: number; memoryAvailable: number }>();
    const bootGates = new Map<string, Deferred.Deferred<void>>();
    const transport: ColabTransport = {
      isAvailable: Effect.succeed(true),
      start: (session, flags) =>
        Effect.gen(function* () {
          boots.push({ session, flags });
          const gate = holdBoot ? yield* Deferred.make<void>() : undefined;
          if (gate !== undefined) bootGates.set(session, gate);
          yield* Queue.offer(booting, session);
          if (gate !== undefined) yield* Deferred.await(gate);
        }),
      probe: (session) =>
        Effect.sync(() => load.get(session) ?? { cpu: 0.4, memoryAvailable: 0.5 }),
      run: (session, jobId) =>
        Effect.gen(function* () {
          const gate = yield* Deferred.make<void>();
          gates.set(jobId, gate);
          yield* Queue.offer(started, { session, jobId });
          yield* Deferred.await(gate);
          return successful;
        }),
      clean: (_session, jobId) => Queue.offer(cleaned, jobId).pipe(Effect.asVoid),
      stop: (session) => Queue.offer(stopped, session).pipe(Effect.asVoid),
      reconcileOrphans: () => Effect.void,
      balance: Effect.succeed(72.5),
    };
    let id = 0;
    return {
      transport,
      newId: Effect.sync(() => String(++id)),
      boots,
      booting,
      gates,
      started,
      stopped,
      cleaned,
      load,
      bootGates,
    };
  });

const setup = fakeTransport();

it.effect("coalesces cold CPU jobs and multiplexes two agents on the same VM", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* fakeTransport(true);
      const pool = yield* make({ ...fake });
      const first = yield* pool.runJob(job);
      const firstVm = yield* Queue.take(fake.booting);
      const second = yield* pool.runJob({ ...job, threadId: other });
      yield* TestClock.adjust(0);
      expect(fake.boots).toEqual([{ session: firstVm, flags: [] }]);
      const third = yield* pool.runJob(job);
      const secondVm = yield* Queue.take(fake.booting);
      expect(secondVm).not.toBe(firstVm);
      expect(yield* pool.poolStatus()).toMatchObject({
        vms: [
          { sessionName: firstVm, state: "provisioning" },
          { sessionName: secondVm, state: "provisioning" },
        ],
        queuedJobs: 3,
      });
      yield* Deferred.succeed(fake.bootGates.get(firstVm)!, undefined);
      const assignments = yield* Queue.takeBetween(fake.started, 2, 2);
      expect(assignments).toContainEqual({ session: firstVm, jobId: first.jobId });
      expect(assignments).toContainEqual({ session: firstVm, jobId: second.jobId });
      yield* Deferred.succeed(fake.bootGates.get(secondVm)!, undefined);
      expect(yield* Queue.take(fake.started)).toEqual({ session: secondVm, jobId: third.jobId });
      expect(yield* pool.poolStatus()).toMatchObject({
        computeUnitBalance: 72.5,
        queuedJobs: 0,
        vms: [
          { hardware: "cpu", activeJobs: 2, maxJobs: 2 },
          { hardware: "cpu", activeJobs: 1, maxJobs: 2 },
        ],
      });
      expect(yield* pool.jobStatus(other, second.jobId)).toMatchObject({
        hardware: "cpu",
        status: "running",
      });
      expect((yield* Effect.flip(pool.jobStatus(other, first.jobId))).message).toContain(
        "not found",
      );
    }),
  ),
);

it.effect("scales at the slot limit and dispatches waiting jobs in FIFO order", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* setup;
      const pool = yield* make({ ...fake });
      const initial = [];
      for (let index = 0; index < 4; index++) {
        yield* pool.runJob(job);
        initial.push(yield* Queue.take(fake.started));
      }
      expect(initial.map(({ session }) => session)).toEqual([
        fake.boots[0]?.session,
        fake.boots[0]?.session,
        fake.boots[1]?.session,
        fake.boots[1]?.session,
      ]);
      const fifth = yield* pool.runJob(job);
      const sixth = yield* pool.runJob(job);
      expect(yield* pool.poolStatus()).toMatchObject({ queuedJobs: 2, maxVms: 2 });
      const [first, second] = initial;
      if (first === undefined || second === undefined)
        return yield* Effect.die("Missing started jobs");
      yield* pool.cancelJob(thread, first.jobId);
      expect(yield* Queue.take(fake.started)).toEqual({
        session: first.session,
        jobId: fifth.jobId,
      });
      yield* pool.cancelJob(thread, second.jobId);
      expect(yield* Queue.take(fake.started)).toEqual({
        session: second.session,
        jobId: sixth.jobId,
      });
      expect(fake.boots).toHaveLength(2);
    }),
  ),
);

it.effect(
  "reserves slots under load and picks the least loaded VM when capacity is available",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fake = yield* setup;
        const pool = yield* make({ ...fake });
        yield* pool.runJob(job);
        const first = yield* Queue.take(fake.started);
        fake.load.set(first.session, { cpu: 0.9, memoryAvailable: 0.5 });
        const reserved = yield* pool.runJob(job);
        yield* TestClock.adjust(0);
        expect(yield* pool.jobStatus(thread, reserved.jobId)).toMatchObject({ status: "queued" });
        expect(fake.boots).toHaveLength(1);
        const third = yield* pool.runJob(job);
        const second = yield* Queue.take(fake.started);
        expect(second.session).not.toBe(first.session);
        expect(second.jobId).toBe(reserved.jobId);
        expect(yield* Queue.take(fake.started)).toEqual({
          session: second.session,
          jobId: third.jobId,
        });
        fake.load.set(first.session, { cpu: 0.3, memoryAvailable: 0.1 });
        const fourth = yield* pool.runJob(job);
        yield* TestClock.adjust(0);
        expect(yield* pool.jobStatus(thread, fourth.jobId)).toMatchObject({ status: "queued" });
        fake.load.set(first.session, { cpu: 0.3, memoryAvailable: 0.5 });
        yield* TestClock.adjust("5 seconds");
        expect(yield* Queue.take(fake.started)).toEqual({
          session: first.session,
          jobId: fourth.jobId,
        });

        yield* pool.cancelJob(thread, first.jobId);
        yield* pool.cancelJob(thread, second.jobId);
        fake.load.set(first.session, { cpu: 0.5, memoryAvailable: 0.5 });
        fake.load.set(second.session, { cpu: 0.1, memoryAvailable: 0.5 });
        const fifth = yield* pool.runJob(job);
        expect(yield* Queue.take(fake.started)).toEqual({
          session: second.session,
          jobId: fifth.jobId,
        });
      }),
    ),
);

it.effect("cancels queued and running jobs and releases a running slot after cleanup", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* setup;
      const pool = yield* make({ ...fake, maxVms: 1, maxCpuJobs: 1 });
      const first = yield* pool.runJob(job);
      const running = yield* Queue.take(fake.started);
      const second = yield* pool.runJob(job);
      const third = yield* pool.runJob(job);
      expect(yield* pool.cancelJob(thread, second.jobId)).toMatchObject({ status: "cancelled" });
      expect(yield* Queue.size(fake.cleaned)).toBe(0);
      expect(yield* pool.cancelJob(thread, first.jobId)).toMatchObject({ status: "cancelled" });
      expect(yield* Queue.take(fake.cleaned)).toBe(first.jobId);
      expect(yield* Queue.take(fake.started)).toEqual({ ...running, jobId: third.jobId });
      expect(yield* pool.jobStatus(thread, second.jobId)).toMatchObject({ status: "cancelled" });
    }),
  ),
);

it.effect("returns command output in wait mode and times out queued and running jobs", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* setup;
      const pool = yield* make({ ...fake, maxVms: 1, maxCpuJobs: 1 });
      const run = yield* pool.runJob({ ...job, mode: "wait" }).pipe(Effect.forkChild);
      const running = yield* Queue.take(fake.started);
      const queued = yield* pool
        .runJob({ ...job, mode: "wait", timeoutSeconds: 2 })
        .pipe(Effect.forkChild);
      yield* TestClock.adjust("2 seconds");
      expect(yield* Fiber.join(queued)).toMatchObject({ status: "timed_out", queuedMs: 2000 });
      yield* Deferred.succeed(fake.gates.get(running.jobId)!, undefined);
      expect(yield* Fiber.join(run)).toMatchObject({ ...successful, status: "completed" });

      const timeout = yield* pool
        .runJob({ ...job, mode: "wait", timeoutSeconds: 2 })
        .pipe(Effect.forkChild);
      const next = yield* Queue.take(fake.started);
      yield* TestClock.adjust("2 seconds");
      expect(yield* Fiber.join(timeout)).toMatchObject({
        jobId: next.jobId,
        status: "timed_out",
        durationMs: 2000,
      });
    }),
  ),
);

it.effect("rejects path traversal and invalid environment keys before allocating", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* setup;
      const pool = yield* make({ ...fake });
      for (const options of [
        { artifactPaths: ["../escape"] },
        { artifactPaths: ["results/../../escape"] },
        { artifactPaths: ["C:\\outside"] },
        { cwd: "/etc" },
        { env: { "BAD-KEY": "value" } },
      ]) {
        yield* Effect.flip(pool.runJob({ ...job, ...options }));
      }
      expect(fake.boots).toEqual([]);
    }),
  ),
);

it.effect("reaps idle VMs and cleans up busy VMs when the scope closes", () =>
  Effect.gen(function* () {
    const fake = yield* setup;
    const busy = yield* Effect.scoped(
      Effect.gen(function* () {
        const pool = yield* make({ ...fake, idleTimeoutSeconds: 10 });
        yield* pool.runJob({ ...job, hardware: "tpu-v5e1" });
        const running = yield* Queue.take(fake.started);
        const idle = yield* pool.runJob({ ...job, hardware: "cpu-high-mem" });
        const idleVm = yield* Queue.take(fake.started);
        yield* pool.cancelJob(thread, idle.jobId);
        yield* TestClock.adjust("9 seconds");
        expect(yield* Queue.size(fake.stopped)).toBe(0);
        yield* TestClock.adjust("1 second");
        expect(yield* Queue.take(fake.stopped)).toBe(idleVm.session);
        expect(yield* pool.poolStatus()).toMatchObject({
          vms: [{ sessionName: running.session, state: "ready", activeJobs: 1, maxJobs: 1 }],
        });
        expect(fake.boots.map(({ flags }) => flags)).toEqual([["--tpu", "v5e1"], ["--high-mem"]]);
        return running;
      }),
    );
    expect(yield* Queue.take(fake.stopped)).toBe(busy.session);
    expect(yield* Queue.take(fake.cleaned)).not.toBe(busy.jobId);
    expect(yield* Queue.take(fake.cleaned)).toBe(busy.jobId);
  }),
);

it.effect("interrupts a waiting caller's job and stops busy VMs only when forced", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* setup;
      const pool = yield* make({ ...fake, maxVms: 1 });
      const caller = yield* pool.runJob({ ...job, mode: "wait" }).pipe(Effect.forkChild);
      const interrupted = yield* Queue.take(fake.started);
      yield* Fiber.interrupt(caller);
      expect(yield* Queue.take(fake.cleaned)).toBe(interrupted.jobId);
      expect(yield* pool.jobStatus(thread, interrupted.jobId)).toMatchObject({
        status: "cancelled",
      });

      yield* pool.runJob(job);
      const busy = yield* Queue.take(fake.started);
      expect(yield* pool.stop({ sessionName: busy.session })).toEqual([]);
      expect(
        yield* Effect.flip(pool.stop({ threadId: other, sessionName: busy.session, force: true })),
      ).toMatchObject({
        _tag: "ColabPoolError",
        message: expect.stringContaining("another thread"),
      });
      expect(yield* pool.jobStatus(thread, busy.jobId)).toMatchObject({ status: "running" });
      expect(
        yield* pool.stop({ threadId: thread, sessionName: busy.session, force: true }),
      ).toEqual([busy.session]);
      expect(yield* pool.jobStatus(thread, busy.jobId)).toMatchObject({ status: "cancelled" });
      expect(yield* Queue.take(fake.stopped)).toBe(busy.session);
    }),
  ),
);

it.effect("evicts an idle CPU VM immediately when a GPU job needs the only pool slot", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* setup;
      const pool = yield* make({ ...fake, maxVms: 1 });
      const cpu = yield* pool.runJob(job);
      const vm = yield* Queue.take(fake.started);
      yield* pool.cancelJob(thread, cpu.jobId);

      const gpu = yield* pool.runJob({ ...job, hardware: "gpu-t4" });
      expect(yield* Queue.take(fake.stopped)).toBe(vm.session);
      const replacement = yield* Queue.take(fake.started);
      expect(replacement.jobId).toBe(gpu.jobId);
      expect(replacement.session).not.toBe(vm.session);
      expect(fake.boots.map(({ flags }) => flags)).toEqual([[], ["--gpu", "T4", "--high-mem"]]);
    }),
  ),
);

it.effect("retires a VM after unconfirmed cleanup and lets its other job finish first", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* setup;
      let firstJobId: string | undefined;
      const pool = yield* make({
        ...fake,
        maxVms: 1,
        transport: {
          ...fake.transport,
          clean: (session, jobId) =>
            fake.transport
              .clean(session, jobId)
              .pipe(
                Effect.andThen(
                  jobId === firstJobId
                    ? Effect.fail(new ColabPoolError({ message: "Processes survived cleanup" }))
                    : Effect.void,
                ),
              ),
        },
      });
      const first = yield* pool.runJob(job);
      firstJobId = first.jobId;
      const vm = yield* Queue.take(fake.started);
      const second = yield* pool.runJob(job);
      yield* Queue.take(fake.started);
      yield* pool.cancelJob(thread, first.jobId);
      expect(yield* pool.poolStatus()).toMatchObject({
        vms: [{ state: "draining", activeJobs: 1 }],
      });
      const next = yield* pool.runJob(job);
      yield* pool.cancelJob(thread, second.jobId);
      expect(yield* Queue.take(fake.stopped)).toBe(vm.session);
      expect(yield* Queue.take(fake.started)).toEqual({
        session: expect.not.stringMatching(vm.session),
        jobId: next.jobId,
      });
    }),
  ),
);

it.effect("reconciles orphans in a supervised fiber while the pool accepts jobs", () =>
  Effect.gen(function* () {
    const fake = yield* setup;
    const scan = yield* Deferred.make<(session: string) => boolean>();
    const interrupted = yield* Deferred.make<void>();
    yield* Effect.scoped(
      Effect.gen(function* () {
        const pool = yield* make({
          ...fake,
          transport: {
            ...fake.transport,
            reconcileOrphans: (ownsSession) =>
              Deferred.succeed(scan, ownsSession).pipe(
                Effect.andThen(Effect.never),
                Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
              ),
          },
        });
        const ownsSession = yield* Deferred.await(scan);
        yield* pool.runJob(job);
        const vm = yield* Queue.take(fake.started);
        expect(ownsSession(vm.session)).toBe(true);
        expect(ownsSession("t3-orphan")).toBe(false);
      }),
    );
    yield* Deferred.await(interrupted);
  }),
);

const fakeHome = "/colab-test";
const poolDir = fakeHome + "/.t3/colab-pool";
const session = "t3-session";

const makeTransportFixture = (holdRun = false) =>
  Effect.gen(function* () {
    const files = new Map([
      [fakeHome + "/.local/bin/colab", ""],
      [fakeHome + "/.ssh/t3_colab_ed25519", ""],
    ]);
    const operations: Array<string> = [];
    const requests = yield* Queue.unbounded<{ args: ReadonlyArray<string>; stdin: string }>();
    const checks = yield* Queue.unbounded<void>();
    const checkResults = yield* Queue.unbounded<number>();
    const masterExit = yield* Deferred.make<number>();
    const masters = yield* Queue.unbounded<ChildProcess.StandardCommand>();
    const missing = FileSystem.makeNoop({});
    const stat = (path: string) =>
      files.has(path)
        ? Effect.succeed({ type: "File", mode: 0o700 } as FileSystem.File.Info)
        : missing.stat(path);
    const fileSystem = FileSystem.makeNoop({
      stat,
      lstat: stat,
      exists: (path) => Effect.sync(() => files.has(path)),
      makeDirectory: () => Effect.void,
      readDirectory: (path) =>
        Effect.sync(() =>
          Array.from(files.keys())
            .filter((file) => file.startsWith(path + "/"))
            .map((file) => file.slice(path.length + 1)),
        ),
      readFileString: (path) =>
        files.has(path) ? Effect.succeed(files.get(path)!) : missing.readFileString(path),
      writeFileString: (path, text) =>
        Effect.sync(() => {
          files.set(path, text);
        }),
      remove: (path) =>
        Effect.sync(() => {
          operations.push("remove " + path);
          files.delete(path);
        }),
    });
    const spawner = ChildProcessSpawner.make(
      Effect.fnUntraced(function* (command) {
        if (!ChildProcess.isStandardCommand(command))
          return yield* Effect.die("Expected one child");
        const { options } = command;
        const args = command.args.map((arg) =>
          typeof arg === "string" ? arg : Redacted.value(arg),
        );
        let exitCode = Effect.succeed(0);
        if (args[2] === "new") {
          files.set(args[1]!, "");
          files.set(args[1]! + ".lock", "");
        } else if (args[2] === "stop") {
          operations.push("stop " + args[4]);
        } else if (args.includes("-M")) {
          files.set(poolDir + "/" + args[args.indexOf("-S") + 1], "");
          exitCode = Deferred.await(masterExit);
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              operations.push("close master scope");
            }),
          );
          yield* Queue.offer(masters, command);
        } else if (args.includes("check")) {
          exitCode = Queue.take(checkResults);
          yield* Queue.offer(checks, undefined);
        } else if (args.includes("exit")) {
          operations.push("exit " + args[args.indexOf("-S") + 1]);
        } else if (args.at(-1)?.includes(" 'clean' '")) {
          operations.push("remote cleanup");
        } else {
          const stdin = options.stdin;
          if (stdin !== undefined && typeof stdin === "object" && !("stream" in stdin)) {
            const data = yield* Stream.runCollect(stdin);
            yield* Queue.offer(requests, { args, stdin: Buffer.concat(data).toString() });
            if (holdRun) exitCode = Effect.never;
          }
        }
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(42),
          exitCode: Effect.map(exitCode, ChildProcessSpawner.ExitCode),
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.empty,
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        });
      }),
    );
    const transport = yield* makeTransport.pipe(
      Effect.provide(Path.layer),
      Effect.provideService(HostProcess.HomeDirectory, fakeHome),
      Effect.provideService(HostProcess.Platform, "linux"),
      Effect.provideService(FileSystem.FileSystem, fileSystem),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );
    return { transport, files, operations, requests, checks, checkResults, masterExit, masters };
  });

const transportFixture = makeTransportFixture();

it.effect(
  "holds the foreground SSH master open after verifying readiness, and closes it on stop",
  () =>
    Effect.gen(function* () {
      const fake = yield* transportFixture;
      const start = yield* fake.transport.start(session, []).pipe(Effect.forkChild);
      const master = yield* Queue.take(fake.masters);
      expect(master.args).toEqual([
        "-F",
        "/dev/null",
        "-N",
        "-M",
        "-S",
        session + ".sock",
        "-o",
        "ControlPersist=no",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ConnectTimeout=30",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "BatchMode=yes",
        "-o",
        expect.stringContaining("'ssh' '--proxy-mode'"),
        "-i",
        fakeHome + "/.ssh/t3_colab_ed25519",
        "root@colab-runtime",
      ]);
      expect(master.options.cwd).toBe(poolDir);
      yield* Queue.take(fake.checks);
      yield* Queue.offer(fake.checkResults, 1);
      yield* TestClock.adjust("200 millis");
      yield* Queue.take(fake.checks);
      expect(start.pollUnsafe()).toBeUndefined();
      yield* Queue.offer(fake.checkResults, 0);
      yield* Fiber.join(start);
      expect(fake.operations).toEqual([]);
      yield* fake.transport.stop(session);
      expect(fake.operations).toEqual([
        "exit " + session + ".sock",
        "close master scope",
        "remove " + poolDir + "/" + session + ".sock",
        "stop " + session,
        "remove " + poolDir + "/" + session + ".json",
        "remove " + poolDir + "/" + session + ".json.lock",
        "remove " + poolDir + "/" + session + ".owner",
      ]);
    }),
);

it.effect.each(["exit", "timeout"] as const)(
  "fails SSH startup on master %s even while the readiness check is stalled",
  (failure) =>
    Effect.gen(function* () {
      const fake = yield* transportFixture;
      const start = yield* fake.transport.start(session, []).pipe(Effect.flip, Effect.forkChild);
      yield* Queue.take(fake.checks);
      if (failure === "exit") yield* Deferred.succeed(fake.masterExit, 255);
      else yield* TestClock.adjust("45 seconds");
      expect(yield* Fiber.join(start)).toMatchObject({
        message: "Could not open the Colab SSH ControlMaster.",
      });
      yield* fake.transport.stop(session);
      expect(fake.operations).toContain("close master scope");
    }),
);

it.effect("sends the job working directory, environment, and command only through SSH stdin", () =>
  Effect.gen(function* () {
    const fake = yield* transportFixture;
    const input = { cwd: "subdir", env: { TOKEN: "sensitive" }, command: "echo command-as-data" };
    yield* fake.transport.run(session, "job", { ...job, ...input, syncWorkspace: false });
    const request = yield* Queue.take(fake.requests);
    const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
    expect(request.args.at(-1)).toBe(
      ["python3", "-c", supervisor, "run", "job"].map(quote).join(" "),
    );
    expect(JSON.parse(request.stdin)).toEqual(input);
  }),
);

it.effect("runs remote cleanup after cancellation and keeps the clean VM available", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fake = yield* makeTransportFixture(true);
      let id = 0;
      const pool = yield* make({
        transport: fake.transport,
        newId: Effect.sync(() => String(++id)),
      });
      const running = yield* pool.runJob({ ...job, syncWorkspace: false });
      yield* Queue.take(fake.checks);
      yield* Queue.offer(fake.checkResults, 0);
      yield* Queue.take(fake.requests);
      expect(yield* pool.cancelJob(thread, running.jobId)).toMatchObject({ status: "cancelled" });
      expect(fake.operations).toEqual(["remote cleanup"]);
    }),
  ),
);

it.effect("stops orphaned VM sessions and removes their files while skipping owned sessions", () =>
  Effect.gen(function* () {
    const fake = yield* transportFixture;
    for (const name of ["t3-orphan", "t3-current", "t3-other", "unrelated"]) {
      for (const suffix of [".json", ".sock", ".json.lock"])
        fake.files.set(poolDir + "/" + name + suffix, "");
    }
    fake.files.set(poolDir + "/t3-other.owner", String(process.pid));
    yield* fake.transport.reconcileOrphans((name) => name === "t3-current");
    expect(fake.operations).toContain("exit t3-orphan.sock");
    expect(fake.operations).toContain("stop t3-orphan");
    expect(fake.operations.filter((operation) => operation.startsWith("stop "))).toEqual([
      "stop t3-orphan",
    ]);
    expect(Array.from(fake.files.keys()).some((file) => file.includes("t3-orphan"))).toBe(false);
    for (const name of ["t3-current", "t3-other", "unrelated"])
      expect(fake.files.has(poolDir + "/" + name + ".json")).toBe(true);
  }),
);
