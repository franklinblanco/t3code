import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Config from "effect/Config";
import * as Path from "effect/Path";
import type { PlatformError as FileSystemError } from "effect/PlatformError";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/process/ChildProcess";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { isProcessAlive } from "../serverRuntimeState.ts";
import { ColabPoolError, type ColabTransport } from "./ColabComputePool.ts";
import { probe, supervisor } from "./ColabComputeRemote.ts";

const host = "root@colab-runtime";
const MAX_LOG = 64 * 1024;
const fallbackExclusions = [".git", "node_modules", "target", "dist", ".t3", ".venv"];

const quote = (value: string) => "'" + value.replaceAll("'", "'\\''") + "'";
const shellCommand = (args: ReadonlyArray<string>) => args.map(quote).join(" ");

const tail = <E>(stream: Stream.Stream<Uint8Array, E>) =>
  Stream.runFold(
    stream,
    () => Buffer.alloc(0),
    (bytes, chunk) => {
      const end = Buffer.from(chunk.subarray(-MAX_LOG));
      if (end.length === MAX_LOG) return end;
      return Buffer.concat([bytes.subarray(Math.max(0, bytes.length + end.length - MAX_LOG)), end]);
    },
  ).pipe(Effect.map((bytes) => bytes.toString("utf8")));

const encodePaths = Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));
const encodeJob = Schema.encodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      cwd: Schema.String,
      env: Schema.Record(Schema.String, Schema.String),
      command: Schema.String,
    }),
  ),
);

const Load = Schema.Struct({ cpu: Schema.Number, memoryAvailable: Schema.Number });
const decodeLoad = Schema.decodeUnknownEffect(Schema.fromJsonString(Load));

export const makeTransport = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const files = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const home = yield* HostProcess.HomeDirectory;
  const platform = yield* HostProcess.Platform;
  const poolDir = path.join(home, ".t3", "colab-pool");
  const masters = new Map<string, Scope.Closeable>();
  const key = path.join(home, ".ssh", "t3_colab_ed25519");
  const pathSeparator = platform === "win32" ? ";" : ":";
  const hostPath = yield* Config.String("PATH").pipe(Effect.orElseSucceed(() => ""));
  const searchPaths = [
    path.join(home, ".local", "bin"),
    "/opt/homebrew/bin",
    "/usr/local/bin",
    ...hostPath.split(pathSeparator).filter(Boolean),
  ];
  const env = { PATH: searchPaths.join(pathSeparator) };

  const cli = Effect.gen(function* () {
    if (platform === "win32") return undefined;
    for (const directory of searchPaths) {
      const program = path.resolve(directory, "colab");
      const info = yield* files.stat(program).pipe(Effect.orElseSucceed(() => undefined));
      if (info?.type === "File" && (info.mode & 0o111) !== 0) return program;
    }
    return undefined;
  });

  const requireCli = cli.pipe(
    Effect.filterOrFail(
      (program) => program !== undefined,
      () => new ColabPoolError({ message: "The Colab CLI is unavailable." }),
    ),
  );

  const lstatIfPresent = (file: string) =>
    files.lstat(file).pipe(
      Effect.catchIf(
        (error) => error.reason._tag === "NotFound",
        () => Effect.undefined,
      ),
    );

  const publishArtifacts = Effect.fn("ColabComputeTransport.publishArtifacts")(
    function* (source: string, workspace: string) {
      const root = yield* files.realPath(workspace);
      const entries: Array<{ from: string; to: string; type: string; mode: number }> = [];
      const message = "Colab artifacts conflict with paths in the local workspace.";
      // Untraced because each archive entry is checked within this publish span.
      const inspect = Effect.fnUntraced(function* (
        relative: string,
      ): Effect.fn.Return<void, FileSystemError | ColabPoolError> {
        const from = path.join(source, relative);
        const to = path.join(root, relative);
        const info = yield* files.lstat(from);
        const existing = yield* lstatIfPresent(to);
        if (
          relative.split(path.sep).some((part) => part.toLowerCase() === ".git") ||
          (info.type !== "Directory" && info.type !== "File") ||
          (existing !== undefined && info.type !== existing.type)
        )
          return yield* new ColabPoolError({ message });
        entries.push({ from, to, type: info.type, mode: info.mode });
        if (info.type === "Directory") {
          for (const child of yield* files.readDirectory(from)) {
            yield* inspect(path.join(relative, child));
          }
        }
      });
      for (const entry of yield* files.readDirectory(source)) yield* inspect(entry);
      for (const { from, to, type, mode } of entries) {
        const parent = path.dirname(to);
        if ((yield* files.realPath(parent)) !== parent)
          return yield* new ColabPoolError({ message });
        if (type === "Directory") {
          const existing = yield* lstatIfPresent(to);
          if (existing === undefined) yield* files.makeDirectory(to);
          else if (existing.type !== "Directory") return yield* new ColabPoolError({ message });
        } else {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const stage = yield* files.makeTempDirectoryScoped({
                directory: parent,
                prefix: ".t3-artifact-",
              });
              const stagedFile = path.join(stage, "file");
              yield* files.copyFile(from, stagedFile);
              yield* files.chmod(stagedFile, mode & 0o777);
              if ((yield* files.realPath(parent)) !== parent)
                return yield* new ColabPoolError({ message });
              yield* files.rename(stagedFile, to);
            }),
          );
        }
      }
    },
    Effect.mapError(
      () =>
        new ColabPoolError({
          message: "Colab artifacts conflict with paths in the local workspace.",
        }),
    ),
  );

  // OpenSSH also appends a temporary suffix when binding a control socket.
  const socketName = (session: string) => session + ".sock";
  const socket = (session: string) => path.join(poolDir, socketName(session));
  const config = (session: string) => path.join(poolDir, session + ".json");
  const owner = (session: string) => path.join(poolDir, session + ".owner");

  const local = (
    program: string,
    args: ReadonlyArray<string>,
    options?: ChildProcess.CommandOptions,
  ) =>
    ChildProcess.make(program, args, {
      env,
      extendEnv: true,
      stdin: "ignore",
      forceKillAfter: "1 second",
      ...options,
    });

  const ssh = (
    session: string,
    args: ReadonlyArray<string>,
    options?: ChildProcess.CommandOptions,
  ) =>
    local(
      "ssh",
      [
        "-F",
        "/dev/null",
        "-S",
        socketName(session),
        "-o",
        "BatchMode=yes",
        // A dead socket must not turn a job channel into another WebSocket connection.
        "-o",
        "ProxyCommand=false",
        "--",
        host,
        shellCommand(args),
      ],
      { ...options, cwd: poolDir },
    );

  const collect = Effect.fn("ColabComputeTransport.collect")(function* (
    command: ChildProcess.Command,
  ) {
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const child = yield* spawner.spawn(command);
        const [code, stdout, stderr] = yield* Effect.all(
          [child.exitCode, tail(child.stdout), tail(child.stderr)],
          { concurrency: "unbounded" },
        );
        return { code, stdout, stderr };
      }),
    );
  });

  const checked = Effect.fn("ColabComputeTransport.checked")(function* (
    command: ChildProcess.Command,
    message: string,
    timeoutSeconds = 15,
  ) {
    const result = yield* collect(command).pipe(
      Effect.timeout(`${timeoutSeconds} seconds`),
      Effect.mapError(() => new ColabPoolError({ message })),
    );
    if (result.code !== 0) return yield* new ColabPoolError({ message });
    return result;
  });

  const pipe = Effect.fn("ColabComputeTransport.pipe")(function* (
    source: ChildProcess.Command,
    destination: ChildProcess.StandardCommand,
    message: string,
  ) {
    yield* Effect.scoped(
      Effect.gen(function* () {
        const producer = yield* spawner.spawn(source);
        const consumer = yield* spawner.spawn(destination);
        const [sourceCode, destinationCode] = yield* Effect.all(
          [
            producer.exitCode,
            consumer.exitCode,
            Stream.run(producer.stdout, consumer.stdin),
            Stream.runDrain(producer.stderr),
            Stream.runDrain(consumer.stderr),
            Stream.runDrain(consumer.stdout),
          ],
          { concurrency: "unbounded" },
        );
        if (sourceCode !== 0 || destinationCode !== 0)
          return yield* new ColabPoolError({ message });
      }),
    ).pipe(Effect.mapError(() => new ColabPoolError({ message })));
  });

  const remote = (
    session: string,
    action: string,
    jobId: string,
    args: ReadonlyArray<string> = [],
    options?: ChildProcess.CommandOptions,
  ) => ssh(session, ["python3", "-c", supervisor, action, jobId, ...args], options);

  const keyLock = yield* Semaphore.make(1);
  const ensureKey = keyLock.withPermits(1)(
    Effect.gen(function* () {
      const message = "Could not prepare the Colab SSH key.";
      yield* files
        .makeDirectory(path.dirname(key), { recursive: true, mode: 0o700 })
        .pipe(Effect.mapError(() => new ColabPoolError({ message })));
      const present = yield* files
        .exists(key)
        .pipe(Effect.mapError(() => new ColabPoolError({ message })));
      if (!present)
        yield* checked(local("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-f", key]), message);
    }),
  );

  const gitFiles = Effect.fn("ColabComputeTransport.gitFiles")(function* (cwd: string) {
    const repo = yield* collect(local("git", ["rev-parse", "--is-inside-work-tree"], { cwd })).pipe(
      Effect.timeout("10 seconds"),
      Effect.orElseSucceed(() => undefined),
    );
    if (repo?.code !== 0 || repo.stdout.trim() !== "true") return undefined;
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const process = yield* spawner.spawn(
          local(
            "git",
            [
              "ls-files",
              "--cached",
              "--others",
              "--exclude-standard",
              "--deduplicate",
              "-z",
              "--",
              ".",
            ],
            { cwd },
          ),
        );
        const [code, chunks] = yield* Effect.all(
          [process.exitCode, Stream.runCollect(process.stdout), Stream.runDrain(process.stderr)],
          { concurrency: "unbounded" },
        );
        if (code !== 0)
          return yield* new ColabPoolError({ message: "Could not list Colab workspace files." });
        const root = yield* files.realPath(cwd);
        const entries = Buffer.concat(chunks).toString("utf8").split("\0").filter(Boolean);
        const archiveFiles = yield* Effect.forEach(
          entries,
          (name) =>
            Effect.gen(function* () {
              if (name.split("/").some((part) => [".git", ".t3"].includes(part.toLowerCase())))
                return "";
              const fullPath = path.join(root, name);
              const parent = path.dirname(fullPath);
              const resolved = yield* files
                .realPath(parent)
                .pipe(Effect.orElseSucceed(() => undefined));
              if (resolved !== parent) return "";
              const entry = yield* lstatIfPresent(fullPath);
              return entry?.type === "File" || entry?.type === "SymbolicLink" ? name + "\0" : "";
            }),
          { concurrency: 32 },
        );
        return archiveFiles.join("");
      }),
    ).pipe(
      Effect.mapError(
        () => new ColabPoolError({ message: "Could not list Colab workspace files." }),
      ),
    );
  });

  const upload = Effect.fn("ColabComputeTransport.upload")(function* (
    session: string,
    job: string,
    cwd: string,
  ) {
    const list = yield* gitFiles(cwd);
    const options = list === undefined ? { cwd } : { cwd, stdin: Stream.make(Buffer.from(list)) };
    const args =
      list === undefined
        ? [...fallbackExclusions.map((entry) => "--exclude=" + entry), "."]
        : ["--null", "-T", "-"];
    yield* pipe(
      local("tar", ["-czf", "-", ...args], { ...options, env: { ...env, COPYFILE_DISABLE: "1" } }),
      remote(session, "upload", job, [], { stdin: "pipe" }),
      "Could not upload the Colab workspace.",
    );
  });

  const download = Effect.fn("ColabComputeTransport.download")(function* (
    session: string,
    job: string,
    cwd: string,
    paths: ReadonlyArray<string>,
  ) {
    const message =
      "Could not retrieve Colab artifacts. Use existing files or directories without symlinks.";
    const stage = yield* files
      .makeTempDirectoryScoped({ prefix: "t3-colab-artifacts-" })
      .pipe(Effect.mapError(() => new ColabPoolError({ message })));
    const normalized = [
      ...new Set(paths.map((entry) => path.normalize(entry).replace(/\/$/u, ""))),
    ];
    const encodedPaths = yield* encodePaths(normalized).pipe(
      Effect.mapError(() => new ColabPoolError({ message })),
    );
    yield* pipe(
      remote(session, "artifacts", job, [encodedPaths]),
      local("tar", ["-xzf", "-", "--no-same-owner", "--no-same-permissions"], {
        cwd: stage,
        stdin: "pipe",
      }),
      message,
    );
    yield* publishArtifacts(stage, cwd);
    return normalized;
  }, Effect.scoped);

  const control = (session: string, operation: "check" | "exit") =>
    local("ssh", ["-F", "/dev/null", "-S", socketName(session), "-O", operation, host], {
      cwd: poolDir,
    });

  const stop = Effect.fn("ColabComputeTransport.stop")(function* (session: string) {
    const message = "Could not stop the Colab VM.";
    yield* checked(control(session, "exit"), message, 5).pipe(Effect.ignore);
    const scope = masters.get(session);
    if (scope !== undefined) {
      yield* Scope.close(scope, Exit.void);
      masters.delete(session);
    }
    yield* files.remove(socket(session), { force: true }).pipe(Effect.ignore);
    const present = yield* files
      .exists(config(session))
      .pipe(Effect.mapError(() => new ColabPoolError({ message })));
    if (present) {
      const program = yield* requireCli;
      yield* checked(
        local(program, ["--config", config(session), "stop", "-s", session]),
        message,
        30,
      );
      yield* files
        .remove(config(session), { force: true })
        .pipe(Effect.mapError(() => new ColabPoolError({ message })));
    }
    yield* files.remove(config(session) + ".lock", { force: true }).pipe(Effect.ignore);
    yield* files.remove(owner(session), { force: true }).pipe(Effect.ignore);
  }, Effect.uninterruptible);

  return {
    isAvailable: Effect.map(cli, (program) => program !== undefined),
    start: Effect.fn("ColabComputeTransport.start")(function* (session, flags) {
      const program = yield* requireCli;
      const message = "Could not prepare Colab pool files.";
      yield* files
        .makeDirectory(poolDir, { recursive: true, mode: 0o700 })
        .pipe(Effect.mapError(() => new ColabPoolError({ message })));
      yield* ensureKey;
      yield* files
        .writeFileString(owner(session), String(process.pid), { mode: 0o600 })
        .pipe(Effect.mapError(() => new ColabPoolError({ message })));
      yield* checked(
        local(program, ["--config", config(session), "new", "-s", session, ...flags]),
        "Could not allocate a Colab VM. Check CLI authentication, quota, and requested hardware.",
        120,
      );
      const proxy = shellCommand([
        program,
        "--config",
        config(session),
        "ssh",
        "--proxy-mode",
        "-s",
        session,
        "-i",
        key,
      ]).replaceAll("%", "%%");
      const scope = yield* Scope.make();
      masters.set(session, scope);
      const failed = new ColabPoolError({ message: "Could not open the Colab SSH ControlMaster." });
      const child = yield* spawner
        .spawn(
          local(
            "ssh",
            [
              "-F",
              "/dev/null",
              "-N",
              "-M",
              "-S",
              socketName(session),
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
              "ProxyCommand=" + proxy,
              "-i",
              key,
              host,
            ],
            { cwd: poolDir, stdout: "ignore", stderr: "ignore" },
          ),
        )
        .pipe(
          Scope.provide(scope),
          Effect.mapError(() => failed),
        );
      const ready = Effect.gen(function* () {
        while (true) {
          if (
            (yield* files.exists(socket(session))) &&
            (yield* collect(control(session, "check"))).code === 0
          )
            return;
          yield* Effect.sleep("200 millis");
        }
      });
      yield* ready.pipe(
        Effect.raceFirst(child.exitCode.pipe(Effect.andThen(Effect.fail(failed)))),
        Effect.timeout("45 seconds"),
        Effect.mapError(() => failed),
      );
    }),
    probe: Effect.fn("ColabComputeTransport.probe")(function* (session) {
      const message = "Could not check Colab VM capacity.";
      const result = yield* checked(ssh(session, ["python3", "-c", probe]), message, 5);
      return yield* decodeLoad(result.stdout).pipe(
        Effect.mapError(() => new ColabPoolError({ message })),
      );
    }),
    run: Effect.fn("ColabComputeTransport.run")(function* (session, job, input) {
      yield* checked(
        remote(session, "prepare", job),
        "Could not prepare the isolated job directory.",
      );
      if (input.syncWorkspace !== false) yield* upload(session, job, input.workspace);
      const request = yield* encodeJob({
        cwd: path.normalize(input.cwd ?? "."),
        env: input.env ?? {},
        command: input.command,
      }).pipe(
        Effect.mapError(() => new ColabPoolError({ message: "Could not encode the Colab job." })),
      );
      const result = yield* collect(
        remote(session, "run", job, [], { stdin: Stream.make(Buffer.from(request)) }),
      ).pipe(
        Effect.mapError(
          () => new ColabPoolError({ message: "The Colab command connection failed." }),
        ),
      );
      if (result.code === 255)
        return yield* new ColabPoolError({
          message: "The Colab SSH connection closed during the command.",
        });
      const output = { exitCode: result.code, stdout: result.stdout, stderr: result.stderr };
      if (!input.artifactPaths?.length) return { ...output, artifacts: [] };
      return yield* download(session, job, input.workspace, input.artifactPaths).pipe(
        Effect.map((artifacts) => ({ ...output, artifacts })),
        Effect.catch((error) => Effect.succeed({ ...output, artifacts: [], error: error.message })),
      );
    }),
    clean: Effect.fn("ColabComputeTransport.clean")(function* (session, job) {
      yield* checked(remote(session, "clean", job), "Could not clean the Colab job directory.");
    }),
    stop,
    reconcileOrphans: Effect.fn("ColabComputeTransport.reconcileOrphans")(
      function* (ownsSession) {
        if (platform === "win32") return;
        const entries = yield* files.readDirectory(poolDir).pipe(
          Effect.catchIf(
            (error) => error.reason._tag === "NotFound",
            () => Effect.succeed([]),
          ),
        );
        for (const entry of entries) {
          if (!/^t3-[a-z0-9-]+\.json$/u.test(entry)) continue;
          const session = entry.slice(0, -5);
          if (ownsSession(session) || masters.has(session)) continue;
          if ((yield* lstatIfPresent(config(session)))?.type !== "File") continue;
          const pid = yield* files.readFileString(owner(session)).pipe(
            Effect.map(Number),
            Effect.catchIf(
              (error) => error.reason._tag === "NotFound",
              () => Effect.succeed(0),
            ),
          );
          if (isProcessAlive(pid)) continue;
          if (!ownsSession(session) && !masters.has(session))
            yield* stop(session).pipe(Effect.ignore({ log: true }));
        }
      },
      Effect.ignore({ log: true }),
    ),
    balance: requireCli.pipe(
      Effect.flatMap((program) =>
        checked(local(program, ["usage"]), "Could not fetch Colab compute-unit balance."),
      ),
      Effect.map(({ stdout }) => {
        const balance = stdout.match(/Current balance:\s*(-?\d+(?:\.\d+)?)/u)?.[1];
        return balance === undefined ? null : Number(balance);
      }),
      Effect.orElseSucceed(() => null),
    ),
  } satisfies ColabTransport;
});
