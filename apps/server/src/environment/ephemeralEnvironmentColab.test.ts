import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import * as ProcessRunner from "../processRunner.ts";
import {
  buildColabProvisionCommand,
  buildColabTeardownCommand,
} from "./ephemeralEnvironmentColab.ts";

const platform = HostProcess.Platform.defaultValue();

it.live.runIf(platform === "darwin" || platform === "linux")(
  "keeps the Colab SSH tunnel alive after the provision process scope closes",
  () =>
    Effect.gen(function* () {
      const runner = yield* ProcessRunner.ProcessRunner;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const home = yield* fs.makeTempDirectoryScoped();
      const bin = path.join(home, ".local", "bin");
      const leaseId = "tunnel-fixture";
      const handle = `t3-${leaseId}`;
      const stateDir = path.join(home, ".t3", "ephemeral-colab");
      yield* fs.makeDirectory(bin, { recursive: true });
      yield* fs.makeDirectory(path.join(home, ".ssh"));
      yield* fs.writeFileString(path.join(home, ".ssh", "t3_colab_ed25519"), "fixture");

      const executables = {
        colab: `if [ "$1" = ssh ]; then
  cat >/dev/null
  printf '%s\\n' '{"environmentId":"fixture-env","bearerToken":"fixture-token"}'
fi`,
        curl: `pid="$(cat "$HOME/.t3/ephemeral-colab/${handle}.pid")"
case "$(ps -ww -p "$pid" -o command=)" in
  "ssh -N "*) exit 0 ;;
  *) exit 1 ;;
esac`,
        python3: `if [ "$#" -eq 2 ]; then
  printf '43123\\n'
else
  cat
fi`,
        ssh: 'exec -a "ssh $*" /bin/sleep 30',
      };
      for (const [name, script] of Object.entries(executables)) {
        yield* fs.writeFileString(path.join(bin, name), `#!/bin/bash\n${script}\n`, {
          mode: 0o700,
        });
      }
      const run = (command: string) =>
        runner.run({
          command: "/bin/sh",
          args: ["-c", command],
          env: { HOME: home, T3_EPHEMERAL_LEASE_ID: leaseId },
          timeout: "5 seconds",
        });

      yield* Effect.gen(function* () {
        const provision = yield* run(
          buildColabProvisionCommand({
            kind: "colab",
            hardware: "cpu",
            syncLocalCredentials: false,
            provisionTimeoutSeconds: 240,
          }),
        );
        assert.strictEqual(provision.code, 0, provision.stderr);
        const pid = (yield* fs.readFileString(path.join(stateDir, `${handle}.pid`))).trim();
        const tunnel = yield* runner.run({
          command: "ps",
          args: ["-ww", "-p", pid, "-o", "command="],
        });
        assert.include(tunnel.stdout, `HostKeyAlias=${handle}`);
      }).pipe(Effect.ensuring(run(buildColabTeardownCommand()).pipe(Effect.ignore)));
    }).pipe(
      Effect.scoped,
      Effect.provide(ProcessRunner.layer.pipe(Layer.provideMerge(NodeServices.layer))),
    ),
);
