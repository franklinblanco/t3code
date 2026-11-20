import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import { McpSchema, McpServer } from "effect/ai";
import { HttpRouter } from "effect/http";

import { ColabComputePool, ColabPoolError } from "../orchestration-v2/ColabComputePool.ts";
import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { ProjectService } from "../project/ProjectService.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as McpHttpServer from "./McpHttpServer.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import { McpSessionRegistry } from "./McpSessionRegistry.ts";
import { liveThreadShell } from "./McpToolAccess.testkit.ts";

const worktree = ThreadId.make("worktree");
const project = ThreadId.make("project");
const restricted = ThreadId.make("restricted");
const plan = ThreadId.make("plan");

const invocation = (threadId: ThreadId): McpInvocationContext.McpThreadInvocationScope => ({
  environmentId: EnvironmentId.make("environment"),
  requestNamespace: "provider-session",
  thread: {
    threadId,
    providerSessionId: "provider-session",
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
  colabAvailable: true,
  issuedAt: 1,
});

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "mcp-test", version: "1.0.0" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "mcp-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const service = Layer.mergeAll(
  Layer.mock(ThreadManagementService)({
    getThreadShell: (threadId) =>
      Effect.succeed({
        ...liveThreadShell(threadId, {
          runtimeMode: threadId === restricted ? "approval-required" : "full-access",
          interactionMode: threadId === plan ? "plan" : "default",
        }),
        worktreePath: threadId === worktree ? "/checkout/worktree" : null,
      }),
  }),
  Layer.mock(ProjectService)({
    getById: () => Effect.succeedSome({ workspaceRoot: "/checkout/project" } as never),
  }),
);

const poolLayer = (available: Effect.Effect<boolean>) =>
  Layer.mock(ColabComputePool)({
    isAvailable: () => available,
    runJob: ({ workspace, threadId }) =>
      Effect.succeed({
        hardware: "cpu",
        status: "completed",
        jobId: threadId,
        stdout: workspace,
        queuedMs: 0,
        durationMs: 0,
        artifacts: [],
      }),
    stop: ({ sessionName, threadId, force }) =>
      sessionName === "t3-busy" && force && threadId === worktree
        ? Effect.succeed([sessionName])
        : Effect.fail(
            new ColabPoolError({ message: "A selected Colab VM has jobs from another thread." }),
          ),
  });

const layer = McpHttpServer.layerColabToolkit.pipe(
  Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
  Layer.provide(Layer.merge(service, poolLayer(Effect.succeed(true)))),
);

const call = (
  scope: McpInvocationContext.McpInvocationScope,
  name = "colab_run_job",
  args: Record<string, string | boolean> = { command: "npm test" },
) =>
  McpServer.McpServer.pipe(
    Effect.flatMap((server) =>
      server.callTool({
        name,
        arguments: args,
      }),
    ),
    Effect.provideService(McpInvocationContext.McpInvocationContext, scope),
    Effect.provideService(McpSchema.McpServerClient, client),
  );

it.effect(
  "advertises valid object schemas for all five tools and hides them when unavailable",
  () =>
    Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      expect(server.tools.map(({ tool }) => tool.name).sort()).toEqual([
        "colab_cancel_job",
        "colab_job_status",
        "colab_pool_status",
        "colab_pool_stop",
        "colab_run_job",
      ]);
      for (const { tool } of server.tools) {
        expect(tool.inputSchema.type).toBe("object");
      }
      const enabled = Effect.sync(McpInvocationContext.colabToolEnabled);
      expect(
        yield* enabled.pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, invocation(worktree)),
        ),
      ).toBe(true);
      expect(
        yield* enabled.pipe(
          Effect.provideService(McpInvocationContext.McpInvocationContext, {
            ...invocation(worktree),
            colabAvailable: false,
          }),
        ),
      ).toBe(false);
    }).pipe(Effect.provide(layer)),
);

it.effect("passes the calling thread's identity when force-stopping a busy VM", () =>
  Effect.gen(function* () {
    const args = { sessionName: "t3-busy", force: true };
    const denied = yield* call(invocation(project), "colab_pool_stop", args);
    expect(denied.content[0]).toMatchObject({
      text: expect.stringContaining("jobs from another thread"),
    });
    const stopped = yield* call(invocation(worktree), "colab_pool_stop", args);
    expect(stopped.structuredContent).toEqual({ stopped: ["t3-busy"] });
  }).pipe(Effect.provide(layer)),
);

it.effect("updates Colab discovery and calls per HTTP request with the same bearer", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const available = yield* Ref.make(false);
      const bearer = "stable-colab-bearer";
      const scope = { ...invocation(worktree), colabAvailable: false };
      const routes = McpHttpServer.layerMcpTransport(McpHttpServer.layerColabToolkit).pipe(
        Layer.provide(
          Layer.mergeAll(
            service,
            poolLayer(Ref.get(available)),
            ServerSettings.layerTest(),
            Layer.mock(ProjectionStoreV2)({
              getThreadShell: (threadId) =>
                Effect.succeed(liveThreadShell(threadId, { runtimeMode: "full-access" })),
            }),
            Layer.mock(McpSessionRegistry)({
              resolve: (token) => Effect.succeed(token === bearer ? scope : undefined),
            }),
          ),
        ),
      );
      const { handler } = yield* Effect.acquireRelease(
        Effect.sync(() => HttpRouter.toWebHandler(routes, { disableLogger: true })),
        ({ dispose }) => Effect.promise(dispose),
      );
      const rpc = (method: string, params: { name?: string; arguments?: object } = {}) =>
        Effect.gen(function* () {
          const response = yield* Effect.promise(() =>
            handler(
              new Request("http://localhost/mcp", {
                method: "POST",
                headers: {
                  authorization: "Bearer " + bearer,
                  accept: "application/json, text/event-stream",
                  "content-type": "application/json",
                  "mcp-method": method,
                  "mcp-protocol-version": "2026-07-28",
                  ...(params.name === undefined ? {} : { "mcp-name": params.name }),
                },
                body: JSON.stringify({
                  jsonrpc: "2.0",
                  id: 1,
                  method,
                  params: {
                    ...params,
                    _meta: {
                      "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                      "io.modelcontextprotocol/clientCapabilities": {},
                      "io.modelcontextprotocol/clientInfo": { name: "mcp-test", version: "1.0.0" },
                    },
                  },
                }),
              }),
            ),
          );
          const body: unknown = yield* Effect.promise(() => response.json());
          return { status: response.status, body };
        });
      expect(yield* rpc("server/discover")).toMatchObject({ status: 200 });
      expect(yield* rpc("tools/list")).toMatchObject({
        status: 200,
        body: { result: { tools: [] } },
      });
      yield* Ref.set(available, true);
      expect(yield* rpc("tools/list")).toMatchObject({
        status: 200,
        body: {
          result: {
            tools: expect.arrayContaining([expect.objectContaining({ name: "colab_run_job" })]),
          },
        },
      });
      const run = { name: "colab_run_job", arguments: { command: "npm test" } };
      expect(yield* rpc("tools/call", run)).toMatchObject({
        status: 200,
        body: { result: { structuredContent: { jobId: worktree } } },
      });
      yield* Ref.set(available, false);
      expect(yield* rpc("tools/list")).toMatchObject({
        status: 200,
        body: { result: { tools: [] } },
      });
      const denied = (yield* rpc("tools/call", run)).body as {
        error?: unknown;
        result?: { isError?: boolean };
      };
      expect(denied.error !== undefined || denied.result?.isError === true).toBe(true);
    }),
  ),
);

it.effect("runs in the calling thread's worktree or project root", () =>
  Effect.gen(function* () {
    for (const [thread, workspace] of [
      [worktree, "/checkout/worktree"],
      [project, "/checkout/project"],
    ] as const) {
      const result = yield* call(invocation(thread));
      expect(result.isError).toBe(false);
      expect(result.structuredContent).toMatchObject({ jobId: thread, stdout: workspace });
    }
  }).pipe(Effect.provide(layer)),
);

it.effect("refuses approval-required, Plan, and outside read-only callers", () =>
  Effect.gen(function* () {
    for (const thread of [restricted, plan]) {
      const denied = yield* call(invocation(thread));
      expect(denied.content).toEqual([
        {
          type: "text",
          text: expect.stringContaining(
            "Colab compute requires a Full access thread in Default mode.",
          ),
        },
      ]);
    }
    const outside = yield* Effect.exit(
      call({
        ...invocation(worktree),
        thread: undefined,
        client: { sessionId: "outside", label: "Outside", access: "read-only" },
      }),
    );
    expect(outside._tag === "Failure" || outside.value.isError === true).toBe(true);
  }).pipe(Effect.provide(layer)),
);
