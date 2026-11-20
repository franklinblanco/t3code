import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import {
  ColabComputePool,
  type ColabPoolError,
} from "../../../orchestration-v2/ColabComputePool.ts";
import { ProjectService } from "../../../project/ProjectService.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { unavailable } from "../../threadAccess.ts";
import { ColabToolkit } from "./tools.ts";

const failure = (error: ColabPoolError) =>
  new OrchestratorMcpFailure({ code: "orchestration_error", message: error.message });

const handlers = {
  colab_run_job: McpToolAccess.colab((input, thread) =>
    Effect.gen(function* () {
      const projects = yield* ProjectService;
      const project = yield* projects.getById(thread.projectId).pipe(Effect.mapError(unavailable));
      if (Option.isNone(project)) {
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The calling thread's project was not found.",
        });
      }
      const pool = yield* ColabComputePool;
      return yield* pool
        .runJob({
          ...input,
          threadId: thread.id,
          workspace: thread.worktreePath ?? project.value.workspaceRoot,
        })
        .pipe(Effect.mapError(failure));
    }),
  ),
  colab_job_status: McpToolAccess.colab(({ jobId }, thread) =>
    ColabComputePool.use((pool) => pool.jobStatus(thread.id, jobId)).pipe(Effect.mapError(failure)),
  ),
  colab_cancel_job: McpToolAccess.colab(({ jobId }, thread) =>
    ColabComputePool.use((pool) => pool.cancelJob(thread.id, jobId)).pipe(Effect.mapError(failure)),
  ),
  colab_pool_status: McpToolAccess.colab(() => ColabComputePool.use((pool) => pool.poolStatus())),
  colab_pool_stop: McpToolAccess.colab((input, thread) =>
    ColabComputePool.use((pool) => pool.stop({ ...input, threadId: thread.id })).pipe(
      Effect.map((stopped) => ({ stopped })),
      Effect.mapError(failure),
    ),
  ),
} satisfies McpToolAccess.Handlers<typeof ColabToolkit.tools>;

export const layer = McpToolAccess.toLayer(ColabToolkit, handlers);
