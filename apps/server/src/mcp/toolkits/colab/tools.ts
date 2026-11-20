import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { McpSchema, Tool, Toolkit } from "effect/ai";

import {
  ColabComputePool,
  ColabJobOptions,
  ColabJobStatus,
  ColabPoolStatus,
} from "../../../orchestration-v2/ColabComputePool.ts";
import { ProjectService } from "../../../project/ProjectService.ts";
import { McpInvocationContext, colabToolEnabled } from "../../McpInvocationContext.ts";
import { ThreadManagementService } from "../../../orchestration-v2/ThreadManagementService.ts";

const dependencies = [McpInvocationContext, ThreadManagementService, ColabComputePool];
const failure = OrchestratorMcpFailure;
const job = Schema.Struct({ jobId: Schema.String });

const Run = Tool.make("colab_run_job", {
  description:
    "Build, compile, or test on a shared Google Colab VM. Defaults to CPU, workspace sync, and waiting up to 300 seconds. Async returns a job ID. cwd and artifactPaths are workspace-relative. env passes explicit variables.",
  parameters: ColabJobOptions,
  success: ColabJobStatus,
  failure,
  failureMode: "return",
  dependencies: [...dependencies, ProjectService],
})
  .annotate(Tool.OpenWorld, true)
  .annotate(McpSchema.EnabledWhen, colabToolEnabled);

const Status = Tool.make("colab_job_status", {
  description: "Get a Colab job's state, exit code, log tails, and retrieved artifacts.",
  parameters: job,
  success: ColabJobStatus,
  failure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Readonly, true)
  .annotate(McpSchema.EnabledWhen, colabToolEnabled);

const Cancel = Tool.make("colab_cancel_job", {
  description: "Cancel this thread's queued or running Colab job and free its slot.",
  parameters: job,
  success: ColabJobStatus,
  failure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(McpSchema.EnabledWhen, colabToolEnabled);

const Pool = Tool.make("colab_pool_status", {
  description: "List Colab VMs, hardware, active and queued jobs, and the compute-unit balance.",
  success: ColabPoolStatus,
  failure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.OpenWorld, true)
  .annotate(McpSchema.EnabledWhen, colabToolEnabled);

const Stop = Tool.make("colab_pool_stop", {
  description:
    "Stop idle Colab VMs. Omit sessionName to select all VMs. force also cancels jobs on selected busy VMs.",
  parameters: Schema.Struct({
    sessionName: Schema.optional(Schema.String),
    force: Schema.optional(Schema.Boolean),
  }),
  success: Schema.Struct({ stopped: Schema.Array(Schema.String) }),
  failure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, true)
  .annotate(McpSchema.EnabledWhen, colabToolEnabled);

export const ColabToolkit = Toolkit.make(Run, Status, Cancel, Pool, Stop);
