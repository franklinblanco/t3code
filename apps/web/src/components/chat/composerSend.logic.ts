import type { AtomCommandResult } from "@t3tools/client-runtime/state/runtime";
import {
  EnvironmentAuthorizationError,
  OrchestrationV2DispatchCommandError,
  OrchestrationV2GetThreadProjectionError,
  OrchestrationV2ThreadLaunchError,
  PersistChatAttachmentsError,
  type EnvironmentId,
  type EphemeralEnvironmentAcquireResult,
  type EphemeralEnvironmentLeaseId,
  type ThreadContextRecord,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { msg } from "gt-react";
import { m, type MessageResolver } from "../../i18n/messages";
import { collectInlineContextIds } from "../../lib/composerContextReferences";
import type { ComposerThreadDraftState } from "../../composerDraftStore";

const isRejectedBeforeTurnStart = Schema.is(
  Schema.Union([
    EnvironmentAuthorizationError,
    OrchestrationV2GetThreadProjectionError,
    PersistChatAttachmentsError,
  ]),
);
const isStartCommandError = Schema.is(
  Schema.Union([OrchestrationV2ThreadLaunchError, OrchestrationV2DispatchCommandError]),
);
const preCommitRejectionNames = new Set([
  "OrchestratorCommandRejectedError",
  "OrchestratorThreadAlreadyExistsError",
  "OrchestratorProjectionError",
  "OrchestratorProviderAdapterError",
  "OrchestratorCommandPreviouslyRejectedError",
  "OrchestratorCommandIdConflictError",
  "OrchestratorSubagentThreadReadOnlyError",
  "OrchestratorThreadAboveModeLimitError",
  "OrchestratorProviderSelectionBusyError",
  "OrchestratorRuntimeModeAboveCeilingError",
  "ThreadManagementWorktreeRestoreError",
  "AttachmentClaimError",
  "ServerRuntimeStartupError",
  "ThreadManagementProviderInstanceDisabledError",
  "ThreadLaunchBaseRefError",
  "ThreadLaunchBranchExistsError",
  "ThreadLaunchBranchConflictError",
  "ThreadLaunchNotRepositoryError",
]);

export function turnStartDisposition(result: AtomCommandResult<unknown, unknown>) {
  if (result._tag === "Success") return "accepted";
  // Generic launch and dispatch errors can arrive after the turn commits.
  const rejected =
    result.cause.reasons.length > 0 &&
    result.cause.reasons.every(
      (reason) =>
        reason._tag === "Fail" &&
        (isRejectedBeforeTurnStart(reason.error) ||
          (isStartCommandError(reason.error) &&
            reason.error.cause instanceof Error &&
            preCommitRejectionNames.has(reason.error.cause.name))),
    );
  return rejected ? "rejected" : "uncertain";
}

export async function withEphemeralLease(input: {
  readonly acquire: () => Promise<EphemeralEnvironmentAcquireResult>;
  readonly release: (leaseId: EphemeralEnvironmentLeaseId) => Promise<void>;
  readonly prepare: (lease: EphemeralEnvironmentAcquireResult["lease"]) => Promise<() => void>;
  readonly send: (
    onStartSettled: (result: AtomCommandResult<unknown, unknown>) => void,
  ) => Promise<void>;
}) {
  // Keep waiting for the result after cancellation so a new lease can still be released.
  const { lease, reused } = await input.acquire();
  let dispatched = false;
  let restoreRoute: (() => void) | undefined;
  try {
    restoreRoute = await input.prepare(lease);
    await input.send((result) => {
      if (turnStartDisposition(result) !== "rejected") dispatched = true;
    });
    if (!dispatched) throw new Error(m(msg("Review the draft and try sending again.")));
  } finally {
    if (!dispatched) {
      try {
        restoreRoute?.();
      } finally {
        if (!reused) await input.release(lease.leaseId);
      }
    }
  }
}

/** Compare immutable user content without copying image bytes or upload metadata. */
export function composerDraftContentChanged(
  before: ComposerThreadDraftState | null,
  after: ComposerThreadDraftState | null,
): boolean {
  if (before === after) return false;
  if (!before || !after) return true;
  const sameIds = (left: ReadonlyArray<{ id: string }>, right: ReadonlyArray<{ id: string }>) =>
    left.length === right.length && left.every((item, index) => item.id === right[index]?.id);
  return (
    before.prompt !== after.prompt ||
    !sameIds(before.images, after.images) ||
    !sameIds(before.files, after.files) ||
    before.terminalContexts !== after.terminalContexts ||
    before.previewAnnotations !== after.previewAnnotations ||
    before.reviewComments !== after.reviewComments ||
    before.threadContexts !== after.threadContexts ||
    before.environmentContexts !== after.environmentContexts ||
    before.replyTo !== after.replyTo
  );
}

/** Attached threads the prompt still links to; replacing linked text leaves stale records. */
export function referencedThreadContexts(draft: {
  readonly prompt: string;
  readonly threadContexts: ReadonlyArray<ThreadContextRecord>;
}): ReadonlyArray<ThreadContextRecord> {
  if (draft.threadContexts.length === 0) return [];
  const referenced = new Set(collectInlineContextIds(draft.prompt));
  return draft.threadContexts.filter((record) => referenced.has(record.contextId));
}

/** Why a send waits for the user to pick a machine when no machine was chosen automatically. */
export function loadBalancingSendWarning(
  input: {
    readonly pending: boolean;
    readonly threadContexts: ReadonlyArray<ThreadContextRecord>;
    readonly candidateEnvironmentIds: ReadonlyArray<EnvironmentId>;
  },
  resolve: MessageResolver = m,
) {
  if (input.pending) {
    return {
      title: resolve(msg("Checking machine resources")),
      description: resolve(
        msg("Resource checks are still running. You can choose a machine in the composer."),
      ),
    };
  }
  const noMachineReadsEveryThread =
    input.threadContexts.length > 0 &&
    !input.candidateEnvironmentIds.some((environmentId) =>
      input.threadContexts.every((record) => record.environmentId === environmentId),
    );
  return {
    title: resolve(msg("Choose a machine to continue")),
    description: noMachineReadsEveryThread
      ? resolve(
          msg(
            "No machine can read every attached thread. Choose a machine in the composer or remove the threads.",
          ),
        )
      : resolve(
          msg(
            "No eligible machine has available resources. Choose a machine in the composer to override.",
          ),
        ),
  };
}

/**
 * Why a multiple-model send cannot start: the server predates worktree bootstrap, or the
 * draft is not a new thread on a Git branch with at least one model.
 */
export function multipleModelSendBlock(input: {
  readonly selectionCount: number;
  readonly serverSupportsWorktreeBootstrap: boolean;
  readonly isLocalDraftThread: boolean;
  readonly isGitRepo: boolean;
  readonly branch: string | null;
}): "server-outdated" | "needs-new-git-thread" | null {
  if (!input.serverSupportsWorktreeBootstrap) return "server-outdated";
  if (
    !input.isLocalDraftThread ||
    !input.isGitRepo ||
    !input.branch ||
    input.selectionCount === 0
  ) {
    return "needs-new-git-thread";
  }
  return null;
}

/** Why a message sent over pending approvals did not go out, or null to stop quietly. */
export function approvalRedirectNotice(
  input: {
    readonly draftChanged: boolean;
    readonly declined: boolean;
    readonly approvalResponding: boolean;
    readonly approvalArrived: boolean;
  },
  resolve: MessageResolver = m,
): string | null {
  if (input.draftChanged) {
    return input.declined
      ? resolve(msg("Approval declined. Your message changed, so it wasn't sent."))
      : resolve(msg("Your message changed, so it wasn't sent."));
  }
  if (input.approvalResponding) {
    return resolve(msg("An approval is already being answered, so your message wasn't sent."));
  }
  if (input.declined && input.approvalArrived) {
    return resolve(msg("Another approval arrived, so your message wasn't sent."));
  }
  return null;
}
