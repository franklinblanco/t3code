import {
  CommandId,
  EnvironmentId,
  EphemeralEnvironmentLeaseId,
  EphemeralEnvironmentTemplateId,
  OrchestrationV2DispatchCommandError,
  OrchestrationV2ThreadLaunchError,
  ProjectId,
  ThreadId,
  type EphemeralEnvironmentLease,
} from "@t3tools/contracts";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import * as Cause from "effect/Cause";
import * as Schema from "effect/Schema";
import { AsyncResult } from "effect/reactivity";
import { RpcClientError } from "effect/rpc";
import { describe, expect, it, vi } from "vite-plus/test";
import type { ComposerThreadDraftState } from "../../composerDraftStore";
import { formatInlineContextReference } from "../../lib/composerContextReferences";
import { threadContextReference } from "../../lib/composerContextRecords";
import { threadContextRecord } from "@t3tools/shared/composerContextReferences";
import {
  approvalRedirectNotice,
  composerDraftContentChanged,
  loadBalancingSendWarning,
  multipleModelSendBlock,
  referencedThreadContexts,
  turnStartDisposition,
  withEphemeralLease,
} from "./composerSend.logic";

const local = EnvironmentId.make("local");
const remote = EnvironmentId.make("remote");
const localThread = threadContextRecord(scopeThreadRef(local, ThreadId.make("a")), "Local");
const remoteThread = threadContextRecord(scopeThreadRef(remote, ThreadId.make("b")), "Remote");

const encodeLaunchError = Schema.encodeSync(OrchestrationV2ThreadLaunchError);
const decodeLaunchError = Schema.decodeSync(OrchestrationV2ThreadLaunchError);
const launchError = (name: string) => {
  const error = new OrchestrationV2ThreadLaunchError({
    commandId: CommandId.make("launch"),
    projectId: ProjectId.make("project"),
    message: "Could not launch the thread",
    cause: Object.assign(new Error("Launch failed"), { name }),
  });
  return decodeLaunchError(encodeLaunchError(error));
};

const dispatchError = (name: string) =>
  new OrchestrationV2DispatchCommandError({
    commandId: CommandId.make("dispatch"),
    commandType: "message.dispatch",
    message: "Could not dispatch the message",
    cause: Object.assign(new Error("Dispatch failed"), { name }),
  });

const rejectedStart = AsyncResult.failure(Cause.fail(launchError("ThreadLaunchBaseRefError")));

it.each([
  { name: "accepted", result: AsyncResult.success(undefined), expected: "accepted" },
  { name: "rejected", result: rejectedStart, expected: "rejected" },
  {
    name: "dispatch rejection",
    result: AsyncResult.failure(Cause.fail(dispatchError("OrchestratorCommandRejectedError"))),
    expected: "rejected",
  },
  {
    name: "dispatch error that can occur after commit",
    result: AsyncResult.failure(Cause.fail(dispatchError("OrchestratorDispatchError"))),
    expected: "uncertain",
  },
  {
    name: "lost reply",
    result: AsyncResult.failure(
      Cause.fail(
        new RpcClientError.RpcClientError({
          reason: new RpcClientError.RpcClientDefect({ message: "Connection lost", cause: null }),
        }),
      ),
    ),
    expected: "uncertain",
  },
  {
    name: "interrupted",
    result: AsyncResult.failure(Cause.interrupt()),
    expected: "uncertain",
  },
  {
    name: "launch error that can occur after commit",
    result: AsyncResult.failure(Cause.fail(launchError("ThreadLaunchError"))),
    expected: "uncertain",
  },
])("classifies an overflow turn start with a $name result", ({ result, expected }) => {
  expect(turnStartDisposition(result)).toBe(expected);
});

it.each([
  {
    outcome: "accepted",
    reused: false,
    result: AsyncResult.success(undefined),
    restores: false,
    releases: false,
  },
  { outcome: "rejected", reused: false, result: rejectedStart, restores: true, releases: true },
  { outcome: "rejected", reused: true, result: rejectedStart, restores: true, releases: false },
  {
    outcome: "uncertain",
    reused: false,
    result: AsyncResult.failure(Cause.interrupt()),
    restores: false,
    releases: false,
  },
])(
  "cleans up a $outcome overflow send (reused=$reused)",
  async ({ reused, result, restores, releases }) => {
    const lease: EphemeralEnvironmentLease = {
      leaseId: EphemeralEnvironmentLeaseId.make("lease"),
      templateId: EphemeralEnvironmentTemplateId.make("overflow"),
      generation: 1,
      state: "ready",
      allocationHandle: "sandbox",
      environmentId: remote,
      httpUrl: "https://overflow.example",
      wsUrl: "wss://overflow.example",
      bearerToken: null,
      pairingToken: null,
      workspaceRoot: null,
      clientRequestId: "request",
      lastError: null,
      createdAt: "2026-11-10T01:00:00.000Z",
      updatedAt: "2026-11-10T01:00:00.000Z",
      idleDeadlineAt: null,
    };
    const restoreRoute = vi.fn();
    const release = vi.fn(async () => {});
    const send = withEphemeralLease({
      acquire: async () => ({ lease, reused }),
      release,
      prepare: async () => restoreRoute,
      send: async (onStartSettled) => {
        onStartSettled(result);
      },
    });
    if (restores) await expect(send).rejects.toThrow("Review the draft and try sending again.");
    else await expect(send).resolves.toBeUndefined();
    expect(restoreRoute).toHaveBeenCalledTimes(restores ? 1 : 0);
    if (releases) expect(release).toHaveBeenCalledExactlyOnceWith(lease.leaseId);
    else expect(release).not.toHaveBeenCalled();
  },
);

it("detects draft edits during upload preparation without treating upload metadata as an edit", () => {
  const draft: ComposerThreadDraftState = {
    prompt: "before",
    images: [],
    files: [
      {
        type: "file",
        id: "file",
        name: "a.pdf",
        mimeType: "application/pdf",
        sizeBytes: 1,
        file: null,
      },
    ],
    nonPersistedImageIds: [],
    persistedAttachments: [],
    terminalContexts: [],
    previewAnnotations: [],
    reviewComments: [],
    threadContexts: [],
    environmentContexts: [],
    replyTo: null,
    modelSelectionByProvider: {},
    activeProvider: null,
    runtimeMode: null,
    interactionMode: null,
  };
  expect(
    composerDraftContentChanged(draft, {
      ...draft,
      files: [{ ...draft.files[0]!, uploadedAttachmentId: "upload", uploadEnvironmentId: local }],
    }),
  ).toBe(false);
  expect(composerDraftContentChanged(draft, { ...draft, prompt: "after" })).toBe(true);
  expect(composerDraftContentChanged(draft, { ...draft, files: [] })).toBe(true);
  expect(composerDraftContentChanged(draft, { ...draft, threadContexts: [localThread] })).toBe(
    true,
  );
});

describe("referencedThreadContexts", () => {
  it("keeps only the attached threads the prompt still links", () => {
    const prompt = `see ${formatInlineContextReference(threadContextReference(remoteThread))}`;
    expect(
      referencedThreadContexts({ prompt, threadContexts: [localThread, remoteThread] }),
    ).toEqual([remoteThread]);
  });
});

describe("loadBalancingSendWarning", () => {
  it("asks the user to wait while resource checks run", () => {
    expect(
      loadBalancingSendWarning({
        pending: true,
        threadContexts: [localThread, remoteThread],
        candidateEnvironmentIds: [],
      }).title,
    ).toBe("Checking machine resources");
  });

  it("blames the attached threads when no machine can read all of them", () => {
    expect(
      loadBalancingSendWarning({
        pending: false,
        threadContexts: [localThread, remoteThread],
        candidateEnvironmentIds: [local, remote],
      }).description,
    ).toMatch(/No machine can read every attached thread/);
  });

  it("blames resources when one machine can read every attached thread", () => {
    expect(
      loadBalancingSendWarning({
        pending: false,
        threadContexts: [localThread],
        candidateEnvironmentIds: [remote, local],
      }).description,
    ).toMatch(/No eligible machine has available resources/);
  });
});

describe("multipleModelSendBlock", () => {
  const ready = {
    selectionCount: 2,
    serverSupportsWorktreeBootstrap: true,
    isLocalDraftThread: true,
    isGitRepo: true,
    branch: "main",
  };

  it("lets a new Git draft with a branch and models start", () => {
    expect(multipleModelSendBlock(ready)).toBeNull();
  });

  it("asks for a server update before checking the draft", () => {
    expect(
      multipleModelSendBlock({
        ...ready,
        serverSupportsWorktreeBootstrap: false,
        isGitRepo: false,
      }),
    ).toBe("server-outdated");
  });

  it.each([
    { isLocalDraftThread: false },
    { isGitRepo: false },
    { branch: null },
    { selectionCount: 0 },
  ])("needs a new Git thread with a branch and models (%o)", (override) => {
    expect(multipleModelSendBlock({ ...ready, ...override })).toBe("needs-new-git-thread");
  });
});

describe("approvalRedirectNotice", () => {
  const settled = {
    draftChanged: false,
    declined: true,
    approvalResponding: false,
    approvalArrived: false,
  };

  it("says the decline went through when the draft changed after it", () => {
    expect(approvalRedirectNotice({ ...settled, draftChanged: true })).toBe(
      "Approval declined. Your message changed, so it wasn't sent.",
    );
    expect(approvalRedirectNotice({ ...settled, draftChanged: true, declined: false })).toBe(
      "Your message changed, so it wasn't sent.",
    );
  });

  it("explains an approval already being answered", () => {
    expect(
      approvalRedirectNotice({ ...settled, declined: false, approvalResponding: true }),
    ).toMatch(/already being answered/);
  });

  it("explains an approval that arrived during the decline", () => {
    expect(approvalRedirectNotice({ ...settled, approvalArrived: true })).toMatch(
      /Another approval arrived/,
    );
  });

  it("stays quiet when the decline failed or the user left the thread", () => {
    expect(approvalRedirectNotice({ ...settled, declined: false })).toBeNull();
    expect(approvalRedirectNotice(settled)).toBeNull();
  });
});
