import { describe, expect, it } from "vite-plus/test";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

import { AssetCreateUrlResult, AttachmentCreateUploadUrlResult } from "./assets.ts";
import { AuthSettingsWriteScope, AuthTerminalOperateScope } from "./auth.ts";
import { clientRpcRequiredScopes } from "./clientRpcPermissions.ts";
import { EphemeralEnvironmentReleaseInput } from "./ephemeralEnvironment.ts";
import { ORCHESTRATION_V2_WS_METHODS } from "./orchestrationV2.ts";
import { WS_METHODS, WsRpcGroup } from "./rpc.ts";

describe("WebSocket RPC contracts", () => {
  it("only accepts asset and upload URLs on their own server routes", () => {
    const decodeAsset = Schema.decodeUnknownExit(AssetCreateUrlResult);
    const decodeUpload = Schema.decodeUnknownExit(AttachmentCreateUploadUrlResult);
    const expiresAt = 10_000;

    expect(
      Exit.isSuccess(
        decodeAsset({ relativeUrl: "/api/assets/token.sig/my%20file.png", expiresAt }),
      ),
    ).toBe(true);
    expect(
      Exit.isSuccess(
        decodeUpload({
          relativeUrl: "/api/attachments/upload/token.sig",
          expiresAt,
          attachmentId: "attachment",
        }),
      ),
    ).toBe(true);

    for (const relativeUrl of [
      "data:text/plain,hello",
      "file:///tmp/document",
      "https://other.example/document",
      "//other.example/document",
      "/api/assets/token/../../../other",
    ]) {
      expect(Exit.isFailure(decodeAsset({ relativeUrl, expiresAt }))).toBe(true);
      expect(
        Exit.isFailure(decodeUpload({ relativeUrl, expiresAt, attachmentId: "attachment" })),
      ).toBe(true);
    }
  });

  it("rejects server-internal commands sent to dispatchCommand", () => {
    const dispatchCommand = WsRpcGroup.requests.get(ORCHESTRATION_V2_WS_METHODS.dispatchCommand);
    if (dispatchCommand === undefined) throw new Error("dispatchCommand is not registered");
    const decode = Schema.decodeUnknownExit(dispatchCommand.payloadSchema);

    expect(
      Exit.isFailure(
        decode({
          type: "checkpoint.rollback.fail",
          commandId: "forged-rollback-failure",
          threadId: "thread-1",
          requestId: "rollback-1",
          message: "Forged failure.",
        }),
      ),
    ).toBe(true);
    expect(
      Exit.isSuccess(
        decode({
          type: "checkpoint.rollback",
          commandId: "rollback-1",
          threadId: "thread-1",
          scopeId: "scope-1",
          checkpointId: "checkpoint-1",
        }),
      ),
    ).toBe(true);
  });

  it("lets the server pick a new worktree's folder", () => {
    const createWorktree = WsRpcGroup.requests.get(WS_METHODS.vcsCreateWorktree);
    if (createWorktree === undefined) throw new Error("vcs.createWorktree is not registered");
    const decode = Schema.decodeUnknownExit(createWorktree.payloadSchema);
    const request = { cwd: "/repo", refName: "main", newRefName: "feature/x" };

    expect(Exit.isSuccess(decode({ ...request, path: null }))).toBe(true);
    expect(Exit.isFailure(decode({ ...request, path: "/repo/.git/hooks" }))).toBe(true);
  });

  it.each([
    WS_METHODS.ephemeralEnvironmentsUpsertTemplate,
    WS_METHODS.ephemeralEnvironmentsDeleteTemplate,
  ])("requires settings and terminal scopes for %s", (method) => {
    expect(clientRpcRequiredScopes(method, undefined)).toEqual([
      AuthSettingsWriteScope,
      AuthTerminalOperateScope,
    ]);
  });

  it("drops the unused force option from ephemeral release requests", () => {
    const decode = Schema.decodeUnknownSync(EphemeralEnvironmentReleaseInput);
    expect(decode({ leaseId: "lease-1", force: true })).toEqual({ leaseId: "lease-1" });
  });

  it("returns an ephemeral lease before the provisioner has connection details", () => {
    const acquire = WsRpcGroup.requests.get(WS_METHODS.ephemeralEnvironmentsAcquire);
    if (acquire === undefined) throw new Error("ephemeralEnvironments.acquire is not registered");
    const decode = Schema.decodeUnknownSync(acquire.successSchema);
    const lease = {
      leaseId: " lease-1 ",
      templateId: "worker",
      generation: 0,
      state: "provisioning",
      allocationHandle: null,
      environmentId: null,
      httpUrl: null,
      wsUrl: null,
      bearerToken: null,
      pairingToken: null,
      workspaceRoot: null,
      clientRequestId: "request-1",
      lastError: null,
      createdAt: "2026-07-01T00:00:00.000Z",
      updatedAt: "2026-07-01T00:00:00.000Z",
      idleDeadlineAt: null,
    };

    expect(decode({ lease, reused: false })).toEqual({
      lease: { ...lease, leaseId: "lease-1" },
      reused: false,
    });
    expect(() => decode({ lease: { ...lease, generation: -1 }, reused: false })).toThrow();
  });
});
