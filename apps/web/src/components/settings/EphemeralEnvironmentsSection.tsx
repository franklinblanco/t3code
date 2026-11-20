import { useAtomValue } from "@effect/atom-react";
import { thrownErrorMessage } from "@t3tools/client-runtime/errors";
import {
  type AtomCommandResult,
  settlePromise,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  EphemeralEnvironmentColabHardwareSchema,
  type EnvironmentId,
  type EphemeralEnvironmentColabHardware,
  type EphemeralEnvironmentLease,
  type EphemeralEnvironmentListResult,
  type EphemeralEnvironmentProvisioner,
  type EphemeralEnvironmentTemplate,
  EphemeralEnvironmentTemplateId,
} from "@t3tools/contracts";
import { msg, T, useGT, useMessages } from "gt-react";
import { useId, useState } from "react";

import { serverEnvironment } from "~/state/server";
import { connectEphemeralEnvironment } from "~/lib/ephemeralEnvironmentRouting";
import { randomUUID } from "~/lib/utils";
import { useAtomCommand } from "~/state/use-atom-command";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import {
  Dialog,
  DialogFooter,
  DialogHeader,
  DialogPanel,
  DialogPopup,
  DialogTitle,
} from "../ui/dialog";
import { Input } from "../ui/input";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { Switch } from "../ui/switch";
import { toastManager } from "../ui/toast";

export const loadPreferences = [
  { value: 100, label: msg("Prefer") },
  { value: 50, label: msg("Normal", { $context: "load balancing preference" }) },
  { value: 25, label: msg("Less often") },
  { value: 0, label: msg("Manual only") },
] as const;

export function loadPreferenceForWeight(weight: number | undefined) {
  if (weight === undefined || weight === 50) return 50;
  if (weight === 0) return 0;
  return weight < 50 ? 25 : 100;
}

const leaseStates = {
  dormant: { label: msg("Dormant"), variant: "secondary" },
  provisioning: { label: msg("Provisioning"), variant: "info" },
  ready: { label: msg("Ready"), variant: "success" },
  draining: { label: msg("Draining"), variant: "warning" },
  suspended: { label: msg("Dormant"), variant: "secondary" },
  releasing: { label: msg("Draining"), variant: "warning" },
  released: { label: msg("Dormant"), variant: "secondary" },
  failed: { label: msg("Failed"), variant: "error" },
} as const;

const colabHardwareLabels = {
  "cpu-high-mem": msg("CPU (High-RAM)"),
  cpu: msg("CPU (Standard)"),
  "gpu-t4": msg("T4 GPU"),
  "gpu-l4": msg("L4 GPU"),
  "gpu-g4": msg("G4 GPU (96 GB)"),
  "gpu-a100": msg("A100 GPU"),
  "gpu-h100": msg("H100 GPU"),
  "tpu-v5e1": msg("TPU v5e-1"),
  "tpu-v6e1": msg("TPU v6e-1"),
} satisfies Record<EphemeralEnvironmentColabHardware, ReturnType<typeof msg>>;

const providerOptions = [
  { value: "colab", label: msg("Google Colab") },
  { value: "command", label: msg("Custom command") },
] as const;

function TemplateWeightSelect({
  weight,
  disabled,
  onChange,
}: {
  weight: number;
  disabled: boolean;
  onChange: (weight: number) => void;
}) {
  const gt = useGT();
  const m = useMessages();
  const items = loadPreferences.map(({ value, label }) => ({ value, label: m(label) }));
  return (
    <Select
      items={items}
      value={loadPreferenceForWeight(weight)}
      disabled={disabled}
      onValueChange={(value) => {
        if (value !== null) onChange(value);
      }}
    >
      <SelectTrigger size="xs" aria-label={gt("Load preference")}>
        <SelectValue />
      </SelectTrigger>
      <SelectPopup align="end" alignItemWithTrigger={false}>
        {items.map(({ value, label }) => (
          <SelectItem key={value} value={value}>
            {label}
          </SelectItem>
        ))}
      </SelectPopup>
    </Select>
  );
}

function TemplateForm({
  template,
  busy,
  onClose,
  onSave,
}: {
  template: EphemeralEnvironmentTemplate | null;
  busy: boolean;
  onClose: () => void;
  onSave: (template: EphemeralEnvironmentTemplate) => Promise<void>;
}) {
  const gt = useGT();
  const m = useMessages();
  const formId = useId();
  const [weight, setWeight] = useState(template?.weight ?? 50);
  const savedProvisioner = template?.provisioner;
  const savedColab = savedProvisioner?.kind === "colab" ? savedProvisioner : undefined;
  const savedCommand = savedProvisioner?.kind === "command" ? savedProvisioner : undefined;
  const [kind, setKind] = useState<EphemeralEnvironmentProvisioner["kind"]>(
    savedProvisioner?.kind ?? "colab",
  );
  const [hardware, setHardware] = useState<EphemeralEnvironmentColabHardware>(
    savedColab?.hardware ?? "cpu-high-mem",
  );
  const [syncLocalCredentials, setSyncLocalCredentials] = useState(
    savedColab?.syncLocalCredentials ?? true,
  );
  const [label, setLabel] = useState(template?.label ?? null);
  const [provisionCommand, setProvisionCommand] = useState(savedCommand?.provisionCommand ?? "");
  const [teardownCommand, setTeardownCommand] = useState(savedCommand?.teardownCommand ?? "");
  const providers = providerOptions.map(({ value, label }) => ({ value, label: m(label) }));
  const hardwareOptions = EphemeralEnvironmentColabHardwareSchema.literals.map((value) => ({
    value,
    label: m(colabHardwareLabels[value]),
  }));
  const fields = [
    {
      name: "idle",
      label: gt("Idle timeout (minutes)"),
      type: "number",
      value: template?.idleTimeoutMinutes ?? 15,
      min: 1,
      max: 1440,
      required: true,
    },
    {
      name: "concurrency",
      label: gt("Maximum concurrent leases"),
      type: "number",
      value: template?.maxConcurrentLeases ?? 1,
      min: 1,
      max: 16,
      required: true,
    },
    {
      name: "repositories",
      label: gt("Repository keys (comma-separated)"),
      value: template?.eligibleRepositoryKeys.join(", "),
    },
  ];
  return (
    <>
      <DialogHeader>
        <DialogTitle>
          {template ? gt("Edit ephemeral environment") : gt("Add ephemeral environment")}
        </DialogTitle>
      </DialogHeader>
      <DialogPanel>
        <form
          id={formId}
          className="grid gap-3"
          onSubmit={(event) => {
            event.preventDefault();
            if (busy) return;
            const data = new FormData(event.currentTarget);
            const read = (name: string) => String(data.get(name) ?? "").trim();
            const label = read("label");
            if (!label || (kind === "command" && !provisionCommand.trim())) return;
            void onSave({
              id: template?.id ?? EphemeralEnvironmentTemplateId.make(randomUUID()),
              label,
              enabled: template?.enabled ?? true,
              weight,
              provisioner:
                kind === "colab"
                  ? {
                      kind,
                      hardware,
                      syncLocalCredentials,
                      provisionTimeoutSeconds: savedColab?.provisionTimeoutSeconds ?? 240,
                    }
                  : {
                      kind,
                      provisionCommand: provisionCommand.trim(),
                      teardownCommand: teardownCommand.trim() || null,
                      provisionTimeoutSeconds: savedCommand?.provisionTimeoutSeconds ?? 180,
                    },
              idleTimeoutMinutes: Number(read("idle")),
              maxConcurrentLeases: Number(read("concurrency")),
              eligibleRepositoryKeys: [
                ...new Set(
                  read("repositories")
                    .split(",")
                    .map((key) => key.trim())
                    .filter(Boolean),
                ),
              ],
            });
          }}
        >
          <div className="grid gap-1.5 text-sm">
            <label htmlFor={`${formId}-provider`}>{gt("Provider")}</label>
            <Select
              items={providers}
              value={kind}
              disabled={busy}
              onValueChange={(value) => {
                if (value !== null) setKind(value);
              }}
            >
              <SelectTrigger id={`${formId}-provider`}>
                <SelectValue />
              </SelectTrigger>
              <SelectPopup>
                {providers.map(({ value, label }) => (
                  <SelectItem key={value} value={value}>
                    {label}
                  </SelectItem>
                ))}
              </SelectPopup>
            </Select>
          </div>
          <label className="grid gap-1.5 text-sm">
            {gt("Label")}
            <Input
              name="label"
              value={label ?? (kind === "colab" ? "Google Colab" : "")}
              onChange={(event) => setLabel(event.target.value)}
              maxLength={80}
              required
              disabled={busy}
            />
          </label>
          {kind === "colab" ? (
            <>
              <div className="grid gap-1.5 text-sm">
                <label htmlFor={`${formId}-hardware`}>{gt("Hardware")}</label>
                <Select
                  items={hardwareOptions}
                  value={hardware}
                  disabled={busy}
                  onValueChange={(value) => {
                    if (value !== null) setHardware(value);
                  }}
                >
                  <SelectTrigger id={`${formId}-hardware`}>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectPopup>
                    {hardwareOptions.map(({ value, label }) => (
                      <SelectItem key={value} value={value}>
                        {label}
                      </SelectItem>
                    ))}
                  </SelectPopup>
                </Select>
              </div>
              <div className="flex items-center justify-between gap-3 text-sm">
                <div className="grid gap-1">
                  <label id={`${formId}-credentials-label`} htmlFor={`${formId}-credentials`}>
                    {gt("Sync local credentials")}
                  </label>
                  <p
                    id={`${formId}-credentials-description`}
                    className="text-xs text-muted-foreground"
                  >
                    <T>Forward Codex, Claude Code, GitHub, and Git credentials over SSH.</T>
                  </p>
                </div>
                <Switch
                  id={`${formId}-credentials`}
                  aria-labelledby={`${formId}-credentials-label`}
                  aria-describedby={`${formId}-credentials-description`}
                  checked={syncLocalCredentials}
                  disabled={busy}
                  onCheckedChange={setSyncLocalCredentials}
                />
              </div>
            </>
          ) : (
            <>
              <label className="grid gap-1.5 text-sm">
                {gt("Provision command")}
                <Input
                  value={provisionCommand}
                  onChange={(event) => setProvisionCommand(event.target.value)}
                  placeholder="./provision-environment"
                  required
                  disabled={busy}
                />
                <span className="text-xs text-muted-foreground">
                  <T>Write one JSON object to stdout with these fields.</T>{" "}
                  <code>{"{ httpUrl, wsUrl, allocationHandle }"}</code>
                </span>
              </label>
              <label className="grid gap-1.5 text-sm">
                {gt("Teardown command")}
                <Input
                  value={teardownCommand}
                  onChange={(event) => setTeardownCommand(event.target.value)}
                  disabled={busy}
                />
              </label>
            </>
          )}
          {fields.map(({ name, label, value, ...props }) => (
            <label key={name} className="grid gap-1.5 text-sm">
              {label}
              <Input name={name} defaultValue={value} disabled={busy} {...props} />
            </label>
          ))}
          <div className="flex items-center justify-between gap-3 text-sm">
            <T>Load preference</T>
            <TemplateWeightSelect weight={weight} disabled={busy} onChange={setWeight} />
          </div>
        </form>
      </DialogPanel>
      <DialogFooter>
        <Button variant="outline" disabled={busy} onClick={onClose}>
          <T>Cancel</T>
        </Button>
        <Button type="submit" form={formId} disabled={busy}>
          <T>Save</T>
        </Button>
      </DialogFooter>
    </>
  );
}

function isActiveLease(lease: EphemeralEnvironmentLease) {
  return (
    lease.state === "provisioning" ||
    lease.state === "ready" ||
    lease.state === "draining" ||
    lease.state === "releasing"
  );
}

export function EphemeralEnvironmentsSection({
  environmentId,
  data,
}: {
  environmentId: EnvironmentId;
  data: EphemeralEnvironmentListResult;
}) {
  const gt = useGT();
  const m = useMessages();
  const upsert = useAtomCommand(serverEnvironment.ephemeralEnvironmentsUpsertTemplate, {
    reportFailure: false,
  });
  const remove = useAtomCommand(serverEnvironment.ephemeralEnvironmentsDeleteTemplate, {
    reportFailure: false,
  });
  const acquire = useAtomCommand(serverEnvironment.ephemeralEnvironmentsAcquire, {
    reportFailure: false,
  });
  const release = useAtomCommand(serverEnvironment.ephemeralEnvironmentsRelease, {
    reportFailure: false,
  });
  const canEdit = useAtomValue(
    serverEnvironment.ephemeralEnvironmentsUpsertTemplate.permissionAtom(environmentId),
  );
  const canRemove = useAtomValue(
    serverEnvironment.ephemeralEnvironmentsDeleteTemplate.permissionAtom(environmentId),
  );
  const canStart = useAtomValue(
    serverEnvironment.ephemeralEnvironmentsAcquire.permissionAtom(environmentId),
  );
  const canRelease = useAtomValue(
    serverEnvironment.ephemeralEnvironmentsRelease.permissionAtom(environmentId),
  );
  const [editing, setEditing] = useState<{ template: EphemeralEnvironmentTemplate | null } | null>(
    null,
  );
  const [removing, setRemoving] = useState<EphemeralEnvironmentTemplate | null>(null);
  const [startPrompt, setStartPrompt] = useState<{
    template: EphemeralEnvironmentTemplate;
    repositoryKey: string;
  } | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const run = async (key: string, action: () => Promise<AtomCommandResult<unknown, unknown>>) => {
    setBusy(key);
    const outcome = await settlePromise(action);
    setBusy(null);
    const result = outcome._tag === "Success" ? outcome.value : outcome;
    if (result._tag === "Success") return true;
    toastManager.add({
      type: "error",
      title: gt("Could not update ephemeral environment"),
      description:
        thrownErrorMessage(squashAtomCommandFailure(result), m) ?? gt("An error occurred."),
    });
    return false;
  };
  const start = (template: EphemeralEnvironmentTemplate, repositoryKey?: string) =>
    run(template.id, async () => {
      const result = await acquire({
        environmentId,
        input: {
          templateId: template.id,
          clientRequestId: randomUUID(),
          ...(repositoryKey === undefined ? {} : { repositoryKey }),
        },
      });
      if (result._tag === "Failure") return result;
      const { lease } = result.value;
      if (lease.state !== "ready") {
        throw new Error(lease.lastError ?? gt("The ephemeral environment did not become ready."));
      }
      await connectEphemeralEnvironment({ lease, label: template.label });
      return result;
    });

  return (
    <div className="grid gap-3 border-t px-3 py-3 sm:px-4">
      {data.templates.map((template) => {
        const { provisioner } = template;
        const providerSummary =
          provisioner.kind === "colab"
            ? gt("{provider} · {hardware}", {
                provider: m(providerOptions[0].label),
                hardware: m(colabHardwareLabels[provisioner.hardware]),
              })
            : m(providerOptions[1].label);
        const leases = data.leases.filter((lease) => lease.templateId === template.id);
        const active = leases.filter(isActiveLease);
        const latest = leases.reduce<EphemeralEnvironmentLease | null>(
          (last, lease) => (!last || lease.updatedAt > last.updatedAt ? lease : last),
          null,
        );
        const visibleLeases = active.length > 0 ? active : [latest];
        const onStart = () => {
          const [repositoryKey] = template.eligibleRepositoryKeys;
          if (repositoryKey !== undefined && template.eligibleRepositoryKeys.length > 1) {
            setStartPrompt({ template, repositoryKey });
          } else {
            void start(template, repositoryKey);
          }
        };
        return (
          <div key={template.id} className="grid gap-2 border-b pb-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="min-w-0">
                <div className="break-words text-sm font-medium">{template.label}</div>
                <div className="text-xs text-muted-foreground">{providerSummary}</div>
                <div className="text-xs text-muted-foreground">
                  {gt("Idle timeout: {minutes} min · Lease limit: {limit}", {
                    minutes: template.idleTimeoutMinutes,
                    limit: template.maxConcurrentLeases,
                  })}
                </div>
              </div>
              <div className="flex items-center gap-2">
                <TemplateWeightSelect
                  weight={template.weight}
                  disabled={!canEdit || busy !== null}
                  onChange={(weight) =>
                    void run(template.id, () =>
                      upsert({ environmentId, input: { template: { ...template, weight } } }),
                    )
                  }
                />
                <Switch
                  aria-label={gt("Enable {environment}", { environment: template.label })}
                  checked={template.enabled}
                  disabled={!canEdit || busy !== null}
                  onCheckedChange={(enabled) =>
                    void run(template.id, () =>
                      upsert({ environmentId, input: { template: { ...template, enabled } } }),
                    )
                  }
                />
              </div>
            </div>
            {visibleLeases.map((lease) => {
              const state = leaseStates[lease?.state ?? "dormant"];
              return (
                <div
                  key={lease?.leaseId ?? "dormant"}
                  className="flex flex-wrap items-center gap-2"
                >
                  <Badge variant={state.variant}>{m(state.label)}</Badge>
                  {lease?.lastError ? (
                    <span className="break-all text-xs text-muted-foreground">
                      {lease.lastError}
                    </span>
                  ) : null}
                  {lease &&
                  ((isActiveLease(lease) && lease.state !== "releasing") ||
                    (lease.state === "failed" && lease.allocationHandle !== null)) ? (
                    <Button
                      size="xs"
                      variant="ghost"
                      disabled={!canRelease || busy !== null}
                      onClick={() =>
                        void run(template.id, () =>
                          release({ environmentId, input: { leaseId: lease.leaseId } }),
                        )
                      }
                    >
                      <T>Release</T>
                    </Button>
                  ) : null}
                </div>
              );
            })}
            <div className="flex flex-wrap items-center gap-1">
              {active.every((lease) => lease.state !== "provisioning") &&
              active.length < template.maxConcurrentLeases ? (
                <Button
                  size="xs"
                  variant="outline"
                  disabled={!canStart || !template.enabled || busy !== null}
                  onClick={onStart}
                >
                  {latest?.state === "failed" ? gt("Retry") : gt("Start")}
                </Button>
              ) : null}
              <Button
                size="xs"
                variant="ghost"
                disabled={!canEdit || busy !== null}
                onClick={() => setEditing({ template })}
              >
                <T>Edit</T>
              </Button>
              <Button
                size="xs"
                variant="ghost"
                disabled={!canRemove || busy !== null}
                onClick={() => setRemoving(template)}
              >
                <T>Remove</T>
              </Button>
            </div>
          </div>
        );
      })}
      <div>
        <Button
          variant="outline"
          size="sm"
          disabled={!canEdit || busy !== null}
          onClick={() => setEditing({ template: null })}
        >
          <T>Add ephemeral environment</T>
        </Button>
      </div>
      <Dialog
        open={editing !== null}
        onOpenChange={(open) => {
          if (!open && busy === null) setEditing(null);
        }}
      >
        {editing ? (
          <DialogPopup>
            <TemplateForm
              template={editing.template}
              busy={busy !== null}
              onClose={() => setEditing(null)}
              onSave={async (template) => {
                if (await run(template.id, () => upsert({ environmentId, input: { template } })))
                  setEditing(null);
              }}
            />
          </DialogPopup>
        ) : null}
      </Dialog>
      <Dialog
        open={startPrompt !== null}
        onOpenChange={(open) => {
          if (!open && busy === null) setStartPrompt(null);
        }}
      >
        {startPrompt ? (
          <DialogPopup>
            <DialogHeader>
              <DialogTitle>{gt("Choose a repository")}</DialogTitle>
            </DialogHeader>
            <DialogPanel>
              <Select
                items={startPrompt.template.eligibleRepositoryKeys.map((key) => ({
                  value: key,
                  label: key,
                }))}
                value={startPrompt.repositoryKey}
                onValueChange={(repositoryKey) => {
                  if (repositoryKey !== null) setStartPrompt({ ...startPrompt, repositoryKey });
                }}
              >
                <SelectTrigger aria-label={gt("Repository")}>
                  <SelectValue />
                </SelectTrigger>
                <SelectPopup>
                  {startPrompt.template.eligibleRepositoryKeys.map((key) => (
                    <SelectItem key={key} value={key}>
                      {key}
                    </SelectItem>
                  ))}
                </SelectPopup>
              </Select>
            </DialogPanel>
            <DialogFooter>
              <Button
                variant="outline"
                disabled={busy !== null}
                onClick={() => setStartPrompt(null)}
              >
                <T>Cancel</T>
              </Button>
              <Button
                disabled={busy !== null || !canStart}
                onClick={() => {
                  setStartPrompt(null);
                  void start(startPrompt.template, startPrompt.repositoryKey);
                }}
              >
                <T>Start</T>
              </Button>
            </DialogFooter>
          </DialogPopup>
        ) : null}
      </Dialog>
      <Dialog
        open={removing !== null}
        onOpenChange={(open) => {
          if (!open && busy === null) setRemoving(null);
        }}
      >
        {removing ? (
          <DialogPopup>
            <DialogHeader>
              <DialogTitle>
                {gt("Remove {environment}?", { environment: removing.label })}
              </DialogTitle>
            </DialogHeader>
            <DialogFooter>
              <Button variant="outline" disabled={busy !== null} onClick={() => setRemoving(null)}>
                <T>Cancel</T>
              </Button>
              <Button
                variant="destructive"
                disabled={busy !== null}
                onClick={async () => {
                  if (
                    await run(removing.id, () =>
                      remove({ environmentId, input: { templateId: removing.id } }),
                    )
                  )
                    setRemoving(null);
                }}
              >
                <T>Remove</T>
              </Button>
            </DialogFooter>
          </DialogPopup>
        ) : null}
      </Dialog>
    </div>
  );
}
