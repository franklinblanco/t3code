import { resolveEnvironmentMachineKind, type EnvironmentId } from "@t3tools/contracts";
import { msg, T, useGT, useMessages } from "gt-react";
import { AsyncResult } from "effect/reactivity";

import {
  useClientSettings,
  useClientSettingsHydrated,
  updateClientSettings,
} from "~/hooks/useSettings";
import type { MessageResolver } from "~/i18n/messages";
import type { EnvironmentPresentation } from "~/state/environments";
import { useEphemeralEnvironments } from "~/state/ephemeralEnvironments";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { Switch } from "../ui/switch";
import { EnvironmentRow, EnvironmentTransportLabel } from "./EnvironmentRow";
import { FoldedSettingsSection } from "./FoldedSettingsSection";
import {
  EphemeralEnvironmentsSection,
  loadPreferenceForWeight,
  loadPreferences,
} from "./EphemeralEnvironmentsSection";
import { useSearchableSetting } from "./settingsSearch";
import { useSettingsSearchTarget } from "./settingsLayout";

const summaryLabels = {
  100: msg("{environment} prefer"),
  25: msg("{environment} less often"),
  0: msg("{environment} manual only"),
} as const;

function summarizeLoadPreferences(
  environments: ReadonlyArray<Pick<EnvironmentPresentation, "environmentId" | "label">>,
  weights: Readonly<Record<string, number>>,
  resolve: MessageResolver,
): string | null {
  const parts = environments.flatMap((environment) => {
    const preference = loadPreferenceForWeight(weights[environment.environmentId]);
    return preference === 50
      ? []
      : [resolve(summaryLabels[preference], { environment: environment.label })];
  });
  return parts.length === 0 ? null : parts.join(" · ");
}

export function LoadBalancingSettings({
  environments,
  controllerId,
}: {
  environments: ReadonlyArray<EnvironmentPresentation>;
  controllerId: EnvironmentId | null;
}) {
  const searchableSetting = useSearchableSetting();
  const settings = useClientSettings((current) => ({
    loadBalancingEnabled: current.loadBalancingEnabled,
    loadBalancingWeights: current.loadBalancingWeights,
  }));
  const settingsHydrated = useClientSettingsHydrated();
  const gt = useGT();
  const m = useMessages();
  const items = loadPreferences.map((entry) => ({ ...entry, label: m(entry.label) }));
  const ephemeralData = AsyncResult.getOrElse(useEphemeralEnvironments(controllerId), () => null);
  const { id, title } = searchableSetting("load-balancing");
  const entryTargetRef = useSettingsSearchTarget<HTMLElement>(id);
  const ephemeralSection =
    controllerId !== null && ephemeralData !== null ? (
      <EphemeralEnvironmentsSection
        key={controllerId}
        environmentId={controllerId}
        data={ephemeralData}
      />
    ) : null;
  if (environments.length < 2 && !ephemeralData?.templates.length) {
    return ephemeralSection ? (
      <section id={id} ref={entryTargetRef} tabIndex={-1}>
        {ephemeralSection}
      </section>
    ) : null;
  }
  return (
    <FoldedSettingsSection
      id={id}
      title={title}
      summary={
        settings.loadBalancingEnabled
          ? summarizeLoadPreferences(environments, settings.loadBalancingWeights, m)
          : gt("Off", { $context: "load balancing is turned off" })
      }
      control={
        <Switch
          aria-label={gt("Automatically balance load")}
          checked={settings.loadBalancingEnabled}
          disabled={!settingsHydrated}
          onCheckedChange={(loadBalancingEnabled) => updateClientSettings({ loadBalancingEnabled })}
        />
      }
    >
      <p className="px-3 py-2.5 text-xs text-muted-foreground sm:px-4">
        <T>
          New threads in shared projects start on the machine with the most free CPU and memory,
          weighted by each machine's preference.
        </T>
      </p>
      {environments.map((environment) => (
        <EnvironmentRow
          key={environment.environmentId}
          kind={resolveEnvironmentMachineKind(environment.serverConfig)}
          label={environment.label}
          subtitle={<EnvironmentTransportLabel environment={environment} />}
        >
          <Select
            items={items}
            value={loadPreferenceForWeight(
              settings.loadBalancingWeights[environment.environmentId],
            )}
            disabled={!settingsHydrated || !settings.loadBalancingEnabled}
            onValueChange={(value) => {
              if (value === null) return;
              updateClientSettings({
                loadBalancingWeights: {
                  ...settings.loadBalancingWeights,
                  [environment.environmentId]: value,
                },
              });
            }}
          >
            <SelectTrigger
              size="xs"
              className="w-32"
              aria-label={gt("{environment} load preference", { environment: environment.label })}
            >
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
        </EnvironmentRow>
      ))}
      {ephemeralSection}
    </FoldedSettingsSection>
  );
}
