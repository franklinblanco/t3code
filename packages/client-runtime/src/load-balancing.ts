import {
  resolveProjectProviderInstanceEnabled,
  type EphemeralEnvironmentLease,
  type EphemeralEnvironmentLeaseState,
  type EphemeralEnvironmentTemplate,
  type EphemeralEnvironmentTemplateId,
  type HostResourcesSnapshot,
  type LoadBalancingWeights,
  type ProjectId,
  type ProviderDriverKind,
  type ProviderInstanceId,
  type ServerConfig,
  type ServerProvider,
} from "@t3tools/contracts";

export function isRoutingProviderAvailable(input: {
  provider: ServerProvider;
  driver: ProviderDriverKind;
  instanceId: ProviderInstanceId | null;
  projectId: ProjectId | null;
  settings: ServerConfig["settings"] | undefined;
}) {
  const { provider, driver, instanceId, projectId, settings } = input;
  return (
    (instanceId === null || provider.instanceId === instanceId) &&
    provider.driver === driver &&
    (settings
      ? resolveProjectProviderInstanceEnabled(settings, projectId, provider.instanceId)
      : provider.enabled) &&
    provider.installed &&
    provider.status !== "error" &&
    provider.auth.status !== "unauthenticated" &&
    provider.availability !== "unavailable"
  );
}

/** Callers supply only connected machines hosting the project and selected provider. */
export function chooseLoadBalancedEnvironment(
  candidates: ReadonlyArray<{
    environmentId: string;
    resources: HostResourcesSnapshot | null;
    /** Client receipt time avoids comparing clocks on different machines. */
    receivedAt?: number;
    weight: number;
  }>,
  now: number,
): string | null {
  let selected: string | null = null;
  let bestScore = 0;
  for (const { environmentId, resources, receivedAt, weight } of candidates) {
    const sampledAt = receivedAt ?? resources?.sampledAt ?? 0;
    if (
      !resources ||
      !Number.isFinite(weight) ||
      weight <= 0 ||
      now - sampledAt > 15_000 ||
      sampledAt > now + 5_000 ||
      resources.cpuUtilization === null ||
      resources.cpuUtilization >= 0.95 ||
      resources.totalMemoryBytes <= 0 ||
      resources.cpuCount <= 0
    ) {
      continue;
    }
    const memoryAvailable = resources.availableMemoryBytes / resources.totalMemoryBytes;
    if (memoryAvailable <= 0.05) continue;
    const score = weight * resources.cpuCount * (1 - resources.cpuUtilization) * memoryAvailable;
    if (score > bestScore) {
      selected = environmentId;
      bestScore = score;
    }
  }
  return selected;
}

const activeLeaseStates: ReadonlySet<EphemeralEnvironmentLeaseState> = new Set([
  "provisioning",
  "ready",
  "draining",
  "releasing",
]);

/** A template needs spare capacity unless allowReuseReady is true and it has a ready lease. */
export function chooseEphemeralOverflowTemplate(input: {
  templates: ReadonlyArray<EphemeralEnvironmentTemplate>;
  activeLeases: ReadonlyArray<EphemeralEnvironmentLease>;
  repositoryKey: string | null | undefined;
  weights?: typeof LoadBalancingWeights.Type;
  allowReuseReady?: boolean;
}): {
  template: EphemeralEnvironmentTemplate;
  reusableLease: EphemeralEnvironmentLease | null;
} | null {
  const leasesByTemplate = new Map<
    EphemeralEnvironmentTemplateId,
    { count: number; readyLease: EphemeralEnvironmentLease | null }
  >();
  for (const lease of input.activeLeases) {
    if (!activeLeaseStates.has(lease.state)) continue;
    let summary = leasesByTemplate.get(lease.templateId);
    if (!summary) {
      summary = { count: 0, readyLease: null };
      leasesByTemplate.set(lease.templateId, summary);
    }
    summary.count++;
    if (lease.state === "ready") summary.readyLease ??= lease;
  }

  let selected: ReturnType<typeof chooseEphemeralOverflowTemplate> = null;
  let bestWeight = 0;
  for (const template of input.templates) {
    const override =
      input.weights && Object.hasOwn(input.weights, template.id)
        ? input.weights[template.id]
        : undefined;
    const weight = override ?? template.weight;
    if (!template.enabled || !Number.isFinite(weight) || weight <= bestWeight) continue;
    if (
      template.eligibleRepositoryKeys.length > 0 &&
      (input.repositoryKey == null ||
        !template.eligibleRepositoryKeys.includes(input.repositoryKey))
    ) {
      continue;
    }

    const summary = leasesByTemplate.get(template.id);
    const reusableLease = input.allowReuseReady ? (summary?.readyLease ?? null) : null;
    if ((summary?.count ?? 0) >= template.maxConcurrentLeases && reusableLease === null) continue;

    selected = { template, reusableLease };
    bestWeight = weight;
  }
  return selected;
}
