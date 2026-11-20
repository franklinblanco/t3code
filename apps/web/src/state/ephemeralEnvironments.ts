import { useAtomValue } from "@effect/atom-react";
import { connectionRoutes, type ConnectionCatalogEntry } from "@t3tools/client-runtime/connection";
import type { EnvironmentId, EphemeralEnvironmentListResult } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult, Atom } from "effect/reactivity";
import type { EnvironmentPresentation } from "./environments";
import { serverEnvironment } from "./server";

export const EPHEMERAL_CONNECTION_ID_PREFIX = "ephemeral:";

export function isEphemeralConnection(entry: ConnectionCatalogEntry): boolean {
  const savedRoutes = connectionRoutes(entry).filter(
    ({ profile }) =>
      !Option.exists(
        profile,
        (route) => route._tag === "BearerConnectionProfile" && route.learned === true,
      ),
  );
  return (
    savedRoutes.length > 0 &&
    savedRoutes.every(
      ({ target }) =>
        target._tag === "BearerConnectionTarget" &&
        target.connectionId.startsWith(EPHEMERAL_CONNECTION_ID_PREFIX),
    )
  );
}

export function selectEphemeralControllerId(
  primaryId: EnvironmentId | null,
  environments: ReadonlyArray<EnvironmentPresentation>,
): EnvironmentId | null {
  return (
    primaryId ??
    environments.find(
      ({ connection, entry }) =>
        entry.enabled && connection.phase !== "unsupported" && !isEphemeralConnection(entry),
    )?.environmentId ??
    null
  );
}

const emptyEphemeralEnvironments = Atom.make<
  AsyncResult.AsyncResult<EphemeralEnvironmentListResult, unknown>
>(AsyncResult.initial());

export function useEphemeralEnvironments(environmentId: EnvironmentId | null) {
  return useAtomValue(
    environmentId === null
      ? emptyEphemeralEnvironments
      : serverEnvironment.ephemeralEnvironmentsList({ environmentId, input: {} }),
  );
}
