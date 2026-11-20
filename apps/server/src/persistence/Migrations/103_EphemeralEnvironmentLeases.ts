import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Lease tokens are stored in ServerSecretStore. The lease ID names the secret.
  yield* sql`
    CREATE TABLE ephemeral_environment_leases (
      lease_id TEXT PRIMARY KEY NOT NULL,
      template_id TEXT NOT NULL,
      generation INTEGER NOT NULL,
      state TEXT NOT NULL,
      allocation_handle TEXT,
      environment_id TEXT,
      http_url TEXT,
      ws_url TEXT,
      workspace_root TEXT,
      client_request_id TEXT,
      caller_id TEXT NOT NULL,
      repository_key TEXT,
      last_error TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      idle_deadline_at TEXT,
      idle_timeout_minutes INTEGER NOT NULL,
      teardown_command TEXT,
      teardown_timeout_seconds INTEGER NOT NULL,
      teardown_pending INTEGER NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX idx_ephemeral_environment_leases_template_state
    ON ephemeral_environment_leases (template_id, state)
  `;
  yield* sql`
    CREATE INDEX idx_ephemeral_environment_leases_state_deadline
    ON ephemeral_environment_leases (state, idle_deadline_at)
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_ephemeral_environment_leases_active_request
    ON ephemeral_environment_leases (
      template_id, client_request_id, caller_id, (repository_key IS NULL), COALESCE(repository_key, '')
    )
    WHERE client_request_id IS NOT NULL AND state IN ('provisioning', 'ready')
  `;
});
