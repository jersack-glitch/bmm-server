-- MCP per-identity token auth + audit log (BMM).
-- Run once in the Supabase SQL editor (BMM project). Server-side access only —
-- these tables are read/written by the MCP server over the pooler connection,
-- never exposed through PostgREST, so no RLS policies are defined.

CREATE TABLE IF NOT EXISTS mcp_token (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  label       text NOT NULL,                 -- who this token identifies: "jeremy", "kevin", "mike"
  token_hash  text NOT NULL UNIQUE,          -- sha256 hex of the raw token; the raw token is never stored
  scopes      text[] NOT NULL DEFAULT '{*}', -- '*' = all tools, bare names allowlist, '!name' denies
                                             -- even against '*': broker default is '{*,!raw_query}'
  created_at  timestamptz NOT NULL DEFAULT now(),
  expires_at  timestamptz,                   -- null = no expiry
  revoked_at  timestamptz,                   -- null = live
  last_used_at timestamptz
);

CREATE TABLE IF NOT EXISTS mcp_audit_log (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  at          timestamptz NOT NULL DEFAULT now(),
  token_id    uuid REFERENCES mcp_token(id),
  identity    text NOT NULL,                 -- token label, or 'legacy-shared-secret' / 'open-dev'
  tool        text NOT NULL,
  args        jsonb,
  ok          boolean NOT NULL,
  error       text,
  duration_ms integer
);

CREATE INDEX IF NOT EXISTS mcp_audit_log_at_idx    ON mcp_audit_log (at DESC);
CREATE INDEX IF NOT EXISTS mcp_audit_log_token_idx ON mcp_audit_log (token_id, at DESC);
