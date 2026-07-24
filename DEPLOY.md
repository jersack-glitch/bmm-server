# BMM Remote MCP Server — Render Deployment

Remote MCP server for the BMM brokerage operating system. Speaks **Streamable HTTP**
(single `/mcp` endpoint), connectable from claude.ai as a Custom Connector. The local
stdio server (`bmm-server.js` in the ghl-mcp-server repo) is unchanged and still used by
Kevin/Mike via the Desktop installer — this is the remote counterpart.

## Transport

Streamable HTTP, stateless. SSE (the old `/sse` + `/messages` transport) is deprecated and
not used here. claude.ai web Custom Connectors default to Streamable HTTP.

## Environment variables (set in Render dashboard, never commit)

| Variable | Value |
|---|---|
| `BMM_DATABASE_URL` | Supabase transaction pooler connection string (project `eejewpmmztyhwyaeoseo`, port 6543). Example: `postgresql://USER:PASSWORD@aws-0-us-west-2.pooler.supabase.com:6543/postgres` |
| `BMM_SHARED_SECRET` | **Legacy, optional.** Old single shared secret; still honored so existing connectors keep working. Unset once every broker has a per-identity token (below). |
| `BMM_DATABASE_CA_CERT` | Optional. Supabase CA certificate (PEM or base64 of it) — when set, DB TLS is fully verified. Download: Supabase dashboard → Settings → Database → SSL. |
| `NODE_ENV` | `production` (skips local `.env.bmm` loading) |
| `PORT` | Auto-set by Render — do not add manually |

## Deploy

1. Push this repo to GitHub (`jersack-glitch/bmm-server`).
2. Render dashboard → New → Web Service → connect the repo. Render reads `render.yaml`
   (Node runtime, `npm install` / `npm start`, health check `/health`).
3. Set `BMM_DATABASE_URL` and `BMM_SHARED_SECRET` in the Environment tab (both `sync: false`
   in the blueprint, so Render prompts for them).
4. Deploy. Verify health:
   ```
   curl https://YOUR-APP.onrender.com/health
   ```
   Returns `{"status":"ok","service":"bmm-server","transport":"streamable-http",...}`.

## Auth: per-broker tokens

Every broker gets their own token — sha256-hashed at rest, with per-token tool scopes and
optional expiry, individually revocable, and every tool call lands in `mcp_audit_log` with
the broker's identity. The old single `BMM_SHARED_SECRET` is still honored (nothing breaks
on deploy) until you unset it.

**One-time setup:** run `supabase/mcp_auth.sql` in the Supabase SQL editor (creates
`mcp_token` + `mcp_audit_log`).

**Mint one token per broker** (locally, using `.env.bmm`):

```bash
node manage-tokens.js create jeremy                          # full access, raw_query included
node manage-tokens.js create kevin --scopes '*,!raw_query'   # everything except direct SQL
node manage-tokens.js create mike  --scopes '*,!raw_query'
node manage-tokens.js list
node manage-tokens.js revoke kevin                           # effective within 30s, others unaffected
node manage-tokens.js audit --limit 50                       # who called what, when
node manage-tokens.js audit --identity mike                  # one broker's trail
```

Scopes: `*` = all tools, bare names allowlist, `!name` denies even against `*`. A revoked or
expired token stops working within 30 seconds (server-side cache TTL) — no redeploy, and the
other brokers' tokens are untouched.

**Migration from the shared secret:** deploy this version → run the SQL → mint tokens →
update each broker's connector URL → unset `BMM_SHARED_SECRET` in Render. In production the
server fails closed: no valid credential, no access (local dev without `BMM_SHARED_SECRET`
stays open for convenience).

## Connect from claude.ai / Claude Desktop

Settings → Connectors → Add custom connector (each broker uses **their own** token):

- **URL:** `https://YOUR-APP.onrender.com/mcp?token=<that broker's token>`
- **Auth:** None

Claude's connector UI only offers "None" or OAuth — there is no bearer-token / custom-header
field ([anthropics/claude-ai-mcp#112](https://github.com/anthropics/claude-ai-mcp/issues/112)).
So the token rides in the URL and the connector is set to None. The server validates the
token before any tool runs. Tradeoff: the token is stored in the connector config and appears
in Render request logs — which is why tokens are per-broker and individually revocable.
Clients that support headers can send `Authorization: Bearer <token>` instead. OAuth 2.1 can
be added later without touching the tools or transport — it's a separate auth layer in front
of the same `/mcp` endpoint.

Kevin and Mike add it the same way in Claude Desktop, which retires their local stdio
`bmm-server.js` (and with it, the shared DB credentials on their machines).

## Local smoke test

```bash
npm install
# create .env.bmm with BMM_DATABASE_URL and BMM_SHARED_SECRET (gitignored)
npm start
curl localhost:3000/health
# auth check: bad token -> 401, good token -> MCP initialize response
```

## Ongoing

- **Updates:** push to GitHub → Render auto-redeploys.
- **Logs:** Render dashboard → service → Logs.
- **Cold starts:** free/starter tier sleeps after inactivity; first morning request may lag
  ~30s. Upgrade the plan if always-on is needed.
