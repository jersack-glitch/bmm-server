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
| `BMM_SHARED_SECRET` | Shared secret, passed in the connector URL as `?token=`. Generate: `node -e "console.log(require('crypto').randomUUID())"` |
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

## Connect from claude.ai / Claude Desktop

Settings → Connectors → Add custom connector:

- **URL:** `https://YOUR-APP.onrender.com/mcp?token=<BMM_SHARED_SECRET>`
- **Auth:** None

Claude's connector UI only offers "None" or OAuth — there is no bearer-token / custom-header
field ([anthropics/claude-ai-mcp#112](https://github.com/anthropics/claude-ai-mcp/issues/112)).
So the secret rides in the URL and the connector is set to None. The server validates the
token before any tool runs. Tradeoff: the secret is stored in the connector config and appears
in Render request logs. OAuth 2.1 can be added later without touching the tools or transport —
it's a separate auth layer in front of the same `/mcp` endpoint.

Same URL + token for all three brokers. Kevin and Mike add it the same way in Claude Desktop,
which retires their local stdio `bmm-server.js`.

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
