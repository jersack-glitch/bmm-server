# BMM Remote Deployment — Railway
## One-time setup

### 1. Create GitHub repo
```bash
cd /Users/jeremysacker/ghl-mcp-server
git init   # if not already a git repo
git add bmm-server-http.js package.json Procfile .gitignore
git commit -m "Add Railway deployment files"
```
Create a new repo at github.com (name: bmm-server or similar).
Push:
```bash
git remote add origin https://github.com/YOUR_USERNAME/bmm-server.git
git push -u origin main
```

### 2. Deploy to Railway
1. Go to railway.app — sign up/log in with GitHub
2. New Project → Deploy from GitHub repo → select your repo
3. Railway auto-detects Node.js and runs `npm start`

### 3. Set environment variables in Railway
In your Railway project → Variables tab, add:

| Variable | Value |
|---|---|
| `BMM_DATABASE_URL` | Your full Supabase transaction pooler URL (same as .env.bmm) |
| `BMM_SHARED_SECRET` | Generate one: `node -e "console.log(require('crypto').randomUUID())"` |
| `NODE_ENV` | `production` |

Railway auto-sets PORT — don't add that one.

### 4. Get your Railway URL
After deploy succeeds: Settings → Domains → copy the URL
It will look like: `https://bmm-server-production-xxxx.up.railway.app`

Test it:
```
https://YOUR-APP.railway.app/health
```
Should return: `{"status":"ok","service":"bmm-server",...}`

### 5. Update Claude Desktop config on ALL machines
Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

Replace the local bmm entry:
```json
"bmm": {
  "command": "node",
  "args": ["/Users/jeremysacker/ghl-mcp-server/bmm-server.js"]
}
```

With the remote entry:
```json
"bmm": {
  "url": "https://YOUR-APP.railway.app/sse?token=YOUR_BMM_SHARED_SECRET"
}
```

For Kevin and Mike — same config, same URL, same token.
Restart Claude Desktop after saving.

### 6. Verify
Open a new Claude Desktop conversation and ask:
"Search BMM contacts for test"

You should get results (or empty array). The local server on your main machine
can be left running or decommissioned — the remote server is now the source of truth.

---
## Ongoing

- **Updates**: push to GitHub → Railway auto-redeploys
- **Logs**: Railway dashboard → Deployments → View Logs
- **Cost**: Free tier to start. Upgrade to Developer ($5/mo) if you need always-on
  without cold starts. You'll know if it's needed — cold start shows as a ~10 second
  delay on first morning query.
