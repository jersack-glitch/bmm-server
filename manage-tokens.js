#!/usr/bin/env node
// manage-tokens.js — mint, list, revoke, and audit MCP access tokens.
//
// Uses the same env as the server (BMM_DATABASE_URL; .env.bmm locally).
// Run supabase/mcp_auth.sql once before first use.
//
//   node manage-tokens.js create <label> [--scopes tool1,tool2] [--expires 90d|2027-01-01]
//   node manage-tokens.js list
//   node manage-tokens.js revoke <id-or-label>
//   node manage-tokens.js audit [--limit 50] [--identity <label>]
//
// Scopes: '*' = all tools, bare names allowlist, '!name' denies even against '*'.
// Broker default: --scopes '*,!raw_query' (everything except direct SQL).
//
// The raw token is printed exactly once at create time — only its sha256 is stored.

const path = require('path');
const crypto = require('crypto');
const { Pool } = require('pg');

if (process.env.NODE_ENV !== 'production') {
  try {
    require('dotenv').config({ path: path.join(__dirname, '.env.bmm'), override: true, quiet: true });
  } catch (_) {}
}

function dbSsl() {
  const raw = process.env.BMM_DATABASE_CA_CERT;
  if (!raw) return { rejectUnauthorized: false };
  const ca = raw.includes('-----BEGIN') ? raw : Buffer.from(raw, 'base64').toString('utf8');
  return { ca, rejectUnauthorized: true };
}

const pool = new Pool({ connectionString: process.env.BMM_DATABASE_URL, ssl: dbSsl(), max: 2 });

function parseFlags(argv) {
  const flags = {};
  const positional = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i].startsWith('--')) {
      flags[argv[i].slice(2)] = argv[i + 1];
      i++;
    } else {
      positional.push(argv[i]);
    }
  }
  return { flags, positional };
}

function parseExpires(s) {
  if (!s) return null;
  const days = /^(\d+)d$/.exec(s);
  if (days) return new Date(Date.now() + Number(days[1]) * 86400000).toISOString();
  const d = new Date(s);
  if (isNaN(d.getTime())) throw new Error(`Cannot parse --expires "${s}" (use e.g. 90d or 2027-01-01)`);
  return d.toISOString();
}

async function create(label, flags) {
  if (!label) throw new Error('Usage: create <label> [--scopes tool1,tool2] [--expires 90d]');
  const scopes = flags.scopes ? flags.scopes.split(',').map(s => s.trim()).filter(Boolean) : ['*'];
  const expiresAt = parseExpires(flags.expires);
  const raw = crypto.randomBytes(32).toString('base64url');
  const hash = crypto.createHash('sha256').update(raw).digest('hex');
  const r = await pool.query(
    `INSERT INTO mcp_token (label, token_hash, scopes, expires_at) VALUES ($1, $2, $3, $4)
     RETURNING id, label, scopes, expires_at`,
    [label, hash, scopes, expiresAt]
  );
  const t = r.rows[0];
  console.log(`Created token for "${t.label}" (id ${t.id})`);
  console.log(`  scopes:  ${t.scopes.join(', ')}`);
  console.log(`  expires: ${t.expires_at || 'never'}`);
  console.log('');
  console.log('Token (shown once, store it now):');
  console.log(`  ${raw}`);
  console.log('');
  console.log('Connector URL:');
  console.log(`  https://YOUR-APP.onrender.com/mcp?token=${raw}`);
}

async function list() {
  const r = await pool.query(
    `SELECT id, label, scopes, created_at, expires_at, revoked_at, last_used_at
     FROM mcp_token ORDER BY created_at DESC`
  );
  if (!r.rows.length) return console.log('No tokens.');
  for (const t of r.rows) {
    const state = t.revoked_at ? 'REVOKED' : (t.expires_at && new Date(t.expires_at) < new Date() ? 'EXPIRED' : 'live');
    console.log(`${state.padEnd(8)} ${t.label.padEnd(24)} id=${t.id}`);
    console.log(`         scopes=${t.scopes.join(',')} created=${t.created_at.toISOString().slice(0, 10)} expires=${t.expires_at ? t.expires_at.toISOString().slice(0, 10) : 'never'} last_used=${t.last_used_at ? t.last_used_at.toISOString() : 'never'}`);
  }
}

async function revoke(idOrLabel) {
  if (!idOrLabel) throw new Error('Usage: revoke <id-or-label>');
  const r = await pool.query(
    `UPDATE mcp_token SET revoked_at = now()
     WHERE revoked_at IS NULL AND (id::text = $1 OR label = $1)
     RETURNING id, label`,
    [idOrLabel]
  );
  if (!r.rows.length) return console.log(`No live token matches "${idOrLabel}".`);
  for (const t of r.rows) console.log(`Revoked "${t.label}" (id ${t.id})`);
  console.log('Takes effect within 30s (server-side token cache TTL).');
}

async function audit(flags) {
  const limit = Number(flags.limit || 50);
  const params = [];
  let where = '';
  if (flags.identity) {
    params.push(flags.identity);
    where = `WHERE identity = $1`;
  }
  params.push(limit);
  const r = await pool.query(
    `SELECT at, identity, tool, ok, error, duration_ms FROM mcp_audit_log
     ${where} ORDER BY at DESC LIMIT $${params.length}`,
    params
  );
  if (!r.rows.length) return console.log('No audit entries.');
  for (const row of r.rows) {
    console.log(`${row.at.toISOString()} ${row.ok ? 'ok ' : 'ERR'} ${row.identity.padEnd(24)} ${row.tool}${row.error ? ` — ${row.error}` : ''}${row.duration_ms != null ? ` (${row.duration_ms}ms)` : ''}`);
  }
}

async function main() {
  const [cmd, ...rest] = process.argv.slice(2);
  const { flags, positional } = parseFlags(rest);
  if (['create', 'list', 'revoke', 'audit'].includes(cmd) && !process.env.BMM_DATABASE_URL) {
    throw new Error('BMM_DATABASE_URL is not set (via env or .env.bmm).');
  }
  switch (cmd) {
    case 'create': return create(positional[0], flags);
    case 'list':   return list();
    case 'revoke': return revoke(positional[0]);
    case 'audit':  return audit(flags);
    default:
      console.log('Usage: node manage-tokens.js <create|list|revoke|audit> ...');
      console.log('  create <label> [--scopes tool1,tool2] [--expires 90d|2027-01-01]');
      console.log('  list');
      console.log('  revoke <id-or-label>');
      console.log('  audit [--limit 50] [--identity <label>]');
      process.exitCode = cmd ? 1 : 0;
  }
}

main()
  .catch(err => { console.error(err.message); process.exitCode = 1; })
  .finally(() => pool.end());
