// bmm-server-http.js
// BMM MCP Server — HTTP/SSE transport for Railway deployment
// Drop this file alongside your existing bmm-server.js (stdio stays for local use)
//
// Railway env vars required:
//   BMM_DATABASE_URL  — Supabase transaction pooler connection string
//   BMM_SHARED_SECRET — random string; add to Claude Desktop config as ?token=
//   PORT              — auto-set by Railway
//
// Claude Desktop config (all machines):
//   "bmm": { "url": "https://YOUR-APP.railway.app/sse?token=YOUR_SECRET" }

const express = require('express');
const { Server } = require('@modelcontextprotocol/sdk/server/index.js');
const { SSEServerTransport } = require('@modelcontextprotocol/sdk/server/sse.js');
const { CallToolRequestSchema, ListToolsRequestSchema } = require('@modelcontextprotocol/sdk/types.js');
const { Pool } = require('pg');
const path = require('path');

// Local dev reads .env.bmm; Railway injects env vars directly
if (process.env.NODE_ENV !== 'production') {
  try { require('dotenv').config({ path: path.join(__dirname, '.env.bmm') }); } catch (_) {}
}

const pool = new Pool({
  connectionString: process.env.BMM_DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10
});

const SHARED_SECRET = process.env.BMM_SHARED_SECRET;

// ── Auth ────────────────────────────────────────────────────────────────────
function auth(req, res, next) {
  if (!SHARED_SECRET) return next(); // dev: no secret = open
  const token =
    req.query.token ||
    req.headers['x-api-key'] ||
    (req.headers['authorization'] || '').replace('Bearer ', '');
  if (token !== SHARED_SECRET) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ── DB helpers ───────────────────────────────────────────────────────────────
async function dbQuery(sql, params = []) {
  const client = await pool.connect();
  try { return await client.query(sql, params); }
  finally { client.release(); }
}

function buildUpdateClause(updates, startIdx = 1) {
  const keys = Object.keys(updates);
  const sets = keys.map((k, i) => {
    if (k === 'context') return `${k} = COALESCE(${k}, '{}'::jsonb) || $${startIdx + i}::jsonb`;
    return `${k} = $${startIdx + i}`;
  });
  const values = keys.map(k =>
    k === 'context' && typeof updates[k] === 'object'
      ? JSON.stringify(updates[k])
      : updates[k]
  );
  return { clause: sets.join(', '), values };
}

function genericInsert(table, args) {
  const keys = Object.keys(args);
  const cols = keys.join(', ');
  const placeholders = keys.map((_, i) => `$${i + 1}`).join(', ');
  const vals = keys.map(k =>
    k === 'expenses' && typeof args[k] === 'object' ? JSON.stringify(args[k]) : args[k]
  );
  return dbQuery(`INSERT INTO ${table} (${cols}) VALUES (${placeholders}) RETURNING *`, vals);
}

// ── Tool definitions ─────────────────────────────────────────────────────────
const TOOLS = [
  {
    name: 'create_contact',
    description: 'Create a new contact. Roles: seller, buyer, referral, attorney, cpa, banker, lender, other.',
    inputSchema: {
      type: 'object',
      properties: {
        role: { type: 'string', enum: ['seller','buyer','referral','attorney','cpa','banker','lender','other'] },
        first_name: { type: 'string' },
        last_name: { type: 'string' },
        email: { type: 'string' },
        phone_mobile: { type: 'string' },
        phone_work: { type: 'string' },
        phone_home: { type: 'string' },
        company: { type: 'string' },
        title: { type: 'string' },
        city: { type: 'string' },
        state: { type: 'string' },
        zip: { type: 'string' },
        address: { type: 'string' },
        county: { type: 'string' },
        notes: { type: 'string' },
        source: { type: 'string' },
        referred_by: { type: 'string' },
        assigned_to: { type: 'string' }
      },
      required: ['role', 'last_name']
    }
  },
  {
    name: 'search_contacts',
    description: 'Search contacts by name, company, email, or any text. Returns up to 20 matches.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string' },
        role: { type: 'string' },
        limit: { type: 'number', default: 20 }
      },
      required: ['query']
    }
  },
  {
    name: 'get_contact',
    description: 'Get full contact details by ID, including related entities, deals, buyer profiles, and recent interactions.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  },
  {
    name: 'update_contact',
    description: 'Update an existing contact. Only provided fields are updated.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        role: { type: 'string' },
        first_name: { type: 'string' },
        last_name: { type: 'string' },
        email: { type: 'string' },
        phone_mobile: { type: 'string' },
        phone_work: { type: 'string' },
        phone_home: { type: 'string' },
        company: { type: 'string' },
        title: { type: 'string' },
        city: { type: 'string' },
        state: { type: 'string' },
        zip: { type: 'string' },
        address: { type: 'string' },
        county: { type: 'string' },
        notes: { type: 'string' },
        tags: { type: 'array', items: { type: 'string' } },
        source: { type: 'string' },
        referred_by: { type: 'string' },
        context: { type: 'object' },
        assigned_to: { type: 'string' }
      },
      required: ['id']
    }
  },
  {
    name: 'create_deal',
    description: 'Create a new deal/listing.',
    inputSchema: {
      type: 'object',
      properties: {
        title: { type: 'string' },
        listing_id: { type: 'string' },
        seller_contact_id: { type: 'string' },
        entity_id: { type: 'string' },
        asking_price: { type: 'number' },
        status: { type: 'string', enum: ['prospect','active','under_loi','under_contract','closed','withdrawn'], default: 'prospect' },
        organization_type: { type: 'string' },
        year_established: { type: 'number' },
        business_city: { type: 'string' },
        business_state: { type: 'string' },
        business_address: { type: 'string' },
        naics: { type: 'string' },
        reason_for_sale: { type: 'string' },
        business_overview: { type: 'string' },
        assigned_to: { type: 'string' }
      },
      required: ['title']
    }
  },
  {
    name: 'get_deal',
    description: 'Get full deal details by ID, including seller info, entity, financials, balance sheet, documents, and flags.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  },
  {
    name: 'list_deals',
    description: 'List deals with optional filters.',
    inputSchema: {
      type: 'object',
      properties: {
        status: { type: 'string' },
        assigned_to: { type: 'string' },
        limit: { type: 'number', default: 20 }
      }
    }
  },
  {
    name: 'update_deal',
    description: 'Update a deal. Only provided fields are updated.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        title: { type: 'string' },
        status: { type: 'string', enum: ['prospect','active','under_loi','under_contract','closed','withdrawn'] },
        asking_price: { type: 'number' },
        engagement_price: { type: 'number' },
        commission_rate: { type: 'number' },
        commission_min: { type: 'number' },
        expiration_date: { type: 'string' },
        closing_date: { type: 'string' },
        reason_for_sale: { type: 'string' },
        business_overview: { type: 'string' },
        facilities: { type: 'string' },
        products_services: { type: 'string' },
        competition_industry: { type: 'string' },
        sales_marketing: { type: 'string' },
        assets_description: { type: 'string' },
        growth_potential: { type: 'string' },
        showing_instructions: { type: 'string' },
        employees_ft: { type: 'number' },
        employees_pt: { type: 'number' },
        inventory_value: { type: 'number' },
        inventory_included: { type: 'boolean' },
        ffe_value: { type: 'number' },
        ffe_included: { type: 'boolean' },
        ar_value: { type: 'number' },
        ar_included: { type: 'boolean' },
        lease_monthly_rent: { type: 'number' },
        lease_expire_date: { type: 'string' },
        lease_sqft: { type: 'number' }
      },
      required: ['id']
    }
  },
  {
    name: 'create_entity',
    description: 'Create a business entity linked to a contact.',
    inputSchema: {
      type: 'object',
      properties: {
        contact_id: { type: 'string' },
        entity_name: { type: 'string' },
        entity_type: { type: 'string', enum: ['LLC','S-Corp','C-Corp','Partnership','Sole Proprietorship','Other'] },
        ein: { type: 'string' },
        state_of_formation: { type: 'string' },
        formation_date: { type: 'string' },
        registered_agent: { type: 'string' },
        status: { type: 'string' },
        address: { type: 'string' },
        city: { type: 'string' },
        state: { type: 'string' },
        zip: { type: 'string' },
        notes: { type: 'string' }
      },
      required: ['contact_id', 'entity_name']
    }
  },
  {
    name: 'create_buyer_profile',
    description: 'Create a buyer profile (buy box) linked to a contact.',
    inputSchema: {
      type: 'object',
      properties: {
        contact_id: { type: 'string' },
        min_price: { type: 'number' },
        max_price: { type: 'number' },
        preferred_industries: { type: 'array', items: { type: 'string' } },
        preferred_locations: { type: 'array', items: { type: 'string' } },
        financing_type: { type: 'string' },
        notes: { type: 'string' }
      },
      required: ['contact_id']
    }
  },
  {
    name: 'update_buyer_profile',
    description: 'Update a buyer profile.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        min_price: { type: 'number' },
        max_price: { type: 'number' },
        preferred_industries: { type: 'array', items: { type: 'string' } },
        preferred_locations: { type: 'array', items: { type: 'string' } },
        financing_type: { type: 'string' },
        notes: { type: 'string' }
      },
      required: ['id']
    }
  },
  {
    name: 'find_buyers',
    description: 'Find potential buyers for a deal based on buy box criteria.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        limit: { type: 'number', default: 10 }
      },
      required: ['deal_id']
    }
  },
  {
    name: 'log_interaction',
    description: 'Log a meeting, call, email, showing, or note. Accepts contact_id and/or deal_id.',
    inputSchema: {
      type: 'object',
      properties: {
        contact_id: { type: 'string' },
        deal_id: { type: 'string' },
        type: { type: 'string', enum: ['meeting','call','email','showing','note','text'] },
        direction: { type: 'string', enum: ['inbound','outbound','internal'] },
        summary: { type: 'string' },
        notes: { type: 'string' },
        follow_up_date: { type: 'string' },
        created_by: { type: 'string' }
      },
      required: ['type', 'summary']
    }
  },
  {
    name: 'create_flag',
    description: 'Create a flag/task for follow-up.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        contact_id: { type: 'string' },
        type: { type: 'string', enum: ['document','follow_up','legal','financial','compliance','other'] },
        priority: { type: 'string', enum: ['low','medium','high','critical'], default: 'medium' },
        description: { type: 'string' },
        due_date: { type: 'string' },
        created_by: { type: 'string' }
      },
      required: ['description']
    }
  },
  {
    name: 'list_flags',
    description: 'List open flags, optionally filtered by broker, deal, priority, or type.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        contact_id: { type: 'string' },
        priority: { type: 'string' },
        type: { type: 'string' },
        created_by: { type: 'string' },
        resolved: { type: 'boolean', default: false }
      }
    }
  },
  {
    name: 'resolve_flag',
    description: 'Mark a flag as resolved.',
    inputSchema: {
      type: 'object',
      properties: { id: { type: 'string' } },
      required: ['id']
    }
  },
  {
    name: 'my_followups',
    description: "Get open follow-ups for a broker, sorted by date.",
    inputSchema: {
      type: 'object',
      properties: {
        broker: { type: 'string', description: 'Broker name (defaults to Jeremy)' },
        limit: { type: 'number', default: 20 }
      }
    }
  },
  {
    name: 'pipeline',
    description: 'Get pipeline summary — deal counts and values by stage, optionally filtered by broker.',
    inputSchema: {
      type: 'object',
      properties: { assigned_to: { type: 'string' } }
    }
  },
  {
    name: 'save_match',
    description: "Save a buyer-deal match with Claude's reasoning.",
    inputSchema: {
      type: 'object',
      properties: {
        buyer_contact_id: { type: 'string' },
        deal_id: { type: 'string' },
        score: { type: 'number', description: '0-100' },
        reasoning: { type: 'string' },
        status: { type: 'string', enum: ['potential','presented','interested','declined'], default: 'potential' }
      },
      required: ['buyer_contact_id', 'deal_id', 'score', 'reasoning']
    }
  },
  {
    name: 'track_document',
    description: "Track a document's status for a deal.",
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        contact_id: { type: 'string' },
        doc_type: { type: 'string', enum: ['engagement_agreement','corporate_resolution','llc_certificate','spouse_consent','disclosure','equipment_list','lease','financials','loi','apa','other'] },
        status: { type: 'string', enum: ['requested','received','reviewed','signed','filed'], default: 'requested' },
        filename: { type: 'string' },
        notes: { type: 'string' }
      },
      required: ['doc_type']
    }
  },
  {
    name: 'add_financials',
    description: 'Add a year of financial data to a deal.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        year: { type: 'number' },
        source: { type: 'string', enum: ['tax_return','p&l','recast','interim'], default: 'tax_return' },
        revenue: { type: 'number' },
        cogs: { type: 'number' },
        gross_profit: { type: 'number' },
        net_income: { type: 'number' },
        total_addbacks: { type: 'number' },
        sde: { type: 'number' },
        normalized_salary: { type: 'number' },
        adj_ebitda: { type: 'number' },
        da: { type: 'number' },
        adj_ebit: { type: 'number' },
        expenses: { type: 'object', description: 'Key-value pairs of expense line items' }
      },
      required: ['deal_id', 'year']
    }
  },
  {
    name: 'raw_query',
    description: 'Run a raw SQL SELECT query. Read-only.',
    inputSchema: {
      type: 'object',
      properties: {
        sql: { type: 'string' },
        params: { type: 'array' }
      },
      required: ['sql']
    }
  }
];

// ── Tool handlers ────────────────────────────────────────────────────────────
async function handleTool(name, args) {
  switch (name) {

    case 'create_contact': {
      const r = await genericInsert('contacts', args);
      return JSON.stringify(r.rows[0], null, 2);
    }

    case 'search_contacts': {
      const { query, role, limit = 20 } = args;
      let sql = `
        SELECT id, role, first_name, last_name, email, phone_mobile, company, city, state, tags, assigned_to
        FROM contacts
        WHERE (first_name ILIKE $1 OR last_name ILIKE $1 OR email ILIKE $1 OR
               company ILIKE $1 OR CONCAT(first_name, ' ', last_name) ILIKE $1)
      `;
      const params = [`%${query}%`];
      if (role) { sql += ` AND role = $${params.length + 1}`; params.push(role); }
      sql += ` ORDER BY last_name, first_name LIMIT $${params.length + 1}`;
      params.push(limit);
      const r = await dbQuery(sql, params);
      return JSON.stringify(r.rows, null, 2);
    }

    case 'get_contact': {
      const r = await dbQuery('SELECT * FROM contacts WHERE id = $1', [args.id]);
      if (!r.rows.length) return 'Contact not found';
      const contact = r.rows[0];
      const [entities, deals, profiles, interactions] = await Promise.all([
        dbQuery('SELECT * FROM entities WHERE contact_id = $1', [args.id]),
        dbQuery('SELECT id, title, status, asking_price FROM deals WHERE seller_contact_id = $1', [args.id]),
        dbQuery('SELECT * FROM buyer_profiles WHERE contact_id = $1', [args.id]),
        dbQuery('SELECT * FROM interactions WHERE contact_id = $1 ORDER BY created_at DESC LIMIT 5', [args.id])
      ]);
      return JSON.stringify({
        ...contact,
        entities: entities.rows,
        deals: deals.rows,
        buyer_profiles: profiles.rows,
        recent_interactions: interactions.rows
      }, null, 2);
    }

    case 'update_contact': {
      const { id, ...updates } = args;
      if (!Object.keys(updates).length) return 'No fields to update';
      const { clause, values } = buildUpdateClause(updates);
      const r = await dbQuery(
        `UPDATE contacts SET ${clause}, updated_at = NOW() WHERE id = $${values.length + 1} RETURNING *`,
        [...values, id]
      );
      return r.rows.length ? JSON.stringify(r.rows[0], null, 2) : 'Contact not found';
    }

    case 'create_deal': {
      const r = await genericInsert('deals', args);
      return JSON.stringify(r.rows[0], null, 2);
    }

    case 'get_deal': {
      const r = await dbQuery('SELECT * FROM deals WHERE id = $1', [args.id]);
      if (!r.rows.length) return 'Deal not found';
      const deal = r.rows[0];
      const [financials, docs, flags, seller] = await Promise.all([
        dbQuery('SELECT * FROM deal_financials WHERE deal_id = $1 ORDER BY year DESC', [args.id]),
        dbQuery('SELECT * FROM documents WHERE deal_id = $1', [args.id]),
        dbQuery('SELECT * FROM flags WHERE deal_id = $1 AND resolved = false', [args.id]),
        deal.seller_contact_id
          ? dbQuery('SELECT id, first_name, last_name, email, phone_mobile, company FROM contacts WHERE id = $1', [deal.seller_contact_id])
          : Promise.resolve({ rows: [] })
      ]);
      return JSON.stringify({
        ...deal,
        seller: seller.rows[0] || null,
        financials: financials.rows,
        documents: docs.rows,
        open_flags: flags.rows
      }, null, 2);
    }

    case 'list_deals': {
      const { status, assigned_to, limit = 20 } = args;
      let sql = 'SELECT id, listing_id, title, status, asking_price, business_city, business_state, assigned_to, created_at FROM deals WHERE 1=1';
      const params = [];
      if (status) { sql += ` AND status = $${params.length + 1}`; params.push(status); }
      if (assigned_to) { sql += ` AND assigned_to ILIKE $${params.length + 1}`; params.push(`%${assigned_to}%`); }
      sql += ` ORDER BY created_at DESC LIMIT $${params.length + 1}`;
      params.push(limit);
      const r = await dbQuery(sql, params);
      return JSON.stringify(r.rows, null, 2);
    }

    case 'update_deal': {
      const { id, ...updates } = args;
      if (!Object.keys(updates).length) return 'No fields to update';
      const { clause, values } = buildUpdateClause(updates);
      const r = await dbQuery(
        `UPDATE deals SET ${clause}, updated_at = NOW() WHERE id = $${values.length + 1} RETURNING *`,
        [...values, id]
      );
      return r.rows.length ? JSON.stringify(r.rows[0], null, 2) : 'Deal not found';
    }

    case 'create_entity': {
      const r = await genericInsert('entities', args);
      return JSON.stringify(r.rows[0], null, 2);
    }

    case 'create_buyer_profile': {
      const r = await genericInsert('buyer_profiles', args);
      return JSON.stringify(r.rows[0], null, 2);
    }

    case 'update_buyer_profile': {
      const { id, ...updates } = args;
      const { clause, values } = buildUpdateClause(updates);
      const r = await dbQuery(
        `UPDATE buyer_profiles SET ${clause} WHERE id = $${values.length + 1} RETURNING *`,
        [...values, id]
      );
      return r.rows.length ? JSON.stringify(r.rows[0], null, 2) : 'Profile not found';
    }

    case 'find_buyers': {
      const { deal_id, limit = 10 } = args;
      const deal = await dbQuery('SELECT asking_price FROM deals WHERE id = $1', [deal_id]);
      if (!deal.rows.length) return 'Deal not found';
      const price = deal.rows[0].asking_price || 0;
      const r = await dbQuery(`
        SELECT c.id, c.first_name, c.last_name, c.email, c.phone_mobile, c.company,
               bp.id as profile_id, bp.min_price, bp.max_price,
               bp.preferred_industries, bp.preferred_locations, bp.notes
        FROM buyer_profiles bp
        JOIN contacts c ON c.id = bp.contact_id
        WHERE (bp.max_price IS NULL OR bp.max_price >= $1)
          AND (bp.min_price IS NULL OR bp.min_price <= $2)
        ORDER BY c.last_name
        LIMIT $3
      `, [price * 0.7, price * 1.3, limit]);
      return JSON.stringify(r.rows, null, 2);
    }

    case 'log_interaction': {
      const r = await genericInsert('interactions', args);
      return JSON.stringify(r.rows[0], null, 2);
    }

    case 'create_flag': {
      const r = await genericInsert('flags', args);
      return JSON.stringify(r.rows[0], null, 2);
    }

    case 'list_flags': {
      const { deal_id, contact_id, priority, type, created_by, resolved = false } = args;
      let sql = `
        SELECT f.*, d.title as deal_title, c.first_name || ' ' || c.last_name as contact_name
        FROM flags f
        LEFT JOIN deals d ON d.id = f.deal_id
        LEFT JOIN contacts c ON c.id = f.contact_id
        WHERE f.resolved = $1
      `;
      const params = [resolved];
      if (deal_id) { sql += ` AND f.deal_id = $${params.length + 1}`; params.push(deal_id); }
      if (contact_id) { sql += ` AND f.contact_id = $${params.length + 1}`; params.push(contact_id); }
      if (priority) { sql += ` AND f.priority = $${params.length + 1}`; params.push(priority); }
      if (type) { sql += ` AND f.type = $${params.length + 1}`; params.push(type); }
      if (created_by) { sql += ` AND f.created_by ILIKE $${params.length + 1}`; params.push(`%${created_by}%`); }
      sql += ' ORDER BY f.due_date ASC NULLS LAST, f.created_at DESC';
      const r = await dbQuery(sql, params);
      return JSON.stringify(r.rows, null, 2);
    }

    case 'resolve_flag': {
      const r = await dbQuery(
        'UPDATE flags SET resolved = true, resolved_at = NOW() WHERE id = $1 RETURNING *',
        [args.id]
      );
      return r.rows.length ? JSON.stringify(r.rows[0], null, 2) : 'Flag not found';
    }

    case 'my_followups': {
      const { broker = 'Jeremy', limit = 20 } = args;
      const r = await dbQuery(`
        SELECT 'interaction' as source, i.id, i.follow_up_date as due_date,
               i.summary, i.notes, i.created_by,
               CONCAT(c.first_name, ' ', c.last_name) as contact_name,
               d.title as deal_title
        FROM interactions i
        LEFT JOIN contacts c ON c.id = i.contact_id
        LEFT JOIN deals d ON d.id = i.deal_id
        WHERE i.follow_up_date IS NOT NULL
          AND i.follow_up_date >= CURRENT_DATE
          AND i.created_by ILIKE $1
        UNION ALL
        SELECT 'flag' as source, f.id, f.due_date,
               f.description as summary, '' as notes, f.created_by,
               CONCAT(c.first_name, ' ', c.last_name) as contact_name,
               d.title as deal_title
        FROM flags f
        LEFT JOIN contacts c ON c.id = f.contact_id
        LEFT JOIN deals d ON d.id = f.deal_id
        WHERE f.resolved = false
          AND f.created_by ILIKE $1
        ORDER BY due_date ASC NULLS LAST
        LIMIT $2
      `, [`%${broker}%`, limit]);
      return JSON.stringify(r.rows, null, 2);
    }

    case 'pipeline': {
      const { assigned_to } = args;
      let sql = `
        SELECT status, COUNT(*) as count,
               SUM(asking_price) as total_value,
               AVG(asking_price) as avg_value
        FROM deals WHERE 1=1
      `;
      const params = [];
      if (assigned_to) { sql += ` AND assigned_to ILIKE $${params.length + 1}`; params.push(`%${assigned_to}%`); }
      sql += ' GROUP BY status ORDER BY status';
      const r = await dbQuery(sql, params);
      return JSON.stringify(r.rows, null, 2);
    }

    case 'save_match': {
      const { buyer_contact_id, deal_id, score, reasoning, status = 'potential' } = args;
      const r = await dbQuery(
        `INSERT INTO matches (buyer_contact_id, deal_id, score, reasoning, status)
         VALUES ($1, $2, $3, $4, $5) RETURNING *`,
        [buyer_contact_id, deal_id, score, reasoning, status]
      );
      return JSON.stringify(r.rows[0], null, 2);
    }

    case 'track_document': {
      const r = await genericInsert('documents', args);
      return JSON.stringify(r.rows[0], null, 2);
    }

    case 'add_financials': {
      const fields = { ...args };
      if (fields.expenses && typeof fields.expenses === 'object') {
        fields.expenses = JSON.stringify(fields.expenses);
      }
      const keys = Object.keys(fields);
      const updateKeys = keys.filter(k => k !== 'deal_id' && k !== 'year');
      const r = await dbQuery(`
        INSERT INTO deal_financials (${keys.join(', ')})
        VALUES (${keys.map((_, i) => `$${i + 1}`).join(', ')})
        ON CONFLICT (deal_id, year)
        DO UPDATE SET ${updateKeys.map(k => `${k} = EXCLUDED.${k}`).join(', ')}
        RETURNING *
      `, keys.map(k => fields[k]));
      return JSON.stringify(r.rows[0], null, 2);
    }

    case 'raw_query': {
      const normalized = args.sql.trim().toUpperCase();
      if (!normalized.startsWith('SELECT') && !normalized.startsWith('WITH')) {
        return 'Error: Only SELECT/WITH queries permitted';
      }
      const r = await dbQuery(args.sql, args.params || []);
      return JSON.stringify(r.rows, null, 2);
    }

    default:
      return `Unknown tool: ${name}`;
  }
}

// ── MCP Server factory (one instance per SSE connection) ────────────────────
function createMCPServer() {
  const server = new Server(
    { name: 'bmm-server', version: '1.0.0' },
    { capabilities: { tools: {} } }
  );
  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));
  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    try {
      const text = await handleTool(name, args || {});
      return { content: [{ type: 'text', text }] };
    } catch (error) {
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({ error: error.message, tool: name, detail: error.detail || null })
        }]
      };
    }
  });
  return server;
}

// ── Express app ──────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());

// Active SSE transports keyed by sessionId
const transports = {};

// Health check — Railway uses this to verify the service is up
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'bmm-server', timestamp: new Date().toISOString() });
});

// SSE endpoint — Claude Desktop connects here
app.get('/sse', auth, async (req, res) => {
  const mcpServer = createMCPServer();
  const transport = new SSEServerTransport('/messages', res);
  transports[transport.sessionId] = transport;
  res.on('close', () => delete transports[transport.sessionId]);
  await mcpServer.connect(transport);
});

// Message endpoint — Claude Desktop posts tool calls here
app.post('/messages', auth, async (req, res) => {
  const transport = transports[req.query.sessionId];
  if (!transport) return res.status(404).json({ error: 'Session not found' });
  await transport.handlePostMessage(req, res);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`BMM MCP Server (HTTP) running on port ${PORT}`);
});
