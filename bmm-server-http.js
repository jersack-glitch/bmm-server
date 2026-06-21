// bmm-server-http.js
// BMM MCP Server v2.1 — Streamable HTTP transport for Render deployment
//
// Remote variant of bmm-server.js. The stdio server (bmm-server.js) stays the
// local entrypoint for Kevin/Mike; this file is the deployable remote server
// that claude.ai connects to as a Custom Connector.
//
// Transport: Streamable HTTP (single /mcp endpoint, stateless). SSE is deprecated.
//
// Required env vars (set in Render dashboard):
//   BMM_DATABASE_URL   — Supabase transaction pooler connection string
//   BMM_SHARED_SECRET  — shared secret; clients pass it in the connector URL as ?token=<secret>
//   PORT               — auto-set by Render
//   NODE_ENV           — set to "production" on Render (skips local .env load)
//
// claude.ai / Claude Desktop Custom Connector config:
//   URL:  https://YOUR-APP.onrender.com/mcp?token=<BMM_SHARED_SECRET>
//   Auth: None
//
// (Claude's connector UI has no bearer-token/custom-header field — it only offers
//  "None" or OAuth — so the secret rides in the URL. OAuth can be added later
//  without touching the tools or transport.)

const express = require('express');
const { Server } = require('@modelcontextprotocol/sdk/server/index.js');
const { StreamableHTTPServerTransport } = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
const {
  CallToolRequestSchema,
  ListToolsRequestSchema
} = require('@modelcontextprotocol/sdk/types.js');
const { Pool } = require('pg');
const path = require('path');

// Local dev reads .env.bmm; Render injects env vars directly.
if (process.env.NODE_ENV !== 'production') {
  try {
    require('dotenv').config({ path: path.join(__dirname, '.env.bmm'), override: true });
  } catch (_) {}
}

const pool = new Pool({
  connectionString: process.env.BMM_DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  // Pool hardening. connectionTimeoutMillis surfaces a starved pool.connect() as
  // an error after 10s instead of hanging indefinitely (likely cause of the
  // multi-minute update_match hang, since the checkout wait precedes any SQL).
  max: 10,
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000
});

const SHARED_SECRET = process.env.BMM_SHARED_SECRET;

// -- Helper: run query and return result --
async function query(sql, params = []) {
  const client = await pool.connect();
  try {
    const result = await client.query(sql, params);
    return result;
  } finally {
    client.release();
  }
}

// -- Helper: build SET clause from object (JSONB merge, array cast) --
function buildUpdateClause(updates, startIdx = 1) {
  const keys = Object.keys(updates);
  const sets = keys.map((k, i) => {
    if (k === 'context') return `${k} = COALESCE(${k}, '{}'::jsonb) || $${startIdx + i}::jsonb`;
    if (Array.isArray(updates[k])) return `${k} = $${startIdx + i}::text[]`;
    return `${k} = $${startIdx + i}`;
  });
  const values = keys.map(k => {
    if (k === 'context' && typeof updates[k] === 'object') return JSON.stringify(updates[k]);
    return updates[k];
  });
  return { clause: sets.join(', '), values };
}

// -- Helper: PII detection (NON-NEGOTIABLE - blocks SSN, EIN, account numbers, etc.) --
const PII_PATTERNS = [
  /\b\d{3}-?\d{2}-?\d{4}\b/,                          // SSN
  /\b\d{2}-?\d{7}\b/,                                  // EIN
  /\b\d{9,17}\b/,                                       // Bank account numbers
  /\b(?:routing|aba|rtn)\s*#?\s*\d{9}\b/i,             // Routing with prefix
  /\b\d{9}\b(?=.*(?:routing|aba|rtn))/i,               // Routing with suffix context
  /\b(?:acct|account)\s*#?\s*\d{6,}\b/i,               // Account numbers with prefix
  /\b(?:tax\s*id|tin|itin|ssn|ein)\s*[:=#]?\s*\d/i,    // Tax IDs with label
  /\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b/,       // Credit card numbers
];

function containsPII(text) {
  return PII_PATTERNS.some(p => p.test(text));
}

// -- Valid enums for memory entries --
const VALID_SOURCES = ['broker', 'hal_explicit', 'hal_implicit'];
const VALID_CATEGORIES = ['identity', 'deal_context', 'relationship', 'preference', 'observation', 'action_context'];
const VALID_CONFIDENCE = ['high', 'medium', 'low'];

// -- Valid match outcomes (shared by save_match and update_match) --
const MATCH_OUTCOMES = ['live', 'under_loi', 'dead', 'passed', 'withdrawn', 'closed'];

// -- Tool Definitions --
const TOOLS = [
  // == DOMAIN LOGIC: CONTACTS ==
  {
    name: 'create_contact',
    description: 'Create a new contact. Roles: seller, buyer, referral, attorney, cpa, banker. Industry tags: manufacturing, construction, trade-services, service, business-services, consumer-services, food-production, agriculture, retail, transportation. Context is a JSON object for unstructured notes.',
    inputSchema: {
      type: 'object',
      properties: {
        first_name: { type: 'string' },
        last_name: { type: 'string' },
        email: { type: 'string' },
        phone: { type: 'string' },
        phone_alt: { type: 'string' },
        company_name: { type: 'string' },
        title: { type: 'string' },
        website: { type: 'string' },
        address1: { type: 'string' },
        city: { type: 'string' },
        state: { type: 'string' },
        zip: { type: 'string' },
        county: { type: 'string' },
        roles: { type: 'array', items: { type: 'string' } },
        industry_tags: { type: 'array', items: { type: 'string' } },
        source: { type: 'string' },
        source_detail: { type: 'string' },
        spouse_name: { type: 'string' },
        spouse_email: { type: 'string' },
        assigned_broker: { type: 'string' },
        co_broker: { type: 'string', description: 'Secondary broker. Values: Jeremy, Kevin, Mike' },
        follow_up_interval_days: { type: 'number', description: 'Recurring keep-warm cadence in days. When a flag resolves, next flag auto-creates at this interval.' },
        context: { type: 'object' },
        created_by: { type: 'string' }
      },
      required: ['first_name', 'last_name']
    }
  },
  {
    name: 'update_contact',
    description: 'Update an existing contact. Pass contact_id and any fields to update. Context is merged (not replaced) - add new keys without losing existing ones. Supports co_broker (Jeremy/Kevin/Mike) and follow_up_interval_days for recurring keep-warm cadence.',
    inputSchema: {
      type: 'object',
      properties: {
        contact_id: { type: 'string' },
        updates: { type: 'object', description: 'Fields to update' }
      },
      required: ['contact_id', 'updates']
    }
  },
  {
    name: 'search_contacts',
    description: 'Search contacts by name, company, email, or any text. Uses fuzzy matching. Optionally filter by role or industry tag.',
    inputSchema: {
      type: 'object',
      properties: {
        query: { type: 'string', description: 'Search text (name, company, email)' },
        role: { type: 'string', description: 'Filter by role: seller, buyer, referral, etc.' },
        industry: { type: 'string', description: 'Filter by industry tag' },
        assigned_broker: { type: 'string', description: 'Filter by broker' },
        limit: { type: 'number', description: 'Max results (default 25)' }
      },
      required: ['query']
    }
  },
  {
    name: 'get_contact',
    description: 'Get full contact details by ID, including related entities, deals, buyer profiles, and recent interactions.',
    inputSchema: {
      type: 'object',
      properties: {
        contact_id: { type: 'string' }
      },
      required: ['contact_id']
    }
  },

  // == DOMAIN LOGIC: BUYER PROFILES ==
  {
    name: 'update_buyer_profile',
    description: 'Update a buyer profile. Context is merged.',
    inputSchema: {
      type: 'object',
      properties: {
        profile_id: { type: 'string' },
        updates: { type: 'object' }
      },
      required: ['profile_id', 'updates']
    }
  },

  // == DOMAIN LOGIC: INTERACTIONS ==
  {
    name: 'log_interaction',
    description: 'Log a meeting, call, email, showing, or note. Links to a contact and optionally a deal. Set follow_up_date and follow_up_action to create a task.',
    inputSchema: {
      type: 'object',
      properties: {
        contact_id: { type: 'string' },
        deal_id: { type: 'string' },
        interaction_type: { type: 'string', description: 'meeting, call, email, showing, note, web-form' },
        direction: { type: 'string', description: 'inbound or outbound' },
        broker: { type: 'string' },
        occurred_at: { type: 'string' },
        summary: { type: 'string' },
        notes: { type: 'string' },
        follow_up_date: { type: 'string' },
        follow_up_action: { type: 'string' },
        context: { type: 'object' },
        created_by: { type: 'string' }
      },
      required: ['interaction_type', 'broker', 'summary']
    }
  },

  // == DOMAIN LOGIC: FLAGS ==
  {
    name: 'create_flag',
    description: 'Create a flag/task for follow-up. Types: missing-doc, legal-review, valuation-needed, follow-up, data-quality, third-party. Priority: low, normal, high, urgent. Pass dedupe_key for idempotent creation: if an open flag with the same dedupe_key already exists it is updated (due_date refreshed) rather than duplicated.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        contact_id: { type: 'string' },
        flag_type: { type: 'string' },
        description: { type: 'string' },
        priority: { type: 'string' },
        assigned_to: { type: 'string' },
        due_date: { type: 'string' },
        dedupe_key: { type: 'string', description: 'Stable idempotency key. Re-creating an open flag with the same key updates it instead of inserting a duplicate. Use for recurring jobs such as the morning briefing.' },
        context: { type: 'object' },
        created_by: { type: 'string' }
      },
      required: ['flag_type', 'description']
    }
  },
  {
    name: 'list_flags',
    description: 'List open flags, optionally filtered by broker, deal, priority, or type. Returns flags assigned to broker directly OR flags on contacts where broker is co_broker.',
    inputSchema: {
      type: 'object',
      properties: {
        assigned_to: { type: 'string' },
        deal_id: { type: 'string' },
        priority: { type: 'string' },
        flag_type: { type: 'string' },
        include_resolved: { type: 'boolean' }
      }
    }
  },
  {
    name: 'resolve_flag',
    description: 'Mark a flag as resolved. If the linked contact has follow_up_interval_days set, automatically creates the next follow-up flag. Pass description to set the next recurring flag text; if omitted a clean dated text is generated (the stale prior description is never copied).',
    inputSchema: {
      type: 'object',
      properties: {
        flag_id: { type: 'string' },
        resolution: { type: 'string' },
        resolved_by: { type: 'string' },
        description: { type: 'string', description: 'Optional fresh text for the auto-created recurring follow-up flag. If omitted, a clean dated description is generated.' }
      },
      required: ['flag_id']
    }
  },

  // == DOMAIN LOGIC: MATCHING ==
  {
    name: 'find_buyers',
    description: 'Find potential buyers for a deal. Returns buyers whose structured criteria overlap (industry, price range, geography) along with their full context for Claude to evaluate fit. Use this as the starting point for intelligent matching - Claude reads the context and makes judgment calls.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        price_tolerance_pct: { type: 'number', description: 'Expand price range by this % to catch near-misses (default 20)' },
        include_flexible: { type: 'boolean', description: 'Include buyers marked price_flexible even if outside range (default true)' }
      },
      required: ['deal_id']
    }
  },

  // == DOMAIN LOGIC: VIEWS ==
  {
    name: 'my_followups',
    description: 'Get open follow-ups for a broker, sorted by date.',
    inputSchema: {
      type: 'object',
      properties: {
        broker: { type: 'string' },
        days_ahead: { type: 'number', description: 'Look ahead N days (default 7)' }
      },
      required: ['broker']
    }
  },

  // == DOMAIN LOGIC: MEMORY ==
  {
    name: 'hal_remember',
    description: 'Write a memory entry to broker_memory_entries. PII is hard-blocked server-side. Source: broker, hal_explicit, hal_implicit. Category: identity, deal_context, relationship, preference, observation, action_context.',
    inputSchema: {
      type: 'object',
      properties: {
        broker: { type: 'string', description: 'Broker name: Jeremy, Kevin, Mike' },
        source: { type: 'string', description: 'broker, hal_explicit, or hal_implicit' },
        category: { type: 'string', description: 'identity, deal_context, relationship, preference, observation, action_context' },
        content: { type: 'string', description: 'The memory entry text. No PII allowed.' },
        contact_id: { type: 'string' },
        deal_id: { type: 'string' },
        interaction_id: { type: 'string' },
        confidence: { type: 'string', description: 'high, medium, low - required for hal_explicit/hal_implicit' },
        visibility: { type: 'string', description: 'shared (default) or private' },
        expires_at: { type: 'string', description: 'ISO timestamp for auto-expiry, or null for permanent' }
      },
      required: ['broker', 'source', 'category', 'content']
    }
  },
  {
    name: 'dismiss_memory',
    description: 'Dismiss a memory entry. Sets dismissed_at timestamp.',
    inputSchema: {
      type: 'object',
      properties: {
        memory_id: { type: 'string' },
        dismissed_by: { type: 'string', description: 'Broker name' }
      },
      required: ['memory_id', 'dismissed_by']
    }
  },
  {
    name: 'get_broker_context',
    description: 'Load broker profile, active memory entries, and session summary. Call at session start after confirming who is in the session.',
    inputSchema: {
      type: 'object',
      properties: {
        broker: { type: 'string', description: 'Broker name: Jeremy, Kevin, Mike' }
      },
      required: ['broker']
    }
  },

  // == CRUD: retained for Kevin/Mike (Jeremy uses bmm-supabase) ==
  {
    name: 'create_entity',
    description: 'Create a business entity linked to a contact. Entity types: llc, s-corp, c-corp, partnership, sole-prop.',
    inputSchema: {
      type: 'object',
      properties: {
        contact_id: { type: 'string' },
        legal_name: { type: 'string' },
        dba_name: { type: 'string' },
        entity_type: { type: 'string' },
        state_of_formation: { type: 'string' },
        formation_date: { type: 'string' },
        sos_status: { type: 'string' },
        sos_file_number: { type: 'string' },
        registered_agent: { type: 'string' },
        ucc_liens: { type: 'array' },
        officers: { type: 'array' },
        naics_code: { type: 'string' },
        sic_code: { type: 'string' },
        context: { type: 'object' }
      },
      required: ['contact_id', 'legal_name', 'entity_type']
    }
  },
  {
    name: 'create_deal',
    description: 'Create a new deal. Deal types: listing, acquisition, valuation-only. Stages: prospect, valuation, engagement, listing-prep, active-listing, marketing, buyer-qualification, negotiation, due-diligence, closing, closed, dead.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_name: { type: 'string' },
        deal_type: { type: 'string' },
        seller_id: { type: 'string' },
        entity_id: { type: 'string' },
        assigned_broker: { type: 'string' },
        stage: { type: 'string' },
        industry_tags: { type: 'array', items: { type: 'string' } },
        business_category: { type: 'string' },
        business_address: { type: 'string' },
        business_city: { type: 'string' },
        business_state: { type: 'string' },
        business_zip: { type: 'string' },
        business_county: { type: 'string' },
        year_established: { type: 'number' },
        employees_ft: { type: 'number' },
        employees_pt: { type: 'number' },
        asking_price: { type: 'number' },
        revenue: { type: 'number' },
        sde: { type: 'number' },
        ebitda: { type: 'number' },
        reason_for_sale: { type: 'string' },
        context: { type: 'object' },
        created_by: { type: 'string' }
      },
      required: ['deal_name', 'deal_type', 'assigned_broker']
    }
  },
  {
    name: 'update_deal',
    description: 'Update a deal. Context is merged. Stage changes automatically update stage_changed_at via Postgres trigger.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        updates: { type: 'object' }
      },
      required: ['deal_id', 'updates']
    }
  },
  {
    name: 'get_deal',
    description: 'Get full deal details by ID, including seller info, entity, financials, balance sheet, documents, flags, and matches.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' }
      },
      required: ['deal_id']
    }
  },
  {
    name: 'list_deals',
    description: 'List deals with optional filters. Great for pipeline views.',
    inputSchema: {
      type: 'object',
      properties: {
        stage: { type: 'string' },
        assigned_broker: { type: 'string' },
        deal_type: { type: 'string' },
        industry: { type: 'string' },
        limit: { type: 'number' }
      }
    }
  },
  {
    name: 'add_financials',
    description: 'Add a year of financial data to a deal. Upserts - if the year already exists, it updates.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        fiscal_year: { type: 'number' },
        source: { type: 'string' },
        gross_revenue: { type: 'number' },
        cogs: { type: 'number' },
        gross_profit: { type: 'number' },
        owner_salary: { type: 'number' },
        officer_compensation: { type: 'number' },
        salaries_wages: { type: 'number' },
        rent: { type: 'number' },
        depreciation: { type: 'number' },
        amortization: { type: 'number' },
        interest: { type: 'number' },
        total_expenses: { type: 'number' },
        net_income: { type: 'number' },
        total_addbacks: { type: 'number' },
        sde: { type: 'number' },
        adj_ebitda: { type: 'number' },
        context: { type: 'object' }
      },
      required: ['deal_id', 'fiscal_year']
    }
  },
  {
    name: 'create_buyer_profile',
    description: 'Create a buyer profile (buy box) linked to a contact. A buyer can have multiple profiles (primary, secondary, opportunistic).',
    inputSchema: {
      type: 'object',
      properties: {
        contact_id: { type: 'string' },
        profile_name: { type: 'string' },
        industry_tags: { type: 'array', items: { type: 'string' } },
        industry_exclude: { type: 'array', items: { type: 'string' } },
        price_min: { type: 'number' },
        price_max: { type: 'number' },
        price_flexible: { type: 'boolean' },
        min_revenue: { type: 'number' },
        min_sde: { type: 'number' },
        min_ebitda: { type: 'number' },
        max_multiple: { type: 'number' },
        preferred_states: { type: 'array', items: { type: 'string' } },
        preferred_counties: { type: 'array', items: { type: 'string' } },
        max_distance_miles: { type: 'number' },
        relocatable: { type: 'boolean' },
        has_cash: { type: 'number' },
        needs_financing: { type: 'boolean' },
        sba_prequalified: { type: 'boolean' },
        industry_experience: { type: 'array', items: { type: 'string' } },
        management_experience: { type: 'boolean' },
        owns_business: { type: 'boolean' },
        absentee_ok: { type: 'boolean' },
        franchise_ok: { type: 'boolean' },
        employees_max: { type: 'number' },
        urgency: { type: 'string' },
        target_close: { type: 'string' },
        context: { type: 'object' },
        created_by: { type: 'string' }
      },
      required: ['contact_id']
    }
  },
  {
    name: 'track_document',
    description: 'Track a document status for a deal. Statuses: needed, requested, received, signed, filed. Timestamps auto-set by Postgres trigger.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        doc_type: { type: 'string' },
        doc_name: { type: 'string' },
        status: { type: 'string' },
        storage_url: { type: 'string' },
        context: { type: 'object' }
      },
      required: ['deal_id', 'doc_type']
    }
  },
  {
    name: 'save_match',
    description: 'Save a buyer-deal match with reasoning. Match types: strong, moderate, near-miss. Pass outcome (live, under_loi, dead, passed, withdrawn, closed) to record disposition at save time; a non-live outcome requires reasoning (>=20 chars) and contacted_by.',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        buyer_id: { type: 'string' },
        buyer_profile_id: { type: 'string' },
        match_type: { type: 'string' },
        match_score: { type: 'number' },
        reasoning: { type: 'string' },
        matches_on: { type: 'array', items: { type: 'string' } },
        gaps: { type: 'array', items: { type: 'string' } },
        outcome: { type: 'string', description: 'live, under_loi, dead, passed, withdrawn, closed. Non-live requires reasoning >=20 chars and contacted_by.' },
        contacted: { type: 'boolean' },
        contacted_by: { type: 'string', description: 'Broker who contacted the buyer. Required when outcome is non-live.' },
        contacted_at: { type: 'string' },
        context: { type: 'object' }
      },
      required: ['deal_id', 'buyer_id', 'match_type', 'reasoning']
    }
  },
  {
    name: 'pipeline',
    description: 'Get pipeline summary - deal counts and values by stage, optionally filtered by broker.',
    inputSchema: {
      type: 'object',
      properties: {
        assigned_broker: { type: 'string' }
      }
    }
  },
  {
    name: 'raw_query',
    description: 'Run a raw SQL SELECT query. For ad-hoc analysis only. Only SELECT statements allowed.',
    inputSchema: {
      type: 'object',
      properties: {
        sql: { type: 'string' },
        params: { type: 'array' }
      },
      required: ['sql']
    }
  },
  {
    name: 'send_broker_message',
    description: 'Send a message to another broker that surfaces at their next session start. Use for deal thoughts, heads-up notes, or anything that needs broker-to-broker attention.',
    inputSchema: {
      type: 'object',
      properties: {
        to_broker:   { type: 'string', description: 'Jeremy | Kevin | Mike' },
        from_broker: { type: 'string', description: 'Jeremy | Kevin | Mike' },
        message:     { type: 'string', description: 'The message content' },
        contact_id:  { type: 'string', description: 'Optional - link to a contact' },
        deal_id:     { type: 'string', description: 'Optional - link to a deal' }
      },
      required: ['to_broker', 'from_broker', 'message']
    }
  },
  {
    name: 'get_broker_messages',
    description: 'Get unread messages for a broker. Call at session start to surface any broker-to-broker notes before the action list.',
    inputSchema: {
      type: 'object',
      properties: {
        broker: { type: 'string', description: 'Jeremy | Kevin | Mike' },
        mark_read: { type: 'boolean', description: 'Mark messages as read after retrieval. Default true.' }
      },
      required: ['broker']
    }
  },
  {
    name: 'add_balance_sheet',
    description: 'Add or update a deal balance sheet. One per deal: updates the existing non-deleted row if present, otherwise inserts. Values are point-in-time (as_of_date).',
    inputSchema: {
      type: 'object',
      properties: {
        deal_id: { type: 'string' },
        as_of_date: { type: 'string' },
        cash: { type: 'number' },
        accounts_receivable: { type: 'number' },
        inventory: { type: 'number' },
        leasehold_improvements: { type: 'number' },
        furniture_fixtures: { type: 'number' },
        equipment: { type: 'number' },
        vehicles: { type: 'number' },
        buildings: { type: 'number' },
        land: { type: 'number' },
        accumulated_depreciation: { type: 'number' },
        other_assets: { type: 'number' },
        total_assets: { type: 'number' },
        accounts_payable: { type: 'number' },
        bank_debt: { type: 'number' },
        current_notes_payable: { type: 'number' },
        accrued_expense: { type: 'number' },
        lt_notes_payable: { type: 'number' },
        lt_lease_payable: { type: 'number' },
        other_liabilities: { type: 'number' },
        total_liabilities: { type: 'number' },
        equity: { type: 'number' },
        recast_data: { type: 'object' },
        context: { type: 'object' }
      },
      required: ['deal_id']
    }
  },
  {
    name: 'update_document',
    description: 'Update a document by id. Status transitions: needed, requested, received, reviewed, signed, filed. The requested_at/received_at/signed_at timestamps are auto-set by a Postgres trigger.',
    inputSchema: {
      type: 'object',
      properties: {
        document_id: { type: 'string' },
        updates: { type: 'object', description: 'Fields: status, storage_url, doc_name, doc_type, context' }
      },
      required: ['document_id', 'updates']
    }
  },
  {
    name: 'update_match',
    description: 'Update a saved match by id. Advance outcome (live, under_loi, dead, passed, withdrawn, closed); record contacted, contacted_by, contacted_at, response. A non-live outcome requires reasoning (>=20 chars) and contacted_by to be set on the row.',
    inputSchema: {
      type: 'object',
      properties: {
        match_id: { type: 'string' },
        updates: { type: 'object', description: 'Fields: outcome, contacted, contacted_by, contacted_at, response, match_type, match_score, reasoning, matches_on, gaps, context' }
      },
      required: ['match_id', 'updates']
    }
  },
  {
    name: 'update_entity',
    description: 'Update a business entity by id. context is merged (not replaced); ucc_liens and officers are JSON arrays.',
    inputSchema: {
      type: 'object',
      properties: {
        entity_id: { type: 'string' },
        updates: { type: 'object', description: 'Fields: legal_name, dba_name, entity_type, state_of_formation, formation_date, sos_status, sos_file_number, registered_agent, ucc_liens, officers, naics_code, sic_code, context' }
      },
      required: ['entity_id', 'updates']
    }
  },
  {
    name: 'update_broker_profile',
    description: 'Write the broker_memory JSONB scratchpad (merged into existing, not replaced) and basic profile fields for a broker. broker_memory is broker-controlled freeform JSON; companion to get_broker_context.',
    inputSchema: {
      type: 'object',
      properties: {
        broker: { type: 'string', description: 'Jeremy, Kevin, Mike' },
        broker_memory: { type: 'object', description: 'JSON merged into existing broker_memory' },
        email: { type: 'string' },
        phone: { type: 'string' }
      },
      required: ['broker']
    }
  },
  {
    name: 'update_email_archive',
    description: 'Patch a parsed email_archive row by id (HAL write path, no full re-ingest). Editable: from_email, from_name, to_emails, cc_emails, bmm_contact_id, bmm_deal_id, processed, processing_notes. Corpus is immutable: message_id, body_text, sent_at, raw_size_bytes, imported_at cannot be changed. to_emails and cc_emails merge (union of existing + new) rather than replace.',
    inputSchema: {
      type: 'object',
      properties: {
        archive_id: { type: 'number' },
        updates: { type: 'object', description: 'Whitelisted fields only. to_emails/cc_emails are arrays and merge rather than replace.' }
      },
      required: ['archive_id', 'updates']
    }
  }
];

// -- Tool Handlers (domain logic - identical to bmm-server.js stdio) --
async function handleToolCall(name, args) {
  try {
    // -- create_contact (dedup gate) --
    if (name === 'create_contact') {
      if (args.email) {
        const existing = await query(
          `SELECT id, first_name, last_name, email FROM contacts WHERE email ILIKE $1 AND status != 'deleted' LIMIT 1`,
          [args.email]
        );
        if (existing.rows.length > 0) {
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                duplicate_detected: true,
                existing_contact: existing.rows[0],
                message: `Contact with email ${args.email} already exists. Use update_contact to modify.`
              }, null, 2)
            }]
          };
        }
      }
      const fields = { ...args };
      if (fields.context) fields.context = JSON.stringify(fields.context);

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => {
        if (['roles', 'industry_tags'].includes(columns[i])) return `$${i + 1}::text[]`;
        if (columns[i] === 'context') return `$${i + 1}::jsonb`;
        return `$${i + 1}`;
      });
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO contacts (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- update_contact (JSONB merge) --
    if (name === 'update_contact') {
      const { clause, values } = buildUpdateClause(args.updates);
      values.push(args.contact_id);
      const result = await query(
        `UPDATE contacts SET ${clause} WHERE id = $${values.length} RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- search_contacts --
    if (name === 'search_contacts') {
      let sql = `SELECT id, first_name, last_name, company_name, email, phone, roles, industry_tags, assigned_broker, co_broker, status, context
           FROM contacts WHERE status != 'deleted' AND (
             search_text ILIKE '%' || $1 || '%'
             OR email ILIKE '%' || $1 || '%'
           )`;
      const params = [args.query];
      let idx = 2;

      if (args.role) {
        sql += ` AND $${idx} = ANY(roles)`;
        params.push(args.role);
        idx++;
      }
      if (args.industry) {
        sql += ` AND $${idx} = ANY(industry_tags)`;
        params.push(args.industry);
        idx++;
      }
      if (args.assigned_broker) {
        sql += ` AND assigned_broker = $${idx}`;
        params.push(args.assigned_broker);
        idx++;
      }
      sql += ` ORDER BY last_name, first_name LIMIT $${idx}`;
      params.push(args.limit || 25);

      const result = await query(sql, params);
      return { content: [{ type: 'text', text: JSON.stringify(result.rows, null, 2) }] };
    }

    // -- get_contact (multi-table join + memory) --
    if (name === 'get_contact') {
      const contact = await query('SELECT * FROM contacts WHERE id = $1', [args.contact_id]);
      const entities = await query('SELECT * FROM entities WHERE contact_id = $1', [args.contact_id]);
      const deals = await query('SELECT * FROM deals WHERE seller_id = $1 OR buyer_id = $1', [args.contact_id]);
      const profiles = await query('SELECT * FROM buyer_profiles WHERE contact_id = $1', [args.contact_id]);
      const interactions = await query(
        'SELECT * FROM interactions WHERE contact_id = $1 ORDER BY occurred_at DESC LIMIT 10',
        [args.contact_id]
      );
      const memory = await query(
        `SELECT * FROM shared_memory WHERE contact_id = $1 ORDER BY category, created_at DESC`,
        [args.contact_id]
      );
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            contact: contact.rows[0],
            entities: entities.rows,
            deals: deals.rows,
            buyer_profiles: profiles.rows,
            recent_interactions: interactions.rows,
            memory_entries: memory.rows
          }, null, 2)
        }]
      };
    }

    // -- update_buyer_profile (JSONB merge) --
    if (name === 'update_buyer_profile') {
      const { clause, values } = buildUpdateClause(args.updates);
      values.push(args.profile_id);
      const result = await query(
        `UPDATE buyer_profiles SET ${clause} WHERE id = $${values.length} RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- log_interaction --
    if (name === 'log_interaction') {
      const fields = { ...args };
      if (fields.context) fields.context = JSON.stringify(fields.context);

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => {
        if (columns[i] === 'context') return `$${i + 1}::jsonb`;
        return `$${i + 1}`;
      });
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO interactions (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- create_flag (dedupe gate: idempotent on dedupe_key, else exact-duplicate guard) --
    if (name === 'create_flag') {
      const fields = { ...args };

      // dedupe_key is not a column. Fold it into context so it persists and can be matched.
      const dedupeKey = fields.dedupe_key || (fields.context && fields.context.dedupe_key) || null;
      delete fields.dedupe_key;
      if (dedupeKey) {
        fields.context = { ...(fields.context || {}), dedupe_key: dedupeKey };
      }

      // Dedupe gate. Mirrors the resolve_flag pattern (status != 'resolved').
      // Primary key: dedupe_key, used by recurring jobs like the morning briefing.
      // Fallback: block an EXACT (parent, flag_type, description) open duplicate. We
      // deliberately do NOT dedupe on (deal_id, flag_type) alone, because one deal can
      // legitimately carry several distinct open flags of the same type.
      let existing = { rows: [] };
      if (dedupeKey) {
        const params = [dedupeKey];
        let sql = `SELECT * FROM flags WHERE status != 'resolved' AND context->>'dedupe_key' = $1`;
        if (fields.deal_id) { sql += ` AND deal_id = $2`; params.push(fields.deal_id); }
        sql += ` LIMIT 1`;
        existing = await query(sql, params);
      } else if (fields.deal_id) {
        existing = await query(
          `SELECT * FROM flags WHERE status != 'resolved' AND deal_id = $1 AND flag_type = $2 AND description = $3 LIMIT 1`,
          [fields.deal_id, fields.flag_type, fields.description]
        );
      } else if (fields.contact_id) {
        existing = await query(
          `SELECT * FROM flags WHERE status != 'resolved' AND contact_id = $1 AND flag_type = $2 AND description = $3 LIMIT 1`,
          [fields.contact_id, fields.flag_type, fields.description]
        );
      }

      if (existing.rows.length > 0) {
        const hit = existing.rows[0];
        // Re-surfacing an open item: refresh due_date if a new one was supplied, else skip.
        if (fields.due_date) {
          const upd = await query(
            `UPDATE flags SET due_date = $1 WHERE id = $2 RETURNING *`,
            [fields.due_date, hit.id]
          );
          return { content: [{ type: 'text', text: JSON.stringify({ deduped: true, action: 'updated_due_date', flag: upd.rows[0] }, null, 2) }] };
        }
        return { content: [{ type: 'text', text: JSON.stringify({ deduped: true, action: 'skipped', flag: hit }, null, 2) }] };
      }

      if (fields.context) fields.context = JSON.stringify(fields.context);

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => {
        if (columns[i] === 'context') return `$${i + 1}::jsonb`;
        return `$${i + 1}`;
      });
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO flags (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- list_flags (co_broker JOIN, priority ordering) --
    if (name === 'list_flags') {
      let sql = 'SELECT f.*, d.deal_name, c.first_name || \' \' || c.last_name AS contact_name FROM flags f LEFT JOIN deals d ON f.deal_id = d.id LEFT JOIN contacts c ON f.contact_id = c.id WHERE 1=1';
      const params = [];
      let idx = 1;

      if (!args.include_resolved) { sql += ` AND f.status != 'resolved'`; }
      if (args.assigned_to) {
        sql += ` AND (f.assigned_to = $${idx} OR c.co_broker = $${idx})`;
        params.push(args.assigned_to);
        idx++;
      }
      if (args.deal_id) { sql += ` AND f.deal_id = $${idx}`; params.push(args.deal_id); idx++; }
      if (args.priority) { sql += ` AND f.priority = $${idx}`; params.push(args.priority); idx++; }
      if (args.flag_type) { sql += ` AND f.flag_type = $${idx}`; params.push(args.flag_type); idx++; }
      sql += ' ORDER BY CASE f.priority WHEN \'urgent\' THEN 1 WHEN \'high\' THEN 2 WHEN \'normal\' THEN 3 WHEN \'low\' THEN 4 END, f.due_date NULLS LAST';

      const result = await query(sql, params);
      return { content: [{ type: 'text', text: JSON.stringify(result.rows, null, 2) }] };
    }

    // -- resolve_flag (auto-cadence scheduling) --
    if (name === 'resolve_flag') {
      const result = await query(
        `UPDATE flags SET status = 'resolved', resolved_at = NOW(), resolved_by = $1, resolution = $2 WHERE id = $3 RETURNING *`,
        [args.resolved_by || null, args.resolution || null, args.flag_id]
      );
      const resolvedFlag = result.rows[0];

      let nextFlag = null;
      if (resolvedFlag && resolvedFlag.contact_id) {
        const contact = await query(
          `SELECT follow_up_interval_days, assigned_broker FROM contacts WHERE id = $1`,
          [resolvedFlag.contact_id]
        );
        const c = contact.rows[0];
        if (c && c.follow_up_interval_days) {
          const existing = await query(
            `SELECT id FROM flags WHERE contact_id = $1 AND status != 'resolved' AND flag_type = $2 LIMIT 1`,
            [resolvedFlag.contact_id, resolvedFlag.flag_type]
          );
          if (existing.rows.length === 0) {
            // Do not copy resolvedFlag.description: it propagates stale text like
            // "OVERDUE 12 DAYS...". Use a caller-supplied description if present,
            // otherwise generate a clean dated one in SQL.
            const next = await query(
              `INSERT INTO flags (contact_id, flag_type, description, priority, assigned_to, due_date, created_by)
               VALUES ($1, $2,
                 COALESCE($3, 'Recurring follow-up due ' || to_char(CURRENT_DATE + $6::integer, 'YYYY-MM-DD')),
                 $4, $5, CURRENT_DATE + $6::integer, $7)
               RETURNING *`,
              [
                resolvedFlag.contact_id,
                resolvedFlag.flag_type,
                args.description || null,
                resolvedFlag.priority || 'normal',
                resolvedFlag.assigned_to,
                c.follow_up_interval_days,
                args.resolved_by || null
              ]
            );
            nextFlag = next.rows[0];
          } else {
            nextFlag = { duplicate_skipped: true, existing_flag_id: existing.rows[0].id };
          }
        }
      }

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            resolved: resolvedFlag,
            next_flag: nextFlag
          }, null, 2)
        }]
      };
    }

    // -- find_buyers (multi-criteria matching) --
    if (name === 'find_buyers') {
      const deal = await query('SELECT * FROM deals WHERE id = $1', [args.deal_id]);
      if (deal.rows.length === 0) return { content: [{ type: 'text', text: 'Deal not found' }] };

      const d = deal.rows[0];
      const tolerance = (args.price_tolerance_pct || 20) / 100;
      const includeFlexible = args.include_flexible !== false;
      const priceLow = d.asking_price ? d.asking_price * (1 - tolerance) : 0;
      const priceHigh = d.asking_price ? d.asking_price * (1 + tolerance) : 999999999;

      let sql = `
        SELECT
          bp.*,
          c.id AS contact_id, c.first_name, c.last_name, c.email, c.phone,
          c.company_name, c.city, c.state, c.context AS contact_context
        FROM buyer_profiles bp
        JOIN contacts c ON bp.contact_id = c.id
        WHERE c.status = 'active'
        AND 'buyer' = ANY(c.roles)
        AND (
          bp.industry_tags && $1::text[]
          OR (bp.price_min IS NULL OR bp.price_min <= $3)
          AND (bp.price_max IS NULL OR bp.price_max >= $2)
      `;
      const params = [d.industry_tags || [], priceLow, priceHigh];

      if (includeFlexible) {
        sql += ` OR bp.price_flexible = true`;
      }
      sql += `)`;
      if (d.industry_tags && d.industry_tags.length > 0) {
        sql += ` AND NOT (bp.industry_exclude && $4::text[])`;
        params.push(d.industry_tags);
      }
      sql += ` ORDER BY c.last_name`;

      const result = await query(sql, params);
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            deal: { id: d.id, name: d.deal_name, asking_price: d.asking_price, industry: d.industry_tags, city: d.business_city, context: d.context },
            candidate_count: result.rows.length,
            candidates: result.rows
          }, null, 2)
        }]
      };
    }

    // -- my_followups (co_broker visibility) --
    if (name === 'my_followups') {
      const daysAhead = args.days_ahead || 7;
      const result = await query(
        `SELECT f.* FROM open_followups f
         LEFT JOIN contacts c ON f.contact_id = c.id
         WHERE (f.broker = $1 OR c.co_broker = $1)
         AND f.follow_up_date <= CURRENT_DATE + $2::integer
         ORDER BY f.follow_up_date`,
        [args.broker, daysAhead]
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows, null, 2) }] };
    }

    // == MEMORY handlers ==

    // -- hal_remember (PII hard-block enforced server-side) --
    if (name === 'hal_remember') {
      // NON-NEGOTIABLE: Block PII
      if (containsPII(args.content)) {
        return {
          content: [{
            type: 'text',
            text: JSON.stringify({
              error: 'PII_BLOCKED',
              message: 'Content contains patterns matching PII (SSN, EIN, account numbers, tax IDs, credit card numbers). Memory entry rejected. Rephrase without sensitive identifiers.'
            }, null, 2)
          }]
        };
      }

      // Validate source
      if (!VALID_SOURCES.includes(args.source)) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'INVALID_SOURCE', valid: VALID_SOURCES }) }] };
      }

      // Validate category
      if (!VALID_CATEGORIES.includes(args.category)) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'INVALID_CATEGORY', valid: VALID_CATEGORIES }) }] };
      }

      // Require confidence for HAL entries
      if (args.source !== 'broker' && !args.confidence) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'CONFIDENCE_REQUIRED', message: 'confidence (high/medium/low) is required for hal_explicit and hal_implicit entries' }) }] };
      }
      if (args.confidence && !VALID_CONFIDENCE.includes(args.confidence)) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'INVALID_CONFIDENCE', valid: VALID_CONFIDENCE }) }] };
      }

      // Look up broker_id
      const broker = await query(
        `SELECT id FROM broker_profiles WHERE broker_name = $1 AND active = true`,
        [args.broker]
      );
      if (broker.rows.length === 0) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'BROKER_NOT_FOUND', broker: args.broker }) }] };
      }
      const brokerId = broker.rows[0].id;

      // Build INSERT
      const fields = {
        broker_id: brokerId,
        source: args.source,
        category: args.category,
        content: args.content,
        visibility: args.visibility || 'shared',
        pii_confirmed_clean: true
      };
      if (args.contact_id) fields.contact_id = args.contact_id;
      if (args.deal_id) fields.deal_id = args.deal_id;
      if (args.interaction_id) fields.interaction_id = args.interaction_id;
      if (args.confidence) fields.confidence = args.confidence;
      if (args.expires_at) fields.expires_at = args.expires_at;
      if (args.visibility === 'private') fields.private_broker_id = brokerId;

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => `$${i + 1}`);
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO broker_memory_entries (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- dismiss_memory --
    if (name === 'dismiss_memory') {
      const result = await query(
        `UPDATE broker_memory_entries SET dismissed_at = NOW(), dismissed_by = $1 WHERE id = $2 RETURNING *`,
        [args.dismissed_by, args.memory_id]
      );
      if (result.rows.length === 0) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'MEMORY_NOT_FOUND', id: args.memory_id }) }] };
      }
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- get_broker_context --
    if (name === 'get_broker_context') {
      const broker = await query(
        `SELECT * FROM broker_profiles WHERE broker_name = $1`,
        [args.broker]
      );
      if (broker.rows.length === 0) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'BROKER_NOT_FOUND', broker: args.broker }) }] };
      }
      const profile = broker.rows[0];

      // Update last_session_at
      await query(
        `UPDATE broker_profiles SET last_session_at = NOW() WHERE id = $1`,
        [profile.id]
      );

      // Get active memory entries (shared + this broker's private)
      const memory = await query(
        `SELECT * FROM broker_memory_entries
         WHERE deleted_at IS NULL
           AND dismissed_at IS NULL
           AND archived_at IS NULL
           AND (expires_at IS NULL OR expires_at > NOW())
           AND (visibility = 'shared' OR private_broker_id = $1)
         ORDER BY category, created_at DESC`,
        [profile.id]
      );

      // Session summary
      const dealCount = await query(
        `SELECT COUNT(*) as count FROM active_deals WHERE assigned_broker = $1`,
        [args.broker]
      );
      const flagCount = await query(
        `SELECT COUNT(*) as count FROM open_flags WHERE assigned_to = $1`,
        [args.broker]
      );

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            profile: profile,
            memory_entries: memory.rows,
            session_summary: {
              active_deals: parseInt(dealCount.rows[0].count),
              open_flags: parseInt(flagCount.rows[0].count)
            }
          }, null, 2)
        }]
      };
    }

    // == CRUD handlers: retained for Kevin/Mike (Jeremy uses bmm-supabase) ==

    // -- create_entity --
    if (name === 'create_entity') {
      const fields = { ...args };
      if (fields.context) fields.context = JSON.stringify(fields.context);
      if (fields.ucc_liens) fields.ucc_liens = JSON.stringify(fields.ucc_liens);
      if (fields.officers) fields.officers = JSON.stringify(fields.officers);

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => {
        if (['context', 'ucc_liens', 'officers'].includes(columns[i])) return `$${i + 1}::jsonb`;
        return `$${i + 1}`;
      });
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO entities (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- create_deal --
    if (name === 'create_deal') {
      const fields = { ...args };
      if (fields.context) fields.context = JSON.stringify(fields.context);

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => {
        if (columns[i] === 'industry_tags') return `$${i + 1}::text[]`;
        if (columns[i] === 'context') return `$${i + 1}::jsonb`;
        return `$${i + 1}`;
      });
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO deals (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- update_deal (stage_changed_at handled by Postgres trigger) --
    if (name === 'update_deal') {
      const { clause, values } = buildUpdateClause(args.updates);
      values.push(args.deal_id);
      const result = await query(
        `UPDATE deals SET ${clause} WHERE id = $${values.length} RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- get_deal (+ memory) --
    if (name === 'get_deal') {
      const deal = await query('SELECT * FROM deals WHERE id = $1', [args.deal_id]);
      if (deal.rows.length === 0) return { content: [{ type: 'text', text: 'Deal not found' }] };

      const d = deal.rows[0];
      const seller = d.seller_id ? await query('SELECT * FROM contacts WHERE id = $1', [d.seller_id]) : { rows: [] };
      const entity = d.entity_id ? await query('SELECT * FROM entities WHERE id = $1', [d.entity_id]) : { rows: [] };
      const financials = await query('SELECT * FROM deal_financials WHERE deal_id = $1 ORDER BY fiscal_year DESC', [args.deal_id]);
      const balance = await query('SELECT * FROM deal_balance_sheet WHERE deal_id = $1', [args.deal_id]);
      const docs = await query('SELECT * FROM documents WHERE deal_id = $1', [args.deal_id]);
      const flags = await query("SELECT * FROM flags WHERE deal_id = $1 AND status != 'resolved'", [args.deal_id]);
      const matches = await query('SELECT m.*, c.first_name, c.last_name, c.company_name FROM matches m JOIN contacts c ON m.buyer_id = c.id WHERE m.deal_id = $1', [args.deal_id]);
      const memory = await query(
        `SELECT * FROM shared_memory WHERE deal_id = $1 ORDER BY category, created_at DESC`,
        [args.deal_id]
      );

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            deal: d,
            seller: seller.rows[0] || null,
            entity: entity.rows[0] || null,
            financials: financials.rows,
            balance_sheet: balance.rows[0] || null,
            documents: docs.rows,
            open_flags: flags.rows,
            matches: matches.rows,
            memory_entries: memory.rows
          }, null, 2)
        }]
      };
    }

    // -- list_deals --
    if (name === 'list_deals') {
      let sql = 'SELECT d.*, c.first_name || \' \' || c.last_name AS seller_name FROM deals d LEFT JOIN contacts c ON d.seller_id = c.id WHERE 1=1';
      const params = [];
      let idx = 1;

      if (args.stage) { sql += ` AND d.stage = $${idx}`; params.push(args.stage); idx++; }
      if (args.assigned_broker) { sql += ` AND d.assigned_broker = $${idx}`; params.push(args.assigned_broker); idx++; }
      if (args.deal_type) { sql += ` AND d.deal_type = $${idx}`; params.push(args.deal_type); idx++; }
      if (args.industry) { sql += ` AND $${idx} = ANY(d.industry_tags)`; params.push(args.industry); idx++; }
      sql += ` ORDER BY d.updated_at DESC LIMIT $${idx}`;
      params.push(args.limit || 25);

      const result = await query(sql, params);
      return { content: [{ type: 'text', text: JSON.stringify(result.rows, null, 2) }] };
    }

    // -- add_financials (upsert) --
    if (name === 'add_financials') {
      const fields = { ...args };
      if (fields.context) fields.context = JSON.stringify(fields.context);

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => {
        if (columns[i] === 'context') return `$${i + 1}::jsonb`;
        return `$${i + 1}`;
      });
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO deal_financials (${columns.join(', ')}) VALUES (${placeholders.join(', ')})
         ON CONFLICT (deal_id, fiscal_year) DO UPDATE SET ${columns.filter(c => c !== 'deal_id' && c !== 'fiscal_year').map(c => `${c} = EXCLUDED.${c}`).join(', ')}
         RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- create_buyer_profile --
    if (name === 'create_buyer_profile') {
      const fields = { ...args };
      if (fields.context) fields.context = JSON.stringify(fields.context);

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => {
        const col = columns[i];
        if (col === 'context') return `$${i + 1}::jsonb`;
        if (['industry_tags', 'industry_exclude', 'preferred_states', 'preferred_counties', 'industry_experience'].includes(col)) return `$${i + 1}::text[]`;
        return `$${i + 1}`;
      });
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO buyer_profiles (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- track_document (timestamps handled by Postgres trigger) --
    if (name === 'track_document') {
      const fields = { ...args };
      if (fields.context) fields.context = JSON.stringify(fields.context);

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => {
        if (columns[i] === 'context') return `$${i + 1}::jsonb`;
        return `$${i + 1}`;
      });
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO documents (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- save_match --
    if (name === 'save_match') {
      // Validate outcome (same enum as update_match) so a bad value returns a clean
      // error instead of a raw constraint violation. The dynamic INSERT below carries
      // outcome/contacted/contacted_by/contacted_at through automatically.
      if (args.outcome && !MATCH_OUTCOMES.includes(args.outcome)) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'INVALID_OUTCOME', valid: MATCH_OUTCOMES }) }] };
      }
      // A non-live outcome must satisfy matches_terminal_outcome_requires_reason:
      // reasoning >=20 chars AND contacted_by NOT NULL. Check here for a clean error.
      if (args.outcome && args.outcome !== 'live') {
        if (!args.contacted_by) {
          return { content: [{ type: 'text', text: JSON.stringify({ error: 'CONTACTED_BY_REQUIRED', message: 'A non-live outcome requires contacted_by.' }) }] };
        }
        if (!args.reasoning || args.reasoning.length < 20) {
          return { content: [{ type: 'text', text: JSON.stringify({ error: 'REASONING_REQUIRED', message: 'A non-live outcome requires reasoning of at least 20 characters.' }) }] };
        }
      }
      const fields = { ...args };
      if (fields.context) fields.context = JSON.stringify(fields.context);

      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => {
        const col = columns[i];
        if (col === 'context') return `$${i + 1}::jsonb`;
        if (['matches_on', 'gaps'].includes(col)) return `$${i + 1}::text[]`;
        return `$${i + 1}`;
      });
      const values = columns.map(k => fields[k]);

      const result = await query(
        `INSERT INTO matches (${columns.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- pipeline --
    if (name === 'pipeline') {
      let sql = 'SELECT * FROM pipeline_summary';
      const params = [];
      if (args.assigned_broker) {
        sql = `SELECT * FROM pipeline_summary WHERE assigned_broker = $1`;
        params.push(args.assigned_broker);
      }
      const result = await query(sql, params);
      return { content: [{ type: 'text', text: JSON.stringify(result.rows, null, 2) }] };
    }

    // -- raw_query (SELECT only) --
    if (name === 'raw_query') {
      const trimmed = args.sql.trim().toUpperCase();
      if (!trimmed.startsWith('SELECT')) {
        return { content: [{ type: 'text', text: 'Only SELECT queries allowed via raw_query.' }] };
      }
      const result = await query(args.sql, args.params || []);
      return { content: [{ type: 'text', text: JSON.stringify(result.rows, null, 2) }] };
    }

    // -- send_broker_message --
    if (name === 'send_broker_message') {
      const { to_broker, from_broker, message, contact_id, deal_id } = args;
      const fields = { to_broker, from_broker, message };
      if (contact_id) fields.contact_id = contact_id;
      if (deal_id) fields.deal_id = deal_id;
      const columns = Object.keys(fields);
      const placeholders = columns.map((_, i) => `$${i + 1}`);
      const values = columns.map(k => fields[k]);
      const result = await query(
        `INSERT INTO broker_messages (${columns.join(', ')})
         VALUES (${placeholders.join(', ')}) RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(result.rows[0], null, 2) }] };
    }

    // -- get_broker_messages --
    if (name === 'get_broker_messages') {
      const { broker, mark_read = true } = args;
      const messages = await query(
        `SELECT m.*,
                c.first_name || ' ' || c.last_name AS contact_name,
                d.deal_name
         FROM broker_messages m
         LEFT JOIN contacts c ON m.contact_id = c.id
         LEFT JOIN deals d ON m.deal_id = d.id
         WHERE m.to_broker = $1
           AND m.read_at IS NULL
           AND m.deleted_at IS NULL
         ORDER BY m.created_at ASC`,
        [broker]
      );
      if (mark_read && messages.rows.length > 0) {
        const ids = messages.rows.map(r => r.id);
        await query(
          `UPDATE broker_messages SET read_at = NOW()
           WHERE id = ANY($1::uuid[])`,
          [ids]
        );
      }
      return { content: [{ type: 'text', text: JSON.stringify(messages.rows, null, 2) }] };
    }

    // -- add_balance_sheet (one per deal: update existing else insert) --
    if (name === 'add_balance_sheet') {
      const fields = { ...args };
      if (fields.context) fields.context = JSON.stringify(fields.context);
      if (fields.recast_data) fields.recast_data = JSON.stringify(fields.recast_data);
      const jsonbCols = ['context', 'recast_data'];

      const existing = await query(
        `SELECT id FROM deal_balance_sheet WHERE deal_id = $1 AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1`,
        [args.deal_id]
      );

      if (existing.rows.length > 0) {
        const updates = { ...fields };
        delete updates.deal_id;
        const cols = Object.keys(updates);
        if (cols.length === 0) return { content: [{ type: 'text', text: 'No fields to update' }] };
        const sets = cols.map((c, i) => (jsonbCols.includes(c) ? `${c} = $${i + 1}::jsonb` : `${c} = $${i + 1}`));
        const values = cols.map(c => updates[c]);
        values.push(existing.rows[0].id);
        const r = await query(`UPDATE deal_balance_sheet SET ${sets.join(', ')} WHERE id = $${values.length} RETURNING *`, values);
        return { content: [{ type: 'text', text: JSON.stringify(r.rows[0], null, 2) }] };
      }

      const cols = Object.keys(fields);
      const placeholders = cols.map((c, i) => (jsonbCols.includes(c) ? `$${i + 1}::jsonb` : `$${i + 1}`));
      const values = cols.map(c => fields[c]);
      const r = await query(`INSERT INTO deal_balance_sheet (${cols.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`, values);
      return { content: [{ type: 'text', text: JSON.stringify(r.rows[0], null, 2) }] };
    }

    // -- update_document (status timestamps auto-set by trigger) --
    if (name === 'update_document') {
      const { clause, values } = buildUpdateClause(args.updates);
      values.push(args.document_id);
      const r = await query(
        `UPDATE documents SET ${clause}, updated_at = NOW() WHERE id = $${values.length} RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: r.rows.length ? JSON.stringify(r.rows[0], null, 2) : 'Document not found' }] };
    }

    // -- update_match (outcome enum; non-live outcome requires reasoning + contacted_by via DB check) --
    if (name === 'update_match') {
      if (args.updates && args.updates.outcome && !MATCH_OUTCOMES.includes(args.updates.outcome)) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'INVALID_OUTCOME', valid: MATCH_OUTCOMES }) }] };
      }
      const { clause, values } = buildUpdateClause(args.updates);
      values.push(args.match_id);
      const r = await query(
        `UPDATE matches SET ${clause}, updated_at = NOW() WHERE id = $${values.length} RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: r.rows.length ? JSON.stringify(r.rows[0], null, 2) : 'Match not found' }] };
    }

    // -- update_entity (jsonb: context merged, ucc_liens/officers replaced) --
    if (name === 'update_entity') {
      const updates = { ...args.updates };
      const cols = Object.keys(updates);
      if (cols.length === 0) return { content: [{ type: 'text', text: 'No fields to update' }] };
      const jsonbCols = ['context', 'ucc_liens', 'officers'];
      const sets = cols.map((c, i) => {
        if (c === 'context') return `${c} = COALESCE(${c}, '{}'::jsonb) || $${i + 1}::jsonb`;
        if (jsonbCols.includes(c)) return `${c} = $${i + 1}::jsonb`;
        return `${c} = $${i + 1}`;
      });
      const values = cols.map(c => (jsonbCols.includes(c) && typeof updates[c] === 'object' ? JSON.stringify(updates[c]) : updates[c]));
      values.push(args.entity_id);
      const r = await query(
        `UPDATE entities SET ${sets.join(', ')}, updated_at = NOW() WHERE id = $${values.length} RETURNING *`,
        values
      );
      return { content: [{ type: 'text', text: r.rows.length ? JSON.stringify(r.rows[0], null, 2) : 'Entity not found' }] };
    }

    // -- update_broker_profile (merge broker_memory scratchpad) --
    if (name === 'update_broker_profile') {
      const broker = await query(`SELECT id FROM broker_profiles WHERE broker_name = $1`, [args.broker]);
      if (broker.rows.length === 0) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'BROKER_NOT_FOUND', broker: args.broker }) }] };
      }
      const sets = [];
      const values = [];
      let i = 1;
      if (args.broker_memory !== undefined) {
        sets.push(`broker_memory = COALESCE(broker_memory, '{}'::jsonb) || $${i++}::jsonb`);
        values.push(JSON.stringify(args.broker_memory));
        sets.push('broker_memory_updated_at = NOW()');
      }
      if (args.email !== undefined) { sets.push(`email = $${i++}`); values.push(args.email); }
      if (args.phone !== undefined) { sets.push(`phone = $${i++}`); values.push(args.phone); }
      if (sets.length === 0) {
        return { content: [{ type: 'text', text: 'No fields to update (provide broker_memory, email, or phone)' }] };
      }
      sets.push('updated_at = NOW()');
      values.push(broker.rows[0].id);
      const r = await query(
        `UPDATE broker_profiles SET ${sets.join(', ')} WHERE id = $${values.length}
         RETURNING broker_name, email, phone, broker_memory, broker_memory_updated_at`,
        values
      );
      return { content: [{ type: 'text', text: JSON.stringify(r.rows[0], null, 2) }] };
    }

    // -- update_email_archive (HAL write path; whitelist editable, corpus immutable) --
    if (name === 'update_email_archive') {
      const EDITABLE = ['from_email', 'from_name', 'to_emails', 'cc_emails', 'bmm_contact_id', 'bmm_deal_id', 'processed', 'processing_notes'];
      const ARRAY_FIELDS = ['to_emails', 'cc_emails'];
      const updates = args.updates || {};
      const keys = Object.keys(updates);
      if (keys.length === 0) return { content: [{ type: 'text', text: 'No fields to update' }] };
      const blocked = keys.filter(k => !EDITABLE.includes(k));
      if (blocked.length > 0) {
        return { content: [{ type: 'text', text: JSON.stringify({ error: 'FIELD_NOT_EDITABLE', blocked, editable: EDITABLE, note: 'message_id, body_text, sent_at, raw_size_bytes, imported_at are immutable' }) }] };
      }
      const sets = keys.map((k, i) => {
        // to_emails/cc_emails merge (union of existing + new); scalar fields replace.
        if (ARRAY_FIELDS.includes(k)) {
          return `${k} = (SELECT array(SELECT DISTINCT e FROM unnest(COALESCE(${k}, '{}'::text[]) || $${i + 1}::text[]) AS e))`;
        }
        return `${k} = $${i + 1}`;
      });
      const values = keys.map(k => updates[k]);
      values.push(args.archive_id);
      const r = await query(
        `UPDATE email_archive SET ${sets.join(', ')} WHERE id = $${values.length}
         RETURNING id, from_email, from_name, to_emails, cc_emails, bmm_contact_id, bmm_deal_id, processed, processing_notes`,
        values
      );
      return { content: [{ type: 'text', text: r.rows.length ? JSON.stringify(r.rows[0], null, 2) : 'Email archive row not found' }] };
    }

    return { content: [{ type: 'text', text: `Unknown tool: ${name}` }] };

  } catch (error) {
    return {
      content: [{
        type: 'text',
        text: JSON.stringify({
          error: error.message,
          tool: name,
          detail: error.detail || null,
          hint: error.hint || null
        })
      }]
    };
  }
}

// -- MCP Server factory (one instance per request in stateless mode) --
function createMcpServer() {
  const server = new Server(
    { name: 'bmm-server', version: '2.1.0' },
    { capabilities: { tools: {} } }
  );
  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: TOOLS }));
  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;
    return handleToolCall(name, args || {});
  });
  return server;
}

// -- Auth: shared secret --
// Primary path is the URL token (?token=<secret>), because Claude's connector UI
// has no bearer/header field. Header forms are still accepted for API/testing use.
function auth(req, res, next) {
  if (!SHARED_SECRET) return next(); // dev: no secret configured = open
  const token =
    req.query.token ||
    (req.headers['authorization'] || '').replace(/^Bearer\s+/i, '') ||
    req.headers['x-api-key'];
  if (token !== SHARED_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

// -- Express app --
const app = express();
app.use(express.json({ limit: '4mb' }));

// Health check — Render uses this to verify the service is up (no auth).
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'bmm-server', transport: 'streamable-http', timestamp: new Date().toISOString() });
});

// MCP endpoint — Streamable HTTP, stateless. claude.ai connects here.
app.post('/mcp', auth, async (req, res) => {
  const server = createMcpServer();
  const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
  res.on('close', () => {
    transport.close();
    server.close();
  });
  try {
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  } catch (err) {
    console.error('MCP request error:', err);
    if (!res.headersSent) {
      res.status(500).json({
        jsonrpc: '2.0',
        error: { code: -32603, message: 'Internal server error' },
        id: null
      });
    }
  }
});

// Stateless mode does not support server-initiated GET streams or DELETE sessions.
const methodNotAllowed = (req, res) => {
  res.status(405).json({
    jsonrpc: '2.0',
    error: { code: -32000, message: 'Method not allowed. This server is stateless; use POST /mcp.' },
    id: null
  });
};
app.get('/mcp', methodNotAllowed);
app.delete('/mcp', methodNotAllowed);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`BMM MCP Server v2.1 (Streamable HTTP) running on port ${PORT}`);
});
