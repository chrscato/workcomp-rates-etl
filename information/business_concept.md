# BeaconPoint Health – Transparency & Steerage Platform

## Executive Summary
BeaconPoint Health is building a **data-first transparency and steerage platform** designed to outcompete incumbents like Turquoise Health in the **Workers’ Compensation niche**. Our unique value is in combining:
- **Commercial MRF negotiated rates**
- **State Workers’ Comp Fee Schedules**
- **Medicare benchmarks (ASC, OPPS, Professional)**

…into a **single, queryable warehouse** that powers both **self-service dashboards** for mid-market payers and **steerage guidance agents** for claims adjustors and risk managers.

We organize all data in a robust **Iceberg star schema** with fact and dimension tables (providers, codes, payers, benchmarks):contentReference[oaicite:3]{index=3}. This ensures scalability, integrity, and seamless expansion.

---

## Staged Product Build

### Stage 1 – Data Infrastructure Foundation
- **Deliverable:** Iceberg-based fact + dimension tables, partitioned by payer/state/billing class/procedure set.
- **ETL:** Automated ingestion of MRF + WC + Medicare benchmarks, with idempotent upserts (`fact_uid`).
- **Output:** Pre-cut **tiles** (Parquet files) partitioned at S3 by payer/state/class/set, with NPI/TIN/provider info and negotiated rates.
- **Usage:** Low-cost transparency dashboards for users to explore summary rate benchmarks.

**Stack**
- Storage: AWS S3 (Parquet/ZSTD)
- Table format: Apache Iceberg
- Query: DuckDB (lightweight), Athena/Trino (heavy)
- ETL: Python (Polars, PyArrow, DuckDB), GitHub Actions
- Deployment: DigitalOcean droplet (Streamlit/Django webapp)

**Estimated Cost**
- $200–500/mo (storage + droplet + Athena queries for admin)
- Tiles served via CDN are nearly free per user.

---

### Stage 2 – Commercial Dashboard Offering
- **Deliverable:** White-labeled webapp dashboards for payers, TPAs, and provider networks.
- **Features:**
  - Hierarchical search: payer → state → billing class → procedure set/class/group → taxonomy → billing code
  - Benchmarks: Medicare vs WC FS vs commercial side-by-side
  - Provider network summaries: NPI/TIN rollups, geo filters
- **Monetization:** Subscription tiers
  - Free/Low: Cached tiles (monthly)
  - Pro: Partition-level drilldowns (weekly refresh)
  - Enterprise: Full Iceberg/Trino query access (daily refresh, SLA)

**Stack**
- Frontend: Streamlit or React+Django templates
- Backend: FastAPI for API layer (tile lookup vs compute query)
- LLM integration: MCP/agent for NL-to-SQL query generation (Enterprise tier)
- Authentication: Auth0 / Django allauth (for white-label tenants)

**Estimated Cost**
- $1–3k/mo infra at scale (S3 + Athena/Trino queries)
- 80% margin achievable on subscription pricing

---

### Stage 3 – Steerage Guidance Agent
- **Deliverable:** AI-powered decision agent for claims adjustors and risk managers.
- **Workflow:**
  1. Adjustor enters claim details (injury, order, demographics).
  2. LLM agent predicts expected procedure bundle.
  3. Query provider network (via tiles + Iceberg) for:
     - Closest providers (geo-shards using ZIP3/H3)
     - Cost-effective providers (negotiated vs WC/Medicare benchmarks)
     - High-quality providers (outcomes data as added layer)
  4. Return steerage recommendations.

- **Monetization:** Premium per-seat SaaS for adjustors, risk managers, and municipalities.

**Stack**
- Geo indexing: Uber H3 (lat/lon → cell shards)
- Ranking: Haversine distance + benchmark-adjusted rates
- Agent layer: LangChain/OpenAI MCP → Athena/Trino
- API: JSON endpoints for steerage results
- UI: Embedded widget or standalone dashboard for adjustors

**Estimated Cost**
- $5–10k/mo infra (scales with Athena/Trino compute + LLM API calls)
- ROI: High willingness-to-pay from TPAs and self-insured employers (millions in steerage savings).

---

## Business Breakdown

**Primary Revenue Streams**
1. **Subscription SaaS (Transparency Dashboards)**
   - $500–2,500/mo depending on tier & users
   - Target: small payers, TPAs, provider networks
2. **Enterprise Contracts (Steerage Agent)**
   - $50k–250k/yr depending on adjustor count & integration depth
   - Target: municipalities, large employers, insurers
3. **Data Licensing**
   - Sell de-identified aggregated tiles to analytics partners or consultants

**Competitive Advantage**
- **Niche focus on Workers’ Comp** (underserved market compared to commercial/Medicare)
- **Multi-benchmark integration** (WC FS + Medicare + Commercial in one schema:contentReference[oaicite:4]{index=4})
- **Scalable architecture** (Iceberg + pre-aggregated tiles → serve both indie dashboards and enterprise compute)
- **Dual product lines** (dashboards + steerage AI)

---

## Next Steps
1. Finalize Stage 1 ETL pipeline with append-to-Iceberg + tile builds.
2. Deploy Stage 2 dashboard MVP on DigitalOcean droplet (Streamlit → Django).
3. Build out Stage 3 steerage prototype using H3 geo-shards + LLM query orchestration.
4. Begin outreach to **mid-market TPAs and risk managers** for pilot programs.

