# Digital Product Passport (DPP) Solution Accelerator

A modular, reusable solution accelerator demonstrating how manufacturers can build
EU-compliant Digital Product Passports on the **Databricks Data Intelligence Platform**
with **Lakebase** as the operational backbone.

## Why This Matters

The EU Ecodesign for Sustainable Products Regulation (ESPR) mandates Digital Product
Passports, with batteries first (mandatory 18 Feb 2027), then iron & steel,
textiles, aluminium, tyres, furniture and further groups through the end of the
decade. Manufacturers must provide machine-readable, per-product lifecycle data
covering origin, materials, environmental impact, compliance, circularity, and
disposal.

The EU system is **decentralized**: the Commission runs a thin registry/portal +
standards, but the passport data is held and served by each **economic operator**.
This accelerator is that operator-side backend. See
[docs/positioning.md](docs/positioning.md) for the regulatory driver, the EU
architecture, and where this fits.

> **Scope of this accelerator.** v1.0 is a reference architecture and runnable
> demo, not a turnkey compliance product. In particular it serves passport data
> openly and does not yet implement the regulation's tiered access rights
> (public / restricted / authority-only) or DPP Registry registration and the
> Registry-issued unique identifier. Those are on the [roadmap](ROADMAP.md). The
> economic operator remains responsible for meeting the applicable legal
> requirements.

## Why Databricks

A DPP is not one workload — it is operational lookups, batch analytics, AI, and
secure sharing over the same governed data. Databricks lets a manufacturer build
the whole operator-side backend on **one platform**, instead of stitching
together a transactional database, a warehouse, an app host, and an AI service:

- **Lakebase** serves sub-second QR/passport lookups and supplier writes — OLTP
  next to the lakehouse, no separate operational database to integrate.
- **Unity Catalog** governs every layer (lineage, access, audit) — the basis for
  the public vs. restricted data tiers the regulation requires.
- **Spark Declarative Pipelines, AI Functions, Genie, Databricks Apps** cover
  ingestion, compliance analytics, AI insights, and the consumer/supplier apps.
- **Delta Sharing** addresses cross-company value-chain and cross-border data
  exchange without copying data.

One governed copy of the data, many workloads — which is hard to match by
assembling point products.

## Architecture

The solution uses a hybrid OLTP + analytics architecture:

- **Lakebase (PostgreSQL)** -- Operational store for per-product passport lookups,
  supplier data entry, compliance updates, audit trail writes, and the cached
  AI-insights serving table.
- **Spark Declarative Pipelines (SDP)** -- Snapshot ingestion from Lakebase into Bronze
  streaming tables, Silver conformance layer, and Gold business aggregates
  (passport-complete, multi-tier supplier traceability, carbon, compliance gaps,
  material composition trends, circularity metrics).
- **Databricks Apps** -- Consumer-facing passport viewer and supplier self-service portal,
  each bound to the Lakebase instance as a resource (service-principal auth).
- **AI/BI Dashboard** -- 4-page compliance and sustainability dashboard.
- **Genie Space** -- Natural language SQL exploration for compliance officers.
- **Foundation Model API** -- AI passport insights, pre-computed nightly via
  `ai_query` into `gold.dpp_passport_insights` and served from Lakebase cache.

![Architecture](docs/architecture.png)

> Diagram source: [`docs/architecture.svg`](docs/architecture.svg) (scalable vector,
> official Databricks icons; the PNG is rendered from it). A simplified
> [`docs/architecture.mmd`](docs/architecture.mmd) Mermaid version is also included.

## Modules

| Module | Status | Description |
|--------|--------|-------------|
| **1. Foundation** | Complete | Lakebase schema (12 tables incl. multi-tier `parent_supplier_id` + insights cache), deterministic synthetic data (500 products, 50 suppliers across 3 tiers), self-contained setup job (apply schema + seed via runtime OAuth) |
| **2. Passport Viewer** | Complete | Consumer-facing app with 7 tabs (Origin incl. upstream supplier chain, Materials, Environment, Compliance, Circularity, Disposal, AI Insights), QR codes, category filters, search, pagination |
| **3. Dashboard & Genie** | Complete | 4-page AI/BI compliance dashboard, Genie Space with curated questions |
| **4. Supplier Portal** | Complete | Supplier self-service app with cert renewal (Lakebase writes), audit log, search/filter |
| **5. AI/ML Enrichment** | Complete | AI passport insights (gap detection, carbon analysis, compliance alerts, circularity recommendations) pre-computed nightly via `ai_query` and served from Lakebase cache, with a live Foundation Model fallback |

## End-to-end flow for a new programmer

If you are new to the repo, the fastest way to understand it is to follow one product record and watch it move through each layer.

### 1. Foundation layer: create the source-of-truth database

The operational system starts in Lakebase. The schema is defined in [`src/foundation/01_setup_lakebase_schema.sql`](src/foundation/01_setup_lakebase_schema.sql). That script creates the `dpp` schema and the core tables such as `manufacturer`, `supplier`, `product_passport`, `product_origin`, `product_materials`, `environmental_impact`, `compliance_records`, `circularity_info`, `product_disposal`, and the audit / insights cache tables.

The setup job in [`resources/dpp_jobs.yml`](resources/dpp_jobs.yml) runs [`src/foundation/00_setup_lakebase.py`](src/foundation/00_setup_lakebase.py), which applies the schema, invokes the synthetic data generator, and inserts rows into Lakebase. The generator in [`src/foundation/02_synthetic_data_generator.py`](src/foundation/02_synthetic_data_generator.py) creates a deterministic demo catalog with product passports, suppliers, materials, compliance records, and carbon impacts. The design is deliberately reproducible: the same seed and reference date produce the same output, which makes demos and regression checks reliable.

At this point, Lakebase is the operational truth. It is where product records are created, supplier updates are accepted, certifications are renewed, and audit events are written.

### 2. Bronze: raw snapshot ingestion into the lakehouse

After the operational store is loaded, the pipeline in [`resources/dpp_pipelines.yml`](resources/dpp_pipelines.yml) runs the Bronze stage from [`src/pipelines/bronze/ingest_lakebase_to_bronze.py`](src/pipelines/bronze/ingest_lakebase_to_bronze.py). This reads the Lakebase tables through JDBC and mirrors them into Spark/Delta Bronze tables with ingestion metadata. Bronze is intentionally close to the source: it preserves raw values, row identifiers, and load timestamps, but does not yet enforce business logic.

This is the staging area where operational data becomes data-platform data. It allows the rest of the pipeline to work with governed, queryable copies without directly hitting the OLTP database for every analysis.

### 3. Silver: validation and conformance

The Silver layer lives in [`src/pipelines/silver/clean_and_conform.py`](src/pipelines/silver/clean_and_conform.py). This stage normalizes inconsistent values, filters invalid records, adds expected data quality checks, and prepares a clean semantic layer for downstream analytical use. The file is where data quality routines are applied, such as checking required fields, expected status values, missing suppliers, and incomplete impact records.

The key idea is that Bronze is raw, while Silver is business-ready. If a product record is missing required fields or a compliance status is malformed, the Silver stage can record or quarantine it before it reaches Gold.

### 4. Gold: business aggregates and KPI tables

The Gold layer is the analytics layer used by downstream apps and dashboards. Files under [`src/pipelines/gold/`](src/pipelines/gold/) build curated tables such as:

- `passport_complete` for a rich passport view
- `supply_chain_lineage` for supplier ancestry and traceability
- `compliance_gap_analysis` for compliance findings
- `carbon_footprint_aggregates` for carbon summaries
- `material_circularity_metrics` for circularity and material composition trends
- `app_usage` for operational usage analytics

This is the layer that business users and dashboards query. It is not a raw replica of the database; it is business-oriented and pre-aggregated so dashboards can respond quickly and consistently.

### 5. Consumer-facing app: passport lookup and explanation

The consumer viewer app in [`src/apps/passport_viewer/main.py`](src/apps/passport_viewer/main.py) reads from Lakebase and renders a product passport. It supports QR-based lookups, search, paging, and multiple tabs such as Origin, Materials, Environment, Compliance, Circularity, Disposal, and AI Insights.

The app is not just a website; it is the public representation of the product story. It explains:

- where the product was made and where the materials came from
- whether the product meets key compliance checks
- whether its composition is renewable, recycled, or hazardous
- what the carbon footprint and end-of-life handling look like
- whether there are gaps or warnings requiring action

### 6. Supplier portal: operational updates and certifications

The supplier portal in [`src/apps/supplier_portal/main.py`](src/apps/supplier_portal/main.py) is the operational control plane for the value chain. It allows suppliers to update their records, renew certifications, review due dates, and see how their production contributes to product compliance. It writes back to Lakebase, which is then reflected in the operational store and later in the pipeline.

This matters because accountability is not just a reporting problem — it is an operational workflow. Suppliers need an easy way to update information that supports product traceability and regulatory readiness.

### 7. AI insights and BI: decision support

The AI stage is implemented by [`src/pipelines/gold/precompute_insights.py`](src/pipelines/gold/precompute_insights.py). The pipeline builds a prompt from the Gold tables, calls the Databricks model through `ai_query`, and writes the result to both:

- a Unity Catalog Delta table for analytics and lineage
- a Lakebase `passport_insights` cache table for fast app serving

The Passport Viewer then reads cached insights if they exist and only falls back to a live model call when needed. This avoids unnecessary real-time inference while still giving the app a natural-language explanation of product compliance and improvement opportunities.

The dashboard in [`resources/dashboards/dpp_compliance.lvdash.json`](resources/dashboards/dpp_compliance.lvdash.json) then surfaces KPIs such as overdue compliance items, carbon burden, origin risk, and product completeness to analysts and sustainability teams.

## Business use cases in detail

This accelerator is designed to solve real operational and regulatory problems for product and supply chain teams.

### 1. Product traceability and origin transparency

A manufacturer needs to answer: where did this product come from, which supplier supplied each material, and what is the upstream chain behind it? The `supplier` hierarchy with `parent_supplier_id` and the `product_origin` table allow a battery, furniture item, or component to be traced back through multiple tiers of suppliers.

This matters for:

- due diligence and sourcing checks
- supplier audits and corrective actions
- customer transparency and brand trust
- regulatory traceability obligations

### 2. Regulatory compliance monitoring

The system stores compliance records for specific regulations and product categories. For a battery passport this could include battery-specific requirements, material disclosure, and safety or environmental rules. For furniture or other categories, it can track REACH, RoHS, FSC, PEFC, and similar rules.

The compliance gap analysis in Gold makes it practical to answer questions like:

- Which products are missing certificates?
- Which products are at risk of expiry or non-compliance?
- Which components are failing reuse or circularity requirements?

### 3. Environmental and carbon impact assessment

The `environmental_impact` and related Gold aggregates add a product-level sustainability story. They capture manufacturing, transport, use-phase, and end-of-life emissions, then summarize them at a product or portfolio level.

This supports:

- product carbon footprint analysis
- supplier emissions comparisons
- sustainability reporting for leadership or regulators
- improvement prioritization for high-impact materials and suppliers

### 4. Circularity and end-of-life planning

The schema includes circularity, recycling, and disposal data, enabling the company to answer what happens to a product at the end of its useful life. This is a core requirement in DPP programs: not only what the product is made of, but how it can be reused, repaired, remanufactured, or recycled responsibly.

This helps teams optimize:

- material recovery strategies
- reuse and repurposing programs
- circular design initiatives
- supplier engagement around material choices

### 5. Supplier accountability and auditability

The operational portal and audit log let organizations measure supplier performance, track certification dates, and review what changed over time. This is especially important where a product has multiple suppliers across regions or supply chain tiers.

The audit trail provides an accountability record of who changed what, when, and why — something regulators, internal controls, and auditors all expect.

### 6. Customer and stakeholder transparency

The Passport Viewer is not only for regulators. It is also for customers, retailers, resale partners, and internal teams who need a clear view of a product's origin, sustainability profile, and compliance status. It is the operational version of the digital product passport itself.

This creates trust and supports:

- product authenticity checks
- digital warranties and service history
- resale and secondary-market confidence
- circular economy participation

## Databricks Products Showcased

| Product | Role |
|---------|------|
| **Lakebase** | Operational passport store, supplier writes, audit trail |
| **Spark Declarative Pipelines** | Data ingestion and transformation (Bronze/Silver/Gold) |
| **Unity Catalog** | Data governance, lineage, DQ expectations |
| **Databricks Apps** | Passport Viewer + Supplier Portal |
| **AI/BI Dashboards** | Compliance and sustainability reporting |
| **Genie** | Natural language querying for compliance officers |
| **Foundation Model API** | Real-time AI passport analysis |
| **DABs** | Deployment and CI/CD |

## Adapting to your industry

The schema, SDP pipeline, apps, dashboard, and Genie space are **domain-agnostic** —
the six DPP categories (origin, materials, environmental impact, compliance,
circularity, disposal) are exactly what the EU ESPR mandates for *every* product
group. Only the **synthetic data** is industry-specific, and it is driven by a
selectable **profile**:

| Profile | Manufacturer | Categories | Example regulations |
|---------|--------------|------------|---------------------|
| `battery` (default) | VoltCore Energy AB | EV / Industrial / LMT / Portable Battery | **EU Batteries Regulation 2023/1542**, ESPR, UN 38.3; carbon footprint / recycled content / due diligence phased in post-2027 |
| `furniture` | NordicForm AB | Furniture, Textiles, Lighting, Storage | ESPR, REACH, RoHS, Timber |

The **battery** profile is the default: the battery passport is the first
mandatory EU Digital Product Passport (18 Feb 2027), so it is the highest-impact
lead. It models item-level passports, multi-tier `mine → cell → module → pack`
traceability, and battery **state of health** (dynamic data). Switch to
`furniture` with `--var profile=furniture` at deploy time.

Pick a profile at **deploy** time — the profile is baked into the setup job's
parameters when the bundle is deployed, so passing `--var` to `bundle run`
alone has no effect:

```bash
databricks bundle deploy -t dev --var="profile=battery" --var="dashboard_warehouse_id=<warehouse-id>"
databricks bundle run synthetic_data_setup -t dev
```

To add your own industry (medical devices, textiles, electronics, …), add a new
entry to `PROFILES` in `src/foundation/02_synthetic_data_generator.py` — product
catalog, materials, certifications, carbon ranges, lifecycle splits, regulations,
disposal components — and select it with `--profile`. No schema or app changes
needed. The [EU battery DPP requirements](https://single-market-economy.ec.europa.eu/events/digital-product-passport-batteries-2026-05-27_en)
map directly onto the existing six categories.

> Deeper battery-passport features (tiered access control, daily State-of-Health
> dynamic data, full Annex XIII technical parameters, value-chain aggregation) are
> intentionally out of scope for v1.0 and captured in [ROADMAP.md](ROADMAP.md),
> each mapped to the relevant EU requirement and Databricks capability.

## Step-by-step setup

Follow these steps in order to set up the project in a Databricks workspace.

### 1. Prerequisites

Before you start, confirm you have:

- Databricks workspace access with permission to create a Lakebase instance
- Unity Catalog enabled and a catalog available for pipeline output
- A SQL warehouse to use for the dashboard
- Databricks CLI version 0.278.0 or later
- Python 3.10 or later
- Git installed locally

> If a system-installed `databricks` CLI is older than the required version, update your PATH or call the newer binary explicitly (for example `/usr/local/bin/databricks` on Linux/macOS, or the full path to `databricks.exe` on Windows).

#### Install the tools for your operating system

Skip any tool you already have. Open a **new** terminal after installing so the updated PATH is picked up.

**macOS** (Terminal, zsh/bash; uses [Homebrew](https://brew.sh))

```bash
brew tap databricks/tap
brew install databricks
brew install python git
```

**Linux** (Bash; Debian/Ubuntu shown, use `dnf`/`yum` on Fedora/RHEL)

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
sudo apt update && sudo apt install -y python3 python3-pip git
```

If the installer cannot write to `/usr/local/bin`, re-run it with `sudo`.

**Windows** (CMD or PowerShell; uses `winget`, which ships with Windows 10/11)

```powershell
winget install Databricks.DatabricksCLI
winget install Python.Python.3.12
winget install Git.Git
```

Alternatively, on Windows you can use WSL and follow the Linux instructions inside it.

Verify the installs (same commands on every OS; on some Linux/macOS systems Python is `python3`, on Windows it is `python` or `py`):

```bash
databricks version
python3 --version
git --version
```

#### Where to find the values you need

Several steps need workspace-specific values. Resource IDs from another workspace will not work in yours, so look each one up in the workspace you are deploying to.

| Value | Where to find it |
|-------|------------------|
| **Workspace URL** (`--host`) | Open your Databricks workspace in a browser and copy the base URL, e.g. `https://<workspace-name>.cloud.databricks.com` (AWS), `https://adb-<number>.<number>.azuredatabricks.net` (Azure) or `https://<number>.<number>.gcp.databricks.com` (GCP). Copy only up to the domain, without the path |
| **Workspace ID** (only if asked) | The number after `?o=` in the browser URL, or on Azure the number in `adb-<number>` |
| **SQL warehouse ID** (`dashboard_warehouse_id`) | UI: **SQL Warehouses** in the sidebar, click the warehouse, open the **Overview** tab and copy the ID (also visible in the browser URL after `/sql/warehouses/`). CLI: `databricks warehouses list -o json` (see step 5) |
| **Catalog and schema** | UI: **Catalog** (Catalog Explorer) in the sidebar. CLI: `databricks catalogs list` and `databricks schemas list <catalog>`. Compare against the `catalog` variable in `databricks.yml` (default `dpp_dev`). You need `USE CATALOG` and permission to create schemas/tables in it |
| **Lakebase instance and database** | UI: **Compute**, then the Lakebase / database instances tab (wording varies by UI version). CLI: `databricks database list-database-instances`. Sample values: instance `dpp-passport`, database `databricks_postgres` |
| **Deployed job and pipeline** | UI: **Jobs & Pipelines** (older UIs: **Workflows**) after `bundle deploy` |
| **App URLs** | UI: **Compute**, then **Apps**; or run `databricks apps list` after deployment |
| **Dashboard** | UI: **Dashboards** in the sidebar after deployment |
| **Your CLI profiles** | `databricks auth profiles` lists configured profiles and whether they are valid |

> UI labels change between Databricks releases. If a menu name differs, use the workspace search bar or the CLI command shown.

### 2. Clone the repository

```bash
git clone https://github.com/SiddhantSuwarnkar/dpp-solution-accelerator.git
cd dpp-solution-accelerator
```

These commands are identical on Linux, macOS, Windows CMD and PowerShell.

### 3. Authenticate to Databricks

Use OAuth so the bundle can deploy and run against your workspace without storing secrets in the repo.

First log in. This command is the same on every OS; replace the host with your workspace URL (see [Where to find the values you need](#where-to-find-the-values-you-need)). A browser window opens to complete OAuth login.

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile dpp
```

Then tell the CLI to use that profile for the current terminal session. **The command depends on your shell**: `export` works only on Linux/macOS, not in Windows CMD or PowerShell.

**Linux / macOS (Bash or Zsh)**

```bash
export DATABRICKS_CONFIG_PROFILE=dpp
```

**Windows Command Prompt (CMD)**

```cmd
set DATABRICKS_CONFIG_PROFILE=dpp
```

**Windows PowerShell**

```powershell
$env:DATABRICKS_CONFIG_PROFILE = "dpp"
```

Notes:

- These settings last only for the current terminal window. Re-run them in each new terminal.
- To make it permanent: on Linux/macOS add the `export` line to `~/.bashrc` or `~/.zshrc`; on Windows run `setx DATABRICKS_CONFIG_PROFILE dpp` (applies to terminals opened afterwards, not the current one).
- Alternative that works everywhere without an environment variable: add `--profile dpp` to each command, e.g. `databricks bundle deploy -t dev --profile dpp ...`.

Verify the CLI version and that authentication works:

```bash
databricks version
databricks auth describe
```

### 4. Create the Lakebase instance

This project expects a Lakebase database instance to exist before the app bundle is deployed. Create it once from the CLI:

```bash
databricks database create-database-instance dpp-passport --capacity CU_2
```

The default database name is `databricks_postgres`, which is already used by the bundle configuration. If you need a custom database name, update the `lakebase_database` variable before deployment.

To confirm the instance exists (or to look up an existing name and database), list the instances:

```bash
databricks database list-database-instances
```

You can also find them in the workspace UI under **Compute**, in the Lakebase / database instances area. The instance name must match what the bundle expects (sample: `dpp-passport`). Creation can take a few minutes; wait until the instance shows as available before continuing.

### 5. Identify the SQL warehouse

The dashboard needs a workspace warehouse ID.

```bash
databricks warehouses list -o json
```

The output is a JSON list; each entry has an `id` and a `name`. Copy the `id` of the warehouse you want to use. You will pass it as `dashboard_warehouse_id` in the next deployment step.

To print just the ID, name and state:

**Linux / macOS** (requires `jq`, e.g. `brew install jq` or `sudo apt install jq`)

```bash
databricks warehouses list -o json | jq -r '.[] | [.id, .name, .state] | @tsv'
```

**Windows PowerShell**

```powershell
databricks warehouses list -o json | ConvertFrom-Json | Select-Object id, name, state
```

**Windows CMD** has no built-in JSON parser; use the plain command above and read the `id` field, or use the UI path in the table in step 1.

If the list is empty, create a warehouse in the UI (**SQL Warehouses**, then **Create SQL warehouse**) or ask your workspace admin for access. A serverless warehouse is the simplest option if available.

### 6. Deploy the bundle

This step provisions the jobs, pipeline, dashboard, and both Databricks Apps defined under `resources/`.

First, validate the bundle configuration (optional but recommended):

```bash
databricks bundle validate -t dev --var="dashboard_warehouse_id=<warehouse-id>"
```

Then deploy. The single-line form works in Linux/macOS, CMD and PowerShell:

```bash
databricks bundle deploy -t dev --var="dashboard_warehouse_id=<warehouse-id>" --var="profile=battery"
```

Replace `<warehouse-id>` **including the angle brackets** with the real ID, e.g. `--var="dashboard_warehouse_id=1234567890abcdef"`. Leaving `<` `>` in place can break the command, because they are redirection characters in CMD and PowerShell.

If you prefer to split the command over several lines, the line-continuation character differs by shell:

**Linux / macOS (Bash/Zsh)** uses a backslash `\`:

```bash
databricks bundle deploy -t dev \
  --var="dashboard_warehouse_id=<warehouse-id>" \
  --var="profile=battery"
```

**Windows CMD** uses a caret `^`:

```cmd
databricks bundle deploy -t dev ^
  --var="dashboard_warehouse_id=<warehouse-id>" ^
  --var="profile=battery"
```

**Windows PowerShell** uses a backtick `` ` ``:

```powershell
databricks bundle deploy -t dev `
  --var="dashboard_warehouse_id=<warehouse-id>" `
  --var="profile=battery"
```

Notes:

- `profile` defaults to `battery`, but you can switch to `furniture` if needed.
- The bundle reads `databricks.yml` and the resource files under `resources/`.
- Run all `databricks bundle ...` commands from the repository root (the folder that contains `databricks.yml`).
- The apps are bound to the Lakebase instance as a resource during deployment, so there is no separate `databricks apps create` step.

### 7. Run the setup job

After the bundle is deployed, run the setup job to create the schema and seed Lakebase with the synthetic DPP dataset. The commands in steps 7 to 10 are identical on Linux, macOS, CMD and PowerShell.

```bash
databricks bundle run synthetic_data_setup -t dev
```

This job applies the foundation schema, generates synthetic product and supplier data, and writes it to the configured Lakebase database.

> The setup job can reset (truncate) the seeded demo tables before reseeding. Do not re-run it against data you need to keep.

### 8. Run the ingestion pipeline

Run the DPP ingestion pipeline to load the Bronze, Silver, and Gold tables used by the apps and dashboard.

```bash
databricks bundle run dpp_ingestion_pipeline -t dev
```

### 9. Start the two apps

Once the data is in place, start the consumer and supplier apps.

```bash
databricks bundle run dpp_passport_viewer -t dev
databricks bundle run dpp_supplier_portal -t dev
databricks apps list
```

Use the returned app URLs to open the viewer and supplier portal. You can also find them in the workspace UI under **Compute**, then **Apps**.

### 10. Optional: precompute AI insights

If you want the Passport Viewer's AI Insights tab to show cached results immediately, run the optional insights job.

```bash
databricks bundle run precompute_insights -t dev
```

Without this step, the app still works, but the AI Insights area will show that it is waiting for scheduled generation.

### 11. Optional: create the Genie space

Genie is not deployed automatically by the bundle. The project includes the curated Genie spec in `resources/genie/`.

Open the Databricks UI and create a Genie space manually, then attach the relevant Gold and Silver tables and paste in the description/questions from the bundle spec.

### 12. Validate the installation

After the setup job and pipeline have completed successfully:

- Open the passport viewer app URL and confirm the product data loads.
- Open the supplier portal and confirm supplier records and product updates are visible.
- Open the dashboard and confirm it renders using the configured SQL warehouse.

### Deployment notes

- SQL warehouse: `dashboard_warehouse_id` must be set on every deployment. There is no universal default.
- Catalog: the bundle writes tables to `${var.catalog}` (default `dpp_dev`). If you need a different catalog, set it with `--var="catalog=<your_catalog>"`.
- Lakebase database: the default `databricks_postgres` is usually sufficient. Override it only if you created a custom Lakebase database.
- App authentication: the apps bind to the Lakebase instance as a Databricks resource, so they use service-principal-based access without manual secrets.
- Existing deployments: rerun the setup job before the ingestion pipeline if you are upgrading the schema.

> If you cannot use resource-based database binding in a local or restricted environment, use the native-login fallback described in the main README, but the default bundle flow is the recommended setup path.

## Project Structure

```
dpp-solution-accelerator/
  databricks.yml              DAB root config + variables + targets
  resources/                  DAB resources: pipelines, jobs, apps, dashboards
  src/
    foundation/               00 setup (schema+generate+seed), 01 DDL, 02 generator, 03 seeder
    pipelines/                SDP code: bronze / silver / gold + precompute_insights
    serving/                  Reyden toggle (reference pattern)
    apps/
      passport_viewer/        Consumer-facing passport app (FastAPI + Jinja2)
      supplier_portal/        Supplier self-service portal (FastAPI + Jinja2)
  tests/                      Unit tests (pytest)
  docs/                       architecture.svg/.png (diagram) + architecture.mmd (Mermaid)
  LICENSE.md  NOTICE.md  SECURITY.md  CONTRIBUTING.md  CHANGELOG.md  ROADMAP.md
```

## Multi-tier supply chain traceability

Suppliers form a hierarchy via `supplier.parent_supplier_id`: a tier-2 supplier's
parent is the tier-1 (direct) supplier it delivers to, and tier-3 rolls up to
tier-2. This models the tier-1 → tier-N question regulators and OEMs ask
("where does this component ultimately come from?").

- **Schema**: self-referential FK on `dpp.supplier`.
- **Gold**: `gold_supplier_traceability` flattens each supplier's full ancestry
  (bounded iterative parent walk).
- **App**: the Passport Viewer's Origin tab renders the upstream chain per
  component via a recursive CTE.

## Production hardening

This accelerator uses **snapshot ingestion** (full Lakebase → Bronze read per
pipeline run) for simplicity. For production scale, replace it with continuous
change data capture:

- **Lakebase Sync / reverse ETL** to keep the operational store and the lakehouse
  aligned without full snapshots.
- **Lakeflow Connect (CDC)** for incremental change capture from the source systems.
- Use **secrets + least-privilege Postgres roles** (the apps already support a
  native-login fallback), schedule the pre-compute job, and pin model/runtime versions.
- Consider **Reyden** (high-concurrency serving, optional toggle in `src/serving/`)
  for sub-second QR lookups at scale.

## App access logging (optional)

To track who opens the apps internally, set `ACCESS_LOG_ENABLED=true` in each
app's `app.yaml`. When on, both apps record the accessing user (from the
Databricks Apps SSO `X-Forwarded-*` headers), path, and timestamp to
`dpp.app_access_log`. It is **off by default**: user emails are personal data
(GDPR), so enable it only where appropriate and with the right notices. View
the data by querying the Lakebase table directly, or surface it in the AI/BI
dashboard. Note this logs *authenticated app usage*; requests from users who
lack access are handled by Databricks before reaching the app and are not
recorded here.

## EU ESPR Timeline

Dates below reflect the European Commission's
[DPP overview](https://single-market-economy.ec.europa.eu/single-market/digital-product-passport_en)
(product-group dates are set by the individual ESPR delegated / sector acts and
continue to move; treat them as indicative and confirm against the act that
applies to you).

- **20 July 2026:** EU central DPP Registry operational.
- **Q4 2026:** Iron & steel ESPR delegated act (one of the earliest groups).
- **18 Feb 2027:** Mandatory battery passports (EV, light-transport, industrial).
- **Q2 2027:** Implementing Act for Unique Identifiers (the Registry-issued URI).
- **Q2 2027:** Construction products (Construction Products Regulation).
- **2027 (H2):** Textiles, aluminium, tyres (ESPR delegated acts).
- **2028:** Furniture. **2029:** Mattresses; recycled-content rules.
- Toys and detergents come in through their own legislation (e.g. Toys Safety
  Regulation 2025/2509), not the ESPR delegated acts above.

**Regulatory references** (external, authoritative sources — for context only):

- European Commission — [Digital Product Passport overview](https://single-market-economy.ec.europa.eu/single-market/digital-product-passport_en)
- Ecodesign for Sustainable Products Regulation (ESPR) — [Regulation (EU) 2024/1781](https://eur-lex.europa.eu/eli/reg/2024/1781/oj)
- EU Batteries Regulation — [Regulation (EU) 2023/1542](https://eur-lex.europa.eu/eli/reg/2023/1542/oj)

> This accelerator is an independent Databricks solution accelerator. It is **not
> affiliated with, endorsed by, or produced in association with** the European
> Commission or any EU body. Links above are provided as references to the
> primary regulation and official guidance; always confirm requirements against
> the acts that apply to you.

## Target Audience

Any organization that must place an EU Digital Product Passport on the market,
and the Databricks field and partner teams who support them:

- **Manufacturers, brands, and importers** across all in-scope ESPR categories
  (batteries first, then textiles, furniture, tyres, electronics, iron and steel,
  aluminium), whether based in the EU or selling into it.
- **Solutions Architects, Delivery Solutions Architects, and partners** building
  DPP solutions on the Databricks Data Intelligence Platform.

Originally incubated in the Databricks Manufacturing Community of Practice
(MFG CoP), now applicable to any EU DPP operator-side use case worldwide.

## Dependencies and licenses

All third-party libraries are permissively licensed (Apache-2.0 / BSD / MIT).
The data generator (Faker) and all pipeline/app code run on Databricks
Serverless + Unity Catalog.

| Library | Purpose | License |
|---------|---------|---------|
| faker | Synthetic data generation | MIT |
| fastapi | App web framework | MIT |
| uvicorn | ASGI server | BSD-3-Clause |
| jinja2 | App templating | BSD-3-Clause |
| qrcode[pil] | QR codes (Pillow) | BSD / HPND |
| requests | HTTP (OAuth token mint) | Apache-2.0 |
| databricks-sdk | Databricks API | Apache-2.0 |
| pyspark / Lakeflow | Pipelines (runtime-provided) | Apache-2.0 |
| asyncpg | Lakebase (PostgreSQL) connectivity | Apache-2.0 |

## Detailed technical walkthrough

This section explains the project end to end, in the order you should read the code:
configuration first (`databricks.yml`, `resources/dpp_jobs.yml`), then the Foundation
layer (Lakebase PostgreSQL), then the Spark layer (Bronze, Silver, Gold in Unity
Catalog), then AI insights, the two apps, and the dashboard.

> **Scope note.** The data is synthetic demo data. Table names, catalog names and
> resource IDs can differ per Databricks workspace, so check the actual workspace
> configuration before running commands or querying tables. This is a reference
> architecture, not a complete production compliance solution.

**Contents**

1. [What the project does](#1-what-the-project-does)
2. [High-level architecture and data flow](#2-high-level-architecture-and-data-flow)
3. [Configuration files: start here](#3-configuration-files-start-here)
4. [Foundation layer: PostgreSQL operational data](#4-foundation-layer-postgresql-operational-data)
5. [Lakebase PostgreSQL vs Unity Catalog](#5-lakebase-postgresql-vs-unity-catalog)
6. [Bronze layer: snapshot ingestion](#6-bronze-layer-snapshot-ingestion)
7. [Silver layer: clean and conform](#7-silver-layer-clean-and-conform)
8. [Gold layer: business tables](#8-gold-layer-business-tables)
9. [AI insights: a separate job](#9-ai-insights-a-separate-job)
10. [The two applications](#10-the-two-applications)
11. [Audit log and app access logging](#11-audit-log-and-app-access-logging)
12. [Dashboard and Genie](#12-dashboard-and-genie)
13. [How the full workflow runs](#13-how-the-full-workflow-runs)
14. [Table naming in practice](#14-table-naming-in-practice)
15. [Caveats and boundaries](#15-caveats-and-boundaries)
16. [Quick reference: table counts](#16-quick-reference-table-counts)
17. [Source-file index](#17-source-file-index)
18. [Final mental model](#18-final-mental-model)

---

### 1. What the project does

The accelerator shows how a manufacturer can organize and serve product lifecycle
information using Databricks and Lakebase. It combines:

- **Lakebase PostgreSQL** for operational product and supplier data, app reads/writes,
  audit events, and an AI-insights cache.
- **Spark Declarative Pipelines** for copying operational data into Delta tables and
  transforming it through Bronze, Silver, and Gold.
- **Unity Catalog** for organizing, governing, discovering, and controlling access to
  the Delta tables.
- **Databricks Apps** for the Passport Viewer and Supplier Portal.
- **AI/BI dashboards and Genie** for analytical exploration.
- **Databricks Foundation Model inference** for natural-language passport insights.

### 2. High-level architecture and data flow

```text
Repository configuration
  databricks.yml
       |
       +--> resources/dpp_jobs.yml
       |       +--> synthetic_data_setup
       |       +--> precompute_insights (separate AI job)
       |
       +--> resources/dpp_pipelines.yml
       |       +--> Bronze
       |       +--> Silver
       |       +--> Gold
       |
       +--> app and dashboard resources

Foundation setup scripts
       |
       v
Lakebase PostgreSQL (dpp schema; operational/source data)
       |
       | JDBC snapshot read
       v
Unity Catalog Delta tables
  Bronze (raw-ish source snapshots + ingestion metadata)
       |
       v
  Silver (cleaned, validated, conformed data)
       |
       v
  Gold (business-oriented tables and metrics)
       |
       +--> dashboards / SQL / Genie / analytics
       |
       +--> gold_passport_complete --> AI precompute job
                                      |
                                      +--> Unity Catalog Delta insights table
                                      +--> Lakebase dpp.passport_insights cache

Passport Viewer <---- reads passport details and cached insights ---- Lakebase
Supplier Portal ---- writes supplier/certification updates ----------> Lakebase
```

> **Important.** The main Spark Declarative Pipeline and the AI-insights job are related
> but separate. The six Gold pipeline scripts create **10 Gold tables**.
> `precompute_insights.py` is a separate job; it creates one additional Delta insights
> table and updates the existing PostgreSQL insights cache.

---

### 3. Configuration files: start here

#### `databricks.yml`

This is the Databricks Asset Bundle (DAB) root configuration. It defines the bundle,
variables, deployment targets, and includes the resource YAML files. It is the entry
point for `databricks bundle deploy`.

| Variable | Meaning | Default / sample |
|----------|---------|------------------|
| `catalog` | Unity Catalog catalog where tables are registered | `dpp_dev` |
| `schema` | Schema inside that catalog | `dpp` |
| `profile` | Synthetic-data profile | `battery` or `furniture` |
| Lakebase instance | Name of the Lakebase instance | `dpp-passport` |
| `lakebase_database` | Database inside the instance | `databricks_postgres` |
| `dashboard_warehouse_id` | SQL warehouse used by the dashboard | **no default; you must supply it** |

The actual names and IDs must be checked in the workspace you deploy to. Do not assume
IDs from a previous workspace apply to a new one (see
[Where to find the values you need](#where-to-find-the-values-you-need)).

#### `resources/dpp_jobs.yml`

Defines the jobs, their parameters, code files, dependencies, and execution environment.

| Job | Main file | Purpose | Notes |
|-----|-----------|---------|-------|
| `synthetic_data_setup` | `src/foundation/00_setup_lakebase.py` | Applies the Lakebase schema and seeds synthetic operational data | Run **before** the ingestion pipeline. Review reset/truncation behaviour before running against existing data |
| `precompute_insights` | `src/pipelines/gold/precompute_insights.py` | Generates AI insights from Gold passport data; writes the Delta output and the Lakebase cache | Separate from the Gold pipeline; run manually. Schedules are paused by default |

The jobs declare dependencies such as `faker` (synthetic data) and `asyncpg`
(PostgreSQL connectivity), plus serverless environment settings.

#### Other resource files

| File | Responsibility |
|------|----------------|
| `resources/dpp_pipelines.yml` | Configures the Bronze/Silver/Gold Spark Declarative Pipeline and output location |
| `resources/apps.yml` | Defines the Passport Viewer and Supplier Portal app resources |
| `resources/dpp_dashboards.yml` | Defines the dashboard resource and warehouse configuration |
| `resources/dashboards/dpp_compliance.lvdash.json` | Dashboard layout and SQL queries. Some queries may use literal catalog/schema names, so verify them if your catalog differs |
| `resources/genie/` | Curated Genie specification. The Genie space is **not** created automatically; it needs a manual UI step |

---

### 4. Foundation layer: PostgreSQL operational data

The Foundation layer creates the operational schema and inserts deterministic synthetic
data. It is the **operational source of truth for the apps**. It is *not* the
Bronze/Silver/Gold analytics layer.

| File | Input | Output / effect |
|------|-------|-----------------|
| `src/foundation/01_setup_lakebase_schema.sql` | SQL DDL | Creates the `dpp` schema, tables, relationships, indexes, and audit-related objects |
| `src/foundation/02_synthetic_data_generator.py` | Selected profile, seed, reference date | Generates manufacturers, suppliers, passports, origins, materials, environmental data, compliance, circularity, and disposal records |
| `src/foundation/03_seed_lakebase.py` | Generated records + Lakebase connection | Inserts the records into the PostgreSQL tables |
| `src/foundation/00_setup_lakebase.py` | Setup parameters | Orchestrates schema application and data seeding (this is what the job runs) |

The generator is deterministic for a fixed seed and reference date. The demo models
roughly 500 products and 50 suppliers across three tiers. Verify actual counts in the
deployed database rather than relying on documentation.

#### What is stored in PostgreSQL (Lakebase, schema `dpp`)

These are 12 operational PostgreSQL tables. They are **not** Unity Catalog Delta tables.

| PostgreSQL table | What it stores / why it exists |
|------------------|--------------------------------|
| `dpp.manufacturer` | Manufacturer/company information associated with products |
| `dpp.supplier` | Supplier identity and attributes: tier, `parent_supplier_id`, risk score, certifications, audit date, active status |
| `dpp.product_passport` | Main product passport record and its identifying attributes |
| `dpp.product_origin` | Origin / component-to-supplier links and certification/traceability details |
| `dpp.product_materials` | Material composition per product/component |
| `dpp.environmental_impact` | Carbon/emissions and related impact data, including verification details |
| `dpp.compliance_records` | Regulation/compliance records and statuses per product |
| `dpp.circularity_info` | Repair, reuse, recycling, and other circularity attributes |
| `dpp.disposal_guidelines` | End-of-life, disposal, and handling guidance |
| `dpp.passport_audit_log` | Audit history of operational changes; database triggers may populate it |
| `dpp.passport_insights` | Cache of structured AI-generated insights for fast app reads |
| `dpp.app_access_log` | Optional app access events; may stay empty if access logging is disabled |

**Relationships**

- `product_passport.manufacturer_id` links a passport to its manufacturer.
- Passport feature tables commonly link through `passport_id`.
- `product_origin` connects a passport/component to a supplier.
- `supplier.parent_supplier_id` references another supplier, enabling multi-tier ancestry.
- Supplier tier indicates depth in the chain, not quality: Tier 1 supplies the
  manufacturer directly, Tier 2 supplies Tier 1, Tier 3 supplies Tier 2.

> **Reset behaviour.** The setup process applies the schema and seeds the synthetic
> records, and it has been observed to truncate/reset existing tables before reseeding.
> Treat it as a destructive demo operation: inspect the script and the target database
> before running it if you have modified or valuable data.

---

### 5. Lakebase PostgreSQL vs Unity Catalog

| Topic | Lakebase PostgreSQL | Unity Catalog Delta tables |
|-------|---------------------|----------------------------|
| Main role | Operational/transactional storage (OLTP) | Analytical/lakehouse storage |
| Typical access | App requests, supplier updates, quick passport lookups, audit/cache reads | Spark processing, SQL analytics, dashboards, lineage, data quality, reporting |
| Data shape | Normalized operational tables, relationships, transactional writes | Snapshots, cleaned/conformed datasets, joined/aggregated business tables |
| Example names | `dpp.supplier`, `dpp.product_passport`, `dpp.passport_insights` | `<catalog>.dpp.bronze_supplier`, `<catalog>.dpp.silver_supplier`, `<catalog>.dpp.gold_passport_complete` |
| Updated by | Apps, setup/seeding job, audit mechanisms, AI cache upsert | Spark Declarative Pipeline and the separate AI precompute job |

#### What is Unity Catalog?

Unity Catalog is Databricks' governance and discovery layer for data and AI assets. It
organizes data in a three-part namespace:

```text
catalog.schema.table
```

For example:

```text
<catalog>.dpp.bronze_supplier
<catalog>.dpp.silver_supplier
<catalog>.dpp.gold_supplier_traceability
```

It provides permissions, metadata, discovery, lineage, and auditing. It is **not** a
PostgreSQL database and does not itself clean or transform data. The Spark pipeline code
performs the transformations; Unity Catalog registers and governs the resulting Delta
tables. The catalog name varies by deployment, so check `databricks.yml` and Catalog
Explorer rather than assuming `dpp_dev` or `workspace`.

---

### 6. Bronze layer: snapshot ingestion

**Script:** `src/pipelines/bronze/ingest_lakebase_to_bronze.py`

1. Connects to Lakebase PostgreSQL through JDBC.
2. Reads the source tables in the `dpp` schema.
3. Writes corresponding Delta tables to the configured Unity Catalog catalog/schema.
4. Adds ingestion metadata such as `_ingested_at` and `_source_table`.
5. Silver then reads the Bronze outputs.

This is **snapshot ingestion**: a full source-table read per pipeline run. It should not
be described as incremental CDC just because streaming/declarative constructs are used.

| Input: Lakebase PostgreSQL | Output: Unity Catalog Delta | Description |
|----------------------------|-----------------------------|-------------|
| `dpp.manufacturer` | `bronze_manufacturer` | Raw-ish manufacturer snapshot |
| `dpp.supplier` | `bronze_supplier` | Supplier data and tier/parent hierarchy |
| `dpp.product_passport` | `bronze_product_passport` | Passport source records |
| `dpp.product_origin` | `bronze_product_origin` | Origin and supplier/certification links |
| `dpp.product_materials` | `bronze_product_materials` | Material records |
| `dpp.environmental_impact` | `bronze_environmental_impact` | Environmental-impact source data |
| `dpp.compliance_records` | `bronze_compliance_records` | Compliance source records |
| `dpp.circularity_info` | `bronze_circularity_info` | Circularity source records |
| `dpp.disposal_guidelines` | `bronze_disposal_guidelines` | Disposal guidance source records |
| `dpp.passport_audit_log` | `bronze_passport_audit_log` | Audit events |
| `dpp.app_access_log` | `bronze_app_access_log` | App access events, possibly empty |

`dpp.passport_insights` is **not** ingested by this script. The AI precompute job writes
its own Delta output and updates the PostgreSQL cache separately.

**Why Bronze exists:** it creates a queryable lakehouse copy of operational data so
analytics and downstream transformations do not hit PostgreSQL for every analysis. It
keeps source-level detail and adds load metadata rather than applying business rules.

---

### 7. Silver layer: clean and conform

**Script:** `src/pipelines/silver/clean_and_conform.py`

Silver reads Bronze, normalizes selected text/status fields, validates required data,
applies data-quality expectations, and derives flags used in Gold.

- `@dlt.expect_or_drop(...)`: rows failing the condition are dropped.
- `@dlt.expect(...)`: the row is kept while the expectation is monitored.

| Input: Bronze | Output: Silver | Processing / important fields |
|---------------|----------------|-------------------------------|
| `bronze_manufacturer` | `silver_manufacturer` | Validates identifiers and country/name fields; cleans values |
| `bronze_supplier` | `silver_supplier` | Validates supplier keys and attributes used for joins and reporting |
| `bronze_product_passport` | `silver_product_passport` | Validates fields; derives `is_complete` from required fields (serial number, production facility, country of origin, QR-code URL) |
| `bronze_product_origin` | `silver_product_origin` | Cleans origin/certification values; derives `certification_expiring_soon` (expiry within 30 days) |
| `bronze_product_materials` | `silver_product_materials` | Validates and conforms material records |
| `bronze_environmental_impact` | `silver_environmental_impact` | Cleans fields; derives `needs_verification` when verifier or LCA-methodology info is missing |
| `bronze_compliance_records` | `silver_compliance_records` | Normalizes fields; derives `is_expired` |
| `bronze_circularity_info` | `silver_circularity_info` | Validates and conforms circularity attributes |
| `bronze_disposal_guidelines` | `silver_disposal_guidelines` | Validates and conforms end-of-life information |

There are **9 Silver tables**. None are defined for the audit log, app access log, or the
PostgreSQL insights cache.

**Why Silver exists:** Gold logic should not repeat basic cleaning. Silver provides
reusable flags, which Gold uses to summarize issues and compute metrics:

- `silver_product_passport.is_complete`
- `silver_product_origin.certification_expiring_soon`
- `silver_environmental_impact.needs_verification`
- `silver_compliance_records.is_expired`

---

### 8. Gold layer: business tables

Gold produces business-ready tables for reporting, dashboards, and analysis. The six
Gold scripts define **10 tables**.

#### 8.1 `passport_complete.py`

| Inputs | Output | Processing |
|--------|--------|------------|
| Silver passport, manufacturer, materials, compliance, environmental-impact, circularity | `gold_passport_complete` | Joins core passport info with manufacturer info and aggregated material, compliance, environmental, and circularity details into one wide, denormalized record per passport |

Consumers can use one consolidated dataset instead of repeating joins. It is also the
input to the separate AI precompute job.

#### 8.2 `supply_chain_lineage.py`

| Inputs | Output | Processing |
|--------|--------|------------|
| `silver_product_passport`, `silver_product_origin`, `silver_supplier` | `gold_supply_chain_lineage` | Shows which suppliers are linked to each passport/component. Inner joins mean unmatched records do not appear |
| Same inputs | `gold_supply_chain_summary` | Per passport: supplier counts by tier, average/max supplier risk, expiring certifications, inactive suppliers, unique source countries |
| `silver_supplier` | `gold_supplier_traceability` | Follows `parent_supplier_id` to flatten supplier ancestry. Walks a bounded number of parent hops and assumes at most three tiers |

Supplier risk levels are derived from the supplier risk score: **HIGH** at 7 or above,
**MEDIUM** at 4-6, **LOW** below 4. An inactive-supplier flag is also available. These
are project-defined rules, not regulatory categories.

#### 8.3 `carbon_footprint_aggregates.py`

| Inputs | Output | Processing |
|--------|--------|------------|
| Silver environmental-impact and related product/category/material data | `gold_carbon_by_category` | Carbon totals and averages, min/max/standard deviation, lifecycle-phase totals, energy/water metrics, counts of unverified records, by category |
| Same domains | `gold_carbon_by_product` | Product-level lifecycle metrics, manufacturing-phase share, energy/water, renewable/recycled material shares, LCA verification info |

#### 8.4 `compliance_gap_analysis.py`

| Inputs | Output | Processing |
|--------|--------|------------|
| `silver_product_passport`, `silver_compliance_records`, `silver_product_origin`, `silver_environmental_impact` | `gold_compliance_gap_analysis` | Combines passport completeness, compliance status/expiry, supplier certification expiry, and environmental verification into issue counts, a weighted risk score, and a risk category |

This is **rule-based Spark logic, not AI**. The score is:

| Issue | Contribution |
|-------|-------------:|
| Each non-compliant record | +30 |
| Each pending compliance record | +15 |
| Each expired compliance record | +20 |
| Each expiring supplier certification | +10 |
| Incomplete passport | +15 |
| Environmental data needing verification | +10 |

The risk category is assigned by explicit conditions, in priority order:

1. **CRITICAL** if any non-compliant record exists.
2. **HIGH** if pending or expired compliance records exist.
3. **MEDIUM** if the passport is incomplete or a supplier certification is expiring.
4. **LOW** otherwise.

The category is not a numeric score threshold: the presence of certain issue types takes
priority. It is a prioritization signal, not a legal statement that a product complies
or fails a law.

#### 8.5 `material_circularity_metrics.py`

| Inputs | Output | Processing |
|--------|--------|------------|
| Silver product materials and category fields | `gold_material_composition_trends` | Composition by category/material: recycled, renewable, hazardous, SVHC, REACH-related counts/indicators |
| Silver circularity and material data | `gold_circularity_metrics` | Category-level repairability, durability, recyclability, recycled content, refurbishable/take-back/spare-parts rates, and a composite circularity index |

The composite index combines normalized repairability with recyclability, recycled
content, and take-back measures. It is the project's own indicator, not a universal
regulatory metric.

#### 8.6 `app_usage.py`

| Input | Output | Processing |
|-------|--------|------------|
| `bronze_app_access_log` | `gold_app_usage` | Selects access ID, user/email, app name, path, HTTP method, client IP, timestamp, and derives `access_date` from `accessed_at` |

It may be empty unless `ACCESS_LOG_ENABLED=true` and access events were recorded. User
emails and IP addresses are personal data.

#### Gold output count

| Script | Tables | Output table(s) |
|--------|-------:|-----------------|
| `passport_complete.py` | 1 | `gold_passport_complete` |
| `supply_chain_lineage.py` | 3 | `gold_supply_chain_lineage`, `gold_supply_chain_summary`, `gold_supplier_traceability` |
| `carbon_footprint_aggregates.py` | 2 | `gold_carbon_by_category`, `gold_carbon_by_product` |
| `compliance_gap_analysis.py` | 1 | `gold_compliance_gap_analysis` |
| `material_circularity_metrics.py` | 2 | `gold_material_composition_trends`, `gold_circularity_metrics` |
| `app_usage.py` | 1 | `gold_app_usage` |
| **Total** | **10** | |

---

### 9. AI insights: a separate job

**File:** `src/pipelines/gold/precompute_insights.py`

This is a separate Spark job, not a seventh Gold pipeline script. It reads
`gold_passport_complete` and calls a Databricks Foundation Model through `ai_query` to
generate structured insights.

| Input | Output | Where it lives |
|-------|--------|----------------|
| `<catalog>.<schema>.gold_passport_complete` | `dpp_passport_insights` | Unity Catalog Delta table |
| Generated insight JSON | `dpp.passport_insights` | Existing Lakebase PostgreSQL cache table |

The structured output contains `gaps`, `carbon_analysis`, `compliance_alerts`, and
`circularity_recommendations`. The configuration uses
`databricks-meta-llama-3-3-70b-instruct` with constrained structured output; model
availability can vary by workspace.

**How the Passport Viewer gets AI insights**

1. It checks the Lakebase `dpp.passport_insights` cache first.
2. If cached insights exist, it serves them.
3. If not, it can call the live Foundation Model endpoint.
4. If live inference is unavailable and there is no cache, the UI shows a pending state.

**Counting tables carefully:** 10 tables from the Gold scripts, plus 1 additional Delta
output (`dpp_passport_insights`) from the AI job. The job also updates the existing
PostgreSQL `dpp.passport_insights` table; that is not newly created on each run.

---

### 10. The two applications

#### Passport Viewer

**Main file:** `src/apps/passport_viewer/main.py`. A read-oriented passport interface
that retrieves data from Lakebase.

| Route / feature | Purpose |
|-----------------|---------|
| `/` | Product listing with search, filter, pagination |
| `/passport/{passport_id}` | HTML passport detail page |
| `/api/passport/{passport_id}` | Passport details as JSON |
| `/api/qr/{passport_id}` | QR-code image |
| `/api/insights/{passport_id}` | Cached AI insights, or live inference fallback |
| `/health` | Application/DB health check |
| Demo / how-it-works page | Explains the reference architecture and pipeline |

The detail page has seven tabs: Origin, Materials, Environment, Compliance, Circularity,
Disposal, and AI Insights. The Origin tab includes supplier traceability. The app reads
operational passport data from Lakebase; it does not use Gold as its only live source.
Gold matters mainly for analytics and as the AI precompute input.

#### Supplier Portal

**Main file:** `src/apps/supplier_portal/main.py`.

| Route / feature | Purpose |
|-----------------|---------|
| `/` | Supplier list, search/filter, supplier/product counts |
| `/supplier/{supplier_id}` | Supplier details, components, certifications, expiry information |
| `/supplier/{supplier_id}/renew/{origin_id}` | Certification-renewal form/workflow |
| `/supplier/{supplier_id}/audit` | Audit history |
| `/health` | Application/DB health check |

The renewal workflow updates certification fields in `product_origin` and supplier
update timestamps; a database audit mechanism can record the change.

> **Limitation.** The portal currently behaves more like a manufacturer/compliance
> officer's supplier-management interface than a fully isolated supplier self-service
> system. It shows a supplier list and does not enforce strict per-supplier
> authorization. Do not assume a supplier can only see its own records.

**Why a renewal may not change every risk value.** The portal shows the stored
`supplier.risk_score`, which the renewal endpoint does not recalculate. The Gold
`gold_compliance_gap_analysis` calculates its own product-level score when the pipeline
next runs. These are different measures. Likewise, a supplier certificate and a
regulation-level product compliance record are different things: renewing a certificate
does not automatically change a product compliance record.

---

### 11. Audit log and app access logging

**Audit log.** Supplier/certification updates can be recorded in
`dpp.passport_audit_log` through database audit mechanisms. The Supplier Portal audit
view shows the history, including old/new values where available.

**App access log.** Optional and disabled by default. With `ACCESS_LOG_ENABLED=true`, the
apps record user identity, app, path, method, client IP, and timestamp into
`dpp.app_access_log`.

- Bronze copies it to `bronze_app_access_log`.
- Gold selects it and derives `access_date` in `gold_app_usage`.
- With logging disabled or no events, the Gold table may be empty.
- Emails and IP addresses are personal data; enable and retain only with appropriate
  privacy controls and notices.

---

### 12. Dashboard and Genie

**AI/BI dashboard.** Defined by `resources/dpp_dashboards.yml`, with layout and SQL in
`resources/dashboards/dpp_compliance.lvdash.json`. It uses the configured SQL warehouse.
Subjects include total products and completion rate, passport status distribution,
compliance risk categories, carbon totals, supplier risk counts, and app usage. Relevant
tables include `gold_passport_complete`, `gold_compliance_gap_analysis`,
`gold_carbon_by_category`, `gold_supply_chain_lineage`, and `gold_app_usage`.

> **Catalog caution.** If dashboard SQL contains a literal reference such as
> `dpp_dev.dpp.gold_passport_complete`, it will not work when you deploy to a different
> catalog. Check the SQL references when moving to another workspace/catalog.

**Genie.** The curated specification is under `resources/genie/`. Genie supports
natural-language SQL over selected tables, but the space must be created manually in the
Databricks UI (see setup step 11).

---

### 13. How the full workflow runs

| Step | What runs | Input | Result |
|-----:|-----------|-------|--------|
| 1 | Deploy bundle | `databricks.yml` + `resources/*.yml` | Jobs, pipeline, apps, dashboard are deployed/updated |
| 2 | Run `synthetic_data_setup` | Foundation schema + generator | Lakebase PostgreSQL tables are created/reset and seeded |
| 3 | Run `dpp_ingestion_pipeline` | Lakebase `dpp` tables | Bronze snapshots written; Silver cleans/validates; Gold builds 10 tables |
| 4 | Open Passport Viewer | Lakebase data + insights cache | QR lookup, passport details, AI insights |
| 5 | Use Supplier Portal | Supplier/origin data in Lakebase | Certification updates written and audited |
| 6 | Run `precompute_insights` (optional) | `gold_passport_complete` | AI output written to Delta and upserted into `dpp.passport_insights` |
| 7 | Open dashboard / Genie | Gold and selected Silver tables | Compliance, carbon, supplier, circularity, usage exploration |

**What happens after a supplier update?**

1. The Supplier Portal writes the change to Lakebase PostgreSQL.
2. The audit mechanism records it where configured.
3. The operational apps can read the changed data immediately.
4. The next pipeline run refreshes Bronze and the downstream Silver/Gold tables.
5. Gold metrics depending on that data update when the pipeline recomputes them.
6. Cached AI insights stay stale until `precompute_insights` runs again; there is no
   automatic cache invalidation.

So a portal change is visible in operational data before it shows up in analytical
tables or refreshed AI insights.

---

### 14. Table naming in practice

Delta tables are referenced as `<catalog>.<schema>.<table>`:

```text
<catalog>.dpp.bronze_supplier
<catalog>.dpp.silver_supplier
<catalog>.dpp.gold_compliance_gap_analysis
<catalog>.dpp.gold_passport_complete
<catalog>.dpp.dpp_passport_insights
```

The last one is the AI-precompute Delta output; its exact catalog/schema comes from the
job configuration. In PostgreSQL, the `dpp` schema holds operational tables such as
`dpp.supplier` and `dpp.passport_insights`, but those live in Lakebase, not Unity
Catalog.

A table in **Catalog Explorer** is a Unity Catalog data asset. A PostgreSQL table is
inspected through the Lakebase connection/UI or a SQL client connected to that database.
They are separate storage systems even when table names look similar.

---

### 15. Caveats and boundaries

- **Synthetic data:** demo data, not production manufacturer data.
- **Snapshot ingestion:** Bronze reads full snapshots per run; not a complete CDC implementation.
- **Setup reset:** the setup flow can truncate demo tables before reseeding.
- **Paused schedules:** run jobs manually or configure schedules yourself.
- **AI cache freshness:** cached insights can be stale until recomputed.
- **Supplier authorization:** the portal does not enforce complete per-supplier isolation.
- **Risk metrics:** stored supplier risk and the Gold compliance-gap score differ in calculation and update path.
- **Certificates vs compliance:** related but not interchangeable.
- **Access logging:** optional, off by default; identity/IP data needs privacy handling.
- **Workspace IDs:** warehouse, dashboard, app, and other IDs are workspace-specific.
- **Dashboard catalog references:** literal catalog names may need adjusting.
- **Production readiness:** access tiers, registry registration, continuous CDC, and operational controls need further work.

---

### 16. Quick reference: table counts

| Layer / job | Count | Where tables live | Notes |
|-------------|------:|-------------------|-------|
| Foundation | 12 operational tables | Lakebase PostgreSQL, schema `dpp` | Source of truth for apps and operational updates |
| Bronze | 11 tables | Unity Catalog Delta | Snapshot copies; excludes `passport_insights` |
| Silver | 9 tables | Unity Catalog Delta | Cleaned/conformed data with validation flags |
| Gold pipeline | 10 tables | Unity Catalog Delta | Six scripts produce business outputs |
| AI precompute job | 1 additional Delta table | Unity Catalog Delta | `dpp_passport_insights`; separate from the 10 Gold tables |
| AI cache | Existing table, updated by job | Lakebase PostgreSQL, `dpp.passport_insights` | Not newly created on each run |

---

### 17. Source-file index

| Area | Files |
|------|-------|
| Bundle configuration | `databricks.yml` |
| Jobs | `resources/dpp_jobs.yml` |
| Pipeline configuration | `resources/dpp_pipelines.yml` |
| Lakebase schema/setup | `src/foundation/00_setup_lakebase.py`, `01_setup_lakebase_schema.sql`, `02_synthetic_data_generator.py`, `03_seed_lakebase.py` |
| Bronze | `src/pipelines/bronze/ingest_lakebase_to_bronze.py` |
| Silver | `src/pipelines/silver/clean_and_conform.py` |
| Gold passport | `src/pipelines/gold/passport_complete.py` |
| Gold traceability | `src/pipelines/gold/supply_chain_lineage.py` |
| Gold compliance | `src/pipelines/gold/compliance_gap_analysis.py` |
| Gold carbon | `src/pipelines/gold/carbon_footprint_aggregates.py` |
| Gold materials/circularity | `src/pipelines/gold/material_circularity_metrics.py` |
| Gold app usage | `src/pipelines/gold/app_usage.py` |
| AI precompute | `src/pipelines/gold/precompute_insights.py` |
| Passport Viewer | `src/apps/passport_viewer/main.py` and templates |
| Supplier Portal | `src/apps/supplier_portal/main.py` and templates |
| Dashboard | `resources/dpp_dashboards.yml`, `resources/dashboards/dpp_compliance.lvdash.json` |
| Genie | `resources/genie/` |

---

### 18. Final mental model

- **`databricks.yml`** says how the bundle is configured and targeted.
- **`dpp_jobs.yml`** defines executable jobs, including data setup and the separate AI precompute.
- **Foundation / Lakebase PostgreSQL** stores operational product and supplier data and app updates.
- **Bronze** copies source snapshots into Delta.
- **Silver** cleans, validates, and adds flags.
- **Gold** joins and aggregates data into business tables.
- **Unity Catalog** organizes and governs the Delta tables.
- **Passport Viewer** displays passports; **Supplier Portal** performs supplier/certification workflows.
- **AI precompute** writes natural-language insights to Delta plus the PostgreSQL cache.
- **Dashboard and Genie** support analytical exploration of the governed data.

In one line:

**Configuration → Foundation/Lakebase PostgreSQL → Bronze Delta → Silver Delta → Gold Delta → Analytics; the separate AI job consumes Gold and writes insights to Delta and the Lakebase cache, while the apps read/write the operational database.**

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Contributions are accepted pursuant to a CLA.

## Maintenance

- **Maintainer:** Daniel Dahlin ([@danielvelasquez123-dbx](https://github.com/danielvelasquez123-dbx)).
- **Support:** please open a [GitHub Issue](../../issues) for bugs, questions, or
  enhancement requests. Issues are triaged by the maintainer.
- **Validation cadence:** the end-to-end deploy (bundle deploy, pipelines, apps,
  dashboard, Genie) is re-validated **quarterly** and whenever a new Databricks
  Runtime LTS is released, on a fresh workspace with Serverless + Unity Catalog.
- **Automated checks:** unit tests and a byte-compile run on every push/PR and on
  a **weekly schedule** (see [`.github/workflows/ci.yml`](.github/workflows/ci.yml))
  to catch dependency or platform drift between manual validations.
- **Deprecation:** if the accelerator can no longer be maintained, a notice will be
  added here and to [CHANGELOG.md](CHANGELOG.md) before the repository is archived.

## License

&copy; 2026 Databricks, Inc. Licensed under the Databricks License — see
[LICENSE.md](LICENSE.md). Use is permitted in connection with your use of the
Databricks Platform Services. See [CHANGELOG.md](CHANGELOG.md) for release notes.
