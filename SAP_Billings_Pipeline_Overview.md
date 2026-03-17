# SAP Billings Pipeline Overview

> **Purpose:** Business-level documentation of the Cardinal Health SAP Billings pipeline migrating from Palantir Foundry to Databricks.  
> **Audience:** Data Engineers, Architects, and Business Stakeholders new to Palantir and SAP.

---

## 1. Executive Summary

This pipeline processes **SAP ERP billing transactions** for Cardinal Health's pharmaceutical distribution business. It ingests raw SAP tables (invoices, customers, vendors, materials, plants), applies standardization, cleaning, and business-rule filtering, then enriches the billing fact table with dimensional attributes to produce analytics-ready datasets.

**What it answers from a business perspective:**
* How much product did Cardinal Health sell to each customer, by which material, from which warehouse, over what time period?
* What are the sales trends (time series metrics) at the customer-account, material, manufacturer, and customer-material-pair levels?
* Which transactions are Specialty Pharma Distribution (SPD) vs. standard Pharmaceutical Distribution (PD)?
* What is the Customer Lifetime Value (CLV) forecast for each customer/material combination?
* Which billing documents are tied to specific contract types, and what are the transaction size distributions?

**Scale:** The billing_document fact table contains **1B+ rows in Production**. Dimension tables (customer, material, vendor, plant) range from thousands to low millions of rows. The pipeline produces **45+ output datasets** spanning entity tables, composite pairs, time series metrics, and derived views.

---

## 2. SAP ERP Primer (For Non-SAP Users)

SAP ERP stores data in **transaction tables** (identified by short codes). Understanding these is critical for the pipeline:

| SAP Table | Full Name | Business Meaning | Approximate Scale |
| --- | --- | --- | --- |
| **VBRP** | Billing Document: Item Data | Individual line items on an invoice (one row per product per invoice) | 1B+ rows (largest) |
| **VBRK** | Billing Document: Header Data | Invoice header (one row per invoice with dates, org, currency) | 100M+ rows |
| **KNA1** | Customer Master: General Data | Master record for each customer (name, address, region) | 100K–500K rows |
| **KNVV** | Customer Master: Sales Area Data | Customer settings per sales org/distribution channel/division | 500K–1M rows |
| **KNVP** | Customer Master: Partner Functions | Hierarchical customer relationships (ship-to, sold-to, payer, GPO affiliations) | 1M+ rows |
| **TKUKT** | Customer Group Descriptions | Text descriptions for customer classification groups | <10K rows |
| **TVV1T** | Customer Group 1 Texts | Additional customer grouping text table | <10K rows |
| **LFA1** | Vendor Master: General Data | Master record for each vendor/manufacturer | 50K–200K rows |
| **MARA** | General Material Data | Master record for each material/product (NDC, trade name, generic name) | 500K–1M rows |
| **T001W** | Plants/Warehouses | Physical distribution centers/warehouses | <1K rows |
| **MARC** | Plant Data for Material | Material availability per plant (stock, reorder points) | 1M+ rows |
| **EKPO** | Purchasing Document Item | Purchase order line items | 10M+ rows |
| **EKKO** | Purchasing Document Header | Purchase order headers | 5M+ rows |
| **VBAK** | Sales Document: Header Data | Sales order headers | 50M+ rows |
| **VBAP** | Sales Document: Item Data | Sales order line items | 100M+ rows |

### Key SAP Concepts for This Pipeline

* **Sales Organization (VKORG):** `2220` = Cardinal Health Pharma. The pipeline filters exclusively to this org.
* **Distribution Channel (VTWEG):** `10` = Distribution. Filters to this channel only.
* **Division (SPART):** `10` = PD (Pharmaceutical Distribution), `20` = SPD (Specialty Pharma), `30` = Kinray.
* **SD Document Category (VBTYP):** Determines document type. `N` = Invoice Cancellation, `O` = Credit Memo. These get a `-1` sign multiplier on revenue.
* **Billing Type (FKART):** Specific invoice subtypes. Codes `ZI64`, `ZI65`, `ZZF8` and patterns `ZC*`, `ZV6*` are excluded.
* **Partner Functions (PARVW):** Hierarchical customer relationships: `A1`/`A2`/`A3` = affiliation levels 1–3, `PG` = Primary GPO, `SH` = Ship-to, `SP` = Sold-to.
* **SPD Warehouses:** Plants `P080`, `P090`, `P101`, `P102`, `P104`, `P105` are flagged as Specialty Pharma Distribution warehouses.
* **NDC (National Drug Code):** Unique FDA identifier for drug products, stored in field `YYNDCFDB`.

---

## 3. Two-Layer Architecture

The pipeline has two distinct layers, each from a separate code archive:

### Layer 1: SDDI-Generated Pipeline (Ingestion & Standardization)

**Archive:** `SDDI-Generated-Pipeline.zip` (427 entries, 305 non-directory files)

Palantir's **Software Defined Data Integration (SDDI)** framework, built on the deprecated "Bellhop" platform. This is a **generic, config-driven, reusable framework** — not custom code. It:

1. **Reads raw SAP tables** from a Magritte source connection (one JDBC-like connector to the SAP system)
2. **Preprocesses** them through an SAP-ERP-specific preprocessor that understands SAP column naming conventions, foreign key patterns, and data types
3. **Renames** columns from cryptic SAP field names (e.g., `FKIMG`) to human-readable names (e.g., `billed_quantity_|_fkimg`) following a standardized naming convention
4. **Cleans** data types (decimal → double conversion, null handling, date parsing)
5. **Enriches** by joining related tables at the raw level (e.g., billing item + billing header join)
6. **Produces final standardized element datasets** that serve as input to Layer 2

The framework is configured primarily through YAML files:
* `PipelineConfig.yaml` — Defines workflows, Spark resource profiles per entity, and table registry
* `SourceConfig_*.yaml` — Maps SAP tables to column schemas with data types and descriptions
* `common_elements.yaml` (150KB) — Defines the input/output contracts for all 40+ entity types
* `deployment_elements.yaml` (19KB) — Client-specific overrides (Cardinal Health customizations)

### Layer 2: SAP Ontology Transform Logic (Business Logic & Analytics)

**Archive:** `SAP-Ontology-Transform-Logic.zip` (120 entries, 88 non-directory files)

**Custom, hand-written PySpark** business logic maintained by data engineers. This is the **primary conversion target**. It:

1. **Reads standardized SDDI outputs** from Layer 1 (18 external input datasets)
2. **Builds dimension tables** (customer, material, vendor, plant) with additional enrichment
3. **Constructs the enriched billing_document fact table** by joining the SDDI billing document with all dimension tables
4. **Computes time series metrics** at multiple aggregation levels (customer-material, material, customer-account, manufacturer)
5. **Creates composite entity pairs** for analytics (customer×material, customer×trade_name, customer×generic_name)
6. **Produces derived views** (contract filters, transaction size blocks) and metadata

### Complete Data Flow

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                       RAW SAP ERP TABLES                                    │
│  VBRP (Items) │ VBRK (Headers) │ KNA1 │ KNVV │ KNVP │ LFA1 │ MARA │ T001W  │
│  EKPO │ EKKO │ VBAK │ VBAP │ TKUKT │ TVV1T │ MARC │ + more              │
└────────────────────────┬────────────────────────────────┬─────────────────────┘
                        │                                │
                        ▼                                ▼
┌──────────────────────────────────────────────────────────┐   ┌─────────────────┐
│  LAYER 1: SDDI-Generated Pipeline                        │   │  External Refs  │
│  (Config-driven, generic framework)                      │   │  - FDA Upload    │
│                                                          │   │  - USA States    │
│  preprocess → rename → clean → enrich → final           │   │  - USA ZipCodes  │
│                                                          │   │  - GeoJSON       │
│  Key customization: deployment.py for billing_document    │   │  - Territories   │
│  - Cardinal Health business filters (org 2220, channel 10)│   └────────┬────────┘
│  - Sign flipping for returns (N, O doc categories)        │            │
│  - Tele-sales and service material exclusions             │            │
│  - SPD warehouse flagging (P080, P090, P101-P105)         │            │
└────────────────────────────┬─────────────────────────────┘            │
                            │   18 standardized datasets       │
                            ▼                                   ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│  LAYER 2: SAP Ontology Transform Logic (Custom PySpark)                       │
│                                                                               │
│  DIMENSION BUILDS (Layer 3):                                                   │
│    customer.py → customer + customer_account                                   │
│    material.py → enriched material                                             │
│    vendor.py → enriched vendor                                                 │
│    plant.py → warehouse                                                        │
│    customer_partner_functions.py → A1, A2, A3, AL tables                       │
│                                                                               │
│  FACT TABLE ENRICHMENT (Layer 4):                                              │
│    billing_document.py → Enriched billing fact (1B+ rows)                      │
│      Joins: customer + customer_account + vendor + warehouse + material         │
│      Adds: unit_price, foreign_keys, title                                     │
│      Filter: rolling 25-month window on billing_date                           │
│                                                                               │
│  DERIVED ENTITIES (Layer 4–5):                                                 │
│    generic_name.py, ndc.py, trade_name.py                                      │
│    customer_affiliation.py (A1, A2, A3, PG affiliations)                       │
│    sales_document_item.py                                                      │
│                                                                               │
│  COMPOSITE PAIRS (Layer 5):                                                    │
│    customer_material_pair.py, customer_generic_name_pair.py,                    │
│    customer_trade_name_pair.py                                                  │
│                                                                               │
│  TIME SERIES & ANALYTICS (Layer 5):                                            │
│    billing_doc_ts_pre_agg.py → Pre-aggregated billing metrics                  │
│    billing_doc_ts_metrics.py → 15 time series output datasets                  │
│      (customer-account×material, customer×material, material,                   │
│       customer-account, manufacturer — each with all/PD/SPD splits)            │
│                                                                               │
│  DERIVED VIEWS (Layer 5):                                                      │
│    billing_document_contract_filters.py, billing_document_transaction_blocks.py │
│    metadata_for_backing_datasets.py                                             │
└───────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│  45+ Output Datasets → Downstream Consumers (Dashboards, Ontology Apps)        │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Execution DAG (Directed Acyclic Graph)

The exact execution order, spanning both archives, with dependency chains:

```
                        ┌───────────────────────────────────┐
                        │     SDDI FRAMEWORK (Layer 1)      │
                        │  SAP Source → preprocess → rename │
                        │  → clean → enrich → final         │
                        │  + billing_document/deployment.py  │
                        └─────────┬─────────┬─────────┬──────┘
                 billing_doc│  kna1    │  lfa1    │  mara
                 (SDDI)     │  knvv    │  plant   │  material
                    │       │  knvp    │         │  (SDDI)
            ┌──────┼───────┼─────────┼─────────┼──────┐
            ▼      ▼       ▼         ▼         ▼      ▼
      ┌─────────┐ ┌───────────┐ ┌────────┐ ┌───────┐ ┌─────────────┐
      │ plant   │ │ customer  │ │ vendor │ │ matl  │ │ cust_partner │
      │ (L3)    │ │ (L3)      │ │ (L3)   │ │ (L3)  │ │ _funcs (L3)  │
      └────┬────┘ └──┬───┬────┘ └───┬────┘ └──┬────┘ └─────────────┘
           │      │   │          │      │
           └──────┼───┼──────────┼──────┘
                   ▼   ▼          ▼
           ┌─────────────────────────┐
           │   billing_document (L4)   │
           │   ★ CRITICAL: 1B+ rows     │
           │   5-way dimension join     │
           │   + 25-month date filter   │
           └─────────┬───┬───┬─────────┘
                   │   │   │
       ┌───────────┘   │   └──────────────────┐
       ▼               ▼                          ▼
┌────────────┐ ┌────────────────┐  ┌─────────────────────┐
│ cust_affil │ │ generic_name   │  │ contract_filters     │
│ (L4)       │ │ ndc            │  │ transaction_blocks   │
└────────────┘ │ trade_name (L4)│  │ (L5)                │
               └──────┬─────────┘  └─────────────────────┘
                      │
       ┌──────────────┼─────────────┐
       ▼              ▼             ▼
┌──────────────┐ ┌──────────┐ ┌──────────────┐
│ cust_material│ │ cust_    │ │ cust_trade_  │
│ _pair (L5)   │ │ generic  │ │ name_pair   │
└──────┬───────┘ │ _name   │ │ (L5)        │
       │         │ _pair   │ └──────────────┘
       │         │ (L5)    │
       │         └──────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ billing_doc_ts_pre_agg (L5)             │
└───────────────────┬─────────────────────┘
                    ▼
┌─────────────────────────────────────────┐
│ billing_doc_ts_metrics (L5)  ★ CRITICAL  │
│ → 15 output datasets                     │
│ (customer-acct×material, material,        │
│  customer-acct, manufacturer metrics      │
│  with all/PD/SPD splits)                  │
└─────────────────────────────────────────┘
```

---

## 5. Dependency Mapping (RID to Human-Readable Names)

### 5.1 External Inputs (SDDI Outputs → Ontology Inputs)

| Param Name in Ontology | Inferred Dataset | SAP Source Tables | Consumed By |
| --- | --- | --- | --- |
| `billing_document` | SDDI Final billing_document | VBRP + VBRK (joined) | 10 Ontology files (most-used input) |
| `kna1` | Customer General | KNA1 | customer.py |
| `knvv` | Customer Sales Area | KNVV | customer.py |
| `knvp` | Customer Partner Functions | KNVP | customer.py, customer_partner_functions.py |
| `tkukt` | Customer Group Descriptions | TKUKT | customer.py |
| `tvv1t` | Customer Group 1 Texts | TVV1T | customer.py |
| `lfa1` | Vendor Master | LFA1 | vendor.py |
| `mara` | Material Master (raw) | MARA | material.py |
| `material` | SDDI Enriched Material | MARA + enrichments | material.py, billing_document_contract_filters.py |
| `source_df` (plant) | Plant/Warehouse | T001W | plant.py |
| `source_df` (plant_material) | Plant Material | MARC | plant_material.py |
| `raw_orders` | Purchase Order Item | EKPO + EKKO | purchase_order_item.py |
| `sales_order` | Sales Document Item | VBAP + VBAK | sales_document_item.py |
| `usa_state_metadata` | US State Reference | External | customer.py |
| `usa_zip_code_metadata` | US Zip Code Reference | External | customer.py |
| `kna1_territories_mapping` | Territory Mapping | External/Custom | customer.py |
| `fda_upload_df` | FDA Drug Data | External FDA | parse_fda_datasets.py |
| `source_df` (geo_json) | US GeoJSON | External | usa_states_and_zip_codes_geo_json.py |

### 5.2 Internal Dependencies (Ontology → Ontology)

| Producer | Output Dataset | Consumers |
| --- | --- | --- |
| `customer.py` | customer | billing_document.py, sales_document_item.py, customer_affiliation.py, customer_material_pair.py, customer_generic_name_pair.py, customer_trade_name_pair.py, metadata.py |
| `customer.py` | customer_account | billing_document.py, sales_document_item.py |
| `material.py` | material (enriched) | billing_document.py, sales_document_item.py, generic_name.py, ndc.py, trade_name.py, customer_material_pair.py, metadata.py |
| `vendor.py` | vendor | billing_document.py |
| `plant.py` | warehouse | billing_document.py |
| `billing_document.py` | billing_document (enriched) | ts_pre_agg.py, customer_affiliation.py, contract_filters.py, transaction_blocks.py, usa_geo_json.py |
| `generic_name.py` | generic_name | customer_generic_name_pair.py |
| `trade_name.py` | trade_name | customer_trade_name_pair.py |
| `ts_pre_agg.py` | pre_aggd_df | ts_metrics.py |
| `customer_material_pair.py` | customer_material_pair | metadata.py |

---

## 6. Core Transformations Explained

### 6.1 billing_document.py — Central Fact Table Enrichment (CRITICAL)

**Input:** SDDI billing_document (1B+ rows, already filtered/cleaned by SDDI deployment.py)  
**Process:**
1. **Rolling 25-month date filter** on `billing_date_|_fkdat` — reduces the 1B+ to a rolling window
2. **Ship-to customer enrichment** — left join to customer on `mandt_kunnr_shipto_|_foreign_key_KNA1`
3. **Sold-to customer account enrichment** — left join to customer_account on `mandt_kunrg_vkorg_vtweg_spart_|_foreign_key_KNVV`
4. **Vendor enrichment** — left join to vendor on `mandt_mfrnr_|_foreign_key_LFA1`
5. **Warehouse enrichment** — left join to plant on `mandt_werks_|_foreign_key_T001W`
6. **Material enrichment** — left join to material on `mandt_matnr_|_foreign_key_MARA`
7. **Unit price calculation** — `sales_value / billed_quantity` (with null/zero guards)
8. **Foreign key construction** — composite keys for downstream joins
9. **Title generation** — human-readable invoice title string
10. **Column selection** — final schema projection

**Performance profile:** I/O-bound (5 large joins against dimension tables). Must use `F.broadcast()` for all dimension joins.

### 6.2 billing_document_time_series_metrics.py — Multi-Dimensional Aggregation (CRITICAL)

**Input:** Pre-aggregated billing data (from ts_pre_agg.py)  
**Process:** Computes rolling time series metrics at 6 different aggregation levels:
1. **Customer Account × Material** pair metrics
2. **Customer × Material** pair metrics  
3. **Material** level metrics
4. **Customer Account** level metrics (with PD-only and SPD-only variants)
5. **Manufacturer** level metrics (with PD-only and SPD-only variants)

Each aggregation computes: item counts, distinct customer counts, billed quantity sums, sales value sums, average unit price, cumulative material set sizes with lag.

**Output:** 15 separate datasets (some commented out in source: customer ship-to and customer-level metrics are disabled).

**Performance profile:** CPU-bound (heavy aggregations with window functions). Benefits from AQE and Photon.

### 6.3 customer.py — Customer Dimension Build (HIGH)

**Input:** 9 source datasets (kna1, knvv, knvp, tkukt, tvv1t, billing_document, usa_state_metadata, usa_zip_code_metadata, kna1_territories_mapping)  
**Process:**
1. Builds two outputs: `customer` (KNA1-based) and `customer_account` (KNVV-based)
2. Joins partner function hierarchy (KNVP) to pivot A1/A2/A3/PG relationships into columns
3. Enriches with geographic metadata (state, zip code, territory)
4. Enriches with billing document metrics (aggregated sales statistics per customer)
5. Adds time series metric ID columns for downstream analytics
6. Computes forecasting features (Customer Lifetime Value) using `foundry_lifetimes` library

**Key nuance:** The `foundry_lifetimes` library (for CLV prediction) is Palantir-specific. Databricks equivalent: use the `lifetimes` PyPI package or implement BG/NBD + Gamma-Gamma models directly.

### 6.4 SDDI billing_document/deployment.py — Cardinal Health Business Filters (CRITICAL)

**This file contains the most important business rules in the entire pipeline.** It runs inside the SDDI framework and applies Cardinal Health-specific filtering before the data reaches the Ontology layer:

1. **Tele-sales exclusion** — removes orders with purchase_order_type `TEL`/`TELE` via left_anti join against VBAK
2. **Service material exclusion** — removes materials with type `ZIEN` or item category `ZSER` via left_anti join against MARA
3. **Material attribute enrichment** — joins specialty strength, industry sector, base UoM, NDC, trade name from MARA
4. **Multi-criteria business filter:**
   * Sales org = `2220` (Pharma)
   * Distribution channel = `10` (Distribution)  
   * Division in `[10, 20, 30]` (PD, SPD, Kinray)
   * Exclude intercompany (doc categories 5, 6, U)
   * Exclude specific billing types (`ZI64`, `ZI65`, `ZZF8`, `ZC*`, `ZV6*`)
   * Exclude cancelled invoices (fksto = X or Y)
   * Exclude service billing items (material group = ZSERVICE)
   * USD currency only
5. **Sales value calculation** — `(subtotal_2 - subtotal_11) * sign_multiplier` where sign = -1 for cancellations/credit memos (doc categories N, O)
6. **Total unit strength** — `billed_quantity * specialty_strength_value * sign_multiplier`
7. **SPD flag** — marks warehouses P080, P090, P101–P105 as Specialty Pharma
8. **Source flag** — marks contract_type `S` as source
9. **Ship-to partner enrichment** — joins VBPA to get ship-to customer for each billing document

### 6.5 Composite Entity Pairs (customer_material_pair, customer_generic_name_pair, customer_trade_name_pair)

**Pattern:** Each creates a cross-dimensional entity by:
1. Extracting unique (customer, material/generic_name/trade_name) combinations from billing documents
2. Joining back to enriched customer and material/name dimension tables
3. Computing billing document metrics per pair (counts, sums, averages)
4. Adding time series metric IDs
5. Computing CLV forecasting features per pair

**Business purpose:** Enables analytics like "Which customer buys which materials?" and "What is the predicted future value of each customer-material relationship?"

### 6.6 Derived Views

* **billing_document_contract_filters.py** — Filters billing documents to specific contract types using material attributes
* **billing_document_transaction_blocks.py** — Computes transaction size distributions (relative and absolute) for anomaly detection

---

## 7. SDDI Framework Analysis

### 7.1 Multi-Stage Pipeline

| Stage | What It Does | Where Logic Lives | Databricks Equivalent |
| --- | --- | --- | --- |
| **Preprocessors** | SAP-ERP-specific parsing: column naming, foreign key construction, data type inference | `transforms/preprocessors/sap_erp/` | Not needed if raw tables already landed with proper schemas |
| **Renamed** | Column standardization from SAP codes to human-readable names | `transforms/renamed/transform.py` + `common_elements.yaml` | Column rename mapping in `conf/table_config.py` |
| **Cleaned** | Data type casting (decimal→double), null handling, deduplication | `transforms/cleaned/transform.py` + `cleaned/function_libraries/` | Standard PySpark cleaning in bronze→silver |
| **Enriched** | Cross-table joins at raw level (e.g., billing header+item) | `transforms/enriched/transform.py` | Join logic in silver layer transforms |
| **Final** | Schema projection to output contract, element-specific logic | `transforms/final/transform.py` + `derived/elements/*/common.py` | Included in silver→gold transforms |
| **Post-Process** | Additional downstream transformations | `transforms/post_process/transform.py` | Included in gold layer |
| **Build All** | Orchestration/scheduling wrapper | `transforms/build_all/transform.py` | Lakeflow/Workflow orchestrator |

### 7.2 Config vs. Code Logic

| Logic Type | Location | Examples |
| --- | --- | --- |
| **Schema definitions** | `common_elements.yaml` (150KB) | Column lists, data types, select projections for all 40+ entities |
| **Column mappings** | `deployment_elements.yaml` (19KB) | Input/output contracts, column renames, enrichment table definitions |
| **Business filters** | `deployment.py` files (code) | Cardinal Health-specific org/channel/division filters, sign flipping, tele-sales exclusion |
| **Spark profiles** | `PipelineConfig.yaml` | Resource allocation per entity (billing_document gets 64 executors, large memory) |
| **Source connections** | `SourceConfig_*.yaml` (24KB) | SAP table column schemas with data types and descriptions |
| **Workflow enrichments** | `PipelineConfig.yaml` + YAML configs | Which enrichment joins to apply per workflow |

### 7.3 Recommendation: Collapse vs. Replicate

**Recommendation: COLLAPSE the SDDI multi-stage pattern into 2–3 steps in Databricks.**

Rationale:
* The SDDI framework exists because Palantir needed a generic, config-driven approach for multiple ERP source types (SAP, Oracle, MSSQL, Salesforce). In Databricks, we only need SAP ERP support.
* The preprocess → rename → clean stages can be collapsed into a single "source standardization" step per table.
* The enriched → final → deployment stages should be collapsed into the Ontology-layer transforms where the actual business logic lives.
* The key artifacts to preserve from SDDI are:
  1. The **column rename mappings** (from `common_elements.yaml` and `deployment_elements.yaml`)
  2. The **business filters** in `billing_document/deployment.py` (must be faithfully replicated)
  3. The **Spark resource profiles** (translated to Databricks cluster/pool sizing)
  4. The **schema contracts** (column lists, data types) for each entity

---

## 8. Scale Annotations

| Transform | Est. Input Rows | Est. Output Rows | Bound Type | Key Performance Notes |
| --- | --- | --- | --- | --- |
| **SDDI billing_document (deployment.py)** | 1B+ (raw VBRP×VBRK) | ~500M–800M (after filters) | I/O | Header-item join is the first bottleneck. Left-anti joins for tele-sales/service exclusion. |
| **billing_document.py** (Ontology) | ~500M–800M (SDDI output) | ~300M–500M (25-month filter) | I/O | 5 broadcast joins to dimensions. Rolling date filter reduces significantly. |
| **billing_doc_ts_pre_agg.py** | ~300M–500M | ~50M–100M (grouped) | CPU+I/O | Heavy groupBy with 15+ grouping columns. 6-month filter sub-selects. |
| **billing_doc_ts_metrics.py** | ~50M–100M (pre-agg) | 15 datasets, ~1M–50M each | CPU | Window functions, cumulative sets, lag computations. Most CPU-intensive. |
| **customer.py** | ~500K (kna1) + 1M (knvv) | ~500K (customer) + ~1M (customer_account) | CPU | Partner function pivoting, geographic enrichment, CLV computation. |
| **material.py** | ~500K–1M (material) | ~500K–1M | CPU | approx_count_distinct per column (anti-pattern at scale), billing metric aggregation. |
| **vendor.py** | ~50K–200K (lfa1) | ~50K–200K | CPU | Billing metric aggregation per vendor. |
| **plant.py** | <1K (T001W) | <1K | N/A | Trivial passthrough with null column removal. |
| **plant_material.py** | ~1M (MARC) | ~1M | N/A | Passthrough with SPD flag. |
| **purchase_order_item.py** | ~10M (EKPO) | ~10M | N/A | Passthrough with null column removal. |
| **sales_document_item.py** | ~100M (VBAP) | ~100M | I/O | Joins to material, customer, customer_account dimensions. |
| **customer_partner_functions.py** | ~1M (knvp) | ~4 outputs × ~200K each | CPU | Pivots partner function rows into 4 entity tables (A1, A2, A3, AL). |
| **customer_affiliation.py** | ~500K (customer) | ~4 outputs × ~100K each | CPU | Billing metric aggregation per affiliation level. |
| **customer_material_pair.py** | ~300M–500M (billing_doc) | ~10M–50M pairs | I/O+CPU | Distinct pair extraction + dimension join + billing metrics + CLV. |
| **customer_generic_name_pair.py** | ~300M–500M (billing_doc) | ~5M–20M pairs | I/O+CPU | Similar to customer_material_pair but grouped by generic name. |
| **customer_trade_name_pair.py** | ~300M–500M (billing_doc) | ~5M–20M pairs | I/O+CPU | Similar to customer_material_pair but grouped by trade name. |
| **generic_name.py, ndc.py, trade_name.py** | ~500K–1M (material) | ~10K–100K each | CPU | Grouping materials by generic name/NDC/trade name + billing metrics. |
| **contract_filters.py** | ~300M–500M | ~subset | CPU | Contract-type based filtering of billing documents. |
| **transaction_blocks.py** | ~300M–500M | ~2 small outputs | CPU | Distribution/percentile computation for anomaly detection. |
| **parse_fda_datasets.py** | Small (FDA upload) | Small | N/A | Simple parsing, no performance concern. |
| **metadata.py** | Small (3 dimension inputs) | Small | N/A | Metadata aggregation, no performance concern. |
| **usa_geo_json.py** | Small (GeoJSON source) | Small | N/A | GeoJSON processing for geographic visualization. |

---

## 9. Known Anti-Patterns to Fix During Conversion

| Pattern | Location | Risk | Fix |
| --- | --- | --- | --- |
| `drop_cols_where_all_values_are_the_same()` calls `.collect()` | `utils.py` line 29 | Collects approx_count_distinct for ALL columns to driver | Replace with single-pass agg or remove entirely (let downstream handle) |
| `foundry_lifetimes` library usage | `utils.py` line 11 | Palantir-specific CLV library | Replace with `lifetimes` PyPI package or custom BG/NBD model |
| Per-column `approx_count_distinct` in `.collect()` | `utils.py:drop_cols_where_all_values_are_the_same` | O(columns) collect calls | Collapse into single `.agg()` call |
| `get_cols_sorted_by_null_count()` likely calls `.collect()` | `utils.py` (used by 5+ transforms) | Collects null counts to driver | Replace with single-pass null count agg |
| Foundry-specific `@transform_df`, `@transform`, `Input()`, `Output()` | All 22 transform files | Palantir decorator pattern | Replace with standard PySpark functions reading/writing Delta tables |
| `Check()` / `expectations` DQ framework | All 22 transform files | Palantir-specific DQ | Replace with custom `src/data_quality.py` or Databricks Expectations |
| Hardcoded warehouse codes (`QE9_test_|_100_|_P080`, etc.) | deployment.py, plant_material.py | Environment-specific values | Extract to `conf/pipeline_config.py` |
| Commented-out incremental processing | All transforms | Incremental was disabled | Consider Delta merge patterns for incremental in Databricks |

---

## 10. Glossary of Cardinal Health / Pharma Terms

| Term | Meaning |
| --- | --- |
| **PD** | Pharmaceutical Distribution (Division 10) — standard drug distribution |
| **SPD** | Specialty Pharma Distribution (Division 20) — specialty/high-cost drugs requiring special handling |
| **Kinray** | Division 30 — Cardinal Health's pharmacy distribution subsidiary |
| **GPO** | Group Purchasing Organization — collective buying entity (partner function PG) |
| **NDC** | National Drug Code — unique 10-digit FDA identifier for drug products |
| **CIN** | Cardinal Item Number — Cardinal Health's internal product identifier |
| **GCN/GSN** | Generic Classification Number / Generic Sequence Number — drug classification codes |
| **WAPD** | Weighted Average Product Distribution — a Cardinal Health pricing/distribution metric |
| **CLV** | Customer Lifetime Value — predicted future revenue from a customer relationship |
| **BG/NBD** | Beta-Geometric/Negative Binomial Distribution — statistical model for CLV prediction |
| **Partner Functions** | SAP concept for hierarchical customer relationships (A1/A2/A3 = affiliation levels, SH = ship-to, SP = sold-to, PY = payer) |
| **Sales Organization 2220** | Cardinal Health Pharmaceutical segment |
| **Distribution Channel 10** | Direct distribution channel |
| **Billing Type** | SAP invoice subtype code (ZI64/ZI65 = pass-through, ZC* = cancellations, ZV6* = intra-billing) |