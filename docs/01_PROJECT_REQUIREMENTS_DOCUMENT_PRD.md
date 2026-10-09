# 📋 Project Requirements Document (PRD)
## Enterprise Lakehouse Data Integration: Parent Company (ABC) × Child Company (abc)

---

### 1. Executive Summary & Business Context

**Parent Company (ABC)** is a large, established sports equipment manufacturer operating with mature, predictable, and structured Enterprise Resource Planning (ERP) systems. Its analytical workflows rely on standardized relational databases, consistent monthly financial accounting cycles, rigid dimensional schemas, and established data governance frameworks.

To accelerate market expansion into the high-growth sports nutrition and health segment, Parent Company (ABC) acquired **Child Company (abc)**, a fast-growing startup. While Child Company (abc) brings high operational agility and rapid market adoption, it operates with fragmented, decentralized data sources—including spreadsheets, Cloud/S3 landing buckets, WhatsApp exports, and ad-hoc extracts.

```mermaid
graph LR
    subgraph "Parent Company (ABC)"
        A1[Established Enterprise] --> A2[Mature ERP Systems]
        A2 --> A3[Predictable Monthly Grain]
        A3 --> A4[Standardized Star Schema]
    end

    subgraph "Child Company (abc)"
        B1[Hyper-Growth Startup] --> B2[Fragmented Systems]
        B2 --> B3[Daily Transaction Feeds]
        B3 --> B4[Ad-hoc Formats & Typos]
    end

    A4 --> C{Databricks Lakehouse<br/>Medallion Architecture}
    B4 --> C
    C --> D[Unified Executive Analytics & BI]
```

#### Problem Statement
The core business challenge is to consolidate the data from both companies to provide unified, reliable, and scalable analytics while retaining Child Company (abc)'s unique operational culture and fast-paced workflows:
- **Heterogeneous Sources**: Child Company data originates from disparate transactional CSV files uploaded to AWS S3 storage.
- **Inconsistent Date Conventions**: Raw files contain conflicting date representations (`yyyy/MM/dd`, `dd-MM-yyyy`, `"Tuesday, July 01, 2025"`).
- **Data Quality Anomalies**: Presence of city typos (`Bengaluruu`, `Bengalore`), negative prices (`-84`), unmapped text values (`unknown`), missing foreign keys, and alphanumeric corrupted IDs (`XYZ123`).
- **Granularity Mismatch**: Child Company transactions occur at the **daily individual order line level**, whereas Parent Company reporting operates on a **monthly aggregated grain** (`YYYY-MM-01`).

---

### 2. Project Objectives & Success Criteria

```mermaid
mindmap
  root((Lakehouse Objectives))
    Reliability
      Automated Data Cleansing
      Deterministic Product Hashing
      Idempotent Delta Upserts
    Scalability
      Full & Incremental Pipelines
      Delta Change Data Feed CDF
      Automated S3 Landing Archiving
    Ease of Adoption
      Conformed Star Schema
      Modular PySpark Notebooks
      Unified SQL Semantic Views
```

| Dimension | Target Requirement | Business Impact |
| :--- | :--- | :--- |
| **Reliability** | Provide accurate, consolidated analytics in a single unified dashboard without data loss or corruption. | Prevents revenue distortions from negative prices, duplicate orders, or dropped unmapped records. |
| **Scalability** | The Lakehouse architecture must handle future data volume growth seamlessly, even if complete legacy system integration takes longer than the planned timeline. | Dual-mode pipeline (historical full load + automated daily incremental staging) scales to high-volume feeds. |
| **Ease of Adoption** | The pipeline and data model must be intuitive and modular for future data team hires, keeping the learning curve manageable. | Standard Medallion layers, clean PySpark modules, and conformed Star Schema enable rapid onboarding. |
| **Schema Harmonization** | Seamlessly map and merge Child Company entities into Parent Company conformed dimensions (`dim_customers`, `dim_products`, `dim_gross_price`, `dim_date`, `fact_orders`). | Enables enterprise-wide cross-company reporting without modifying Parent Company legacy structures. |
| **Temporal Granularity** | Aggregate daily transactional sales into monthly conformed sales records aligned with the Parent Company accounting calendar. | Ensures consistent month-over-month revenue tracking across both organizations. |

---

### 3. Technical Architecture & Tech Stack

The solution implements the **Medallion Lakehouse Architecture** on Databricks Delta Lake, leveraging distributed Apache Spark processing and AWS Cloud Storage.

```mermaid
flowchart TB
    subgraph Storage ["Cloud Ingestion Layer (AWS S3)"]
        S1["s3://.../customers/*.csv"]
        S2["s3://.../products/*.csv"]
        S3["s3://.../gross_price/*.csv"]
        S4["s3://.../orders/landing/*.csv"]
    end

    subgraph Bronze ["Bronze Layer (Raw Delta Lake)"]
        B1["fmgc.bronze.customers<br/>+ Ingestion Metadata & CDF"]
        B2["fmgc.bronze.products<br/>+ Ingestion Metadata & CDF"]
        B3["fmgc.bronze.gross_price<br/>+ Ingestion Metadata & CDF"]
        B4["fmgc.bronze.orders<br/>+ Ingestion Metadata & CDF"]
        B5["fmgc.bronze.staging_orders<br/>(Daily Incremental Staging)"]
    end

    subgraph Silver ["Silver Layer (Cleansed & Conformed Delta Lake)"]
        S_C["fmgc.silver.customers<br/>- Trimmed & Title-cased<br/>- Fixed City Typos & Nulls<br/>- Added Market, Platform, Channel"]
        S_P["fmgc.silver.products<br/>- Fixed Spelling Typos<br/>- Extracted Variant & Division<br/>- Generated SHA-256 product_code"]
        S_G["fmgc.silver.gross_price<br/>- Normalized Multi-pattern Dates<br/>- Inverted Negative Prices<br/>- Enriched with product_code"]
        S_O["fmgc.silver.orders<br/>- Stripped Day Names & Parsed Dates<br/>- Filtered Null Qty & Cleaned IDs<br/>- Joined with Silver Products"]
    end

    subgraph Gold ["Gold Layer (Enterprise Star Schema & BI Views)"]
        G_DC[("fmgc.gold.dim_customers<br/>Parent + Child Conformed")]
        G_DP[("fmgc.gold.dim_products<br/>Parent + Child Conformed")]
        G_DG[("fmgc.gold.dim_gross_price<br/>Annualized Price Model")]
        G_DD[("fmgc.gold.dim_date<br/>Calendar Hierarchy (2024-2025)")]
        G_FO[("fmgc.gold.fact_orders<br/>Monthly Aggregated Fact")]
        G_SBF[("fmgc.gold.sb_fact_orders<br/>Child Granular Daily Fact")]
        
        V1["fmgc.gold.vw_fact_orders_enriched<br/>(Unified Analytics View)"]
    end

    subgraph BI ["Analytics & Reporting"]
        D1["Executive Overview Dashboard"]
        D2["Product Performance Analysis"]
        D3["Customer & Market Analysis"]
        D4["Databricks Genie AI Space"]
    end

    Storage --> Bronze
    Bronze --> Silver
    Silver --> Gold
    G_DC & G_DP & G_DG & G_DD & G_FO --> V1
    V1 --> BI
```

#### Technology Stack Specifications
- **Platform**: Databricks (Free / Community / Workspace Edition)
- **Architecture**: Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold layers)
- **Core Stack**: Python (PySpark 3.5+), Spark SQL, Delta Lake, AWS S3
- **Table Storage**: Delta Lake format with Change Data Feed enabled (`delta.enableChangeDataFeed = true`)
- **Data Governance**: Databricks Three-Level Namespace (`fmgc.bronze`, `fmgc.silver`, `fmgc.gold`)
- **Reporting & AI**: Databricks BI Dashboards, SQL Semantic Views, and Databricks Genie for AI-assisted conversational querying

---

### 4. Key Deliverables & Scope Matrix

```mermaid
gantt
    title Project Implementation Milestones
    dateFormat  YYYY-MM-DD
    section Phase 1: Environment & Setup
    Catalog & Schema Hierarchy Setup (fmgc)   :done, 2025-07-01, 2025-07-03
    Calendar Dimension Generator (dim_date)   :done, 2025-07-04, 2025-07-07
    section Phase 2: Dimension Pipelines
    Customer Cleansing & Attribute Alignment  :done, 2025-07-08, 2025-07-15
    Product Normalization & SHA-256 Keys      :done, 2025-07-16, 2025-07-22
    Gross Price Date Parsing & Annual Rollup  :done, 2025-07-23, 2025-07-28
    section Phase 3: Fact Pipelines
    Historical Full Load Fact Pipeline        :done, 2025-07-29, 2025-08-08
    Daily Incremental Fact Staging & Merge    :done, 2025-08-09, 2025-08-18
    section Phase 4: BI Views & Dashboards
    Semantic Layer View (vw_fact_orders)      :done, 2025-08-19, 2025-08-25
    Executive & Category Performance Reports  :done, 2025-08-26, 2025-09-02
```

1. **Consolidated Pipeline (Full & Incremental)**:
   - **Full Load Pipeline** (`1_full_load_fact.py`): Ingests historical batch files, moves landing files to `processed/`, aggregates daily transactions to monthly grain, and merges into `fmgc.gold.fact_orders`.
   - **Incremental Load Pipeline** (`2_incremental_load_fact.py`): Uses staging tables for newly arrived daily batches, isolates impacted monthly partitions, recalculates affected monthly aggregates from `sb_fact_orders`, and executes an idempotent `DeltaTable.merge()`.
2. **Conformed Star Schema Data Model**:
   - `dim_customers`: Conforms Child Company retail partners with Parent Company customer codes and attributes.
   - `dim_products`: Normalizes categories, extracts variants, maps divisions, and generates deterministic SHA-256 `product_code`.
   - `dim_gross_price`: Resolves multi-format dates, inverts negative prices, and aggregates to annual list prices using window ranking.
   - `dim_date`: Calendar dimension with monthly surrogate keys (`yyyyMM`), quarters, and month names.
   - `fact_orders`: Monthly grain enterprise fact table (`date`, `product_code`, `customer_code`, `sold_quantity`).
   - `sb_fact_orders`: Child daily operational fact table (`order_id`, `date`, `customer_code`, `product_code`, `sold_quantity`).
3. **BI Dashboards & Semantic Reporting Layer**:
   - `vw_fact_orders_enriched`: Denormalized SQL view computing derived revenue metrics (`sold_quantity * price_inr`).
   - Dashboards: Executive Overview, Product Performance Analysis, Customer & Market Analysis, and Databricks Genie AI querying space.
