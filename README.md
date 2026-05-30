# 🏪 Near-Real-Time Data Warehouse

> A near-real-time ETL pipeline for Walmart sales data, built on a custom Hybrid Join algorithm with parallel threading and a star schema data warehouse.

---

## 📌 Overview

Large retail chains like Walmart generate **continuous streams of transactional data**. Traditional batch ETL pipelines can't keep up — near-real-time warehousing is required, where streaming data is enriched and loaded into the warehouse almost instantly.

This project implements a **Hybrid Join algorithm** in Python that efficiently joins live streaming transactional data with static master data using parallel threads, then loads the enriched records into a MySQL star schema warehouse for business intelligence queries.

---

## 🗃️ Star Schema Design

The warehouse follows a classic **star schema** — one central fact table surrounded by three dimension tables.

```
CUSTOMER_DIM ──┐
               │
PRODUCT_DIM  ──┤──► SALEFACT
               │
TIME_DIM     ──┘
```

| Table | Type | Key Columns |
|---|---|---|
| `SALEFACT` | Fact | `sales_id`, `order_id`, `customer_id`, `product_id`, `date_id`, `quantity`, `purchase_amount` |
| `CUSTOMER_DIM` | Dimension | `customer_id`, `gender`, `age`, `occupation`, `city_category`, `marital_status` |
| `PRODUCT_DIM` | Dimension | `product_id`, `product_category`, `price`, `store_id`, `supplier_id` |
| `TIME_DIM` | Dimension | `date_id`, `full_date`, `day_of_week`, `month`, `quarter`, `season`, `year` |

---

## ⚙️ Hybrid Join Algorithm

### Why Hybrid Join?

During ETL, two fundamentally different data types must be joined:
- **Streaming data** — continuous, real-time transactional records
- **Master data** — static or slowly-changing dimensional data (customers, products)

A standard join can't handle this efficiently. The Hybrid Join algorithm bridges the gap.

### Architecture: Parallel Threading

The pipeline runs two concurrent threads:

**Thread 1 — Stream Loader**
Reads transactional data and pushes records into a thread-safe stream buffer.

**Thread 2 — Hybrid Join Engine**
- Reads master data into partitioned disk buffers (pre-sorted for efficient lookup)
- Pulls stream tuples from the buffer into a **doubly linked queue** (maintains order)
- Simultaneously inserts each tuple into a **hash table** (enables O(1) lookup)
- Scans master data partitions for the oldest queue key
- On a match → enriches the record and loads it into the warehouse
- On no match → discards the orphan record (strict inner join)

### Data Structures Used

| Component | Purpose |
|---|---|
| Stream Buffer | Thread-safe queue for incoming transactional records |
| Doubly Linked Queue | Maintains FIFO ordering of stream tuples |
| Hash Table | Fast O(1) pointer lookup for join matching (10,000 slots) |
| Disk Buffer | Holds sorted master data partitions for sequential scanning |

---

## 📂 Project Structure

```
├── hybrid_join.py              # Core ETL pipeline with Hybrid Join implementation
├── starSchema.sql              # DDL for the star schema warehouse
├── dummy.sql                   # Sample data inserts
├── Analysis_Queries.sql        # BI analysis queries
├── customer_master_data.csv    # Customer dimension master data
├── product_master_data.csv     # Product dimension master data
├── transactional_data.csv      # Simulated streaming transactional data
└── 232644_report.pdf           # Full project report
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- MySQL Server
- `mysql-connector-python`

```bash
pip install mysql-connector-python
```

### Setup

**1. Initialize the database**
```sql
-- Run in MySQL
source starSchema.sql;
source dummy.sql;
```

**2. Run the pipeline**
```bash
python hybrid_join.py
```

Enter your MySQL host, username, and password when prompted.

**3. Stop the pipeline**
```
CTRL + C  →  threads shut down gracefully
```

---

## 📊 Analysis Queries

Once the warehouse is loaded, `Analysis_Queries.sql` contains BI queries for sales analysis, including breakdowns by customer demographics, product category, season, and store performance.

---

## ⚠️ Known Limitations

- **Aggressive data loss** — Strict inner join discards any stream tuple without a matching master record, which can cause significant record loss
- **Fixed hash table size** — 10,000 slots fill quickly under high-volume streaming
- **Sorted master data required** — The partition scanning relies on pre-sorted master data, adding preprocessing overhead

---

## 🛠️ Tech Stack

- **Python** — `threading`, `queue`, `mysql-connector-python`
- **MySQL** — Star schema warehouse, DDL, analytical queries
- **Data** — Walmart-style retail transaction dataset

---

## 💡 Key Learnings

- Designing a normalized star schema for multidimensional retail analysis
- Implementing a custom ETL pipeline with multithreading in Python
- Understanding the trade-offs between join completeness and memory/speed efficiency
- Bridging the gap between streaming and static data in a warehouse context

---

## 👤 Author

**Ramalah Amir** (23i-2644)
Data Science BS-DS — FAST NUCES Islamabad
