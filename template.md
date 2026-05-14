# Báo cáo dự án — Template

> **Đề tài:** Customer Data Platform (CDP) cho dữ liệu thương mại điện tử Olist trên kiến trúc Lakehouse Medallion
> **Môn học:** Data Integration / Big Data
> **Template này:** liệt kê toàn bộ các phần cần viết, các sơ đồ/chart cần vẽ, các thí nghiệm cần thực hiện. Phần nào CHƯA LÀM được mark `(còn thiếu)` ngay ở tiêu đề con; phần nào ĐÃ CÓ thì có draft sẵn để chỉnh sửa.

---

## Mục lục

1. Trang bìa
2. Project Overview
3. System Analysis & Requirements
4. System Architecture
5. Technology Stack & Justification
6. Data Sources & Schema Design
7. Pipeline Implementation
8. Optimization & Research Depth
9. Evaluation & Testing
10. Deployment & Operations Guide
11. Challenges & Solutions
12. Future Improvements
13. Conclusion
14. Appendices

---

## 1. Trang bìa

**Cần viết:**
- Tên trường, viện
- Tên môn học
- Tiêu đề: "Customer Data Platform on Lakehouse Architecture — A Medallion Pipeline for E-commerce Multi-channel Integration"
- Tên giảng viên hướng dẫn
- Học kỳ, nhóm
- Danh sách thành viên + MSSV
- "Hà Nội, tháng [X] năm 2026"

**Cần thiết kế:** logo HUST + logo SoICT, layout đối xứng (xem mẫu PDF Hotel Booking trang 1).

---

## 2. Project Overview

### 2.1. Problem Statement

**Cần viết:** mô tả bối cảnh, vấn đề thực tế, lý do cần CDP.

**Draft:**

Trong môi trường thương mại điện tử hiện đại, dữ liệu khách hàng bị phân mảnh trên nhiều hệ thống vận hành rời rạc: hệ thống đơn hàng (transactions), hệ thống quản trị khách hàng (CRM), hệ thống chăm sóc khách hàng (helpdesk/Zendesk), và các nguồn dữ liệu địa lý. Mỗi hệ thống sử dụng khóa định danh khác nhau (`customer_id` ở e-commerce, `email` ở support, `customer_unique_id` ở CRM, `zip_code_prefix` ở geo), dẫn đến ba vấn đề nghiêm trọng:

1. **Không có cái nhìn 360 độ về khách hàng:** Marketing và Support không thể trả lời câu hỏi cơ bản như "khách X đã mua gì, đánh giá ra sao, có từng gọi support chưa?".
2. **Phân tích bị giới hạn:** không thể segment khách theo hành vi cross-channel (Recency/Frequency/Monetary), không có view doanh số đa chiều.
3. **Khả năng mở rộng kém:** mỗi khi thêm nguồn dữ liệu mới (loyalty, web clickstream), phải xây dựng pipeline tùy biến.

Đề tài giải quyết vấn đề bằng cách xây dựng một **Customer Data Platform** theo kiến trúc Lakehouse Medallion, hợp nhất dữ liệu từ 11 nguồn của bộ dữ liệu Olist Brazilian E-commerce, áp dụng các nguyên tắc Data Governance (lineage, DQ checks) và cung cấp 4 view dữ liệu nghiệp vụ ở tầng Gold.

### 2.2. Objectives

**Cần viết:** liệt kê 5-7 mục tiêu cụ thể, đo lường được.

**Draft:**

Mục tiêu chính của dự án là xây dựng một pipeline dữ liệu **end-to-end, có khả năng mở rộng, có DQ tự động** để giải quyết bài toán hợp nhất dữ liệu khách hàng. Các mục tiêu cụ thể bao gồm:

1. Triển khai kiến trúc **Medallion 3 tầng (Bronze/Silver/Gold)** trên Delta Lake với storage MinIO (S3-compatible).
2. Xây dựng cơ chế **ingestion incremental** với metadata lineage (`_ingested_at`, `_source_file`) cho 11 nguồn dữ liệu.
3. Triển khai **Data Quality framework** với schema enforcement, null check, range check, halt-on-failure tại tầng Silver.
4. Cung cấp 4 bảng Gold phục vụ 4 nhóm stakeholder: `customer_360` (Marketing), `fact_orders_enriched` (Operations), `agg_sales_by_state_category_month` (BI), `segment_rfm` (Marketing).
5. **Orchestrate** toàn bộ workflow bằng Airflow DAG với dependency graph rõ ràng, parallelism hợp lý.
6. Đảm bảo **data freshness ≤ 24h**, **pipeline reliability ≥ 95%** trong môi trường demo.
7. Cung cấp tài liệu kỹ thuật, hướng dẫn triển khai, và benchmark performance.

### 2.3. Scope

**Cần viết:** trong phạm vi gì, ngoài phạm vi gì.

**Draft:**

**Trong phạm vi (In-scope):**
- Batch ingestion từ file CSV vào MinIO landing zone.
- 3 tầng dữ liệu Delta Lake (Bronze → Silver → Gold) với 11 bảng nguồn.
- Schema enforcement và DQ check tự động ở Silver.
- Airflow DAG cho orchestration với parallelism.
- DQ inline qua exception-based halt.
- Demo trên dataset Olist Brazilian E-commerce + dữ liệu CRM/helpdesk sinh giả (mock).
- Truy vấn ad-hoc Gold layer qua Jupyter notebook.

**Ngoài phạm vi (Out-of-scope) — `(còn thiếu)` và sẽ thảo luận ở Future Improvements:**
- Real-time streaming ingestion (Kafka, Spark Structured Streaming).
- Serving layer ra REST API/NoSQL cho ứng dụng end-user.
- BI dashboard (Metabase/Superset) — chỉ có notebook demo.
- ML model (churn prediction, recommendation) — chỉ có RFM rule-based.
- Multi-node Spark cluster — chỉ chạy single-node local.
- Production-grade observability (Prometheus, Grafana).
- Cross-region replication, HA storage.

---

## 3. System Analysis & Requirements

### 3.1. Stakeholder Analysis

**Cần viết:** bảng stakeholder và nhu cầu.

**Cần vẽ:** bảng đơn giản (markdown table).

**Draft:**

| Stakeholder | Trách nhiệm | Cần view dữ liệu nào | Tần suất truy cập |
|---|---|---|---|
| Marketing | Chạy campaign, segment khách hàng | `customer_360`, `segment_rfm` | Hàng tuần |
| Customer Support | Tra cứu lịch sử khách khi nhận ticket | `customer_360` | Real-time (ad-hoc) |
| BI Analyst | Dashboard doanh số | `agg_sales_by_state_category_month` | Hàng ngày |
| Operations | Theo dõi SLA giao hàng, đánh giá seller | `fact_orders_enriched` | Hàng ngày |
| Data Engineer (nhóm) | Vận hành pipeline | Bronze/Silver lineage | Khi có incident |

### 3.2. Functional Requirements

**Cần viết:** danh sách FR có ID, mô tả, priority.

**Draft:**

| ID | Mô tả | Priority |
|---|---|---|
| FR-01 | Hệ thống phải ingest được tất cả 11 nguồn CSV vào Bronze Delta với metadata lineage | Must |
| FR-02 | Mỗi lần chạy, Bronze chỉ ingest file mới (idempotent, archive sau khi xử lý) | Must |
| FR-03 | Silver layer phải MERGE upsert theo PK, hỗ trợ incremental qua watermark `_ingested_at` | Must |
| FR-04 | DQ check tự động: schema enforcement, null PK, range constraints | Must |
| FR-05 | Gold layer phải cung cấp 4 view: customer_360, fact_orders_enriched, agg_sales, segment_rfm | Must |
| FR-06 | Toàn bộ pipeline được orchestrate qua Airflow DAG, có dependency graph rõ ràng | Must |
| FR-07 | DAG retry tự động ≥ 1 lần khi task fail | Should |
| FR-08 | Hỗ trợ trigger manual và scheduled (cron) | Should |
| FR-09 | Có notebook demo cho stakeholder truy vấn Gold | Should |
| FR-10 | Có script sinh dữ liệu test giả lập (CRM, helpdesk) | Could |

### 3.3. Non-functional Requirements

**Draft:**

| ID | Loại | Yêu cầu | Cách đo |
|---|---|---|---|
| NFR-01 | Performance | End-to-end DAG ≤ 30 phút trên dataset Olist ~100MB | Airflow log |
| NFR-02 | Freshness | Gold trễ ≤ 24h so với landing | `max(_ingested_at)` |
| NFR-03 | Reliability | DAG success rate ≥ 95% trong 30 lần chạy demo | Airflow metadata |
| NFR-04 | Scalability | Code phải migrate được sang multi-node Spark cluster mà không đổi logic | Architectural review |
| NFR-05 | Data Quality | 0 null PK ở Silver, 0 vi phạm range constraint | Exception count |
| NFR-06 | Idempotency | Rerun cùng input ra cùng output | Hash compare 2 lần chạy |
| NFR-07 | Maintainability | 10/10 Silver script tuân theo cùng pattern | Code review |
| NFR-08 | Portability | Toàn bộ stack chạy được trên Docker Compose, 1 lệnh start | `docker compose up` |

### 3.4. Success Metrics

**Draft:**

| Metric | Target | Đo ở đâu |
|---|---|---|
| Data freshness | ≤ 24h | `current_time − max(_ingested_at)` of Gold |
| Data completeness | 100% Bronze record có `_ingested_at` và `_source_file` | DQ check Silver |
| Identity match rate | ≥ 95% khách CRM map được vào ≥1 order hoặc 1 ticket | Count trong `customer_360` |
| Pipeline reliability | ≥ 95% DAG run success | Airflow metadata |
| Query SLA (Gold) | p95 ≤ 5s cho dashboard query | Notebook timer / Spark UI |
| End-to-end latency | ≤ 30 phút full DAG | Airflow `dag_run.duration` |

---

## 4. System Architecture

### 4.1. Architecture Overview

**Cần viết:** giải thích kiến trúc tổng thể, các tầng, vai trò.

**Cần vẽ — 3 sơ đồ (tham khảo PDF Hotel Booking trang 5):**

1. **Component diagram** — các service (MinIO, Spark, Airflow, Postgres, Jupyter) và quan hệ giữa chúng.
2. **Data flow diagram** — luồng data từ landing → bronze → silver → gold.
3. **ERD Gold layer** — 4 bảng và quan hệ logic.

**Draft component diagram (ASCII placeholder — cần vẽ lại bằng draw.io / Mermaid / PlantUML):**

```
        ┌───────────────────────── Docker Compose ─────────────────────────┐
        │                                                                   │
User ───┤   ┌──────────────┐         ┌──────────────────┐                   │
        │   │ MinIO UI     │         │ Airflow Web      │◀─── User          │
        │   │ :9001        │         │ :8091            │                   │
        │   └──────┬───────┘         └─────────┬────────┘                   │
        │   ┌──────▼──────────┐    ┌───────────▼──────────┐                 │
        │   │ MinIO S3-API    │◀───┤ Airflow Scheduler    │                 │
        │   │ :9000           │    │ + LocalExecutor      │                 │
        │   │ bucket olist-data│   │ spark-submit/task    │                 │
        │   │ ├ landing/      │    └───────────┬──────────┘                 │
        │   │ ├ archive/      │                │                            │
        │   │ ├ bronze/       │                ▼                            │
        │   │ ├ silver/       │      ┌──────────────────┐                   │
        │   │ └ gold/         │      │ Postgres (meta)  │                   │
        │   └─────────────────┘      │ :5432            │                   │
        │                            └──────────────────┘                   │
        │   ┌──────────────────┐                                            │
        │   │ Jupyter/Spark    │◀── EDA / Demo                              │
        │   │ :8888 :4040      │                                            │
        │   └──────────────────┘                                            │
        └───────────────────────────────────────────────────────────────────┘
```

**Cần vẽ data flow diagram bằng tool (draw.io / Mermaid), tham khảo PDF Hotel Booking Figure 4.**

**Cần vẽ ERD Gold bằng tool, tham khảo PDF Hotel Booking Section 5.1.**

### 4.2. Architectural Patterns

**Draft:**

- **Lakehouse Architecture:** kết hợp ưu điểm Data Lake (lưu trữ schema-on-read, đa định dạng) và Data Warehouse (ACID transactions, schema enforcement) thông qua Delta Lake.
- **Medallion Architecture:** phân tầng dữ liệu theo độ "chín": Bronze (raw, append-only) → Silver (cleansed, deduplicated) → Gold (business-level aggregates).
- **ELT (Extract-Load-Transform):** dữ liệu thô được load nguyên trạng vào Bronze, transform xảy ra sau đó (ngược với ETL truyền thống).
- **Watermark-based Incremental Processing:** chỉ xử lý record mới dựa trên `_ingested_at`, giảm compute redundancy.
- **Idempotent Pipeline:** mỗi task có thể chạy lại nhiều lần ra cùng kết quả nhờ MERGE upsert theo PK + archive pattern ở Bronze.
- **Orchestration as Code:** DAG được khai báo dưới dạng Python code, version-controlled trong Git.

### 4.3. Design Decisions & Trade-offs

**Cần viết:** bảng các quyết định kiến trúc và lý do.

**Draft:**

| Quyết định | Lựa chọn | Alternative đã xét | Lý do |
|---|---|---|---|
| Storage format | Delta Lake | Apache Iceberg, Apache Hudi | Spark-native, MERGE syntax sạch, mature 2026 |
| Object storage | MinIO | AWS S3, Azure Blob | Free local mirror của S3, không vendor lock-in |
| Compute engine | Apache Spark | DuckDB, Pandas, Polars | Scale-out tiềm năng, Streaming + ML khi mở rộng |
| Orchestration | Apache Airflow | Prefect, Dagster | De-facto standard, ecosystem provider lớn |
| Metadata DB | PostgreSQL | MySQL, SQLite | Mặc định của Airflow, ổn định |
| DQ framework | Inline exception | Great Expectations, Soda, Deequ | Đơn giản, ít dependency; trade-off: ít reporting |
| Schema strategy | inferSchema (Bronze) + explicit (Silver) | Explicit Bronze | Linh hoạt cho schema drift; trade-off: chậm hơn |

---

## 5. Technology Stack & Justification

### 5.1. Stack Overview

**Cần viết:** bảng technology theo từng layer (giống PDF Hotel Booking Section 4).

**Draft:**

#### 5.1.1. Storage & Format Layer

| Category | Technology | Version | Description |
|---|---|---|---|
| Object Storage | MinIO | latest | S3-compatible object storage, lưu landing/archive/bronze/silver/gold |
| Table Format | Delta Lake | 3.1.0 | ACID transactions, MERGE upsert, time travel, schema enforcement |
| File Format | Apache Parquet | 1.13 | Columnar storage, dùng làm underlying format của Delta |

#### 5.1.2. Compute Layer

| Category | Technology | Version | Description |
|---|---|---|---|
| Engine | Apache Spark | 3.5.0 | Distributed compute, batch processing |
| Language | PySpark | 3.5.0 | Python API cho Spark |
| Cloud Connector | hadoop-aws | 3.3.4 | S3A filesystem driver cho Spark |
| AWS SDK | aws-java-sdk-bundle | 1.12.262 | AWS SDK dùng cho S3A |

#### 5.1.3. Orchestration Layer

| Category | Technology | Version | Description |
|---|---|---|---|
| Workflow | Apache Airflow | 2.8.1 | DAG-based orchestration |
| Executor | LocalExecutor | – | Parallel task execution trên 1 node |
| Metadata DB | PostgreSQL | 13 | Lưu DAG runs, task instances |

#### 5.1.4. Interaction Layer

| Category | Technology | Version | Description |
|---|---|---|---|
| Notebook | Jupyter Lab | latest | EDA, demo, ad-hoc query |
| Spark UI | Built-in | 4040 | Monitor Spark job |
| MinIO Console | Built-in | 9001 | Web UI quản lý bucket |
| Airflow Web | Built-in | 8091 | Web UI quản lý DAG |

#### 5.1.5. Infrastructure Layer

| Category | Technology | Description |
|---|---|---|
| Containerization | Docker | Đóng gói từng service |
| Orchestration | Docker Compose | Multi-container deploy 1 lệnh |
| Version Control | Git + GitHub | Source code management |

### 5.2. Justification cho từng lựa chọn

**Cần viết:** giải thích vì sao chọn từng tool, so sánh với alternatives.

#### 5.2.1. Delta Lake vs Iceberg vs Hudi

**Draft:**

Khi chọn table format cho lakehouse, ba ứng cử viên hàng đầu là Delta Lake (Databricks), Apache Iceberg (Netflix/Apache), và Apache Hudi (Uber). So sánh:

| Tiêu chí | Delta Lake | Iceberg | Hudi |
|---|---|---|---|
| Engine support | Spark first-class, Trino/Flink qua connector | Đa engine bậc nhất | Spark/Flink ổn, Hive native |
| MERGE upsert syntax | Sạch nhất (`DeltaTable.forPath().merge()`) | Verbose hơn (cần Spark SQL) | Mạnh nhất cho streaming upsert |
| Time travel | Có | Có | Có (incremental query) |
| Maintenance | OPTIMIZE + Z-ORDER + VACUUM | Compaction + expire snapshots | Cleaner + compaction async |
| JAR phụ thuộc | 2 JAR (`delta-spark`, `delta-storage`) | 1 JAR (`iceberg-spark-runtime`) | 1 JAR (`hudi-spark-bundle`) |
| Maturity 2026 | Rất cao (4.x) | Cao (1.x) | Trung bình-cao (0.14+) |

**Lựa chọn: Delta Lake.** Lý do:
- Stack thuần Spark, không cần multi-engine ngay lập tức → ưu thế Iceberg (đa engine) không phát huy trong scope hiện tại.
- Use case là batch + small incremental, không phải streaming upsert intensive → Hudi không cần.
- Cú pháp MERGE ngắn nhất → 10 Silver script đồng nhất, dễ maintain.
- Hỗ trợ Spark 3.5 mature qua `delta-spark 3.1.0`.

**Trade-off:** nếu sau này cần Trino để serve cho BI dashboard, Iceberg sẽ tích hợp Trino tốt hơn. Đây là technical debt biết trước.

#### 5.2.2. MinIO vs Postgres+ClickHouse vs HDFS

**Draft:**

| Tiêu chí | MinIO (lakehouse) | Postgres+ClickHouse (DB-centric) | HDFS (Hadoop) |
|---|---|---|---|
| Tách storage/compute | Có | Không | Không |
| Mở rộng dung lượng | Add disk, no downtime | Cần resize | Cần balance |
| Tính tương thích cloud | 100% S3 API | Phải migrate | Chỉ on-prem |
| Latency point query | Chậm (Parquet scan) | Nhanh (index) | Trung bình |
| Cost ownership | Thấp (open-source) | Thấp-TB (per node) | TB (cluster) |

**Lựa chọn: MinIO.** Lý do:
- Pattern lakehouse là chuẩn industry 2026 (Databricks, Synapse, Snowflake, AWS Lake Formation đều theo hướng này).
- API 100% tương thích AWS S3 → code chạy local giống prod, zero migration cost khi lên cloud.
- Có thể serve 11 nguồn dữ liệu khác nhau cùng bucket, không cần 11 schema.

**Trade-off:** nếu use case là OLAP dashboard với latency sub-second và <1B rows, ClickHouse sẽ vượt trội. CDP của project này không yêu cầu sub-second nên không cần ClickHouse.

#### 5.2.3. Spark vs DuckDB vs Pandas

**Draft:**

| Tiêu chí | Spark | DuckDB | Pandas |
|---|---|---|---|
| Scale ngang | Distributed (TB-PB) | Single-node (GB-200GB) | Single-node (MB-low GB) |
| Delta support | Native (delta-spark) | Native từ 0.10 | Qua python-deltalake |
| Streaming | Structured Streaming | Không | Không |
| MERGE upsert | Native | Có | Phải tự code |
| Learning curve | Cao | Thấp | Thấp |
| Industry standard | Có (de-facto Big Data) | Đang nổi | Phổ biến nhưng không scale |

**Lựa chọn: Spark.** Lý do:
- Môn học yêu cầu Big Data ecosystem, Spark là chuẩn industry.
- Code có thể migrate sang multi-node cluster mà **không đổi logic** (chỉ đổi `--master`).
- API tương lai cho Streaming consume Kafka, MLlib cho ML.

**Trade-off (đáng được defend trong Q&A):** Olist dataset gốc chỉ ~100MB, DuckDB sẽ nhanh hơn Spark 10-50× ở scale này. Chọn Spark vì **giá trị scale tương lai**, không phải tối ưu cho hiện tại.

#### 5.2.4. Airflow vs Prefect vs Dagster

**Draft:**

Chọn Airflow vì:
- Chuẩn de-facto cho data orchestration từ 2015, ecosystem provider lớn nhất.
- Có sẵn `BashOperator`, `SparkSubmitOperator`, `DeltaTableOperator`.
- Documentation và community lớn, dễ tuyển người maintain.
- Prefect/Dagster modern hơn nhưng community plug-in cho Spark/Delta vẫn nhỏ hơn ở thời điểm 2026.

### 5.3. Big Data Ecosystem Mapping `(còn thiếu — cần bổ sung component)`

**Cần viết:** mapping project với các component "đầy đủ" của Big Data ecosystem để justify rubric "Proper selection".

**Draft (với mark phần thiếu):**

| Component Category | Tool đã dùng | Tool còn thiếu | Plan bổ sung |
|---|---|---|---|
| Batch Ingestion | Spark CSV reader | – | Đã đủ |
| Streaming Ingestion | – | Kafka + Spark Structured Streaming `(còn thiếu)` | Producer Python giả lập, consumer Spark → Bronze |
| Storage (Object) | MinIO | – | Đã đủ |
| Storage (Format) | Delta Lake | – | Đã đủ |
| Compute (Batch) | Spark | – | Đã đủ |
| Orchestration | Airflow | – | Đã đủ |
| Metadata | Postgres (Airflow only) | Hive Metastore / Unity Catalog `(còn thiếu)` | Register Delta tables vào metastore |
| Query Engine (Serving) | – | Trino / Presto `(còn thiếu)` | Trino container đọc Delta cho BI |
| Visualization | Jupyter notebook | Metabase / Superset `(còn thiếu)` | Connect Trino → dashboard |
| NoSQL Serving | – | MongoDB / Redis `(còn thiếu)` | Dump customer_360 ra MongoDB cho API |
| DQ Framework | Inline exception | Great Expectations / Soda Core `(còn thiếu)` | HTML report, DQ history |
| ML / Feature Store | RFM rule-based | MLlib / Feast `(còn thiếu)` | Train churn model với features từ C360 |
| Observability | – | Prometheus + Grafana `(còn thiếu)` | Export Airflow + Spark metrics |

---

## 6. Data Sources & Schema Design

### 6.1. Data Sources

**Cần viết:** mô tả 11 nguồn dữ liệu, kích thước, định dạng, schema.

**Draft:**

Dự án sử dụng bộ dữ liệu **Brazilian E-commerce Public Dataset by Olist** (Kaggle), bổ sung 2 nguồn giả lập (CRM, helpdesk) để mô phỏng môi trường multi-channel thực tế.

| # | Source | Loại | Số record (~) | File | Mô tả |
|---|---|---|---|---|---|
| 1 | olist_orders | Transaction | 99k | olist_orders_dataset.csv | Đơn hàng, status, timestamp |
| 2 | olist_customers | Master | 99k | olist_customers_dataset.csv | Khách hàng, địa lý |
| 3 | olist_order_items | Transaction | 113k | olist_order_items_dataset.csv | Chi tiết đơn |
| 4 | olist_products | Master | 33k | olist_products_dataset.csv | Sản phẩm, category |
| 5 | olist_order_payments | Transaction | 104k | olist_order_payments_dataset.csv | Thanh toán |
| 6 | olist_order_reviews | Transaction | 100k | olist_order_reviews_dataset.csv | Đánh giá |
| 7 | olist_sellers | Master | 3k | olist_sellers_dataset.csv | Người bán |
| 8 | olist_geolocation | Reference | 1M | olist_geolocation_dataset.csv | Tọa độ theo zip |
| 9 | product_category_name_translation | Reference | 71 | product_category_name_translation.csv | Dịch category |
| 10 | crm_identities (mock) | External | 99k | crm_identities.csv | Email/phone từ CRM |
| 11 | helpdesk_tickets (mock) | External | ~20k | helpdesk_tickets.csv | Ticket support |

**Tổng:** ~1.7 triệu record, kích thước raw ~110 MB.

**Cần viết thêm:** giải thích quan hệ giữa các bảng (FK), key chaining để hợp nhất identity (`customer_id` → `customer_unique_id` → `email` → `zip_code_prefix`).

### 6.2. Schema by Layer

**Cần vẽ:** 3 sơ đồ schema cho 3 tầng (Mermaid hoặc ERD diagram tool).

#### 6.2.1. Bronze Schema

**Draft:**

Bronze giữ nguyên schema CSV nguồn, **bổ sung 2 cột lineage** ở mọi bảng:

```
<all_source_columns> + _ingested_at TIMESTAMP + _source_file STRING
```

11 bảng Bronze tương ứng 11 source, partition: chưa có `(còn thiếu — đề xuất partition by date(_ingested_at) ở Future Improvements)`.

#### 6.2.2. Silver Schema

**Cần viết:** bảng chi tiết schema từng Silver table sau khi clean.

**Draft (bảng tóm tắt):**

| Silver Table | PK | Khóa MERGE | Số cột | DQ rules |
|---|---|---|---|---|
| customers | customer_id | customer_id | 7 | Not null PK, lowercase city, bỏ dấu tiếng Bồ |
| orders | order_id | (overwrite) | 10 | Filter `status='delivered'`, cast timestamp, not null PK |
| order_items | (order_id, order_item_id) | composite | 9 | `price ≥ 0`, cast double, NaN check |
| products | product_id | product_id | 11 | Lower category, fillna "unknown" |
| payments | (order_id, payment_sequential) | composite | 7 | `payment_value ≥ 0`, NaN check |
| reviews | review_id | review_id | 5 | `review_score` ∈ [1,5] |
| sellers | seller_id | seller_id | 5 | Not null PK |
| crm | customer_unique_id | customer_unique_id | 4 | Not null email |
| helpdesk | ticket_id | ticket_id | 6 | `satisfaction_rating` ∈ [1,5] |
| geolocation | zip_code_prefix | (overwrite + groupBy) | 5 | Valid coords |

#### 6.2.3. Gold Schema (ERD)

**Cần vẽ ERD chi tiết.** Tham khảo PDF Hotel Booking Section 5.

**Draft (text-form):**

```
customer_360 (PK: customer_unique_id)
├── email, customer_city, customer_state
├── total_orders, total_lifetime_value, last_purchase_date
├── average_review_score, favorite_category
├── total_support_tickets, avg_support_rating
└── lat, lng

segment_rfm (PK: customer_unique_id)  ← 1:1 customer_360
├── (kế thừa tất cả cột customer_360)
└── recency_days   [R, F=total_orders, M=total_lifetime_value]

fact_orders_enriched (PK: order_id)
├── customer_id, customer_city, customer_state
├── order_purchase_timestamp, order_delivered_customer_date
├── total_revenue, total_freight, total_items
├── review_score
├── days_to_deliver, delivery_delay_days

agg_sales_by_state_category_month (PK: composite year+month+state+category)
├── order_year, order_month
├── customer_state, product_category_name
└── total_sales, total_items_sold
```

---

## 7. Pipeline Implementation

### 7.1. Module Overview

**Cần viết:** liệt kê file/script và vai trò (giống PDF Hotel Booking Section 6).

**Draft:**

```
scripts/
├── spark_utils.py                       # Factory SparkSession
├── bronze_check.py                      # Bronze ingestion (1 script, 11 bảng)
├── silver_customers.py                  # Silver — Customers
├── silver_orders.py                     # Silver — Orders (full overwrite)
├── silver_items.py                      # Silver — Order Items
├── silver_products.py                   # Silver — Products
├── silver_payments.py                   # Silver — Payments
├── silver_reviews.py                    # Silver — Reviews
├── silver_sellers.py                    # Silver — Sellers
├── silver_crm_identities.py             # Silver — CRM
├── silver_helpdesk_tickets.py           # Silver — Helpdesk
├── silver_geolocation.py                # Silver — Geo (full overwrite + agg)
├── gold_customer_360.py                 # Gold — Customer 360
├── fact_orders_enriched.py              # Gold — Fact Orders
├── agg_sales_by_state_category_month.py # Gold — Sales Cube
├── customer_segment_rfm.py              # Gold — RFM (derived)
├── linhtinh.py                          # Mock data generator
└── landfill.py                          # Landing zone cleanup utility

dags/
└── cdp_pipeline.py                      # Airflow DAG definition
```

### 7.2. Bronze Layer Logic

**Cần viết:** mô tả chi tiết logic ingest, pseudocode, lineage.

**Draft:**

Bronze layer được triển khai trong file duy nhất `bronze_check.py`, áp dụng pattern **incremental ingestion với archive**:

```
For each table in pipeline_tables (11 bảng):
    1. List files trong s3://olist-data/landing/<folder>/ (qua paginator)
    2. If empty → skip
    3. Spark read CSV với inferSchema, từ danh sách file cụ thể
    4. withColumn("_ingested_at", current_timestamp())
       withColumn("_source_file", input_file_name())
    5. Write .mode("append") vào s3://olist-data/bronze/<table>
    6. Copy từng file sang archive/, sau đó delete khỏi landing
```

**Đặc tính:**
- **Append-only:** không bao giờ update/delete record Bronze, giữ lịch sử raw đầy đủ.
- **Idempotent:** archive ngay sau append, lần chạy sau landing rỗng → skip tự nhiên.
- **Schema-on-read:** `inferSchema=true` linh hoạt với schema drift (trade-off: chậm).

### 7.3. Silver Layer Pattern

**Cần viết:** mô tả common pattern + exception case.

**Draft:**

Tầng Silver có **common pattern** áp dụng cho 8/10 bảng:

```python
# Pseudo-code:

1. Read Bronze Delta

2. Watermark incremental:
   if SILVER exists:
       max_ts = read max(_ingested_at) from SILVER
       df_bronze = df_bronze.filter(_ingested_at > max_ts)
   if df_bronze.isEmpty():
       sys.exit(0)

3. Schema enforcement:
   check REQUIRED_COLUMNS exist, raise if missing

4. Transform & clean:
   - Cast types (string → int/double/timestamp)
   - Normalize text (lowercase, accent removal)
   - DQ check: cast NaN, null PK, range constraints

5. Write:
   if SILVER not exists:
       df_clean.write.format("delta").mode("overwrite").save(SILVER_PATH)
   else:
       DeltaTable.forPath(SILVER_PATH).alias("target").merge(
           df_clean.alias("source"),
           "target.<PK> = source.<PK>"
       ).whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
```

**Hai exception case:**

- **`silver_orders.py`:** full overwrite, filter `status='delivered'`, không có watermark. Lý do: cần re-evaluate status mỗi lần chạy vì Bronze có thể có nhiều bản ghi cho cùng order. Trade-off: tốn compute. `(còn thiếu — sẽ refactor sang incremental MERGE trong Future Improvements)`.
- **`silver_geolocation.py`:** full overwrite + `groupBy(zip).agg(avg(lat), avg(lng))` để dedupe. Lý do: Bronze có nhiều dòng cho cùng zip.

### 7.4. Gold Layer Logic

**Cần viết:** chi tiết business logic của 4 bảng Gold.

**Draft (mục cho mỗi bảng — cần viết chi tiết hơn ở báo cáo cuối):**

#### 7.4.1. `customer_360.py`

Hợp nhất 8 Silver tables thành single-view khách hàng:

```
1. Join ecommerce: orders ⨝ items ⨝ customers ⨝ products(left) ⨝ reviews(left)
2. Aggregate theo customer_unique_id:
   - total_orders, total_lifetime_value, last_purchase_date
   - average_review_score, favorite_category (mode)
3. Aggregate support theo email:
   - total_support_tickets, avg_support_rating
4. Merge cross-channel:
   ecommerce ⨝ CRM (customer_unique_id)
            ⨝ support (email)
            ⨝ geo (zip)
5. Write mode("overwrite") gold/customer_360
```

**`(còn thiếu — issue đã biết)`:** chuỗi join inner-join làm mất khách CRM không mua hàng. Sẽ fix bằng cách lấy `df_crm` làm base.

#### 7.4.2. `fact_orders_enriched.py`

Fact table cho operations monitor SLA giao hàng:

```
1. Pre-aggregate items theo order_id (tránh fan-out)
2. Join orders ⨝ items_agg ⨝ reviews(left) ⨝ customers(left)
3. Tính SLA:
   - days_to_deliver = delivered_date - purchase_date
   - delivery_delay_days = delivered_date - estimated_date
4. Write mode("overwrite")
```

#### 7.4.3. `agg_sales_by_state_category_month.py`

OLAP cube cho BI dashboard:

```
1. Join items ⨝ orders ⨝ customers ⨝ products(left)
2. groupBy(year, month, state, category).agg(sum(price), count(items))
3. Write mode("overwrite")
```

#### 7.4.4. `customer_segment_rfm.py`

Derived table cho Marketing segment:

```
1. Read customer_360 (Gold-on-Gold)
2. Compute recency_days = '2018-10-01' - last_purchase_date
3. F = total_orders, M = total_lifetime_value (kế thừa từ C360)
4. Write mode("overwrite")
```

### 7.5. Airflow DAG Structure

**Cần viết:** giải thích DAG, dependency graph.

**Cần vẽ:** DAG graph (export từ Airflow UI hoặc vẽ tay).

**Draft DAG dependency graph:**

```
                          ┌─ silver_customers ─┐
                          ├─ silver_orders ────┤
                          ├─ silver_items ─────┤
                          ├─ silver_reviews ───┤
bronze_ingestion ────────►├─ silver_products ──┤────►┌─ gold_customer_360 ──► gold_segment_rfm
                          ├─ silver_crm ───────┤     ├─ gold_fact_orders_enriched
                          ├─ silver_geolocation┤     └─ gold_agg_sales
                          ├─ silver_helpdesk ──┤
                          ├─ silver_payments ──┤
                          └─ silver_sellers ───┘
   Phase 1 (1 task)        Phase 2 (10 task ||)        Phase 3 (3 foundation || + 1 derived)
```

**Đặc tính:**
- `schedule_interval=None` (manual trigger demo, dễ chuyển thành `0 2 * * *` cho daily 2 AM).
- `catchup=False`, `retries=1`.
- Mỗi task là `BashOperator` chạy `spark-submit` với 4 JAR (Delta + S3A).

---

## 8. Optimization & Research Depth `(phần này cần đầu tư cho rubric 20%)`

### 8.1. Đã làm

**Draft:**

Các tối ưu đã được áp dụng trong pipeline hiện tại:

1. **Watermark-based incremental processing** ở 8/10 bảng Silver: chỉ filter record có `_ingested_at > max(_ingested_at) của Silver`, tránh re-process toàn bộ Bronze mỗi lần chạy. Đây là tối ưu cốt lõi của pattern Medallion.

2. **MERGE upsert thay vì overwrite** ở Silver: với 8 bảng có PK rõ ràng, dùng `DeltaTable.merge()` chỉ ghi delta thay vì rewrite toàn bộ partition.

3. **Pre-aggregate trước join** ở `fact_orders_enriched.py`: tránh fan-out duplicate khi join `items` (1-N) với `orders` bằng cách `groupBy(order_id).agg()` trước.

4. **Parallel task execution** trong DAG: 10 Silver task chạy song song trong Phase 2, 3 Gold foundation chạy song song trong Phase 3a.

5. **Early-exit khi không có data mới**: `if df.isEmpty(): sys.exit(0)` giảm overhead ghi Delta khi watermark cho 0 record.

### 8.2. Cần làm — Comparative Experiments `(còn thiếu — đây là phần quan trọng cho rubric)`

**Cần làm:** thực hiện 5 benchmark experiments dưới đây, mỗi cái lấy median 5 lần chạy, vẽ chart.

**Cần vẽ:** 5-7 bar chart so sánh (matplotlib trong notebook).

#### Experiment 1: Full overwrite vs Incremental MERGE

| Setup | Wall-clock time | Bytes written | Spark stages |
|---|---|---|---|
| `silver_orders` full overwrite (current) | `(còn thiếu — đo)` | `(còn thiếu)` | `(còn thiếu)` |
| `silver_orders` refactored incremental MERGE | `(còn thiếu)` | `(còn thiếu)` | `(còn thiếu)` |

**Cách đo:** wrap mỗi script bằng `time.time()` ở đầu/cuối, đọc Spark UI metric, lặp 5 lần lấy median.

**Hypothesis:** MERGE nhanh hơn 3-5× và write ít hơn 90% khi delta batch nhỏ so với toàn bộ table.

#### Experiment 2: Partition vs No-partition (Gold cube)

| Setup | Latency cho `WHERE year=2018 AND month=6` | Storage size | Files scanned |
|---|---|---|---|
| `gold/agg_sales` no partition (current) | `(còn thiếu)` | `(còn thiếu)` | `(còn thiếu)` |
| Partition by `(order_year, order_month)` | `(còn thiếu)` | `(còn thiếu)` | `(còn thiếu)` |

**Cách đo:** chạy query 10 lần trong notebook, lấy median; đọc số file scan từ Spark UI plan.

**Hypothesis:** partition giảm latency 10-50× với query có filter date.

#### Experiment 3: inferSchema vs Explicit schema (Bronze ingestion)

| Setup | Ingest time | Spark stages |
|---|---|---|
| `inferSchema=true` (current) | `(còn thiếu)` | `(còn thiếu)` |
| Explicit schema (StructType) | `(còn thiếu)` | `(còn thiếu)` |

**Hypothesis:** explicit schema giảm 30-50% thời gian (vì không cần Spark đọc CSV 2 lần).

#### Experiment 4: Z-ORDER vs No Z-ORDER (point lookup)

| Setup | Latency cho `WHERE customer_unique_id='X'` |
|---|---|
| `customer_360` no Z-ORDER | `(còn thiếu)` |
| Z-ORDER by `customer_unique_id` | `(còn thiếu)` |

**Cách đo:** chạy query với 100 customer_id ngẫu nhiên, lấy median latency.

#### Experiment 5: Scaling — MERGE throughput vs batch size

| Batch size (số row mới) | Wall-clock | Throughput (rec/sec) |
|---|---|---|
| 100 | `(còn thiếu)` | `(còn thiếu)` |
| 10,000 | `(còn thiếu)` | `(còn thiếu)` |
| 1,000,000 | `(còn thiếu)` | `(còn thiếu)` |

**Cách đo:** sinh delta CSV với 100/10k/1M row mới, chạy `silver_customers.py` 5 lần.

### 8.3. Skew Handling `(còn thiếu)`

**Cần viết:** giải thích nếu có skew không, và xử lý gì.

**Draft (đề xuất):**

Trong dataset Olist, skew có thể xảy ra ở:
- `customer_state`: 42% đơn từ São Paulo (SP).
- `product_category`: top 5 category chiếm 60% đơn.

Khi join `orders ⨝ customers` partition theo `customer_state`, partition SP sẽ to gấp 10× các partition khác → straggler task.

**Cách xử lý đề xuất:**
1. Bật **Adaptive Query Execution (AQE)** với `spark.sql.adaptive.skewJoin.enabled=true`.
2. **Salting**: thêm random suffix vào `customer_state` của bảng nhỏ, broadcast.
3. **Broadcast join** cho dimension table nhỏ (`products`, `sellers`, `geolocation`).

Áp dụng và benchmark `(còn thiếu)`.

### 8.4. Delta Maintenance Strategy `(còn thiếu)`

**Cần viết + làm:** giải thích OPTIMIZE, VACUUM, Z-ORDER.

**Draft (plan):**

Sau mỗi lần MERGE, Delta tạo ra nhiều file Parquet nhỏ (small file problem). Cần weekly maintenance:

```python
# Weekly DAG: medallion_maintenance
for path in silver_paths + gold_paths:
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeCompaction()  # Compact small files
    delta_table.vacuum(168)  # Retention 7 days

# Z-ORDER cho point lookup table
DeltaTable.forPath(spark, "s3a://olist-data/gold/customer_360") \
    .optimize() \
    .executeZOrderBy("customer_unique_id")
```

Benchmark before/after maintenance `(còn thiếu)`.

### 8.5. Research Depth Topics `(còn thiếu — chọn 1-2 topic để đào sâu)`

**Đề xuất:** chọn 1-2 topic dưới đây làm "research depth":

1. **SCD Type 2 cho orders:** track lịch sử thay đổi status (`delivered` → `returned`).
2. **CDC với Delta Change Data Feed (CDF):** bật `delta.enableChangeDataFeed=true` ở Silver, Gold chỉ rerun partition affected.
3. **So sánh data quality framework:** inline exception vs Great Expectations vs Soda Core — pros/cons + integration cost.
4. **Spark Structured Streaming với Kafka:** convert Bronze từ batch sang streaming, đo end-to-end latency.

---

## 9. Evaluation & Testing `(còn thiếu — quan trọng cho rubric 15%)`

### 9.1. Data Quality Testing

**Cần viết:** mô tả DQ framework hiện tại + plan upgrade.

**Draft:**

Hiện tại DQ được implement **inline** trong từng Silver script bằng `raise Exception`:

```python
null_id_count = df_cleaned.filter(col("customer_id").isNull()).count()
if null_id_count > 0:
    raise Exception(f"DQ FAIL: Found {null_id_count} rows with null customer_id.")
```

Các loại DQ check đã có:
- **Schema enforcement:** check `REQUIRED_COLUMNS` tồn tại.
- **Null PK check:** không cho phép null ở khóa chính.
- **Range check:** `review_score ∈ [1,5]`, `price ≥ 0`, valid GPS coords.
- **Cast validity check:** sau khi cast double, count NaN, raise nếu có.

**Cần làm `(còn thiếu)`:** upgrade sang Great Expectations với expectation suite riêng cho mỗi bảng, output HTML report.

### 9.2. Unit & Integration Testing `(còn thiếu)`

**Cần làm:**

1. Extract logic transform thành function pure (không phụ thuộc SparkSession ở module level):
   ```python
   def clean_customer_city(df: DataFrame) -> DataFrame:
       return df.withColumn("customer_city", lower(col("customer_city")))
   ```
2. Viết test với `pytest` + `pyspark.sql.SparkSession.builder.master("local[1]")`:
   ```python
   def test_clean_customer_city(spark):
       input_df = spark.createDataFrame([("Sao Paulo",)], ["customer_city"])
       result = clean_customer_city(input_df).collect()[0][0]
       assert result == "sao paulo"
   ```
3. Tối thiểu: 5-10 test case cho các transform function chính.
4. Integration test: chạy DAG end-to-end với 100-row sample, assert row count Gold.

### 9.3. Performance Evaluation `(còn thiếu — phải đo)`

**Cần đo + vẽ chart:**

#### 9.3.1. Latency (per task)

**Cách đo:** parse Airflow log `task_instance.duration` cho 30 DAG run.

**Cần vẽ:** bar chart latency từng task (15 task).

#### 9.3.2. Throughput (records/second)

**Cần vẽ:** stacked bar chart: throughput Bronze ingest vs Silver MERGE vs Gold overwrite.

**Cách đo:** `record_count / task_duration` cho từng task.

#### 9.3.3. End-to-end DAG duration

**Cần vẽ:** line chart DAG duration qua 30 lần chạy.

**Hypothesis:** ổn định trong khoảng 10-15 phút cho Olist dataset.

#### 9.3.4. Resource utilization

**Cần đo:** CPU/RAM peak khi 10 Silver chạy song song (qua `docker stats`).

**Cần vẽ:** time-series chart CPU/RAM trong 1 DAG run.

### 9.4. Accuracy / Correctness `(còn thiếu)`

**Cần làm:**

1. **Idempotency test:** chạy DAG 2 lần liên tiếp, hash output Gold table, assert giống nhau.
2. **Row count invariants:**
   - `count(silver/customers) ≤ count(bronze/olist_customers)`.
   - `count(gold/customer_360) ≤ count(silver/customers)`.
3. **Sum invariants:**
   - `sum(silver/order_items.price) ≈ sum(silver/payments.payment_value)` (sai số do shipping).
4. **Identity match rate:**
   - `count(distinct customer_unique_id in customer_360) / count(silver/crm) ≥ 0.95`.

### 9.5. Test Coverage Summary `(còn thiếu — sẽ điền sau khi viết test)`

| Loại test | Số test case | Coverage | Status |
|---|---|---|---|
| Unit test (transform functions) | `(còn thiếu)` | `(còn thiếu)` | `(còn thiếu)` |
| Integration test (DAG end-to-end) | `(còn thiếu)` | `(còn thiếu)` | `(còn thiếu)` |
| DQ test (inline exception) | 25+ check | – | Implemented |
| Performance benchmark | `(còn thiếu)` | – | `(còn thiếu)` |
| Idempotency test | `(còn thiếu)` | – | `(còn thiếu)` |

---

## 10. Deployment & Operations Guide

### 10.1. Prerequisites

**Draft:**

- Docker Desktop ≥ 4.20 (Windows/Mac/Linux).
- ≥ 8GB RAM cấp cho Docker.
- 4 JAR file trong `notebooks/jars/`: `delta-spark_2.12-3.1.0.jar`, `delta-storage-3.1.0.jar`, `hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar`.

### 10.2. Setup & Launch

**Draft:**

```powershell
# 1. Clone repo
git clone https://github.com/<user>/Data_Intergration_Master_Proj_2026.git
cd Data_Intergration_Master_Proj_2026

# 2. Khởi động stack
docker compose up -d --build

# 3. Verify services
docker compose ps
# Expected: minio, spark-jupyter, postgres-airflow, airflow đều "Up"

# 4. Sinh mock data (nếu cần)
python scripts\linhtinh.py

# 5. Upload CSV lên MinIO console http://localhost:9001
#    (admin/password) → bucket olist-data → landing/<folder>/<file>.csv

# 6. Mở Airflow http://localhost:8091 (admin/admin)
#    → unpause DAG medallion_end_to_end_pipeline → Trigger
```

### 10.3. Service Endpoints

**Draft:**

| Service | URL | Credentials |
|---|---|---|
| MinIO Console | http://localhost:9001 | admin / password |
| Airflow Web | http://localhost:8091 | admin / admin |
| Jupyter Lab | http://localhost:8888 | (token in `docker compose logs spark-jupyter`) |
| Spark UI | http://localhost:4040 | – |
| Postgres | localhost:5432 | airflow / airflow |

### 10.4. Verify Output

**Draft:**

1. MinIO → `olist-data/` → kiểm tra `bronze/`, `silver/`, `gold/` đã có Delta table (folder chứa `_delta_log/` + parquet files).
2. Jupyter → mở `notebooks/demo.ipynb` hoặc `EDA_gold.ipynb` → chạy query Gold table.
3. Airflow → Graph View → tất cả task xanh.

### 10.5. Troubleshooting

**Draft:**

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Airflow không lên | Postgres chưa ready | `docker compose restart airflow` |
| `ClassNotFoundException` Spark | JAR chưa mount | Check folder `notebooks/jars/` |
| DQ Exception | Data sai schema/range | Check log task trong Airflow, fix CSV gốc, rerun |
| MinIO 403 | Sai credentials | Verify `admin/password` trong code |
| Reset toàn bộ | – | `docker compose down`, xoá `minio_data/` + `postgres_data/`, `docker compose up -d --build` |

---

## 11. Challenges & Solutions

### 11.1. Technical Challenges

**Cần viết:** liệt kê 5-7 challenge thực tế đã gặp + solution.

**Draft:**

#### 11.1.1. Schema drift khi Bronze inferSchema

**Vấn đề:** Khi append batch CSV mới có cột toàn null vào Bronze, Spark infer thành `string` thay vì `int` của batch cũ → Delta throw schema mismatch.

**Giải pháp:** chuyển từ `mode("overwrite")` ban đầu sang **schema evolve** với `option("mergeSchema", "true")` cho Bronze. Trade-off: chấp nhận một số cột bị widen type. Tham khảo commit "Change the Schema Enforcement from Overwrite to Evolve".

#### 11.1.2. Bronze không idempotent khi crash giữa chừng

**Vấn đề:** Nếu Spark write thành công nhưng `copy_object` hoặc `delete_object` fail, file vẫn ở landing → lần chạy sau append trùng → Bronze phồng.

**Giải pháp đề xuất `(còn thiếu — chưa implement)`:** dùng Spark Structured Streaming với checkpoint, hoặc viết manifest file `_processed_files.txt` track đã xử lý file nào.

#### 11.1.3. Race condition giữa list / read / archive

**Vấn đề:** giữa `list_objects_v2` và `spark.read(*.csv)`, nếu có file mới upload thì bị Spark đọc nhưng không nằm trong archive list.

**Giải pháp:** thay glob `*.csv` bằng cách Spark đọc danh sách file cụ thể từ `files_to_process`. Đã fix.

#### 11.1.4. Silent data drop khi cast NaN

**Vấn đề:** `col("price").cast("double")` với giá trị non-numeric tạo NULL, sau đó `filter(price >= 0)` drop âm thầm — không có DQ alert.

**Giải pháp:** thêm DQ check đếm NULL **trước** filter, raise exception nếu cast tạo ra NULL. Đã fix ở `silver_items.py`, `silver_payments.py`.

#### 11.1.5. Geolocation duplicate zip → fan-out ở Gold

**Vấn đề:** `silver_geolocation` groupBy `(zip, city, state)` tạo zip trùng. Khi `gold_customer_360` join trên zip, một khách hàng có thể bị nhân lên 2-3 dòng.

**Giải pháp đề xuất `(còn thiếu — chưa fix)`:** groupBy chỉ theo `zip`, dùng `first(city)`/`first(state)` để dedupe.

#### 11.1.6. Customer 360 mất khách CRM không mua hàng

**Vấn đề:** chuỗi inner-join `orders ⨝ items ⨝ customers` ở `customer_360.py` loại bỏ tất cả khách CRM chưa mua delivered → DQ WARNING luôn trigger.

**Giải pháp đề xuất `(còn thiếu — chưa fix)`:** đổi base sang `df_crm` left join các view ecommerce.

#### 11.1.7. `inferSchema=true` đọc CSV 2 lần

**Vấn đề:** mỗi lần Bronze ingest, Spark scan CSV 2 lần (1 lần infer, 1 lần load).

**Giải pháp đề xuất `(còn thiếu — chưa làm)`:** define `StructType` schema cho mỗi nguồn, pass vào `spark.read.schema(my_schema)`.

### 11.2. Team / Process Challenges `(còn thiếu — tùy nhóm)`

**Cần viết:** câu chuyện thực tế của nhóm. Tham khảo PDF Hotel Booking Section 10.2.

**Draft (placeholder — nhóm điền thật):**

- **Git conflict khi nhiều người sửa Silver script song song:** giải pháp branch strategy + PR review.
- **Synchronization giữa thành viên làm Bronze và Silver:** weekly standup.
- **Skill gap về Spark/Delta:** pair programming, knowledge sharing session.
- **Timeline pressure:** agile sprint, ưu tiên MVP trước, optimization sau.

---

## 12. Future Improvements

### 12.1. Short-term (1-2 tuần)

**Draft:**

1. **Fix các bug đã biết:** A1 (geo duplicate), A2 (C360 base), A4 (idempotency Bronze).
2. **Partition Delta tables:** `silver/orders` by year, `gold/agg_sales` by year+month.
3. **OPTIMIZE + Z-ORDER + VACUUM weekly DAG.**
4. **Explicit schema cho Bronze** để giảm 30-50% ingest time.
5. **Bổ sung unit test pytest** cho transform function.

### 12.2. Medium-term (1 tháng)

**Draft:**

1. **Streaming Bronze ingest** qua Kafka + Spark Structured Streaming.
2. **NoSQL serving layer** (MongoDB hoặc Redis) cho real-time `customer_360` API.
3. **Trino + Metabase** cho BI dashboard.
4. **Great Expectations DQ framework** thay inline exception.
5. **Spark on Kubernetes** thay LocalExecutor.

### 12.3. Long-term (1 quý)

**Draft:**

1. **SCD Type 2 cho orders/customers** track lịch sử thay đổi.
2. **CDC với Delta Change Data Feed:** Gold chỉ rerun partition affected.
3. **Feast feature store** cho ML use case.
4. **Churn prediction model** với MLlib, training daily.
5. **Multi-tenant CDP** cho nhiều marketplace.
6. **Data Catalog** (Unity Catalog OSS hoặc Apache Polaris).
7. **Observability stack** (Prometheus + Grafana + ELK).

---

## 13. Conclusion

### 13.1. Project Achievements

**Cần viết:** tổng kết những gì đã làm được.

**Draft:**

Dự án đã triển khai thành công một CDP pipeline end-to-end theo kiến trúc Lakehouse Medallion, giải quyết bài toán hợp nhất dữ liệu khách hàng cross-channel cho thương mại điện tử. Các kết quả chính:

1. **Kiến trúc 3 tầng hoàn chỉnh** với 11 bảng Bronze, 10 bảng Silver, 4 bảng Gold trên Delta Lake.
2. **Pattern incremental** với watermark `_ingested_at` ở 8/10 bảng Silver.
3. **DQ framework inline** với 25+ check tự động halt pipeline khi fail.
4. **Airflow DAG** orchestrate 15 task qua 3 phase với parallelism.
5. **Stack 100% open-source** đóng gói Docker Compose, deploy 1 lệnh.
6. **Documentation đầy đủ:** README, architecture diagram, troubleshooting guide.
7. `(còn thiếu — sau khi làm benchmark)`: benchmark performance trên 5 dimension với chart so sánh.

### 13.2. Learning Outcomes

**Cần viết:** team học được gì.

**Draft:**

Qua dự án, nhóm đã có được kinh nghiệm thực tế ở các mảng:

1. **Lakehouse architecture:** hiểu khác biệt Data Lake vs Warehouse vs Lakehouse, vì sao Delta/Iceberg/Hudi quan trọng.
2. **PySpark hands-on:** DataFrame API, MERGE upsert, window function, optimizer behavior qua Spark UI.
3. **Delta Lake operations:** ACID transactions, schema evolution, time travel, OPTIMIZE/VACUUM.
4. **Workflow orchestration:** Airflow DAG, dependency graph, parallel execution, retry/backoff.
5. **Data Quality engineering:** schema enforcement, range constraint, lineage tracking.
6. **Cloud-native pattern:** S3-compatible storage, container deploy, separation of storage and compute.
7. **DevOps cơ bản:** Docker Compose, multi-service networking, volume persistence.
8. **Team collaboration:** Git branching, code review, agile sprint trên 1 dự án 4-7 thành viên.

### 13.3. Acknowledgments

**Cần viết:** cảm ơn.

**Draft (placeholder):**

Nhóm xin chân thành cảm ơn:
- **Giảng viên hướng dẫn [Tên]** đã định hướng và phản hồi xuyên suốt học kỳ.
- **Viện CNTT-TT, Trường ĐH Bách Khoa Hà Nội** đã cung cấp môi trường học tập.
- **Cộng đồng open-source:** Apache Spark, Delta Lake, MinIO, Apache Airflow.
- **Olist** đã công bố dataset thương mại điện tử Brazil trên Kaggle.
- **Các thành viên trong nhóm** đã đóng góp công sức.

---

## 14. Appendices

### A. Glossary

**Draft:**

| Term | Definition |
|---|---|
| Bronze Layer | Tầng raw data, append-only, giữ nguyên schema nguồn |
| Silver Layer | Tầng cleansed data, deduplicated, có DQ check |
| Gold Layer | Tầng business-level aggregates, phục vụ analytics |
| Delta Lake | Open table format với ACID transactions trên data lake |
| MERGE Upsert | Update record nếu PK match, Insert nếu không |
| Watermark | Thời điểm mốc để xác định data mới (`_ingested_at`) |
| DQ | Data Quality |
| DAG | Directed Acyclic Graph (workflow definition trong Airflow) |
| ELT | Extract-Load-Transform (ngược ETL) |
| SCD2 | Slowly Changing Dimension Type 2 (track history) |
| CDF | Change Data Feed (track row-level changes trong Delta) |
| RFM | Recency-Frequency-Monetary (segment model) |
| Lakehouse | Kiến trúc kết hợp Data Lake + Warehouse |
| CDP | Customer Data Platform |
| Olist | Tên sàn TMĐT Brazil cung cấp dataset |

### B. References `(còn thiếu — cần bổ sung citation)`

**Cần thêm tối thiểu 10 reference:**

1. Armbrust, M., et al. (2020). "Delta Lake: high-performance ACID table storage over cloud object stores." VLDB.
2. Inmon, B., Linstedt, D. (2014). *Data Architecture: A Primer for the Data Scientist*.
3. Olist Brazilian E-Commerce Public Dataset. https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
4. Delta Lake Documentation. https://docs.delta.io/
5. Apache Spark Documentation. https://spark.apache.org/docs/
6. Apache Airflow Documentation. https://airflow.apache.org/docs/
7. MinIO Object Storage Documentation. https://min.io/docs/
8. The Medallion Architecture. Databricks Blog.
9. Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly.
10. `(còn thiếu — thêm reference về DQ frameworks: Great Expectations, Soda)`
11. `(còn thiếu — thêm reference về Lakehouse paradigm: Armbrust et al. CIDR 2021)`

### C. Code Repository

**Draft:**

- GitHub: https://github.com/<username>/Data_Intergration_Master_Proj_2026
- Branch chính: `main`
- Branch development: `dat_dev`
- License: `(còn thiếu — chọn MIT/Apache 2.0)`

### D. Team Contribution `(còn thiếu — bảng phân công thực tế)`

**Cần viết:** bảng phân công công việc thành viên (giống PDF Hotel Booking trang 1 nhưng chi tiết hơn).

| Tên | MSSV | Đóng góp |
|---|---|---|
| `(còn thiếu)` | `(còn thiếu)` | Bronze ingestion + Airflow DAG |
| `(còn thiếu)` | `(còn thiếu)` | Silver layer (5 bảng) |
| `(còn thiếu)` | `(còn thiếu)` | Silver layer (5 bảng) + DQ framework |
| `(còn thiếu)` | `(còn thiếu)` | Gold layer + EDA notebook |
| `(còn thiếu)` | `(còn thiếu)` | Benchmark + documentation |

---

# Phụ lục — Checklist hoàn thiện báo cáo

## Trước khi nộp, kiểm tra:

### Phần đã có (chỉnh sửa, không cần viết lại):
- [x] Project Overview (Section 2) — có draft sẵn
- [x] System Analysis (Section 3) — có draft sẵn
- [x] Architecture text (Section 4.1, 4.2, 4.3) — có draft
- [x] Technology Stack table (Section 5.1) — có draft
- [x] Justification (Section 5.2) — có draft đầy đủ
- [x] Schema text (Section 6) — có draft
- [x] Pipeline logic text (Section 7) — có draft
- [x] Optimization đã làm (Section 8.1) — có draft
- [x] Deployment guide (Section 10) — có draft
- [x] Challenges technical (Section 11.1) — có draft
- [x] Future improvements (Section 12) — có draft
- [x] Conclusion (Section 13) — có draft

### Phần CẦN VẼ (dùng draw.io / Mermaid / PlantUML):
- [ ] Component diagram (Section 4.1) — kiểu PDF Hotel Booking Figure 4
- [ ] Data flow diagram (Section 4.1) — luồng Bronze → Silver → Gold
- [ ] ERD Gold layer (Section 6.2.3) — 4 bảng Gold và quan hệ
- [ ] DAG dependency graph (Section 7.5) — export từ Airflow UI
- [ ] 5-7 benchmark chart (Section 8.2) — matplotlib bar chart
- [ ] Performance chart latency per task (Section 9.3.1)
- [ ] Throughput chart (Section 9.3.2)
- [ ] DAG duration line chart (Section 9.3.3)
- [ ] Resource utilization time-series (Section 9.3.4)

### Phần CẦN LÀM (thí nghiệm + đo đạc):
- [ ] **Benchmark 1:** Full overwrite vs Incremental MERGE
- [ ] **Benchmark 2:** Partition vs No-partition
- [ ] **Benchmark 3:** inferSchema vs Explicit schema
- [ ] **Benchmark 4:** Z-ORDER vs No Z-ORDER
- [ ] **Benchmark 5:** MERGE throughput vs batch size
- [ ] **Test 1:** Idempotency test (chạy 2 lần, hash compare)
- [ ] **Test 2:** Row count invariants
- [ ] **Test 3:** Sum invariants (price ≈ payment)
- [ ] **Test 4:** Identity match rate
- [ ] **Unit test:** 5-10 pytest case cho transform function
- [ ] **Skew analysis:** đo skew + thí nghiệm AQE/salting
- [ ] **Delta maintenance:** OPTIMIZE + VACUUM benchmark
- [ ] **Choose 1-2 research depth topic** (SCD2 / CDF / GE / Streaming)

### Phần CẦN VIẾT (text mới):
- [ ] Team contribution table (Appendix D) — phân công thực tế
- [ ] Team challenges & process (Section 11.2) — story thật của nhóm
- [ ] References bổ sung (Appendix B) — tối thiểu 10 citation
- [ ] Acknowledgments (Section 13.3) — tên giảng viên thật
- [ ] Repository link (Appendix C) — GitHub URL + license

### Phần cần ĐỊNH DẠNG:
- [ ] Trang bìa với logo HUST + SoICT
- [ ] Mục lục tự động (LaTeX `\tableofcontents` hoặc Word TOC)
- [ ] Bullet/table style nhất quán
- [ ] Caption Figure + Table có số thứ tự
- [ ] Cross-reference giữa các section (`xem Section 4.1`)
- [ ] Font + spacing academic (Times New Roman 12pt, line spacing 1.15)
- [ ] Page number footer
- [ ] PDF export cuối cùng

---

# Đề xuất prioritization 7 ngày trước deadline

| Ngày | Việc | Ai làm | Output |
|---|---|---|---|
| D-7 | Vẽ 4 diagram (component, data flow, ERD, DAG) bằng draw.io | 1 người | 4 PNG + embedded |
| D-6 | Chạy 5 benchmark experiment | 1-2 người | Bảng số + chart |
| D-5 | Viết unit test + idempotency test | 1 người | pytest pass + screenshot |
| D-4 | Vẽ 5-7 chart performance | 1 người | matplotlib trong notebook |
| D-3 | Viết Section 11.2 (team) + Appendix D (contribution) | Cả nhóm | Text mới |
| D-2 | Định dạng LaTeX/Word, mục lục, cross-ref | 1 người | Draft PDF |
| D-1 | Review toàn bộ, fix typo, export PDF cuối | Cả nhóm | Final PDF |

---

**Lưu ý cuối:** Template này có 80+ trang nếu fill đầy đủ, tương đương báo cáo PDF mẫu Hotel Booking 33 trang nhưng project data engineering thường dài hơn vì có benchmark + diagram + appendix. Mục tiêu nên là **40-50 trang nội dung chất lượng** thay vì 80 trang loãng.
