# Báo cáo Pipeline Medallion (CDP — Olist E-commerce)

> Tổng hợp quá trình xử lý của từng file trong `scripts/` (bỏ qua `scripts_old/`), cách vận hành theo từng tầng dữ liệu, luồng một lần chạy Airflow, và danh sách các lỗi/vấn đề tìm thấy trong code.

---

## 1. Tổng quan kiến trúc

Hệ thống được đóng gói trong [docker-compose.yml](docker-compose.yml) gồm 4 service:

| Service | Vai trò |
|---|---|
| `minio` (port 9000/9001) | Object storage S3-compatible — đóng vai trò Data Lake, chứa các zone `landing/`, `archive/`, `bronze/`, `silver/`, `gold/` trong bucket `olist-data` |
| `spark-jupyter` (port 8888/4040) | Workspace tương tác (notebook EDA, demo) |
| `postgres-airflow` (port 5432) | Metadata DB của Airflow |
| `airflow` (port 8091) | Scheduler + Webserver, build từ [Dockerfile](Dockerfile) (Airflow 2.8.1 + PySpark 3.5 + delta-spark 3.1 + Java JRE + boto3/s3fs) |

Pipeline đi theo mô hình **Medallion** (Bronze → Silver → Gold), tất cả file sử dụng Delta Lake làm định dạng lưu trữ và dùng helper [spark_utils.py](scripts/spark_utils.py) để tạo `SparkSession` với cấu hình S3A trỏ về MinIO.

---

## 2. Cách xử lý ở từng tầng

### 2.1. Tầng Bronze — [bronze_check.py](scripts/bronze_check.py)

**Mục tiêu:** Ingest dữ liệu thô từ landing zone vào Bronze Delta, theo cơ chế **append-only**, không transform.

**Luồng xử lý:**
1. Kết nối boto3 đến MinIO `http://minio:9000`, bucket `olist-data`.
2. Duyệt qua dictionary `pipeline_tables` (11 bảng: orders, customers, items, products, payments, reviews, sellers, geolocation, translation, crm_identities, helpdesk_tickets).
3. Với mỗi bảng:
   - `list_objects_v2` quét tiền tố `landing/<folder>/` tìm file `*.csv`.
   - Nếu không có file mới → `continue` (skip bảng đó).
   - Spark đọc CSV (`header=true`, `inferSchema=true`) từ `s3a://olist-data/landing/<folder>/*.csv`.
   - **Bổ sung 2 cột lineage:** `_ingested_at` (current_timestamp) và `_source_file` (input_file_name) — đây là metadata gốc của governance, được tận dụng làm **watermark** cho Silver.
   - Ghi `.mode("append")` vào `s3a://olist-data/bronze/<table>`.
   - Sau khi append thành công, **copy file từ `landing/` sang `archive/` rồi delete file ở landing** → đảm bảo idempotent: lần chạy sau nếu landing rỗng thì skip, tránh ingest trùng.

**Đặc điểm:** Không có DQ ở Bronze — triết lý là "giữ nguyên dữ liệu thô".

### 2.2. Tầng Silver — 10 script

Tầng Silver có **pattern chung** áp dụng cho hầu hết các bảng (customers, items, products, payments, reviews, sellers, crm_identities, helpdesk_tickets):

```
1. Watermark Incremental:
   - Nếu Silver đã tồn tại → đọc max(_ingested_at) của Silver
   - Filter Bronze: chỉ giữ record có _ingested_at > watermark
   - Nếu count() == 0 → sys.exit(0) (early stop)

2. Schema enforcement + DQ:
   - Check REQUIRED_COLUMNS (raise Exception nếu thiếu → halt pipeline)
   - Check null primary key, range giá trị, kiểu data
   - Cast kiểu (price/payment_value → double, review_score → int...)

3. Ghi:
   - Nếu Silver chưa tồn tại → overwrite (initial load)
   - Nếu đã tồn tại → MERGE (Upsert) theo primary key
     · whenMatchedUpdateAll → update record cũ
     · whenNotMatchedInsertAll → insert record mới
```

**Khóa MERGE của từng bảng:**

| Script | Khóa MERGE | DQ đặc trưng |
|---|---|---|
| [silver_customers.py](scripts/silver_customers.py) | `customer_id` | Lower + bỏ dấu tiếng Bồ trong `customer_city` (hàm `translate`) |
| [silver_items.py](scripts/silver_items.py) | `order_id + order_item_id` (composite) | `price >= 0`, không null FK |
| [silver_products.py](scripts/silver_products.py) | `product_id` | Lower category, `fillna("unknown")` |
| [silver_payments.py](scripts/silver_payments.py) | `order_id + payment_sequential` | `payment_value >= 0` |
| [silver_reviews.py](scripts/silver_reviews.py) | `review_id` | `review_score` ∈ [1,5] |
| [silver_sellers.py](scripts/silver_sellers.py) | `seller_id` | Không null `seller_id` |
| [silver_crm_identities.py](scripts/silver_crm_identities.py) | `customer_unique_id` | Không null `email` |
| [silver_helpdesk_tickets.py](scripts/silver_helpdesk_tickets.py) | `ticket_id` | `satisfaction_rating` ∈ [1,5] |

**Hai trường hợp ngoại lệ (không theo pattern incremental MERGE):**

- [silver_orders.py](scripts/silver_orders.py): **Full overwrite**. Filter `order_status == "delivered"` (whitelist), cast timestamp. Không có watermark — mỗi lần chạy đọc toàn bộ Bronze và ghi đè Silver. Đây là điểm sẽ tốn compute khi data lớn (có ghi chú trong [TODO_thing.md](TODO_thing.md) về scalability/SCD2/CDC).
- [silver_geolocation.py](scripts/silver_geolocation.py): **Full overwrite + aggregation**. Vì Bronze có nhiều dòng cho cùng một zip code, script này `groupBy(zip, city, state).agg(avg(lat), avg(lng))` để normalize. DQ check tọa độ hợp lệ + cảnh báo nếu zip code vẫn span nhiều city/state.

### 2.3. Tầng Gold — 4 script

Gold là tầng phân tích, **full overwrite** mỗi lần chạy (đơn giản, không cần incremental vì đọc từ Silver đã sạch).

**[gold_customer_360.py](scripts/gold_customer_360.py) — Customer 360 đa kênh:**
- Đọc 8 bảng Silver: customers, orders, items, reviews, products, crm, helpdesk, geolocation.
- Join e-commerce flow: orders ⨝ items ⨝ customers ⨝ products (left) ⨝ reviews (left).
- Aggregate theo `customer_unique_id`: `total_orders`, `total_lifetime_value` (price+freight), `last_purchase_date`, `average_review_score`, `favorite_category` (mode).
- Aggregate hỗ trợ theo `email`: `total_support_tickets`, `avg_support_rating`.
- Merge cross-channel: ecommerce ⨝ CRM (qua `customer_unique_id`) ⨝ support (qua `email`) ⨝ geo (qua zip).
- DQ L3: cảnh báo nếu `count(customer_360) < count(crm)` → mất khách trong merge.

**[fact_orders_enriched.py](scripts/fact_orders_enriched.py) — Fact bảng đơn hàng SLA:**
- Đọc 4 Silver: orders, items, reviews, customers.
- Pre-aggregate items theo `order_id` (tránh fan-out duplicate).
- Tính 2 cột SLA: `days_to_deliver` và `delivery_delay_days` (datediff giữa giao thực tế và dự kiến).
- DQ L3: fail nếu có `order_purchase_timestamp` NULL.

**[agg_sales_by_state_category_month.py](scripts/agg_sales_by_state_category_month.py) — Sales Cube:**
- Cube 4 chiều: `year × month × customer_state × product_category`.
- Metric: `total_sales`, `total_items_sold`.

**[customer_segment_rfm.py](scripts/customer_segment_rfm.py) — RFM Segmentation:**
- Đọc từ `gold/customer_360` (Gold-on-Gold, phụ thuộc C360).
- Recency = `2018-10-01 − last_purchase_date` (mốc hard-code vì dataset Olist).
- Chấm điểm R/F/M bằng `ntile(4) over (order by ...)`.
- Phân nhóm bằng CASE WHEN: Champions / Loyal / At Risk / Hibernating / Potential.

---

## 3. Các file phụ trợ trong `scripts/`

| File | Vai trò |
|---|---|
| [spark_utils.py](scripts/spark_utils.py) | Factory tạo `SparkSession` với Delta extension + S3A config trỏ về MinIO — mọi script đều `from spark_utils import get_spark_session` |
| [linhtinh.py](scripts/linhtinh.py) | Script **sinh dữ liệu giả** (chạy local, không qua Spark): đọc `olist_customers_dataset.csv`, sinh `crm_identities.csv` (email từ id + phone random) và `helpdesk_tickets.csv` (sample 20% khách, gắn ticket/issue/rating). Đầu ra ở `generated_data/`, upload thủ công lên MinIO |
| [landfill.py](scripts/landfill.py) | Script one-shot **sắp xếp lại file trong landing zone** trên MinIO: scan các file CSV ở `landing/` root, dựa vào pattern tên file (`olist_orders` → `orders/`, ...) để di chuyển vào subfolder đúng. Dùng khi mới upload bulk |
| [things.py](scripts/things.py) | **Phiên bản cũ của `bronze_check.py`** — dùng filesystem local (`/opt/airflow/data/landing`) thay vì S3. Hiện không được DAG gọi |

---

## 4. Cách xử lý khi chạy một lần update Airflow

DAG `medallion_end_to_end_pipeline` trong [cdp_pipeline.py](dags/cdp_pipeline.py) có `schedule_interval=None` (manual trigger) và `catchup=False`. Mỗi task là một `BashOperator` chạy `spark-submit` với 4 JAR (delta-spark, delta-storage, hadoop-aws, aws-sdk-bundle) mount từ `/opt/airflow/jars`.

**Đồ thị phụ thuộc (3 phase):**

```
                              ┌─ silver_customers ─┐
                              ├─ silver_orders ────┤
                              ├─ silver_items ─────┤
                              ├─ silver_reviews ───┤
                              ├─ silver_products ──┤   ┌─ gold_customer_360 ──→ gold_segment_rfm
bronze_ingestion ──────►───── ┼─ silver_crm ───────┼──►┼─ gold_fact_orders_enriched
                              ├─ silver_geolocation┤   └─ gold_agg_sales
                              ├─ silver_helpdesk ──┤
                              ├─ silver_payments ──┤
                              └─ silver_sellers ───┘
       Phase 1 (1 task)              Phase 2 (10 task song song)         Phase 3 (3 foundation song song + 1 derived)
```

**Diễn biến chi tiết của một lần chạy:**

1. **Pre-trigger (manual):** User upload CSV mới vào `s3a://olist-data/landing/<folder>/` (qua MinIO console hoặc boto3). Nếu file chưa đúng folder, có thể chạy [landfill.py](scripts/landfill.py) trước để sắp xếp.

2. **Phase 1 — Bronze (1 task):** `bronze_ingestion` chạy `bronze_check.py`. Với mỗi bảng:
   - Quét landing → nếu rỗng thì skip (idempotent).
   - Append CSV mới vào Bronze Delta kèm `_ingested_at` (timestamp lần chạy này) và `_source_file`.
   - Di chuyển file đã xử lý sang `archive/`.

3. **Phase 2 — Silver (10 task song song):** Sau khi Bronze xong, 10 task Silver chạy đồng thời. Mỗi task:
   - Đọc max(`_ingested_at`) của Silver hiện tại → chỉ filter các record Bronze có `_ingested_at > watermark` (incremental).
   - Nếu lần Bronze vừa rồi không thêm record nào cho bảng đó → `sys.exit(0)` (task SUCCESS, không tốn ghi).
   - DQ check (schema + null + range) → raise Exception nếu fail, task FAILED → DAG dừng.
   - MERGE upsert theo PK (hoặc overwrite nếu là `orders`/`geolocation`).

4. **Phase 3a — Gold foundation (3 task song song):** Sau khi **tất cả** 10 Silver xong:
   - `gold_customer_360`, `gold_fact_orders_enriched`, `gold_agg_sales_by_state_category_month` chạy đồng thời.
   - Đọc toàn bộ Silver, join, aggregate, ghi đè (`mode("overwrite")`) lên Gold path.

5. **Phase 3b — Gold derived (1 task):** Sau khi `gold_customer_360` xong, `gold_customer_segment_rfm` mới chạy (vì nó đọc từ `gold/customer_360`).

**Đặc tính incremental & idempotent:**

- **Bronze:** Idempotent bằng cách archive file ngay sau ingest — lần chạy sau, landing rỗng = skip.
- **Silver (8/10 bảng):** Incremental thật sự bằng watermark `_ingested_at`. Có thể chạy lại nhiều lần mà không bị trùng vì MERGE on PK.
- **Silver orders & geolocation:** Mỗi lần đều full-overwrite — đây là **technical debt** ghi nhận trong TODO (cần chuyển sang SCD2 / CDC khi scale).
- **Gold:** Full-overwrite mỗi lần — đơn giản, nhưng tốn compute khi Silver lớn dần.

**Khi pipeline fail giữa chừng:** Vì mỗi script là một spark-submit độc lập, task fail sẽ dừng nhánh đó. Airflow giữ `retries=1` (`default_args`), nên sẽ retry một lần. Do MERGE trên PK là idempotent, retry an toàn. Nhưng Bronze đã di chuyển file sang archive **trước khi** Silver chạy → nếu Silver fail và bug trong logic, dữ liệu vẫn còn trong Bronze để rerun Silver, **không cần re-ingest từ landing** (đây là lợi ích lớn của tách Bronze và Silver).

---

## 5. Các lỗi / vấn đề tìm thấy trong code

### A. Lỗi rõ ràng (cần sửa sớm)

#### A1. `silver_geolocation.py` tạo zip code trùng → fan-out ở Gold
- [silver_geolocation.py:14](scripts/silver_geolocation.py#L14) groupBy theo `(zip_code_prefix, city, state)` chứ không phải chỉ theo `zip_code_prefix`.
- Code có DQ check ở dòng 32-33 in WARNING khi `total_rows != unique_zips`, tức là **biết** có khả năng zip bị trùng — nhưng vẫn ghi ra Silver.
- Hậu quả: [gold_customer_360.py:43](scripts/gold_customer_360.py#L43) join `customer_zip_code_prefix == geolocation_zip_code_prefix` sẽ **nhân đôi/nhân ba dòng khách hàng** nếu zip của họ có nhiều bản ghi geo. Tổng `total_lifetime_value` trong C360 không sai (đã agg trước), nhưng `count(customer)` sẽ phồng lên.

#### A2. `gold_customer_360.py` mất khách CRM không mua hàng nhưng DQ check sai chiều
- Chuỗi join từ [gold_customer_360.py:18-22](scripts/gold_customer_360.py#L18-L22) bắt đầu bằng `orders ⨝ items ⨝ customers` **inner join** → chỉ giữ khách có ít nhất 1 đơn `delivered`.
- Sau đó left join `df_crm` ở dòng 41 — nhưng base lúc này đã thiếu khách rồi.
- DQ check ở dòng 48 `if df_customer_360.count() < df_crm.count()` chỉ in WARNING — đây là behavior dự kiến **luôn xảy ra** với dataset Olist (CRM được sinh từ tất cả customers, nhưng nhiều khách chưa có đơn delivered). Tức là DQ này gần như là noise.
- Nếu mục tiêu CDP là "view 360 cả khách chưa mua", base nên là `df_crm` left join phần ecommerce.

#### A3. `landfill.py` dùng sai credentials và endpoint
- [landfill.py:53-57](scripts/landfill.py#L53-L57): `endpoint_url='http://localhost:9000'`, `aws_access_key_id='minioadmin'`, `aws_secret_access_key='minioadmin'`.
- [docker-compose.yml:9-10](docker-compose.yml#L9-L10): MinIO thực tế là `admin/password` và endpoint từ trong network Docker phải là `http://minio:9000`.
- Hậu quả: script này chỉ chạy được khi đã thay credentials và chỉ từ host máy thật, không chạy được trong Airflow container.

#### A4. `bronze_check.py` không idempotent khi crash giữa chừng
- [bronze_check.py:71-82](scripts/bronze_check.py#L71-L82): luồng là `write append` → `copy archive` → `delete landing`.
- Nếu Spark write thành công nhưng `copy_object` hoặc `delete_object` fail (network blip), file vẫn ở landing → lần chạy sau **append lại dữ liệu trùng** vào Bronze.
- Bronze Delta không có khóa nên downstream Silver sẽ thấy 2 batch giống hệt (chỉ khác `_ingested_at`), MERGE on PK sẽ chỉ giữ bản mới nhất — nhưng row count Bronze thì phồng vĩnh viễn.

#### A5. `bronze_check.py` race condition giữa list / read / archive
- Dòng 42-48 list file → dòng 60-63 Spark đọc `*.csv` (đọc lại bằng glob, không dùng list ở trên) → dòng 76-82 archive chỉ những key có trong `files_to_process`.
- Nếu có file mới rơi vào landing giữa step list và step read: file đó **bị append vào Bronze nhưng không archive** → lần chạy kế tiếp re-ingest.

#### A6. `bronze_check.py` giới hạn 1000 file
- [bronze_check.py:42](scripts/bronze_check.py#L42): `list_objects_v2` mặc định chỉ trả về tối đa 1000 keys. Không có loop với `ContinuationToken`.
- Hậu quả khi scale: nếu landing có >1000 file, các file dư bị **bỏ qua âm thầm** nhưng glob `*.csv` ở Spark vẫn đọc → ingest đủ, nhưng archive thiếu → loop lại tình trạng re-ingest.

#### A7. `silver_items.py` và `silver_payments.py` âm thầm drop bad data
- [silver_items.py:30-32](scripts/silver_items.py#L30-L32): `cast("double")` rồi `filter(price >= 0)`. Nếu Bronze có giá trị `"abc"`, cast thành NULL, NULL không thỏa `>= 0` → bị filter bỏ. Nhưng **không có DQ alert nào** — bug data bị nuốt mất.
- [silver_payments.py:28-29](scripts/silver_payments.py#L28-L29): cùng pattern.

#### A8. `silver_orders.py` không phát hiện thay đổi trạng thái đơn
- Bronze append-only → nếu source gửi lại order_id cũ với status mới (delivered → returned), Bronze có 2 dòng cùng order_id.
- [silver_orders.py:23](scripts/silver_orders.py#L23) filter `status == 'delivered'` rồi `mode("overwrite")` ghi đè toàn bộ Silver. **Không dedupe theo order_id** → Silver orders có thể có 2 dòng cho cùng order_id nếu Bronze có 2 batch ingest cùng order.
- DQ ở dòng 31-33 chỉ check null, không check duplicate primary key.

#### A9. `silver_orders.py` không có watermark — không scalable
- Trong khi 8 file Silver khác đã có incremental, file này đọc **toàn bộ Bronze** mỗi lần chạy → re-process lại lịch sử đơn hàng mỗi ngày.

### B. Vấn đề thiết kế / scalability

#### B1. Bronze `inferSchema=true` có thể gây schema drift
- [bronze_check.py:62](scripts/bronze_check.py#L62): mỗi batch CSV được infer schema riêng. Nếu một batch có cột `product_weight_g` toàn null, Spark infer thành `string`, batch khác infer thành `int` → Delta append sẽ throw schema mismatch.
- Có thể đã gặp lỗi này rồi, vì commit history có nói "Change the Schema Enforcement from Overwrite to Evolve".

#### B2. Gold layer ghi đè toàn bộ mỗi lần
- Cả 4 script Gold dùng `mode("overwrite")` không có `mergeSchema`/incremental. Khi Silver lớn lên (hàng chục triệu dòng), join + overwrite sẽ tốn rất nhiều compute và I/O.

#### B3. `customer_segment_rfm.py` hard-code mốc thời gian
- [customer_segment_rfm.py:11](scripts/customer_segment_rfm.py#L11): `to_date(lit("2018-10-01"))`. Một năm sau khi pipeline chạy production, "recency" sẽ vô nghĩa. Nên dùng `current_date()` hoặc Airflow `{{ ds }}`.

#### B4. `customer_segment_rfm.py` ntile sẽ vỡ khi NULL xuất hiện
- Nếu sau khi sửa C360 (mục A2) mà cho phép khách CRM không có đơn vào bảng, `last_purchase_date`/`total_orders`/`total_lifetime_value` sẽ là NULL → `ntile` xếp NULL không xác định, CASE WHEN trả về `'5. Potential / Others'` cho tất cả NULL — lẫn lộn với nhóm thật sự là Potential.

#### B5. `silver_products.py` drop nhiều cột Bronze
- [silver_products.py:24](scripts/silver_products.py#L24) chỉ select `product_id, product_category_name, _ingested_at, _source_file`. Bronze có thêm `product_name_length`, `product_photos_qty`, `product_weight_g`, dimensions... — bị bỏ. Nếu future analytics cần các cột này thì phải rerun.

#### B6. `gold_customer_360.py` aggregate `mode()` chỉ có từ Spark 3.4+
- Dockerfile đang dùng PySpark 3.5 nên ok, nhưng nếu cluster prod xài 3.3 sẽ fail. Đáng ghi chú.

#### B7. `fact_orders_enriched.py` DQ thừa
- [fact_orders_enriched.py:30-32](scripts/fact_orders_enriched.py#L30-L32): check `order_purchase_timestamp IS NULL`. Nhưng `silver_orders.py` đã filter `status == 'delivered'` (đơn delivered phải có purchase_timestamp). DQ này gần như không bao giờ trigger — không sai nhưng có thể che giấu bug Silver upstream.

### C. Vấn đề hạ tầng (`docker-compose.yml`, DAG)

#### C1. Postgres của Airflow không có volume
- [docker-compose.yml:28-35](docker-compose.yml#L28-L35): service `postgres-airflow` thiếu `volumes:` mapping. Khi container restart, **toàn bộ metadata Airflow (DAG runs, logs, users) bị mất**. Mỗi lần `docker compose down` là phải tạo lại admin user.

#### C2. Airflow `depends_on` không có healthcheck
- [docker-compose.yml:40-41](docker-compose.yml#L40-L41): chỉ `depends_on: postgres-airflow`. Airflow có thể chạy `airflow db migrate` trước khi Postgres listen → fail lần đầu, phải restart bằng tay.

#### C3. Webserver chạy daemon, container không die khi webserver chết
- [docker-compose.yml:54-60](docker-compose.yml#L54-L60): `airflow webserver -D` rồi `airflow scheduler` foreground. Nếu webserver crash sau khi start, Docker không biết, không restart.

#### C4. DAG không giới hạn parallelism
- 10 task Silver chạy song song dưới `LocalExecutor`, mỗi task là một `spark-submit` đầy đủ JVM. Trên máy 8GB RAM rất dễ OOM. Không có `pool` hay `max_active_tasks` config.

#### C5. DAG `start_date=2026-04-20` trong tương lai
- [cdp_pipeline.py:8](dags/cdp_pipeline.py#L8): trong production sẽ gây bối rối, nhưng vì `schedule_interval=None` + `catchup=False` nên thực tế không phá gì.

#### C6. JAR mount comment "comment out this line when work at home"
- [docker-compose.yml:50](docker-compose.yml#L50): cấu hình môi trường khác biệt được handle bằng comment thủ công — dễ quên, dễ commit nhầm. Nên dùng `.env` hoặc compose override.

### D. Lỗi nhỏ / dọn dẹp

#### D1. `count()` early-stop tốn full scan
- Pattern lặp lại ở 8 file Silver: `if df_bronze.count() == 0: sys.exit(0)`. `count()` ép Spark scan toàn bộ filter — nếu Bronze lớn và watermark cho 0 row mới, vẫn phải scan. Có thể dùng `.isEmpty()` (Spark 3.3+) hoặc `.limit(1).count()`.

#### D2. `silver_orders.py` dùng `mergeSchema` với `overwrite`
- [silver_orders.py:43-46](scripts/silver_orders.py#L43-L46): `.mode("overwrite").option("mergeSchema", "true")`. `mergeSchema` chỉ có tác dụng với append. Với overwrite, nên là `overwriteSchema` nếu muốn cho phép thay đổi schema. Không gây lỗi nhưng misleading.

#### D3. `things.py` là code chết
- File cũ dùng filesystem local, không còn được DAG gọi. Nên xoá hoặc move vào `scripts_old/`.

#### D4. `linhtinh.py` random không seed cho phone/email
- [linhtinh.py:31-33](scripts/linhtinh.py#L31-L33): `np.random.randint` không seed → mỗi lần chạy sinh phone khác → nếu rerun để bổ sung dữ liệu, không reproducible.

#### D5. DAG có lệnh tạo admin `|| true`
- [docker-compose.yml:57](docker-compose.yml#L57): che lỗi tạo user. OK cho demo, nhưng cũng che lỗi DB connect thật.

#### D6. Boto3 client tạo lại trong mỗi script
- `bronze_check.py` tạo client với credentials hard-code; `landfill.py` cũng vậy nhưng khác giá trị. Không bị bug ngay nhưng inconsistency → đã dẫn đến A3.

---

## 6. Tóm tắt mức độ ưu tiên

| Ưu tiên | Mục | Tác động |
|---|---|---|
| **Cao** | A1, A2, A3, A4 | Sai dữ liệu hoặc không chạy được |
| **Cao** | C1 | Mất state Airflow mỗi lần restart |
| **Trung** | A5, A6, A7, A8, A9 | Lỗi ngầm khi scale hoặc khi source thay đổi |
| **Trung** | B1, B2, B3 | Đã có trong [TODO_thing.md](TODO_thing.md), là technical debt |
| **Thấp** | B4, B5, B6, B7, C2-C6, D* | Cải thiện chất lượng / dọn dẹp |
