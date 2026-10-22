# NammaMart Pipelines: Submission

## Part 0 – Data profiling and issues log

The full log is `issues_log.csv` (239 issue rows, one per bad value, with the seven required columns:
file, row_key, column, problem, quality_dimension, proposed_rule, severity). Below is the same log
summarised to one line per problem type (32 types, all five files, all six quality dimensions).

| File | Column | Problem | Dimension | Proposed rule | Severity | Rows | Example key |
|---|---|---|---|---|---|---|---|
| customers.csv | city | city is missing | Completeness | city should be present | Warning | 6 | C00355 |
| customers.csv | customer_id | customer_id appears more than once | Uniqueness | one row per customer_id | Critical | 10 | C00103 |
| customers.csv | email | email missing or badly formatted | Validity | valid email format | Warning | 12 | C00337 |
| customers.csv | loyalty_tier | loyalty_tier has inconsistent casing | Consistency | standardise to Title case | Warning | 8 | C00199 |
| customers.csv | phone | phone is not 10 digits | Validity | 10 digits | Warning | 7 | C00151 |
| customers.csv | pincode | pincode is not 6 digits | Validity | 6 digits | Warning | 3 | C00132 |
| customers.csv | signup_date | signup_date is in the future | Timeliness | signup_date <= today | Critical | 3 | C00400 |
| orders.csv | customer_id | customer_id is missing | Completeness | customer_id must not be null | Critical | 18 | ORD002255 |
| orders.csv | delivery_minutes | DELIVERED order has no delivery_minutes | Completeness | required when DELIVERED | Critical | 8 | ORD001554 |
| orders.csv | delivery_minutes | delivery_minutes impossibly large | Accuracy | delivery_minutes < 120 | Critical | 6 | ORD001037 |
| orders.csv | discount_pct | discount outside 0-100 | Validity | 0 <= discount_pct <= 100 | Critical | 3 | ORD002103 |
| orders.csv | order_amount | order_amount does not match the formula | Accuracy | recompute and compare | Critical | 18 | ORD002103 |
| orders.csv | order_id | order_id appears more than once | Uniqueness | keep one row per order_id | Critical | 24 | ORD001462 |
| orders.csv | order_status | status has wrong casing | Validity | standardise to upper case | Warning | 2 | ORD001066 |
| orders.csv | order_status | status not in allowed list | Validity | status in DELIVERED/CANCELLED/RETURNED | Critical | 3 | ORD000500 |
| orders.csv | order_ts | order_ts is in the future | Timeliness | order_ts <= now | Critical | 7 | ORD002119 |
| orders.csv | order_ts | order_ts missing or not a valid timestamp | Validity | order_ts must parse | Critical | 6 | ORD000403 |
| orders.csv | payment_mode | payment_mode not exactly an allowed value (casing/spacing/typo) | Validity | map known variants; reject unknown | Warning | 10 | ORD001812 |
| orders.csv | product_id | product_id not found in products | Consistency | FK must exist | Critical | 9 | ORD000986 |
| orders.csv | quantity | quantity zero or negative | Accuracy | quantity > 0 | Critical | 10 | ORD000469 |
| orders.csv | store_id | store_id not found in stores | Consistency | FK must exist | Critical | 5 | ORD001799 |
| payments.csv | amount_paid | SUCCESS payment differs from order_amount | Consistency | SUCCESS amount == order amount | Critical | 14 | PAY0001568 |
| payments.csv | amount_paid | negative amount_paid | Accuracy | amount_paid >= 0 | Critical | 6 | PAY0001568 |
| payments.csv | order_id | payment refers to an order that does not exist | Consistency | FK must exist | Critical | 8 | PAY0002219 |
| payments.csv | payment_id | payment_id appears more than once | Uniqueness | one row per payment_id | Critical | 8 | PAY0001651 |
| payments.csv | payment_mode | payment_mode not exactly an allowed value | Validity | map known variants | Warning | 10 | PAY0002188 |
| products.csv | category | category is missing | Completeness | category not null | Critical | 3 | P0051 |
| products.csv | cost_price | cost_price is higher than mrp | Accuracy | cost_price <= mrp | Critical | 5 | P0019 |
| products.csv | mrp | mrp is zero or negative | Accuracy | mrp > 0 | Critical | 1 | P0109 |
| products.csv | product_id | product_id appears more than once | Uniqueness | one row per product_id | Critical | 4 | P0030 |
| stores.csv | capacity_orders_per_day | capacity is zero | Accuracy | capacity > 0 | Warning | 1 | S12 |
| stores.csv | zone | zone is missing | Completeness | zone not null | Critical | 1 | S07 |

Notes:
- Duplicate counts here include every copy (for example 24 rows for 12 repeated order ids). The
  pipelines keep the first copy and quarantine only the extras, so their counts are lower (12).
- Rules I tested and found clean: every non-COD payment has a gateway reference.
- Casing and spacing problems (status, payment mode, loyalty tier) are fixed in standardise.
  Values that cannot be fixed safely (a bad phone, a missing customer) are quarantined.

### Questions for the client (Sameer)

1. **Repeated order ids: keep the first row or the latest?**
   12 order ids appear twice in `orders.csv`, and some pairs differ in status or amount.
   *Assumption used:* keep the first row and quarantine the extra. If the latest row is the
   true state, revenue and order status would change for those orders.

2. **Is a zero or negative quantity a return, or a typo?**
   10 orders have a quantity of zero or below.
   *Assumption used:* a typo or bad entry, so the order is quarantined and excluded from
   revenue. If these are returns, they should be reported as negative revenue instead.

3. **If a customer, product or store record is rejected, should its orders still count?**
   Today a rejected master record blocks every order that points to it. This held back 274
   delivered orders (about ₹1.56 lakh) from revenue, even though the order rows themselves look fine.
   *Assumption used:* hold them back and report them, because we cannot trust a join to a record we
   rejected. A softer rule (load the order, flag it) may be better for finance.

The other seven open questions are in the project notes.

## Part A – ETL pipeline

`dags/nammamart_etl_Zubair.py`: Extract → Validate → Transform → Load, then reconcile, report and audit.

- **Extract:** five tasks read the five source files in parallel. Each copies its file into the run's own folder and logs a SHA-256 hash, so a run keeps a frozen copy of what it processed.
- **Validate:** 33 checks (all six quality dimensions). Masters (customers, products, stores) are validated first; orders are then checked against the masters that passed. Bad rows are quarantined with a failure reason.
- **Transform:** standardise values, mask email and phone, and build the star schema.
- **Load:** `etl_fact_orders` with `etl_dim_customer`, `etl_dim_product` and `etl_dim_store`, in one transaction. The load is idempotent: re-running does not change the row counts.
- **After the load:** the October increment is applied when a file exists for the run date, payments are reconciled against orders, reports are built, and a Slack alert plus a `pipeline_runs` audit row are written.

## Part B – ELT pipeline
### One copy of the order-cleaning rules

The increment merge used to have its own copy of the orders SQL (`stg_orders_delta.sql`), identical to `stg_orders.sql` except for the table name and a filter on the source file. A rule change would have needed two edits, and a missed one would make the base load and the daily increment judge the same order differently.

Now `run_merge` builds the delta query from `stg_orders.sql` at run time, swapping only those two pieces. It stops with an error if `stg_orders.sql` changes shape, so the link can't break silently. The old file was removed. Re-test: 160 increment rows, 160 updated, 0 inserted, `stg_orders` 2,550 rows, identical to before.

Remaining duplication: the same rules also exist in Python for ETL (`include/quality.py`). That is inherent to the two approaches, and it is a point for Part G (ETL vs ELT).

## Part C: Scheduling

**Cron expression:** `0 9 * * 1-6`. The fields are minute 0, hour 9, any day of the month, any month, Monday to Saturday.

**Timezone:** both DAGs use `start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata")`, so Airflow reads the 9 as 9:00 AM IST.

**Time shown in UTC:** 03:30 UTC (IST is UTC+5:30). My Airflow page showed the next run as 2026-10-21 09:00:00, which means the UI was displaying IST; in UTC it would read 03:30.

**If the start_date had been left in UTC:** Airflow would read the 9 as 9:00 UTC, which is 14:30 IST. That is five hours after the 9:30 stand-up, so the SLA would be missed.

**If the laptop or Docker is off at 9 AM:** nothing runs, because the scheduler lives inside Docker on the laptop. With `catchup=False`, Airflow does not create a run for every missed day, only for the latest interval (Airflow docs). The missed day's report is late or missing until someone triggers it. In production the DAGs should run on an always-on server with an alert for a missing run.

**Why `catchup=False`:** the start_date (1 October) is in the past, so `catchup=True` would start about 17 runs at once on the first un-pause. Each run reads the whole current CSV files, so the extra runs add no new information, only load and noise in pipeline_runs, dq_results and Slack. The loads are idempotent, so duplicates would not corrupt revenue, but the waste and noise would be real.

**Retries and timeout:** `retries=2`, `retry_delay=5 minutes`, `execution_timeout=30 minutes`. A temporary failure (a file not yet there, a database lock) gets two more tries well before 9:30, and the timeout stops a stuck task from hiding a missed SLA.

### Daily increment pick-up (both scheduled DAGs)

The 9:00 AM run now handles the day's increment file by itself.

- A step sits in each scheduled DAG: ELT `merge_increment` (between `stg_orders` and the marts),
  ETL `apply_increment` (between `load_warehouse` and `reconcile_payments`).
- The file name is built from the run date in IST: `orders_<YYYY-MM-DD>_increment.csv`.
  A DAG parameter `increment_date` overrides it, which is how I tested a past date.
- If no file exists for that day, the step logs `no increment file for <date> - skipping`
  and the run continues normally. Most days will look like this.
- Tested both ways. No file (21 Oct): step skipped, run green.
  `increment_date = 2026-10-01`: ELT merged 160 rows (160 updated, 0 inserted);
  ETL passed 140, quarantined 20 (140 updated, 0 inserted). Fact table stayed at 2,146 rows,
  revenue 1,051,936.00, and the 9 corrections stayed RETURNED. Re-running is safe.
- The ETL increment writes its quality results under a separate run id (`<run_id>__increment`),
  because the increment code deletes `dq_results` by run id and would otherwise erase the main
  run's 35 checks.
- Known limits: the increment has no payments file, so its orders cannot be reconciled
  (reported as such). A missing file on a day that should have one is not an error today:
  I'd add a freshness alert for it.

## Part D – Data quality results: ETL vs ELT

Both pipelines write the same 33 checks to `dq_results` on every run
(ETL: `warehouse_Zubair.duckdb`, ELT: `warehouse_Zubair_elt.duckdb`).

The scorecards are not identical. Every difference has one of two causes:

1. **Dedupe timing.** ETL validates the raw rows (orders 2,412, customers 405).
   ELT validates staging, which is already deduplicated (orders 2,400, customers 400).
   Only the uniqueness checks read raw rows, so they match exactly
   (e.g. order_id_unique: 12 failed in both).
   This is why `product_exists` is 135 in ETL and 132 in ELT on the same base file.
2. **Scope.** ELT staging also holds the increment file (2,550 orders),
   so `customer_exists`, `product_exists` and `store_exists` show extra failures
   from the increment rows (+1, +13, +5).

Checks whose failed counts match exactly: amount_matches_formula (18),
customer_id_not_null (18), delivered_has_minutes (8), phone_10_digits (7),
customer_exists and store_exists on the base file (18, 181).

### Volume and freshness checks (both pipelines, Warning severity)

Both pipelines write 35 checks per run to `dq_results`: the 33 rule checks plus two Warning checks.

| Check | Dimension | Fires when | Result on normal run |
|---|---|---|---|
| volume_drop_over_90pct | Completeness | orders rows this run are under 10% of the previous run of the same DAG | PASS (2,412 rows) |
| latest_order_older_than_1_day | Timeliness | newest clean order is more than 1 day old | WARN (newest clean order: 2026-10-01 22:33) |

Both are Warnings, not Critical: a quiet day or a small file should alert people, not block the load.

**Lesson learned: measure freshness on clean rows only.** My first ELT version took the newest
timestamp over all staging rows. One bad row dated 2027-01-01 (already flagged by
`order_ts_not_future`) made the data look brand new, and the check passed when it should have warned.
The fix was to filter to rows with no `failure_reason`. ETL does not have this problem
because its fact table only ever contains clean rows.

**Lesson learned: verify the scorecard after every code change.** After I added the
fact-table guard to `load.py`, the two `dq_results` lines were lost, and ETL silently stopped
saving its scorecard for four runs. I found it by comparing `dq_results` run ids against
`pipeline_runs`. A run that is green in Airflow is not proof that it wrote its evidence.

## Business questions: SQL and results (both pipelines)

The full SQL and results for all six questions, for ETL and for ELT, are in
`business_answers.md` (generated by `include/business_answers.py`). The CSV results are in
`business_answers/`.

| Question | ETL source | ELT source | ETL vs ELT |
|---|---|---|---|
| Q1 Daily net revenue by zone and store | `etl_daily_zone_revenue` | `mart_daily_store_kpis` | Match (428 rows) |
| Q2 Revenue and margin by category | `etl_category_margin` | `mart_category_margin` | Match (6 rows) |
| Q3 Delivery time and % within 15 min | `etl_fact_orders` + `etl_dim_store` | `mart_delivery_sla` | Match (14 stores) |
| Q4a Revenue by payment mode | `etl_fact_orders` | `stg_orders` (clean, delivered) | Match |
| Q4b Amount not reconciled with gateway | `etl_payment_reconciliation` | `mart_payment_reconciliation` | Match (6 reasons) |
| Q5 Top 10 customers (no email/phone) | `etl_fact_orders` + `etl_dim_customer` | `mart_top_customers` | Match |
| Q6 Rows rejected by file and reason | `etl_rejected_rows` | `mart_rejected_rows` | Differs, explained below |

Total delivered revenue is 1,051,936.00 in both pipelines.

**Why Q6 differs (459 reason-lines in ETL, 452 in ELT):**
- ETL counts the extra copies of repeated keys (order 12, customer 5, payment 4, product 2 = 23),
  because it validates before removing duplicates. ELT removes duplicates in staging first,
  so it never counts them.
- ELT also includes the increment file, which adds rejected orders (customer 1, product 10 net, store 5).
- 459 − 23 + 16 = 452.

Q5 shows the customer id and loyalty tier only, so no email or phone appears in the answer.

  

## Part E – Data governance

### 1. Ownership
| Dataset | Owner (accountable) | Why |
|---|---|---|
| orders | Store operations head | They run the stores and know what a normal order looks like |
| payments | Finance controller | Payment amounts feed the books |
| customers | Customer / CRM lead | They hold the consent and the customer relationship |
| products | Category / merchandising lead | They set mrp and cost_price |
| stores | Operations head | They open, close and staff stores |
| warehouse tables (raw, staging, marts, `dq_results`, `pipeline_runs`) | Data engineering | They build and run the pipelines |

A change to a quality rule, such as the 120-minute delivery limit, is proposed by data engineering, **approved by the data owner** (store operations for delivery time), and recorded as a reviewed code change. Finance is told if the change moves revenue.

### 2. Data dictionary

#### etl_fact_orders

| Column | Type | Meaning | Source | Allowed values | Personal data |
|---|---|---|---|---|---|
| order_id | VARCHAR | Unique order identifier | orders.order_id | ORDnnnnnn, unique | No |
| order_ts | TIMESTAMP | When the order was placed | orders.order_ts | valid timestamp, not in the future | No |
| customer_id | VARCHAR | Customer who ordered (key to etl_dim_customer) | orders.customer_id | must exist in customers | No |
| store_id | VARCHAR | Store that fulfilled it (key to etl_dim_store) | orders.store_id | must exist in stores | No |
| product_id | VARCHAR | Product ordered (key to etl_dim_product) | orders.product_id | must exist in products | No |
| quantity | BIGINT | Units ordered | orders.quantity | greater than 0 | No |
| unit_price | DOUBLE | Price per unit before discount | orders.unit_price | number | No |
| discount_pct | BIGINT | Discount percentage | orders.discount_pct | 0 to 100 | No |
| order_amount | DOUBLE | Amount after discount as supplied | orders.order_amount | equals quantity x unit_price x (1 - discount/100) | No |
| payment_mode | VARCHAR | How the customer paid | orders.payment_mode | UPI, Card, Cash on Delivery, Wallet | No |
| order_status | VARCHAR | Order outcome | orders.order_status | DELIVERED, CANCELLED, RETURNED; only DELIVERED counts as revenue | No |
| channel | VARCHAR | Where the order came from | orders.channel | App, Web | No |
| delivery_minutes | DOUBLE | Minutes from order to delivery | orders.delivery_minutes | required when DELIVERED; below 120 | No |
| order_date | DATE | Date part of order_ts | derived | date | No |
| net_revenue | DOUBLE | Revenue after discount; reports count only DELIVERED | derived from quantity, unit_price, discount_pct | number | No |
| gross_margin | DOUBLE | Net revenue minus quantity x cost_price | derived | number | No |
| within_15_min | BOOLEAN | True if delivered within 15 minutes | derived from delivery_minutes | true / false | No |
| source_file | VARCHAR | Which file this row came from (orders.csv or an increment) | added by pipeline | file name | No |

#### etl_dim_customer

| Column | Type | Meaning | Source | Allowed values | Personal data |
|---|---|---|---|---|---|
| customer_id | VARCHAR | Unique customer identifier | customers.customer_id | Cnnnnn, unique | Pseudonymous id |
| full_name | VARCHAR | Customer name | customers.full_name | text | Yes |
| city | VARCHAR | City | customers.city | text | Yes (location) |
| area | VARCHAR | Neighbourhood | customers.area | text | Yes (location) |
| pincode | VARCHAR | Postal code | customers.pincode | 6 digits | Yes (location) |
| signup_date | DATE | When the customer joined | customers.signup_date | date, not in the future | No |
| loyalty_tier | VARCHAR | Loyalty programme level | customers.loyalty_tier | Bronze, Silver, Gold, Platinum | No |
| is_active | VARCHAR | Whether the account is active | customers.is_active | Y, N | No |
| email_masked | VARCHAR | Email with the middle hidden (pr***@example.com) | customers.email, masked | masked text or INVALID | Masked |
| phone_masked | VARCHAR | Phone with the middle hidden (98******10) | customers.phone, masked | masked text or INVALID | Masked |

#### etl_dim_product

| Column | Type | Meaning | Source | Allowed values | Personal data |
|---|---|---|---|---|---|
| product_id | VARCHAR | Unique product identifier | products.product_id | Pnnnn, unique | No |
| product_name | VARCHAR | Product name | products.product_name | text | No |
| category | VARCHAR | Product category | products.category | required | No |
| sub_category | VARCHAR | Product sub-category | products.sub_category | text | No |
| brand | VARCHAR | Brand | products.brand | text | No |
| mrp | DOUBLE | Maximum retail price | products.mrp | greater than 0 | No |
| cost_price | DOUBLE | What NammaMart pays | products.cost_price | not above mrp | No |
| unit | VARCHAR | Pack size or unit | products.unit | text | No |
| is_perishable | BOOLEAN | Whether it spoils | products.is_perishable | true / false | No |
| launch_date | DATE | When the product was launched | products.launch_date | date | No |

#### etl_dim_store

| Column | Type | Meaning | Source | Allowed values | Personal data |
|---|---|---|---|---|---|
| store_id | VARCHAR | Unique store identifier | stores.store_id | Snn, unique | No |
| store_name | VARCHAR | Store name | stores.store_name | text | No |
| zone | VARCHAR | Delivery zone | stores.zone | East, North, South, West (required) | No |
| area | VARCHAR | Neighbourhood | stores.area | text | No |
| pincode | BIGINT | Postal code | stores.pincode | 6 digits | No |
| open_date | DATE | When the store opened | stores.open_date | date | No |
| manager_name | VARCHAR | Store manager | stores.manager_name | text | Yes (employee) |
| capacity_orders_per_day | BIGINT | Orders the store can handle per day | stores.capacity_orders_per_day | greater than 0 | No |
Personal data: `full_name`, `city`, `area` and `pincode` (customers), and `manager_name` (store manager, an employee) are personal data. `email_masked` and `phone_masked` are masked at transform time. `customer_id` is a pseudonymous identifier: it identifies a person only through a lookup.

### 3. Lineage
I traced the biggest number on the daily store mart: store S02, 19 September, revenue 9,732.00 from
13 delivered orders (the same figure in both pipelines).
- **ELT:** every staging row carries `_source_file` and `_run_id`, and the matching raw rows keep
  `_loaded_at`. I can go from the mart row to the 13 orders, then to the exact file and run they came from
  (all `orders.csv`, scheduled run of 21 Oct). Pure SQL, no code to read.
- **ETL:** the fact table has `source_file` but no run id, so I reach the file but not the run unless
  I open the run's frozen input folder. It works but takes more steps.
- **Easier:** ELT, because the raw layer keeps every version with its run id and nothing is overwritten.

### 4. Personal data
- **Personal data in this dataset:** customers: `full_name`, `email`, `phone`, `city`, `area`, `pincode` (location). Stores: `manager_name` (an employee). `customer_id` is a pseudonymous identifier: it points to a person only through a lookup.
- **How I masked it:** email becomes `pr***@example.com`, phone becomes `98******10`, and an unusable value becomes `INVALID`. Masking happens in the transform step (ETL) and in staging SQL (ELT), so the dimension and marts hold only masked values. Questions such as the top-customers report show the id and tier only.
- **Why mask instead of delete:** analysts still need to join on the customer, spot duplicates and see data-quality problems. A masked value keeps the shape without exposing the person. Deleting the columns would break that and hide bad data.
- **Law:** the Digital Personal Data Protection Act, 2023 (DPDP Act). NammaMart is a Data Fiduciary. It expects: consent and a stated purpose, collecting only what is needed (data minimisation), keeping data accurate, erasing it when the purpose ends, reasonable security safeguards, breach notification to the Data Protection Board and to the people affected, and honouring requests to access, correct or erase data. Penalties can be very large.
- **Gap I accept:** `full_name`, `city`, `area` and `pincode` are still in the dimension unmasked. A stricter design would drop or generalise them.

### 5. Access control
| Layer | Who may read | Notes |
|---|---|---|
| Raw | Data engineering only | Holds unmasked customer data and every version of every row |
| Raw `customers` (unmasked phone) | Nobody by default. Data engineering on-call, only to fix a data problem, with the access logged | There is no business need to see a raw phone number in the warehouse |
| Staging | Data engineering, and analysts on the non-personal columns | |
| Marts | Business users, including finance | Masked and aggregated only |
| Quarantine | Data engineering and the data stewards | Contains the rejected rows themselves |

### 6. Retention
- **Raw:** keep orders and payments for the period finance and legal require (statutory record-keeping, to be confirmed by finance). Raw customers with unmasked phone numbers should be kept for the shortest period that supports re-processing, for example 90 days, then masked or deleted.
- **Quarantine files and run work folders:** keep 30 to 90 days, long enough for the client to fix the source, then delete. Both hold row-level data.
- **Masked customer data:** keep while the customer is active and the purpose continues.
- **After that:** purge by a scheduled job, or aggregate and anonymise if the history is still needed. An erasure request from a customer must reach the raw layer as well.
- **Today:** nothing in my setup deletes anything automatically. Raw grows with every run (one copy of each row per run). This is a known gap.

### 7. Secrets
- The Slack webhook is read by `notify.py` through `get_var("SLACK_WEBHOOK_URL")` from the Airflow environment (set in `.env`, which is in `.gitignore`). The optional AI keys are read the same way, from Airflow Variables. None is in a DAG file or in the repository, and I scanned the code to confirm it.
- **Why they must never be in the DAG file or on GitHub:** Git history is permanent, so a pushed key stays readable after deletion. Anyone holding the webhook can post into the company's Slack as the pipeline. A leaked key means rotating it and checking what it was used for.
- Also: the repository's `.gitignore` excludes `include/work/`, `include/quarantine/` and the DuckDB files, because they hold row-level data, including unmasked customer rows.

### 8. Audit of a rejected order
Example: ORD000677 (DELIVERED, quantity 1 × 292.0 at 10% off, amount 394.2; the formula gives 262.8).
- **ELT:** `raw_orders` shows it exactly as received, once per run. `stg_orders` shows
  `failure_reason = amount_matches_formula` and `dq_status = FAIL`. It has 0 rows with `dq_status = PASS`,
  and the marts read only PASS rows, so it is not in revenue.
- **ETL:** `etl_rejected_rows` has the order id with the same reason. `etl_fact_orders` has 0 rows for it,
  and the original line is in the source CSV and in the run's frozen input copy.

### 9. Change history (DELIVERED on 3 Sept, RETURNED on 1 Oct)
Nine orders changed (ORD000017, 271, 332, 337, 584, 708, 1025, 1067, 1990), worth 3,662.70.
- **September revenue after the increment:** 982,401.20 in both pipelines (was 986,063.90 before).
- **ELT** keeps both versions: raw holds the DELIVERED rows from `orders.csv` and the RETURNED rows
  from the increment file; only staging shows the latest.
- **ETL** replaces the fact row, so the old DELIVERED version survives only in the source file and old run folders.
- **What finance wants:** both views. "As reported on 30 Sept" for closed books, and "current" for the
  true picture. ELT can produce the first by filtering raw on `_loaded_at`. In ETL I would add a
  history table (append the old row with a valid-from and valid-to date before replacing it).

  # Data dictionary: ETL star schema

## Part F – Observability tests

Run audit: both pipelines write one row per run to `pipeline_runs` (see Part F note earlier).
Format below: Expected → Observed → Where I saw it → What I would change.

### Run audit (`pipeline_runs`)

Both pipelines write one row per run to `pipeline_runs` with the same eight columns
(run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded,
rows_quarantined, status). Each warehouse holds its own copy.

- ETL builds the row from task results (XCom). 
- ELT builds it by counting in the warehouse, because the work happens in SQL.
  The audit task uses `trigger_rule="all_done"`, so a failed run is still recorded
  (status FAILED, with the failed task names in the log).

Same run, same data: rows_extracted is 5,212 in both.

Differences to be aware of when comparing the two tables:
- rows_quarantined: ETL 428 vs ELT 424. ETL counts before dedupe, ELT after.
- rows_loaded: ETL 2,146 counts fact_orders rows only. ELT 4,915 counts all clean
  rows across the five staging tables. They are not comparable.

  ### Frozen inputs per run (ETL)

Before: `extract` only counted rows, and `validate` re-read the source folder. If a CSV changed between the two tasks, one run could validate different data from what it extracted.

Now: `extract` copies each source file into the run's own folder (`include/work/<run_id>/input/`) and logs a short SHA-256 hash. `validate` reads only that copy. Example from a run: `orders: read 2412 rows, 13 columns, sha256 c4e7a1905613`.

What this gives me:
- Reproducibility: a run can be re-examined later with the exact files it used.
- Evidence: the hash proves which version of the file a run processed.
- Re-test: row counts and `pipeline_runs` were identical to before (5,212 extracted, 2,146 loaded, 428 quarantined).

Not covered yet: the reconcile step still reads `payments.csv` and `orders.csv` from the source folder. The ELT side loads straight into raw tables stamped with `_run_id` and `_source_file`, which is its own copy of the data.

### 1. Happy path
- **Expected:** ETL reads and validates in parallel; ELT runs as one chain.
- **Observed:** ETL took 21.4 s (19 tasks). The five extracts ran in parallel, then four validations in parallel, then `validate_orders` (it waits for the clean masters). Slowest task: `load_warehouse` (2.6 s). Rows in / quarantined: customers 405/8, orders 2,412/396, payments 2,228/12, products 152/10, stores 15/1 (427 in total). ELT took 27 s with every task in a line, because DuckDB allows one writer.
- **Where:** Gantt screenshots (`exp1_etl_gantt.png`, `exp1_elt_gantt.png`), `pipeline_runs`, `etl_rejected_rows`.
- **Change:** `rows_loaded` in the ETL audit counts only the main load (2,016) and misses the 131 rows added by the increment step; I would add them.

### 2. Re-run (idempotency)
- **Expected:** ETL fact count unchanged; ELT raw grows and staging stays put.
- **Observed:** ETL stayed at 2,147 rows / 1,052,922.85. ELT `raw_orders` grew from 51,452 to 53,864 (+2,412, one more copy of `orders.csv`) while `stg_orders` stayed at 2,550 rows (2,147 PASS). Raw growth is correct: it is append-only history.
- **Surprise:** on 21 Oct the ETL fact table went from 2,146 to 2,147 with no file change. Order ORD001693 is dated 21 Oct 10:15; it was "in the future" for the early runs and valid for later ones. A quality rule tied to the clock makes re-runs non-repeatable.
- **Where:** `exp.py snap` output, `pipeline_runs`, `etl_rejected_rows`.
- **Change:** judge "future" against the run's scheduled time, which is fixed for a run, not the wall clock.

### 3. Incremental load
- **Expected:** corrected orders change status without duplicates.
- **Observed:** the increment has 160 rows. ETL: 140 passed, 20 quarantined; on the first load 9 were updated and 131 inserted. ELT merge on a repeat: 160 updated, 0 inserted. The 9 corrected orders went DELIVERED → RETURNED with no duplicates; September revenue went from 986,063.90 to 982,401.20 in both pipelines. Fact rows stayed unique on `order_id`.
- **Where:** logs of `apply_increment` and `merge_increment`, status table.
- **Change:** the scheduled DAGs now pick the file up by date; I would also alert when a day's file is missing.

### 4. Missing source file
- **Expected:** a loud failure, with retries.
- **Observed:** `extract_products` (ETL) and `raw_products` (ELT) failed with `[Errno 2] No such file or directory: '.../products.csv'`. Attempts were about 5 minutes apart, matching `retry_delay`. All later tasks stayed at `No Status`. In ELT the first three raw loads had already written rows before the failure. I restored the file during the retry window, so attempt 3 succeeded and both runs finished green (10:16 and 10:23). **No Slack message was sent**: `slack_alert` sits after validation and never ran.
- **Not observed:** the final `failed` / `upstream_failed` states, because the retries recovered first.
- **Where:** Gantt and Task Instances screenshots, task log.
- **Change:** add an `on_failure_callback` that posts to Slack.

### 5. New bad data
- **Expected:** each of five bad rows is quarantined with the right reason.
- **Observed:** yes, in both pipelines: null customer → `customer_id_not_null`; negative quantity → `quantity_positive; amount_matches_formula` (two reasons); unknown store → `store_exists`; SHIPPED → `status_allowed`; 2027 timestamp → `order_ts_not_future`. `dq_results` counts went up by exactly 1 each (customer 18→19, quantity 10→11, store 181→182 ETL / 186→187 ELT, status 3→4, future 5→6).
- **Where:** `etl_rejected_rows`, `stg_orders`, `dq_results`.
- **Change:** none; this is the case the rules were built for.

### 6. Schema change
- **Not run in this submission.** No result is claimed.

### 7. Volume anomaly
- **Expected:** a warning when volume drops more than 90%.
- **Observed:** with 50 orders, `volume_drop_over_90pct` was WARN in both pipelines (50 checked, 2,362 fewer than the previous run). In ELT, staging still held all the older rows, because raw is never deleted.
- **Where:** `dq_results`.
- **Change:** compare against a rolling average of several runs, not only the previous one.

### 8. Distribution drift
- **Expected:** at least the mart numbers move.
- **Observed:** no check fired. With `unit_price` and `order_amount` both ×100 for store S01, every row still balanced. Store S01 revenue went from 80,069.00 to 7,335,655.25 and total revenue from 1,052,922.85 to 8,308,509.10. Both pipelines reported the same wrong numbers.
- **Where:** `mart_daily_store_kpis` and `etl_daily_zone_revenue`; `dq_results` unchanged (24 FAIL, 5 PASS, 6 WARN).
- **Change:** add a rule `unit_price <= mrp` and a mart check on each store's daily revenue against its recent average.

### 9. Freshness
- **Expected:** the timeliness check warns.
- **Observed:** `latest_order_older_than_1_day` was WARN in both pipelines when the newest order was 30 days old. My helper also shifted the five 2027-dated orders into the past, so they became valid and the ETL fact table grew from 2,147 to 2,151 rows.
- **Where:** `dq_results`.
- **Change:** freshness is measured on clean rows only, because one future-dated row had earlier made old data look fresh.

### 10. Recovery
- **Not run as specified** (no Clear of a failed task). Related result from experiment 4: recovery by automatic retry re-ran only the failed task (Try Number 3); the tasks that had succeeded were not re-run.

### Five pillars of data observability
| Pillar | Coverage in my setup |
|---|---|
| Freshness | Covered: a Warning check on the newest clean order |
| Volume | Covered: a Warning check against the previous run, plus row counts in `pipeline_runs` |
| Schema | Partly: columns are read by name, so a rename should fail loudly, but I did not test it (experiment 6) |
| Distribution | **Weakest:** experiment 8 changed revenue 92× and nothing noticed |
| Lineage | **Best:** ELT keeps `_source_file`, `_run_id` and every raw version; ETL keeps `source_file` and a frozen copy of each run's input |

## Part G – Recommendation to the client

**To:** Sameer (CTO)   **Subject:** ETL or ELT for NammaMart

**Recommendation: ELT, with personal data masked as it enters the warehouse.**

I built both pipelines on the same data (5,372 rows, 33 quality rules). They agree: 2,147 usable
orders, delivered revenue 1,052,922.85, and the same payment mismatches (45,588.52). So the choice
is about running them, not about correctness.

**Auditability: ELT wins.** Finance asked two questions I could answer in SQL on ELT alone. For a
rejected order, raw shows it exactly as received, staging shows the rule it broke, and it is absent
from revenue. For nine orders that were DELIVERED in September and RETURNED in October, raw keeps
both versions, so I can report September revenue as of 30 September (986,063.90) and as it stands
now (982,401.20). In ETL the old row is overwritten, and the history survives only in old files.

**Data quality guarantees: ETL is stricter, ELT is nearly as good.** ETL never loads a bad row into
the warehouse. ELT loads everything and flags it, so a careless query on raw or staging could use bad
data. In my tests both pipelines caught every deliberately bad row (five test rows, five reasons)
and wrote the same 35 checks to `dq_results`. Both missed one thing: a store's prices multiplied by
100 raised total revenue from 1.05 million to 8.3 million, and no check fired.

**Ease of change: ELT wins.** A rule change in ELT is one SQL file. In ETL the same rule lives in
Python, and the two pipelines still carry the rules twice (SQL and Python), so a change must be made
in both places.

**Growth to 1000×: ELT, but untested.** I only ran 5,372 rows. In theory ELT scales with the
warehouse; ETL holds each file in one machine's memory. Today both run on one laptop with a
single-writer database, which will not survive 1000×. Moving to a real warehouse is needed either way.

**Cost: ETL is cheaper.** ELT's raw layer keeps a copy of every row on every run. After about 25
test runs, `raw_orders` held 64,047 rows from a 2,412-row file. It needs a retention rule.

### Top three risks of this design
1. **Personal data in raw.** Raw customers keep unmasked phone numbers and the layer grows every
   run. Under the DPDP Act 2023 this is the biggest exposure. Mask on entry or restrict access,
   and set a retention period.
2. **Blind spot for slow or silent drift.** The ×100 price test passed every check, and a
   clock-based rule changed results between two runs of the same file. Add distribution checks
   and judge "future" against the run's scheduled time.
3. **A single point of failure for the 9:00 AM promise.** One laptop running Docker, one database
   writer, and a missing file sends no Slack alert. Move to managed infrastructure and add a failure alert.
