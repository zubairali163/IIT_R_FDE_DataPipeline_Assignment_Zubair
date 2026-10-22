# Data dictionary: ETL star schema

## etl_fact_orders

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

## etl_dim_customer

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

## etl_dim_product

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

## etl_dim_store

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
