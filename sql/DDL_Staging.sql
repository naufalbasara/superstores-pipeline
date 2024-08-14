CREATE TABLE "fct_sales" (
  "customer_key" serial,
  "location_key" serial,
  "product_key" serial,
  "time_key" serial,
  "quantity" integer,
  "sales" integer
);

CREATE TABLE "fct_marketing" (
  "customer_key" serial,
  "location_key" serial,
  "time_key" serial,
  "conversion_rate" float,
  "avg_purchase_value" float
);

CREATE TABLE "dim_customer" (
  "customer_id" serial PRIMARY KEY,
  "name" varchar,
  "segment" varchar,
  "registered" timestamp,
  "sex" varchar,
  "points" integer
);

CREATE TABLE "dim_location" (
  "location_id" serial PRIMARY KEY,
  "country" varchar,
  "city" varchar,
  "state" varchar,
  "postal_code" varchar,
  "region" varchar
);

CREATE TABLE "dim_product" (
  "product_id" serial PRIMARY KEY,
  "category" varchar,
  "subcategory" varchar,
  "product_name" varchar,
  "ratings" float
);

CREATE TABLE "dim_time" (
  "time_id" serial PRIMARY KEY,
  "day_of_month" int,
  "week_of_month" int,
  "month" month,
  "year" year,
  "quarter_of_year" int
);

ALTER TABLE "fct_sales" ADD FOREIGN KEY ("customer_key") REFERENCES "dim_customer" ("customer_id");

ALTER TABLE "fct_sales" ADD FOREIGN KEY ("location_key") REFERENCES "dim_location" ("location_id");

ALTER TABLE "fct_sales" ADD FOREIGN KEY ("product_key") REFERENCES "dim_product" ("product_id");

ALTER TABLE "fct_sales" ADD FOREIGN KEY ("time_key") REFERENCES "dim_time" ("time_id");

ALTER TABLE "fct_marketing" ADD FOREIGN KEY ("customer_key") REFERENCES "dim_customer" ("customer_id");

ALTER TABLE "fct_marketing" ADD FOREIGN KEY ("location_key") REFERENCES "dim_location" ("location_id");

ALTER TABLE "fct_marketing" ADD FOREIGN KEY ("time_key") REFERENCES "dim_time" ("time_id");
