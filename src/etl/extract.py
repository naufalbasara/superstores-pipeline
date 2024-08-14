import logging
import os, time, pandas as pd, re

from datetime import datetime

def extract_dim_time(db_conn):
    dim_time = db_conn.query_df(
        """
        select
		TO_CHAR(datum, 'yyyymmdd')::INT as time_key,
		datum::date,
		EXTRACT(DAY FROM datum) AS day_of_month,
		TO_CHAR(DQ.datum, 'W')::INT AS week_of_month,
		extract(month from datum) as month,
		extract(year from datum) as year,
		extract(quarter from datum) as quarter_of_year
        from (
            select (select min(order_date) from sales s)::DATE + SEQUENCE.DAY AS datum
            from GENERATE_SERIES(0, 365*10) AS SEQUENCE (DAY)
            group by SEQUENCE.day
        ) as DQ
        order by 1
        """
    )
    return dim_time

def extract_dim_customer(db_conn, last_update=None):
    try:
        if last_update == None:
            dim_customer = db_conn.query_df(
                """
                select c.customer_id as customer_key, c.name, c.segment, c.registered::date, c.sex, c.points
                from customer c 
                """
            )
            return dim_customer
        
        return db_conn.query_df(
            f"""
                select c.customer_id as customer_key, c.name, c.segment, c.registered::date, c.sex, c.points
                from customer c 
                where c.registered::date > {last_update}
            """
        )
        
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")

    return None

def extract_dim_product(db_conn, last_update=None):
    try:
        if last_update == None:
            dim_customer = db_conn.query_df(
                """
                    select p.product_id as product_key, p.category, p.subcategory, p.product_name, ROUND(cast(avg(sp.ratings) as numeric), 2) as product_rating
                    from product p 
                    left join sale_product sp on p.product_id = sp.product_id
                    group by 1, 4, 3, 2
                """
            )
            return dim_customer
        
        return db_conn.query_df(
            f"""
                select p.product_id as product_key, p.category, p.subcategory, p.product_name, ROUND(cast(avg(sp.ratings) as numeric), 2) as product_rating
                from product p 
                left join sale_product sp on p.product_id = sp.product_id
                group by 1, 4, 3, 2
                where p.registered::date > {last_update}
            """
        )
        
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")

    return None

def extract_dim_location(db_conn):
    try:
        location_df = db_conn.query_df("""
            select l.location_id as location_key, country, city, state, postal_code, region
	        from locations l 
        """)
        return location_df
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")

    return None

def extract_fct_sales(db_conn):
    try:
        fct_sales = db_conn.query_df(
            """
                with dim_time as (
                    select
                        TO_CHAR(datum, 'yyyymmdd')::INT as time_key,
                        datum::date,
                        EXTRACT(DAY FROM datum) AS day_of_month,
                        extract(week from datum)::INT AS week_of_month,
                        extract(month from datum) as month,
                        extract(year from datum) as year,
                        extract(quarter from datum) as quarter_of_year
                    FROM (
                        SELECT (select min(order_date) from sales s)::DATE + SEQUENCE.DAY AS datum
                        FROM GENERATE_SERIES(0, 365*10) AS SEQUENCE (DAY)
                        GROUP BY SEQUENCE.day
                    ) as DQ
                    order by 1
                ), dim_product as (
                    select p.product_id as product_key, p.category, p.subcategory, p.product_name, ROUND(cast(avg(sp.ratings) as numeric), 2) as product_rating
                    from product p 
                    left join sale_product sp on p.product_id = sp.product_id
                    group by 1, 4, 3, 2
                    order by 1 desc
                ), dim_customer as (
                    select c.customer_id as customer_key, c.name, c.segment, c.registered::date, c.sex, c.points
                    from customer c 
                ), dim_location as (
                    select l.location_id as location_key, country, city, state, postal_code, region
                    from locations l 
                ) select s.customer_id as customer_key, s.ship_to as location_key, sp.product_id as product_key, dt.time_key, sum(quantity) as quantity, sum(sp.subtotal) as sales
                from sale_product sp 
                left join sales s on sp.sales_id = s.sales_id
                left join dim_time dt on extract (month from s.order_date) = dt.month and s.order_date::date = dt.datum and extract (year from s.order_date) = dt.year
                group by 2, 1, 3, 4
            """
        )
        return fct_sales
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")

    return None

def extract_sales(db_conn):
    try:
        return db_conn.query_df(
            """
                select * from sales;
            """
        )
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")

    return

def extract_sale_product(db_conn):
    try:
        return db_conn.query_df(
            """
                select * from sale_product;
            """
        )
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")

    return

def extract_visits(db_conn):
    try:
        return db_conn.query_df("""
            select customer_id as customer_key, count(customer_id) as visits
            from web_visit wv
            group by 1;
        """)
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")
    
    return None

def extract_sales_with_product(db_conn):
    try:
        sales_df = db_conn.query_df(
            """
                select s.customer_id as customer_key, s.ship_to as location_key, sp.product_id as product_key, sum(quantity) as quantity, sum(sp.subtotal) as sales
                from sale_product sp 
                left join sales s on sp.sales_id = s.sales_id
                group by 2, 1, 3;
            """
        )

        return sales_df
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")
    return None

def extract_marketing(db_conn):
    try:
        marketing_df = db_conn.query_df(
            """
                
            """
            )
        
        return marketing_df
    except TimeoutError as timeout_err:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [TIMEOUT] {timeout_err}")
    except Exception as error:
        print(f"{datetime.strftime(datetime.now(), format='%D %H:%M:%S')}: [ERROR] {error}")
    
    return None