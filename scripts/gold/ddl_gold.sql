-- Tạo view với bảng dim_customers cho lớp gold
create view gold.dim_customers as
(
    select 
        row_number() over(order by cst_id) as customer_key,
        cst_id as customer_id,
        cst_key as customer_number,
        cst_firstname as first_name,
        cst_lastname as last_name,
        cst_marital_status as marital_status,
        cntry,
        case when cst_gndr != 'N/A' then cst_gndr -- crm is the master for gender info
            else coalesce(gen, 'N/A')
        end as gender,
        bdate as birthdate,
        cst_create_date
    from silver.crm_cust_info as ci
    left join silver.erp_cust_az12 as ca
    on ci.cst_key = ca.cid
    left join silver.erp_loc_a101 as la
    on ci.cst_key = la.cid
);
    



create view gold.dim_products as
(
    select 
        row_number() over(order by prd_start_dt, prd_key) as product_key,
        prd_id as product_id,
        prd_key as product_number,
        prd_nm as product_name,
        cat_id as  category_id,
        cat as category,
        subcat as sub_category,
        maintenance,
        prd_cost as cost,
        prd_line as product_line,
        prd_start_dt as start_date
    from silver.crm_prd_info as pn
    left join silver.erp_px_cat_g1v2 as pc
        on pn.cat_id = pc.id
    where prd_end_dt is null
);


create view gold.fact_sales as 
(
     select sls_ord_num as order_number,
        product_key,
        customer_key,
        sls_sales as sales_amount,
        sls_quantity as quantity,
        sls_price as price,
        sls_order_dt as order_date,
        sls_ship_dt as shipping_date,
        sls_due_dt as due_date
    from silver.crm_sales_details as sd
    left join gold.dim_products as pro
    on sd.sls_prd_key = pro.product_number
    left join gold.dim_customers as cus
    on sd.sls_cust_id = cus.customer_id
);
   