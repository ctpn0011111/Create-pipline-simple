create or alter procedure silver.load_silver as
begin
    DECLARE @start_time datetime, @end_time datetime, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY 
        -- IMPORT CÁC FILE BẰNG PSQL
        SET @batch_start_time = GETDATE();
        PRINT '================================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================================';

        PRINT '----------------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------------------------------------------';

        SET @start_time = GETDATE();
        print '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE table silver.crm_cust_info;

        print '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info
            (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
            )
        SELECT cst_id, cst_key,
            TRIM(cst_firstname) as cst_firstname,
            TRIM(cst_lastname) as cst_lastname,

            CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
                    WHEN UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
                    ELSE 'N/A' END as cst_marital_status,

            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
                    WHEN UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
                    ELSE 'N/A' END as cst_gndr,
            cst_create_date
        FROM (
                select *,
                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
            from bronze.crm_cust_info
            where cst_id is not null
            )as t
        WHERE flag_last = 1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------';



        SET @start_time = GETDATE();
        print '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE table silver.crm_prd_info;

        print '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info
            (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
            )

        select prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) as prd_cost,

            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A' END AS prd_line,

            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
        from bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------';



        SET @start_time = GETDATE();
        print '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE table silver.crm_sales_details;

        print '>> Inserting Data Into: silver.crm_sales_details';
        insert into silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        select sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
                else cast(cast(sls_order_dt as varchar) as date) end as sls_order_dt,

            case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
                else cast(cast(sls_ship_dt as varchar) as date) end as sls_ship_dt,

            case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
                else cast(cast(sls_due_dt as varchar) as date) end as sls_due_dt,

            case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
                    then sls_quantity * abs(sls_price) 
                else sls_sales end as sls_sales,

            sls_quantity,
            case when sls_price is null or sls_price <= 0
                    then sls_sales / nullif(sls_quantity, 0)
                else sls_price end as sls_price
        from bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------';


        
        PRINT '----------------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '----------------------------------------------------------------';

        SET @start_time = GETDATE();
        print '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE table silver.erp_cust_az12;

        print '>> Inserting Data Into: silver.erp_cust_az12';
        insert into silver.erp_cust_az12(
            cid,
            bdate,
            gen
        )
        select 
            case when cid like 'NAS%' then substring(cid, 4, len(cid))
                else cid
            end as cid,

            case when bdate > GETDATE() then null
                else bdate
            end as bdate,
            
            case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
                when upper(trim(gen)) in ('M', 'MALE') then 'Male'
                else 'N/A'
            end as gen
        from bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------';



        SET @start_time = GETDATE();
        print '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE table silver.erp_loc_a101;

        print '>> Inserting Data Into: silver.erp_loc_a101';
        insert into silver.erp_loc_a101(
            cid,
            cntry
        )
        select 
            REPLACE(cid, '-', '') as cid,
            case when trim(cntry) = 'DE' then 'Germany'
                when trim(cntry) in ('US', 'USA') then 'United State'
                when trim(cntry) = '' or cntry is null then 'N/A'
                else trim(cntry)
            end as cntry
        from bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------';



        SET @start_time = GETDATE();
        print '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE table silver.erp_px_cat_g1v2;

        print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        insert into silver.erp_px_cat_g1v2(
            id,
            cat,
            subcat,
            maintenance
        )
        select 
            id,
            cat,
            subcat,
            maintenance
        from bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------------------------';


   SET @batch_end_time = GETDATE();
        PRINT '=====================================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
    END TRY
    BEGIN CATCH
        PRINT '=====================================================';
        PRINT 'ERROR OCCURED DURING LOADING Silver LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=====================================================';
    END CATCH 
end