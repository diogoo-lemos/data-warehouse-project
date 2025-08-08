/*
- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Load and transform the data from the bronze layer to the
silver layer.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
This script will transform the data from the bronze layer 
in order to be studied in the gold layer.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Transformations: 
    Table silver.crm_cust_info: 
        Removes all existing records.
        Inserts the most recent customer record per cst_id.
        Trims names.
        Converts marital_status from M and S to "Married" and "Single"
        and Gender to "Male" and "Female".
    Table silver.crm_prd_info:
        Clears the table.
        Extracts product category ID.
        Extracts product key.
        Maps product line codes into full names like "Mountain".
        Sets missing cost to 0.
        Calculates prd_end_dt as one day before the next start date for the same product.
    Table silver.crm_sales_details:
        Clears the table.
        Parses sales dates (YYYYMMDD) into actual date values, setting invalid/zero dates to NULL.
        Recalculates sls_sales if missing, non-positive, or inconsistent with quantity × price.
        Derives price if missing or invalid (sales / quantity).
    Table silver.erp_cust_az12
        Clears the table.
        Removes "NAS" prefix from cid values if present.
        Sets future birthdates to NULL.
        Normalizes gender values to "Female", "Male", or "n/a".
    Table silver.erp_loc_a101
        Clears the table.
        Removes hyphens from cid.
        Maps "DE" to "Germany", "US"/"USA" to "United States", blanks or nulls to "n/a".
    Table silver.erp_px_cat_g1v2
        Clears the table.
        Copies data from the bronze layer without transformation.
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key FROM 7) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        prd_start_dt::DATE AS prd_start_dt,
        (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE AS prd_end_dt
    FROM bronze.crm_prd_info;

    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
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
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
        END AS sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
            ELSE cid
        END AS cid, 
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid, 
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '------------------------------------------------';
            RAISE NOTICE 'ERROR OCCURED DURING LOADING SILVER LAYER';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'SQL State: %', SQLSTATE;
            RAISE NOTICE '------------------------------------------------';
            RAISE;  
END;
$$;
