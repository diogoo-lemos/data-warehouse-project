/*
- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
Load the data from the CSV files to the bronze layer schemas.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
It will TRUNCATE the bronze tables before loading data and then
will use the COPY command to load the data.
- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
USAGE EXAMPLE: 
	CALL bronze.load_bronze();
- - - - - - - - - - - - - - - - - - - - - - - - - - - - -
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    batch_start_time TIMESTAMP := clock_timestamp();
BEGIN
    BEGIN
		TRUNCATE bronze.crm_cust_info;

		COPY bronze.crm_cust_info
		FROM 'C:\Users\35191\Desktop\data projects\data-warehouse\datasets\source_crm\cust_info.csv' DELIMITER ',' CSV HEADER;


		TRUNCATE bronze.crm_prd_info;

		COPY bronze.crm_prd_info
		FROM 'C:\Users\35191\Desktop\data projects\data-warehouse\datasets\source_crm\prd_info.csv' DELIMITER ',' CSV HEADER;


		TRUNCATE bronze.crm_sales_details;

		COPY bronze.crm_sales_details
		FROM 'C:\Users\35191\Desktop\data projects\data-warehouse\datasets\source_crm\sales_details.csv' DELIMITER ',' CSV HEADER;


		TRUNCATE bronze.erp_cust_az12;

		COPY bronze.erp_cust_az12
		FROM 'C:\Users\35191\Desktop\data projects\data-warehouse\datasets\source_erp\CUST_AZ12.csv' DELIMITER ',' CSV HEADER;


		TRUNCATE bronze.erp_loc_a101;

		COPY bronze.erp_loc_a101
		FROM 'C:\Users\35191\Desktop\data projects\data-warehouse\datasets\source_erp\LOC_A101.csv' DELIMITER ',' CSV HEADER;


		TRUNCATE bronze.erp_px_cat_g1v2;

		COPY bronze.erp_px_cat_g1v2
		FROM 'C:\Users\35191\Desktop\data projects\data-warehouse\datasets\source_erp\PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
	 
	 	RAISE NOTICE 'All tables loaded successfully in %', clock_timestamp() - batch_start_time;
        
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '------------------------------------------------';
            RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error Message: %', SQLERRM;
            RAISE NOTICE 'SQL State: %', SQLSTATE;
            RAISE NOTICE '------------------------------------------------';
            RAISE;  
    END;
END;
$$;
