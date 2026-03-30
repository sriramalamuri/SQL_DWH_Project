-- ======================
-- Load script in pgsql
-- ======================

Create or Replace Procedure bronze.load_bronze()
Language plpgsql
AS $$
Declare 
	starttime timestamp; 
	endtime timestamp;
	batchstart timestamp;
	batchend timestamp;
Begin
batchstart := now();
	Begin
	Raise Notice 'Loading Bronze Layer';
	Raise Notice 'Loading crm tables';
	
	starttime := now();
	Truncate Table bronze.crm_cust_info;
	copy bronze.crm_cust_info 
		from 'C:/SQL_Project/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv' 
		with (format csv, header true);
	endtime := now();
	Raise notice 'Duration = %', endtime - starttime;

	starttime := now();
	Truncate Table bronze.crm_prd_info;
	copy bronze.crm_prd_info 
		from 'C:/SQL_Project/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv' 
		with (format csv, header true);
	endtime := now();
	Raise notice 'Duration = %', endtime - starttime;

	starttime := now();
	Truncate table bronze.crm_sales_details;
	copy bronze.crm_sales_details 
		from 'C:/SQL_Project/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv' 
		with (format csv, header true);
	endtime := now();
	Raise notice 'Duration = %', endtime - starttime;
	
	Raise notice 'Loading erp tables';

	starttime := now();
	Truncate table bronze.erp_cust_az12;
	copy bronze.erp_cust_az12 
		from 'C:/SQL_Project/sql-data-warehouse-project-main/datasets/source_erp/CUST_AZ12.csv' 
		with (format csv, header true);
	endtime := now();
	Raise notice 'Duration = %', endtime - starttime; 

	starttime := now();
	Truncate Table bronze.erp_loc_a101;
	copy bronze.erp_loc_a101 
		from 'C:/SQL_Project/sql-data-warehouse-project-main/datasets/source_erp/LOC_A101.csv' 
		with (format csv, header true);
	endtime := now();
	Raise notice 'Duration = %', endtime - starttime;

	starttime := now();
	Truncate Table bronze.erp_px_cat_g1v2;
	copy bronze.erp_px_cat_g1v2 
		from 'C:/SQL_Project/sql-data-warehouse-project-main/datasets/source_erp/PX_CAT_G1V2.csv' 
		with (format csv, header true);
	endtime := now();
	Raise notice 'Duration = %', endtime - starttime;	
	batchend := now();
	Raise Notice 'Total load duration for Bronze layer is %', batchend - batchstart;
	Exception
		When Others Then
		Raise Notice 'Error in loading Bronze layer';
		Raise Notice 'Error Message: %', SQLERRM;
	End;
End;
$$;
