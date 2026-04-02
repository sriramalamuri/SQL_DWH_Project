--// Customer Dimension in Gold Layer \\ --
Create View gold.dim_customers as
Select 
	Row_Number() Over(order by cst_id) customer_key, --surrogate key
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.cntry as country,
	ci.cst_marital_status as marital_status,
		Case
		When ci.cst_gndr != 'n/a' Then ci.cst_gndr --CRM is the master source for customer data (i.e. gender also)
		Else Coalesce(ca.gen, 'n/a')
	End gender,
	ca.bdate as birth_date,
	ci.cst_create_date as create_date
From silver.crm_cust_info ci
	Left Join silver.erp_cust_az12 ca
	On ci.cst_key = ca.cid
	Left Join silver.erp_loc_a101 la
	On ci.cst_key = la.cid

--// Product Dimenseion in Gold Layer \\ --
Create view gold.dim_product as
Select 
Row_Number() over(order by pi.prd_start_dt, pi.prd_key) as product_key,
pi.prd_id as product_id,
pi.prd_key as product_number,
pi.prd_nm as product_name,
pi.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pi.prd_cost as product_cost,
pi.prd_line as product_line,
pi.prd_start_dt as start_date
from silver.crm_prd_info pi
Left Join silver.erp_px_cat_g1v2 as pc
On pi.cat_id = pc.id_
Where prd_end_dt IS NULL --Filter out historical data

--// Sales Fact in Gold Layer \\ --
Create view gold.fact_sale as
Select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as sales_quantity,
sd.sls_price as price
From silver.crm_sales_details sd
Left Join gold.dim_product pr
On sd.sls_prd_key = pr.product_number
Left Join gold.dim_customers cu
On sd.sls_cust_id = cu.customer_id
