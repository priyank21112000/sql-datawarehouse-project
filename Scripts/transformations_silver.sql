-- Part 1
-- Check for duplicates using primary key
SELECT cst_id, COUNT(*)
FROM  bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1

-- Using window function to rank duplicates and remove older data
SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
FROM bronze.crm_cust_info

-- Check for unwanted spaces in names
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- Transformation to clean unwanted spaces
SELECT cst_id, cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname
FROM bronze.crm_cust_info

-- Check for consistency of values in low cardinality columns
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

-- Make gender either male or female instead of f and m
-- Data consistency and standardization
-- Inseting the clean and consistent data into Silver stage
INSERT INTO silver.crm_cust_info (
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
)
SELECT cst_id, cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     ELSE 'n/a'
END cst_marital_status,

CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a'
END cst_gndr, 
cst_create_date
FROM bronze.crm_cust_info

-- Checking if data is consitent
SELECT * FROM silver.crm_cust_info()



-- Part 2 - Cleaning and transforming prd_info table
INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

SELECT prd_id, 
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, 
prd_nm,
ISNULL(prd_cost, 0) AS prd_cost, -- Replaces Null values with 0 
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain' 
     WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road' 
     WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales' 
     WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
     ELSE 'n/a' 
END AS prd_line,
CAST (prd_start_dt as DATE) as prd_start_dt,
CAST(LEAD(prd_end_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info

-- Check for Nulls or negative numbers
-- First two rows of product column NULL values
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 or prd_cost is NULL

-- Checking Silver product info table
SELECT * FROM silver.crm_prd_info


-- Part 3 - Transforming Sales data 
-- Order date must always be smaller than shipping date or due date
INSERT INTO silver.crm_sales_details(
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
CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) AS sls_order_dt,
CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) AS sls_ship_dt,
CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;

-- Checking for invalid dates
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < = 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20500101


-- Check for Invalid date orders
-- Order date must always be smaller than shipping date or due date
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


SELECT * FROM silver.crm_sales_details



-- Part 4 bronze ERP custaz12
-- Remove NAS substring as it was extra
INSERT INTO silver.erp_cust_az12(cid, bdate, gen)

SELECT  
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
bdate, 
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

--Check for very old customers or Future birthdays higher than present date
-- Clean data
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' or bdate > GETDATE()

--Check values on gender column
-- Has Null values, F and M 
SELECT DISTINCT(gen)
FROM bronze.erp_cust_az12



--Check
SELECT * FROM silver.erp_cust_az12


-- Part 5 customer id and country
-- Cleaned customer id and made country consistent
INSERT INTO silver.erp_loc_a101 (cid, cntry)

SELECT 
REPLACE(cid, '-', '') cid, 
CASE WHEN TRIM(cntry) = 'US' THEN 'United States'
     ELSE TRIM(cntry)
END AS cntry 
FROM bronze.erp_loc_a101


SELECT DISTINCT(cntry)
FROM bronze.erp_loc_a101

-- Check
SELECT * FROM silver.erp_loc_a101



-- PART 6

INSERT INTO silver.erp_px_cat_g1v2(
    id, cat, subcat, maintenance
)
SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2

--Check for unwanted spaces
-- No unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat)

SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2
SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2


SELECT * FROM silver.erp_px_cat_g1v2
