------------------------------------------------------------------------------
-- CREATING CURRENCY EXCHANGE RATES IN SILVER LAYER AND FIRST LOAD
------------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.bc_currency_exchange_rates (
	"Currency_Code" bpchar(3) NOT NULL
	,"Starting_Date" date NOT NULL
	,"Relational_Currency_Code" bpchar(3) NULL
	,"Exchange_Rate_Amount" numeric(18, 6) NOT NULL
	,"Relational_Exch_Rate_Amount" numeric(18, 6) NULL
	,"Adjustment_Exch_Rate_Amount" numeric(18, 6) NULL
	,"Relational_Adjmt_Exch_Rate_Amt" numeric(18, 6) NULL
	,"EDN_Sales_Exch_Rate" numeric(18, 6) NULL
	,"Fix_Exchange_Rate_Amount" varchar(20) NULL
	,"ITI_Exch_Rate_Table_No" varchar(30) NULL
	,"load_ts" timestamptz NULL
	,CONSTRAINT "PK_Exchange_Rates" PRIMARY KEY ("Currency_Code", "Starting_Date")
);

CREATE INDEX "IX_Exchange_Rates_Table_No" ON silver.bc_currency_exchange_rates USING btree ("ITI_Exch_Rate_Table_No");


INSERT INTO silver.bc_currency_exchange_rates (
	"Currency_Code" 
	,"Starting_Date" 
	,"Relational_Currency_Code"
	,"Exchange_Rate_Amount" 
	,"Relational_Exch_Rate_Amount" 
	,"Adjustment_Exch_Rate_Amount"
	,"Relational_Adjmt_Exch_Rate_Amt"
	,"EDN_Sales_Exch_Rate"
	,"Fix_Exchange_Rate_Amount"
	,"ITI_Exch_Rate_Table_No"
	,"load_ts"
	)
SELECT
	cer."Currency_Code" 
	,cer."Starting_Date" 
	,cer."Relational_Currency_Code"
	,cer."Exchange_Rate_Amount" 
	,cer."Relational_Exch_Rate_Amount" 
	,cer."Adjustment_Exch_Rate_Amount"
	,cer."Relational_Adjmt_Exch_Rate_Amt"
	,cer."EDN_Sales_Exch_Rate"
	,cer."Fix_Exchange_Rate_Amount"
	,cer."ITI_Exch_Rate_Table_No"
    ,CURRENT_TIMESTAMP
FROM bronze.bc_currency_exchange_rates cer



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------

CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_currency_exchange_rates()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO SILVER.bc_currency_exchange_rates (
        "Currency_Code"
        ,"Starting_Date"
        ,"Relational_Currency_Code"
        ,"Exchange_Rate_Amount"
        ,"Relational_Exch_Rate_Amount"
        ,"Adjustment_Exch_Rate_Amount"
        ,"Relational_Adjmt_Exch_Rate_Amt"
        ,"EDN_Sales_Exch_Rate"
        ,"Fix_Exchange_Rate_Amount"
        ,"ITI_Exch_Rate_Table_No"
        ,"load_ts"
    )
    VALUES (
        NEW."Currency_Code"
        ,NEW."Starting_Date"
        ,NEW."Relational_Currency_Code"
        ,NEW."Exchange_Rate_Amount"
        ,NEW."Relational_Exch_Rate_Amount"
        ,NEW."Adjustment_Exch_Rate_Amount"
        ,NEW."Relational_Adjmt_Exch_Rate_Amt"
        ,NEW."EDN_Sales_Exch_Rate"
        ,NEW."Fix_Exchange_Rate_Amount"
        ,NEW."ITI_Exch_Rate_Table_No"
        ,CURRENT_TIMESTAMP
    )
    ON CONFLICT ("Currency_Code", "Starting_Date") DO UPDATE
    SET
        "Relational_Currency_Code" = EXCLUDED."Relational_Currency_Code"
        ,"Exchange_Rate_Amount" = EXCLUDED."Exchange_Rate_Amount"
        ,"Relational_Exch_Rate_Amount" = EXCLUDED."Relational_Exch_Rate_Amount"
        ,"Adjustment_Exch_Rate_Amount" = EXCLUDED."Adjustment_Exch_Rate_Amount"
        ,"Relational_Adjmt_Exch_Rate_Amt" = EXCLUDED."Relational_Adjmt_Exch_Rate_Amt"
        ,"EDN_Sales_Exch_Rate" = EXCLUDED."EDN_Sales_Exch_Rate"
        ,"Fix_Exchange_Rate_Amount" = EXCLUDED."Fix_Exchange_Rate_Amount"
        ,"ITI_Exch_Rate_Table_No" = EXCLUDED."ITI_Exch_Rate_Table_No"
        ,"load_ts" = CURRENT_TIMESTAMP;

    RETURN NEW;
END;
$function$;

-------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON POSTED SALES INVOICES HEADER TABLE
-------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_bc_currency_exchange_rates ON bronze.bc_currency_exchange_rates;

CREATE TRIGGER trg_upsert_bc_currency_exchange_rates
AFTER INSERT OR UPDATE ON bronze.bc_currency_exchange_rates
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_bc_currency_exchange_rates();
