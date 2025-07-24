-----------------------------------------------------------------------------
-- CREATING POSTED SALES INVOICES LINES TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------

 
DO $$
DECLARE
-- Tablica z nazwami firm wykorzystywana w pętli dla tworzenia tabel i pierwszego ładowania danych
_firmy text[] := ARRAY[ 'aircon', 'technab', 'zymetric'];
-- zmienne
_firma text;
_tabela text;
_litera_firmy char(1);

BEGIN
	FOREACH _firma IN ARRAY _firmy LOOP
	
	_litera_firmy := UPPER(SUBSTR(_firma,1,1));
	_tabela := format('nav_posted_sales_invoices_header_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"No" text PRIMARY KEY
			,"Key_No_Invoice" text NULL
			,"Sell_to_Customer_No" text NULL
			,"Sell_to_Customer_Name" text NULL
			,"VAT_Registration_No" text NULL
			,"Sell_to_Address" text NULL
			,"Sell_to_Address_2" text NULL
			,"Sell_to_City" text NULL
			,"Sell_to_County" text NULL
			,"Sell_to_Post_Code" text NULL
			,"Document_Date" date NULL
			,"Posting_Date" date NULL
			,"Due_Date" date NULL
			,"Quote_No" text NULL
			,"Order_No" text NULL
			,"Pre_Assigned_No" text NULL
			,"External_Document_No" text NULL
			,"Salesperson_Code" text NULL	
			,"EDN_Factoring_Invoice" bool NULL
			,"EDN_KUKE_Symbol" text NULL
			,"Remaining_Amount" numeric(14,2) null
			,"Currency_Code" text NULL
			,"Currency_Factor" numeric(38,20) NULL
			,"Shipment_Date" date NULL
			,"Payment_Terms_Code" text NULL
			,"Payment_Method_Code" text NULL
			,"Shortcut_Dimension_1_Code" text NULL
			,"Shortcut_Dimension_2_Code" text NULL
			,"Customer_Posting_Group" text NULL
			,"Location_Code" text NULL
			,"Shipment_Method_Code" text NULL
			,"Shipping_Agent_Code" text NULL
			,"Invoice_Type" text NULL	
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
		);
	$ddl$, _tabela, _litera_firmy);

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"No"
			,"Key_No_Invoice"
			,"Sell_to_Customer_No"
			,"Sell_to_Customer_Name"
			,"VAT_Registration_No"
			,"Sell_to_Address"
			,"Sell_to_Address_2"
			,"Sell_to_City"
			,"Sell_to_County"
			,"Sell_to_Post_Code"
			,"Document_Date"
			,"Posting_Date"
			,"Due_Date"
			,"Quote_No"
			,"Order_No"
			,"Pre_Assigned_No"
			,"External_Document_No"
			,"Salesperson_Code"
			,"EDN_Factoring_Invoice"
			,"EDN_KUKE_Symbol"
			,"Remaining_Amount"
			,"Currency_Code"
			,"Currency_Factor"
			,"Shipment_Date"
			,"Payment_Terms_Code"
			,"Payment_Method_Code"
			,"Shortcut_Dimension_1_Code"
			,"Shortcut_Dimension_2_Code"
			,"Customer_Posting_Group"
			,"Location_Code"
			,"Shipment_Method_Code"
			,"Shipping_Agent_Code"
			,"Invoice_Type"
			,"Firma"
			,"load_ts"
		)
		SELECT
			ih."No"
			,concat(%L, '_', ih."No")
			,ih."Sell_to_Customer_No"
			,ih."Sell_to_Customer_Name"
			,REGEXP_REPLACE(ih."VAT_Registration_No", '[^0-9A-Za-z]', '', 'g') AS "VAT_Registration_No"
			,ih."Sell_to_Address"
			,ih."Sell_to_Address_2"
			,INITCAP(TRIM(ih."Sell_to_City"))
			,LOWER(TRIM(ih."Sell_to_County"))
			,ih."Sell_to_Post_Code"
			,ih."Document_Date"
			,ih."Posting_Date"
			,ih."Due_Date"
			,ih."Quote_No"
			,ih."Order_No"
			,ih."Pre_Assigned_No"
			,ih."External_Document_No"
			,ih."Salesperson_Code"
			,ih."EDN_Factoring_Invoice"
			,ih."EDN_KUKE_Symbol"
			,ih."Remaining_Amount"
			,CASE
				WHEN ih."Currency_Code" = '' THEN 'PLN'
				ELSE ih."Currency_Code"
			END
			,ih."Currency_Factor"
			,ih."Shipment_Date"
			,ih."Payment_Terms_Code"
			,ih."Payment_Method_Code"
			,ih."Shortcut_Dimension_1_Code"
			,ih."Shortcut_Dimension_2_Code"
			,ih."Customer_Posting_Group"
			,ih."Location_Code"
			,ih."Shipment_Method_Code"
			,ih."Shipping_Agent_Code"
			,'Faktura'
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I ih


    $insert$, _tabela, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;





