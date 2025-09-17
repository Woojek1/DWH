--------------------------------------------------------------------------
-- CREATING CUSTOMERS LEDGER ENTRIES TABLES IN SILVER LAYER AND FIRST LOAD
--------------------------------------------------------------------------


DO $$
DECLARE
-- Tablica z nazwami firm wykorzystywana w pętli dla tworzenia tabel i pierwszego ładowania danych
_firmy text[] := ARRAY['aircon', 'technab', 'zymetric'];
-- zmienne
_firma text;
_tabela text;
_litera_firmy char(1);

BEGIN
	FOREACH _firma IN ARRAY _firmy LOOP
	
	_litera_firmy := UPPER(SUBSTR(_firma,1,1));
	_tabela := format('bc_customer_ledger_entries_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---


-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Entry_No" int4 NOT NULL
			,"Posting_Date" date NULL
			,"Document_Date" date NULL
			,"Document_Type" text NULL
			,"Document_No" text NULL
			,"Customer_No" text NULL
			,"Customer_Name" text NULL
			,"Description" text NULL
			,"Global_Dimension_1_Code" text NULL
			,"Global_Dimension_2_Code" text NULL
			,"Customer_Posting_Group" text NULL
			,"IC_Partner_Code" text NULL
			,"Salesperson_Code" text NULL
			,"Currency_Code" text NULL
			,"Original_Amount" numeric(14, 2) NULL
			,"Original_Amt_LCY" numeric(14, 2) NULL
			,"Amount" numeric(14, 2) NULL
			,"Amount_LCY" numeric(14, 2) NULL
			,"Debit_Amount" numeric(14, 2) NULL
			,"Debit_Amount_LCY" numeric(14, 2) NULL
			,"Credit_Amount" numeric(14, 2) NULL
			,"Credit_Amount_LCY" numeric(14, 2) NULL
			,"Remaining_Amount" numeric(14, 2) NULL
			,"Remaining_Amt_LCY" numeric(14, 2) NULL
			,"Sales_LCY" numeric(14, 2) NULL
			,"Bal_Account_Type" text NULL
			,"Bal_Account_No" text NULL
			,"Due_Date" date NULL
			,"EDN_Days_Until_Due" int4 NULL
			,"EDN_Days_Overdue" int4 NULL
			,"Payment_Prediction" text NULL
			,"Prediction_Confidence" text NULL
			,"Prediction_Confidence_Percent" numeric(6, 2) NULL
			,"Pmt_Discount_Date" date NULL
			,"Pmt_Disc_Tolerance_Date" date NULL
			,"Original_Pmt_Disc_Possible" numeric(14, 2) NULL
			,"Remaining_Pmt_Disc_Possible" numeric(14, 2) NULL
			,"Max_Payment_Tolerance" numeric(14, 2) NULL
			,"Payment_Method_Code" text NULL
			,"Open" bool NULL
			,"Closed_at_Date" date NULL
			,"On_Hold" text NULL
			,"User_ID" text NULL
			,"Source_Code" text NULL
			,"Reason_Code" text NULL
			,"Reversed" bool NULL
			,"Reversed_by_Entry_No" int4 NULL
			,"Reversed_Entry_No" int4 NULL
			,"Exported_to_Payment_File" bool NULL
			,"Message_to_Recipient" text NULL
			,"Direct_Debit_Mandate_ID" text NULL
			,"Dimension_Set_ID" int4 NULL
			,"ITI_Split_Payment" bool NULL
			,"ITI_VAT_Transaction_Type" text NULL
			,"ITI_Bad_Debt_Rel_Corr_Amt_LCY" numeric(14, 2) NULL
			,"ITI_VAT_Entry_Amount" numeric(14, 2) NULL
			,"ITI_VAT_Entry_Base" numeric(14, 2) NULL
			,"External_Document_No" text NULL
			,"Your_Reference" text NULL
			,"RecipientBankAccount" text NULL
			,"Shortcut_Dimension_3_Code" text NULL
			,"Shortcut_Dimension_4_Code" text NULL
			,"Shortcut_Dimension_5_Code" text NULL
			,"Shortcut_Dimension_6_Code" text NULL
			,"Shortcut_Dimension_7_Code" text NULL
			,"Shortcut_Dimension_8_Code" text NULL
			,"EDN_Factoring_Invoice" bool NULL
			,"EDN_Insurance_Invoice" bool NULL
			,"EDN_KUKE_Symbol" text NULL
			,"EDN_Policy" text NULL
			,"EDN_Insurance_Due_Date" date NULL
			,"SystemModifiedAt" timestamp NULL
			,"SystemCreatedAt" timestamptz NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
 			,PRIMARY KEY ("Entry_No")
		);
	$ddl$, _tabela, _litera_firmy);

 
 
-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Entry_No"
			,"Posting_Date"
			,"Document_Date"
			,"Document_Type"
			,"Document_No"
			,"Customer_No"
			,"Customer_Name"
			,"Description"
			,"Global_Dimension_1_Code"
			,"Global_Dimension_2_Code"
			,"Customer_Posting_Group"
			,"IC_Partner_Code"
			,"Salesperson_Code"
			,"Currency_Code"
			,"Original_Amount"
			,"Original_Amt_LCY"
			,"Amount"
			,"Amount_LCY"
			,"Debit_Amount"
			,"Debit_Amount_LCY"
			,"Credit_Amount"
			,"Credit_Amount_LCY"
			,"Remaining_Amount"
			,"Remaining_Amt_LCY"
			,"Sales_LCY"
			,"Bal_Account_Type"
			,"Bal_Account_No"
			,"Due_Date"
			,"EDN_Days_Until_Due"
			,"EDN_Days_Overdue"
			,"Payment_Prediction"
			,"Prediction_Confidence"
			,"Prediction_Confidence_Percent"
			,"Pmt_Discount_Date"
			,"Pmt_Disc_Tolerance_Date"
			,"Original_Pmt_Disc_Possible"
			,"Remaining_Pmt_Disc_Possible"
			,"Max_Payment_Tolerance"
			,"Payment_Method_Code"
			,"Open"
			,"Closed_at_Date"
			,"On_Hold"
			,"User_ID"
			,"Source_Code"
			,"Reason_Code"
			,"Reversed"
			,"Reversed_by_Entry_No"
			,"Reversed_Entry_No"
			,"Exported_to_Payment_File"
			,"Message_to_Recipient"
			,"Direct_Debit_Mandate_ID"
			,"Dimension_Set_ID"
			,"ITI_Split_Payment"
			,"ITI_VAT_Transaction_Type"
			,"ITI_Bad_Debt_Rel_Corr_Amt_LCY"
			,"ITI_VAT_Entry_Amount"
			,"ITI_VAT_Entry_Base"
			,"External_Document_No"
			,"Your_Reference"
			,"RecipientBankAccount"
			,"Shortcut_Dimension_3_Code"
			,"Shortcut_Dimension_4_Code"
			,"Shortcut_Dimension_5_Code"
			,"Shortcut_Dimension_6_Code"
			,"Shortcut_Dimension_7_Code"
			,"Shortcut_Dimension_8_Code"
			,"EDN_Factoring_Invoice"
			,"EDN_Insurance_Invoice"
			,"EDN_KUKE_Symbol"
			,"EDN_Policy"
			,"EDN_Insurance_Due_Date"
			,"SystemModifiedAt"
			,"SystemCreatedAt"
			,"Firma"
			,"load_ts"
		)
		SELECT
			cle."Entry_No"
			,cle."Posting_Date"
			,cle."Document_Date"
			,cle."Document_Type"
			,cle."Document_No"
			,cle."Customer_No"
			,cle."Customer_Name"
			,cle."Description"
			,cle."Global_Dimension_1_Code"
			,cle."Global_Dimension_2_Code"
			,cle."Customer_Posting_Group"
			,cle."IC_Partner_Code"
			,cle."Salesperson_Code"
			,cle."Currency_Code"
			,cle."Original_Amount"
			,cle."Original_Amt_LCY"
			,cle."Amount"
			,cle."Amount_LCY"
			,cle."Debit_Amount"
			,cle."Debit_Amount_LCY"
			,cle."Credit_Amount"
			,cle."Credit_Amount_LCY"
			,cle."Remaining_Amount"
			,cle."Remaining_Amt_LCY"
			,cle."Sales_LCY"
			,cle."Bal_Account_Type"
			,cle."Bal_Account_No"
			,cle."Due_Date"
			,cle."EDN_Days_Until_Due"
			,cle."EDN_Days_Overdue"
			,cle."Payment_Prediction"
			,cle."Prediction_Confidence"
			,cle."Prediction_Confidence_Percent"
			,cle."Pmt_Discount_Date"
			,cle."Pmt_Disc_Tolerance_Date"
			,cle."Original_Pmt_Disc_Possible"
			,cle."Remaining_Pmt_Disc_Possible"
			,cle."Max_Payment_Tolerance"
			,cle."Payment_Method_Code"
			,cle."Open"
			,cle."Closed_at_Date"
			,cle."On_Hold"
			,cle."User_ID"
			,cle."Source_Code"
			,cle."Reason_Code"
			,cle."Reversed"
			,cle."Reversed_by_Entry_No"
			,cle."Reversed_Entry_No"
			,cle."Exported_to_Payment_File"
			,cle."Message_to_Recipient"
			,cle."Direct_Debit_Mandate_ID"
			,cle."Dimension_Set_ID"
			,cle."ITI_Split_Payment"
			,cle."ITI_VAT_Transaction_Type"
			,cle."ITI_Bad_Debt_Rel_Corr_Amt_LCY"
			,cle."ITI_VAT_Entry_Amount"
			,cle."ITI_VAT_Entry_Base"
			,cle."External_Document_No"
			,cle."Your_Reference"
			,cle."RecipientBankAccount"
			,cle."Shortcut_Dimension_3_Code"
			,cle."Shortcut_Dimension_4_Code"
			,cle."Shortcut_Dimension_5_Code"
			,cle."Shortcut_Dimension_6_Code"
			,cle."Shortcut_Dimension_7_Code"
			,cle."Shortcut_Dimension_8_Code"
			,cle."EDN_Factoring_Invoice"
			,cle."EDN_Insurance_Invoice"
			,cle."EDN_KUKE_Symbol"
			,cle."EDN_Policy"
			,cle."EDN_Insurance_Due_Date"
			,cle."SystemModifiedAt"
			,cle."SystemCreatedAt"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I cle
		
    $insert$, _tabela, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_customer_ledger_entries()  -- ZMIENIĆ NAZWĘ FUNKCJI
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
DECLARE
	firma TEXT;
	litera_firmy CHAR(1);
--	tgt_schema TEXT := 'silver';
	target_table TEXT;
	v_unit_price_pln numeric(18, 2);

BEGIN
-- pobiera argumenty przekazane w CREATE TRIGGER 
	firma := TG_ARGV[0];
	litera_firmy := UPPER(SUBSTR(firma, 1, 1));
-- litera := TG_ARGV[1];
	target_table := format('bc_customer_ledger_entries_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --




EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Entry_No"
		,"Posting_Date"
		,"Document_Date"
		,"Document_Type"
		,"Document_No"
		,"Customer_No"
		,"Customer_Name"
		,"Description"
		,"Global_Dimension_1_Code"
		,"Global_Dimension_2_Code"
		,"Customer_Posting_Group"
		,"IC_Partner_Code"
		,"Salesperson_Code"
		,"Currency_Code"
		,"Original_Amount"
		,"Original_Amt_LCY"
		,"Amount"
		,"Amount_LCY"
		,"Debit_Amount"
		,"Debit_Amount_LCY"
		,"Credit_Amount"
		,"Credit_Amount_LCY"
		,"Remaining_Amount"
		,"Remaining_Amt_LCY"
		,"Sales_LCY"
		,"Bal_Account_Type"
		,"Bal_Account_No"
		,"Due_Date"
		,"EDN_Days_Until_Due"
		,"EDN_Days_Overdue"
		,"Payment_Prediction"
		,"Prediction_Confidence"
		,"Prediction_Confidence_Percent"
		,"Pmt_Discount_Date"
		,"Pmt_Disc_Tolerance_Date"
		,"Original_Pmt_Disc_Possible"
		,"Remaining_Pmt_Disc_Possible"
		,"Max_Payment_Tolerance"
		,"Payment_Method_Code"
		,"Open"
		,"Closed_at_Date"
		,"On_Hold"
		,"User_ID"
		,"Source_Code"
		,"Reason_Code"
		,"Reversed"
		,"Reversed_by_Entry_No"
		,"Reversed_Entry_No"
		,"Exported_to_Payment_File"
		,"Message_to_Recipient"
		,"Direct_Debit_Mandate_ID"
		,"Dimension_Set_ID"
		,"ITI_Split_Payment"
		,"ITI_VAT_Transaction_Type"
		,"ITI_Bad_Debt_Rel_Corr_Amt_LCY"
		,"ITI_VAT_Entry_Amount"
		,"ITI_VAT_Entry_Base"
		,"External_Document_No"
		,"Your_Reference"
		,"RecipientBankAccount"
		,"Shortcut_Dimension_3_Code"
		,"Shortcut_Dimension_4_Code"
		,"Shortcut_Dimension_5_Code"
		,"Shortcut_Dimension_6_Code"
		,"Shortcut_Dimension_7_Code"
		,"Shortcut_Dimension_8_Code"
		,"EDN_Factoring_Invoice"
		,"EDN_Insurance_Invoice"
		,"EDN_KUKE_Symbol"
		,"EDN_Policy"
		,"EDN_Insurance_Due_Date"
		,"SystemModifiedAt"
		,"SystemCreatedAt"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
		$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,
		$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,
		$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75

  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Entry_No") DO UPDATE
	SET
		"Entry_No" = EXCLUDED."Entry_No"
		,"Posting_Date" = EXCLUDED."Posting_Date"
		,"Document_Date" = EXCLUDED."Document_Date"
		,"Document_Type" = EXCLUDED."Document_Type"
		,"Document_No" = EXCLUDED."Document_No"
		,"Customer_No" = EXCLUDED."Customer_No"
		,"Customer_Name" = EXCLUDED."Customer_Name"
		,"Description" = EXCLUDED."Description"
		,"Global_Dimension_1_Code" = EXCLUDED."Global_Dimension_1_Code"
		,"Global_Dimension_2_Code" = EXCLUDED."Global_Dimension_2_Code"
		,"Customer_Posting_Group" = EXCLUDED."Customer_Posting_Group"
		,"IC_Partner_Code" = EXCLUDED."IC_Partner_Code"
		,"Salesperson_Code" = EXCLUDED."Salesperson_Code"
		,"Currency_Code" = EXCLUDED."Currency_Code"
		,"Original_Amount" = EXCLUDED."Original_Amount"
		,"Original_Amt_LCY" = EXCLUDED."Original_Amt_LCY"
		,"Amount" = EXCLUDED."Amount"
		,"Amount_LCY" = EXCLUDED."Amount_LCY"
		,"Debit_Amount" = EXCLUDED."Debit_Amount"
		,"Debit_Amount_LCY" = EXCLUDED."Debit_Amount_LCY"
		,"Credit_Amount" = EXCLUDED."Credit_Amount"
		,"Credit_Amount_LCY" = EXCLUDED."Credit_Amount_LCY"
		,"Remaining_Amount" = EXCLUDED."Remaining_Amount"
		,"Remaining_Amt_LCY" = EXCLUDED."Remaining_Amt_LCY"
		,"Sales_LCY" = EXCLUDED."Sales_LCY"
		,"Bal_Account_Type" = EXCLUDED."Bal_Account_Type"
		,"Bal_Account_No" = EXCLUDED."Bal_Account_No"
		,"Due_Date" = EXCLUDED."Due_Date"
		,"EDN_Days_Until_Due" = EXCLUDED."EDN_Days_Until_Due"
		,"EDN_Days_Overdue" = EXCLUDED."EDN_Days_Overdue"
		,"Payment_Prediction" = EXCLUDED."Payment_Prediction"
		,"Prediction_Confidence" = EXCLUDED."Prediction_Confidence"
		,"Prediction_Confidence_Percent" = EXCLUDED."Prediction_Confidence_Percent"
		,"Pmt_Discount_Date" = EXCLUDED."Pmt_Discount_Date"
		,"Pmt_Disc_Tolerance_Date" = EXCLUDED."Pmt_Disc_Tolerance_Date"
		,"Original_Pmt_Disc_Possible" = EXCLUDED."Original_Pmt_Disc_Possible"
		,"Remaining_Pmt_Disc_Possible" = EXCLUDED."Remaining_Pmt_Disc_Possible"
		,"Max_Payment_Tolerance" = EXCLUDED."Max_Payment_Tolerance"
		,"Payment_Method_Code" = EXCLUDED."Payment_Method_Code"
		,"Open" = EXCLUDED."Open"
		,"Closed_at_Date" = EXCLUDED."Closed_at_Date"
		,"On_Hold" = EXCLUDED."On_Hold"
		,"User_ID" = EXCLUDED."User_ID"
		,"Source_Code" = EXCLUDED."Source_Code"
		,"Reason_Code" = EXCLUDED."Reason_Code"
		,"Reversed" = EXCLUDED."Reversed"
		,"Reversed_by_Entry_No" = EXCLUDED."Reversed_by_Entry_No"
		,"Reversed_Entry_No" = EXCLUDED."Reversed_Entry_No"
		,"Exported_to_Payment_File" = EXCLUDED."Exported_to_Payment_File"
		,"Message_to_Recipient" = EXCLUDED."Message_to_Recipient"
		,"Direct_Debit_Mandate_ID" = EXCLUDED."Direct_Debit_Mandate_ID"
		,"Dimension_Set_ID" = EXCLUDED."Dimension_Set_ID"
		,"ITI_Split_Payment" = EXCLUDED."ITI_Split_Payment"
		,"ITI_VAT_Transaction_Type" = EXCLUDED."ITI_VAT_Transaction_Type"
		,"ITI_Bad_Debt_Rel_Corr_Amt_LCY" = EXCLUDED."ITI_Bad_Debt_Rel_Corr_Amt_LCY"
		,"ITI_VAT_Entry_Amount" = EXCLUDED."ITI_VAT_Entry_Amount"
		,"ITI_VAT_Entry_Base" = EXCLUDED."ITI_VAT_Entry_Base"
		,"External_Document_No" = EXCLUDED."External_Document_No"
		,"Your_Reference" = EXCLUDED."Your_Reference"
		,"RecipientBankAccount" = EXCLUDED."RecipientBankAccount"
		,"Shortcut_Dimension_3_Code" = EXCLUDED."Shortcut_Dimension_3_Code"
		,"Shortcut_Dimension_4_Code" = EXCLUDED."Shortcut_Dimension_4_Code"
		,"Shortcut_Dimension_5_Code" = EXCLUDED."Shortcut_Dimension_5_Code"
		,"Shortcut_Dimension_6_Code" = EXCLUDED."Shortcut_Dimension_6_Code"
		,"Shortcut_Dimension_7_Code" = EXCLUDED."Shortcut_Dimension_7_Code"
		,"Shortcut_Dimension_8_Code" = EXCLUDED."Shortcut_Dimension_8_Code"
		,"EDN_Factoring_Invoice" = EXCLUDED."EDN_Factoring_Invoice"
		,"EDN_Insurance_Invoice" = EXCLUDED."EDN_Insurance_Invoice"
		,"EDN_KUKE_Symbol" = EXCLUDED."EDN_KUKE_Symbol"
		,"EDN_Policy" = EXCLUDED."EDN_Policy"
		,"EDN_Insurance_Due_Date" = EXCLUDED."EDN_Insurance_Due_Date"
		,"SystemModifiedAt" = EXCLUDED."SystemModifiedAt"
		,"SystemCreatedAt" = EXCLUDED."SystemCreatedAt"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."Entry_No"
		,NEW."Posting_Date"
		,NEW."Document_Date"
		,NEW."Document_Type"
		,NEW."Document_No"
		,NEW."Customer_No"
		,NEW."Customer_Name"
		,NEW."Description"
		,NEW."Global_Dimension_1_Code"
		,NEW."Global_Dimension_2_Code"
		,NEW."Customer_Posting_Group"
		,NEW."IC_Partner_Code"
		,NEW."Salesperson_Code"
		,NEW."Currency_Code"
		,NEW."Original_Amount"
		,NEW."Original_Amt_LCY"
		,NEW."Amount"
		,NEW."Amount_LCY"
		,NEW."Debit_Amount"
		,NEW."Debit_Amount_LCY"
		,NEW."Credit_Amount"
		,NEW."Credit_Amount_LCY"
		,NEW."Remaining_Amount"
		,NEW."Remaining_Amt_LCY"
		,NEW."Sales_LCY"
		,NEW."Bal_Account_Type"
		,NEW."Bal_Account_No"
		,NEW."Due_Date"
		,NEW."EDN_Days_Until_Due"
		,NEW."EDN_Days_Overdue"
		,NEW."Payment_Prediction"
		,NEW."Prediction_Confidence"
		,NEW."Prediction_Confidence_Percent"
		,NEW."Pmt_Discount_Date"
		,NEW."Pmt_Disc_Tolerance_Date"
		,NEW."Original_Pmt_Disc_Possible"
		,NEW."Remaining_Pmt_Disc_Possible"
		,NEW."Max_Payment_Tolerance"
		,NEW."Payment_Method_Code"
		,NEW."Open"
		,NEW."Closed_at_Date"
		,NEW."On_Hold"
		,NEW."User_ID"
		,NEW."Source_Code"
		,NEW."Reason_Code"
		,NEW."Reversed"
		,NEW."Reversed_by_Entry_No"
		,NEW."Reversed_Entry_No"
		,NEW."Exported_to_Payment_File"
		,NEW."Message_to_Recipient"
		,NEW."Direct_Debit_Mandate_ID"
		,NEW."Dimension_Set_ID"
		,NEW."ITI_Split_Payment"
		,NEW."ITI_VAT_Transaction_Type"
		,NEW."ITI_Bad_Debt_Rel_Corr_Amt_LCY"
		,NEW."ITI_VAT_Entry_Amount"
		,NEW."ITI_VAT_Entry_Base"
		,NEW."External_Document_No"
		,NEW."Your_Reference"
		,NEW."RecipientBankAccount"
		,NEW."Shortcut_Dimension_3_Code"
		,NEW."Shortcut_Dimension_4_Code"
		,NEW."Shortcut_Dimension_5_Code"
		,NEW."Shortcut_Dimension_6_Code"
		,NEW."Shortcut_Dimension_7_Code"
		,NEW."Shortcut_Dimension_8_Code"
		,NEW."EDN_Factoring_Invoice"
		,NEW."EDN_Insurance_Invoice"
		,NEW."EDN_KUKE_Symbol"
		,NEW."EDN_Policy"
		,NEW."EDN_Insurance_Due_Date"
		,NEW."SystemModifiedAt"
		,NEW."SystemCreatedAt"
	    ,litera_firmy
	    ,CURRENT_TIMESTAMP;
RETURN NEW;
END;
$function$;



------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON CUSTOMERS TABLE
------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'customer_ledger_entries'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
	firmy text[] := ARRAY['aircon', 'technab', 'zymetric'];
	firma text;
BEGIN
	FOREACH firma IN ARRAY firmy LOOP
		EXECUTE format($sql$
			DROP TRIGGER IF EXISTS trg_upsert_bc_%I_%I ON bronze.bc_%I_%I;
			CREATE TRIGGER trg_upsert_bc_%I_%I
			AFTER INSERT OR UPDATE ON bronze.bc_%I_%I
			FOR EACH ROW
			EXECUTE FUNCTION bronze.fn_upsert_bc_%I(%L)
		$sql$, 
		grupa_tabel, firma,   -- dla DROP
		grupa_tabel, firma,   -- dla DROP
		grupa_tabel, firma,   -- dla CREATE
		grupa_tabel, firma,   -- dla CREATE
		grupa_tabel,          -- dla funkcji fn_upsert_bc_<grupa_tabel>
		firma                 -- parametr do funkcji jako tekst
		);
	END LOOP;
END;
$$ LANGUAGE plpgsql;