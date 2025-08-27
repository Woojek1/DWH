----------------------------------------------------------------------
-- CREATING RESERVATION ENTRIES TABLES IN SILVER LAYER AND FIRST LOAD
----------------------------------------------------------------------


DO $$
DECLARE
-- Tablica z nazwami firm wykorzystywana w pętli dla tworzenia tabel i pierwszego ładowania danych
_firmy text[] := ARRAY['aircon', 'zymetric', 'technab'];
-- zmienne
_firma text;
_tabela text;
_litera_firmy char(1);

BEGIN
	FOREACH _firma IN ARRAY _firmy LOOP
	
	_litera_firmy := UPPER(SUBSTR(_firma,1,1));
	_tabela := format('bc_reservation_entry_%s', _firma);  --- ZMIENIĆ NAZWĘ TABELI ŹRÓDŁOWEJ I DOCELOWEJ ---

-- Tworzenie tabeli, jeśli nie istnieje
	EXECUTE format ($ddl$
		CREATE TABLE IF NOT EXISTS silver.%I (
			"Entry_No" int4 NOT NULL
			,"Key_Entry_No" text PRIMARY KEY
			,"Positive" bool NOT NULL
			,"Reservation_Status" text NOT NULL
			,"Item_No" text NOT NULL
			,"Key_Item_No" text NOT NULL 
			,"Variant_Code" text NULL
			,"Location_Code" text NULL
			,"Serial_No" text NULL
			,"Lot_No" text NULL
			,"Package_No" text NULL
			,"Expected_Receipt_Date" date NOT NULL
			,"Shipment_Date" date NULL
			,"Quantity_Base" int4 NOT NULL
			,"ReservEngineMgt_CreateForText_Rec" text NULL
			,"ReservedFrom" text NULL
			,"Description" text NULL
			,"Source_Type" int4 NOT NULL
			,"Source_Subtype" text NULL
			,"Source_ID" text NULL
			,"Source_Batch_Name" text NULL
			,"Source_Ref_No" int4 NULL
			,"Creation_Date" date NOT NULL
			,"Transferred_from_Entry_No" int4 NULL
			,"Firma" char(1) DEFAULT %L
			,"load_ts" timestamptz NULL
    );
$ddl$, _tabela, _litera_firmy);	

-- Pierwsze ładowanie danych z bronze do silver
	EXECUTE format($insert$
		INSERT INTO silver.%I (
			"Entry_No"
			,"Key_Entry_No"
			,"Positive"
			,"Reservation_Status"
			,"Item_No"
			,"Key_Item_No"
			,"Variant_Code"
			,"Location_Code"
			,"Serial_No"
			,"Lot_No"
			,"Package_No"
			,"Expected_Receipt_Date"
			,"Shipment_Date"
			,"Quantity_Base"
			,"ReservEngineMgt_CreateForText_Rec"
			,"ReservedFrom"
			,"Description"
			,"Source_Type"
			,"Source_Subtype"
			,"Source_ID"
			,"Source_Batch_Name"
			,"Source_Ref_No"
			,"Creation_Date"
			,"Transferred_from_Entry_No"
			,"Firma"
			,"load_ts"
		)
		SELECT
			re."Entry_No"
			,concat(%L, '_' , re."Entry_No")
			,re."Positive"
			,re."Reservation_Status"
			,re."Item_No"
			,concat(%L, '_' , re."Item_No")
			,re."Variant_Code"
			,re."Location_Code"
			,re."Serial_No"
			,re."Lot_No"
			,re."Package_No"
			,re."Expected_Receipt_Date"
			,re."Shipment_Date"
			,re."Quantity_Base"
			,re."ReservEngineMgt_CreateForText_Rec"
			,re."ReservedFrom"
			,re."Description"
			,re."Source_Type"
			,re."Source_Subtype"
			,re."Source_ID"
			,re."Source_Batch_Name"
			,re."Source_Ref_No"
			,re."Creation_Date"
			,re."Transferred_from_Entry_No"
			,%L
        	,CURRENT_TIMESTAMP
		FROM bronze.%I re

    $insert$, _tabela, _litera_firmy, _litera_firmy, _litera_firmy, _tabela);

	END LOOP;
END;
$$;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_bc_reservation_entry()  -- ZMIENIĆ NAZWĘ FUNKCJI
RETURNS TRIGGER
LANGUAGE plpgsql
AS $function$
DECLARE
	firma TEXT;
	litera_firmy CHAR(1);
--	tgt_schema TEXT := 'silver';
	target_table TEXT;

BEGIN
-- pobiera argumenty przekazane w CREATE TRIGGER 
	firma := TG_ARGV[0];
	litera_firmy := UPPER(SUBSTR(firma, 1, 1));
-- litera := TG_ARGV[1];
	target_table := format('bc_reservation_entry_%s', firma);  -- ZMIENIĆ NAZWĘ TABELI DOCELOWEJ --

EXECUTE format($etl$
	INSERT INTO silver.%I (
		"Entry_No"
		,"Key_Entry_No"
		,"Positive"
		,"Reservation_Status"
		,"Item_No"
		,"Key_Item_No"
		,"Variant_Code"
		,"Location_Code"
		,"Serial_No"
		,"Lot_No"
		,"Package_No"
		,"Expected_Receipt_Date"
		,"Shipment_Date"
		,"Quantity_Base"
		,"ReservEngineMgt_CreateForText_Rec"
		,"ReservedFrom"
		,"Description"
		,"Source_Type"
		,"Source_Subtype"
		,"Source_ID"
		,"Source_Batch_Name"
		,"Source_Ref_No"
		,"Creation_Date"
		,"Transferred_from_Entry_No"
		,"Firma"
		,"load_ts"
	)
	SELECT 
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26
  -- ilość musi odpowiadać ilości kolumn w tabeli docelowej
	
	ON CONFLICT("Key_Entry_No") DO UPDATE
	SET
		"Entry_No" = EXCLUDED."Entry_No"
		,"Positive" = EXCLUDED."Positive"
		,"Reservation_Status" = EXCLUDED."Reservation_Status"
		,"Item_No" = EXCLUDED."Item_No"
		,"Key_Item_No" = EXCLUDED."Key_Item_No"
		,"Variant_Code" = EXCLUDED."Variant_Code"
		,"Location_Code" = EXCLUDED."Location_Code"
		,"Serial_No" = EXCLUDED."Serial_No"
		,"Lot_No" = EXCLUDED."Lot_No"
		,"Package_No" = EXCLUDED."Package_No"
		,"Expected_Receipt_Date" = EXCLUDED."Expected_Receipt_Date"
		,"Shipment_Date" = EXCLUDED."Shipment_Date"
		,"Quantity_Base" = EXCLUDED."Quantity_Base"
		,"ReservEngineMgt_CreateForText_Rec" = EXCLUDED."ReservEngineMgt_CreateForText_Rec"
		,"ReservedFrom" = EXCLUDED."ReservedFrom"
		,"Description" = EXCLUDED."Description"
		,"Source_Type" = EXCLUDED."Source_Type"
		,"Source_Subtype" = EXCLUDED."Source_Subtype"
		,"Source_ID" = EXCLUDED."Source_ID"
		,"Source_Batch_Name" = EXCLUDED."Source_Batch_Name"
		,"Source_Ref_No" = EXCLUDED."Source_Ref_No"
		,"Creation_Date" = EXCLUDED."Creation_Date"
		,"Transferred_from_Entry_No" = EXCLUDED."Transferred_from_Entry_No"
		,"Firma" = EXCLUDED."Firma"
		,"load_ts" = CURRENT_TIMESTAMP;
	$etl$, target_table)
	USING
		NEW."Entry_No"
		,concat(litera_firmy, '_' , NEW."Entry_No")
		,NEW."Positive"
		,NEW."Reservation_Status"
		,NEW."Item_No"
		,concat(litera_firmy, '_' , NEW."Item_No")
		,NEW."Variant_Code"
		,NEW."Location_Code"
		,NEW."Serial_No"
		,NEW."Lot_No"
		,NEW."Package_No"
		,NEW."Expected_Receipt_Date"
		,NEW."Shipment_Date"
		,NEW."Quantity_Base"
		,NEW."ReservEngineMgt_CreateForText_Rec"
		,NEW."ReservedFrom"
		,NEW."Description"
		,NEW."Source_Type"
		,NEW."Source_Subtype"
		,NEW."Source_ID"
		,NEW."Source_Batch_Name"
		,NEW."Source_Ref_No"
		,NEW."Creation_Date"
		,NEW."Transferred_from_Entry_No"
		,litera_firmy
		,CURRENT_TIMESTAMP;
RETURN NEW;
END;
$function$;



-------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON POSTED SALES INVOICES HEADER TABLE
-------------------------------------------------------------------------


DO $$
DECLARE
	grupa_tabel text := 'reservation_entry'; 		-- ZMIENIĆ NAZWĘ GRUPY TABEL
	firmy text[] := ARRAY['aircon', 'zymetric', 'technab'];
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
