-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD QOUTE TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_quote_item (
	item_id serial4 NOT NULL
	,quote_id int4 DEFAULT 0 NOT NULL
	,created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,product_id int4 NULL
	,store_id int2 NULL
	,parent_item_id int4 NULL
	,is_virtual int2 NULL
	,sku varchar(255) NULL
	,name varchar(255) NULL
	,description text NULL
	,applied_rule_ids text NULL
	,additional_data text NULL
	,is_qty_decimal int2 NULL
	,no_discount int2 DEFAULT 0 NULL
	,weight numeric(12, 4) DEFAULT 0.0000 NULL
	,qty numeric(12, 4) DEFAULT 0.0000 NOT NULL
	,price numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,base_price numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,custom_price numeric(20, 4) NULL
	,discount_percent numeric(12, 4) DEFAULT 0.0000 NULL
	,discount_amount numeric(20, 4) DEFAULT 0.0000 NULL
	,base_discount_amount numeric(20, 4) DEFAULT 0.0000 NULL
	,tax_percent numeric(12, 4) DEFAULT 0.0000 NULL
	,tax_amount numeric(20, 4) DEFAULT 0.0000 NULL
	,base_tax_amount numeric(20, 4) DEFAULT 0.0000 NULL
	,row_total numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,base_row_total numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,row_total_with_discount numeric(20, 4) NULL
	,row_weight numeric(12, 4) DEFAULT 0.0000 NULL
	,product_type varchar(255) NULL
	,base_tax_before_discount numeric(20, 4) NULL
	,tax_before_discount numeric(20, 4) NULL
	,original_custom_price numeric(20, 4) NULL
	,redirect_url varchar(255) NULL
	,base_cost numeric(12, 4) NULL
	,price_incl_tax numeric(20, 4) NULL
	,base_price_incl_tax numeric(20, 4) NULL
	,row_total_incl_tax numeric(20, 4) NULL
	,base_row_total_incl_tax numeric(20, 4) NULL
	,discount_tax_compensation_amount numeric(20, 4) NULL
	,base_discount_tax_compensation_amount numeric(20, 4) NULL
	,event_id int4 NULL
	,weee_tax_applied text NULL
	,weee_tax_applied_amount numeric(12, 4) NULL
	,weee_tax_applied_row_amount numeric(12, 4) NULL
	,weee_tax_disposition numeric(12, 4) NULL
	,weee_tax_row_disposition numeric(12, 4) NULL
	,base_weee_tax_applied_amount numeric(12, 4) NULL
	,base_weee_tax_applied_row_amnt numeric(12, 4) NULL
	,base_weee_tax_disposition numeric(12, 4) NULL
	,base_weee_tax_row_disposition numeric(12, 4) NULL
	,giftregistry_item_id int4 NULL
	,gift_message_id int4 NULL
	,gw_id int4 NULL
	,gw_base_price numeric(12, 4) NULL
	,gw_price numeric(12, 4) NULL
	,gw_base_tax_amount numeric(12, 4) NULL
	,gw_tax_amount numeric(12, 4) NULL
	,free_shipping int2 DEFAULT 0 NOT NULL
	,"load_ts" timestamptz NULL
	,CONSTRAINT magento_prod_quote_item_pkey PRIMARY KEY (item_id)
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_quote_item (
	item_id
	,quote_id
	,created_at
	,updated_at
	,product_id
	,store_id
	,parent_item_id
	,is_virtual
	,sku
	,"name"
	,description
	,applied_rule_ids
	,additional_data
	,is_qty_decimal
	,no_discount
	,weight
	,qty
	,price
	,base_price
	,custom_price
	,discount_percent
	,discount_amount
	,base_discount_amount
	,tax_percent
	,tax_amount
	,base_tax_amount
	,row_total
	,base_row_total
	,row_total_with_discount
	,row_weight
	,product_type
	,base_tax_before_discount
	,tax_before_discount
	,original_custom_price
	,redirect_url
	,base_cost
	,price_incl_tax
	,base_price_incl_tax
	,row_total_incl_tax
	,base_row_total_incl_tax
	,discount_tax_compensation_amount
	,base_discount_tax_compensation_amount
	,event_id
	,weee_tax_applied
	,weee_tax_applied_amount
	,weee_tax_applied_row_amount
	,weee_tax_disposition
	,weee_tax_row_disposition
	,base_weee_tax_applied_amount
	,base_weee_tax_applied_row_amnt
	,base_weee_tax_disposition
	,base_weee_tax_row_disposition
	,giftregistry_item_id
	,gift_message_id
	,gw_id
	,gw_base_price
	,gw_price
	,gw_base_tax_amount
	,gw_tax_amount
	,free_shipping
	,"load_ts"

)

SELECT
	mpqi.item_id
	,mpqi.quote_id
	,mpqi.created_at
	,mpqi.updated_at
	,mpqi.product_id
	,mpqi.store_id
	,mpqi.parent_item_id
	,mpqi.is_virtual
	,mpqi.sku
	,mpqi."name"
	,mpqi.description
	,mpqi.applied_rule_ids
	,mpqi.additional_data
	,mpqi.is_qty_decimal
	,mpqi.no_discount
	,mpqi.weight
	,mpqi.qty
	,mpqi.price
	,mpqi.base_price
	,mpqi.custom_price
	,mpqi.discount_percent
	,mpqi.discount_amount
	,mpqi.base_discount_amount
	,mpqi.tax_percent
	,mpqi.tax_amount
	,mpqi.base_tax_amount
	,mpqi.row_total
	,mpqi.base_row_total
	,mpqi.row_total_with_discount
	,mpqi.row_weight
	,mpqi.product_type
	,mpqi.base_tax_before_discount
	,mpqi.tax_before_discount
	,mpqi.original_custom_price
	,mpqi.redirect_url
	,mpqi.base_cost
	,mpqi.price_incl_tax
	,mpqi.base_price_incl_tax
	,mpqi.row_total_incl_tax
	,mpqi.base_row_total_incl_tax
	,mpqi.discount_tax_compensation_amount
	,mpqi.base_discount_tax_compensation_amount
	,mpqi.event_id
	,mpqi.weee_tax_applied
	,mpqi.weee_tax_applied_amount
	,mpqi.weee_tax_applied_row_amount
	,mpqi.weee_tax_disposition
	,mpqi.weee_tax_row_disposition
	,mpqi.base_weee_tax_applied_amount
	,mpqi.base_weee_tax_applied_row_amnt
	,mpqi.base_weee_tax_disposition
	,mpqi.base_weee_tax_row_disposition
	,mpqi.giftregistry_item_id
	,mpqi.gift_message_id
	,mpqi.gw_id
	,mpqi.gw_base_price
	,mpqi.gw_price
	,mpqi.gw_base_tax_amount
	,mpqi.gw_tax_amount
	,mpqi.free_shipping
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_quote_item mpqi
;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_quote_item()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_quote_item (
		item_id
		,quote_id
		,created_at
		,updated_at
		,product_id
		,store_id
		,parent_item_id
		,is_virtual
		,sku
		,name
		,description
		,applied_rule_ids
		,additional_data
		,is_qty_decimal
		,no_discount
		,weight
		,qty
		,price
		,base_price
		,custom_price
		,discount_percent
		,discount_amount
		,base_discount_amount
		,tax_percent
		,tax_amount
		,base_tax_amount
		,row_total
		,base_row_total
		,row_total_with_discount
		,row_weight
		,product_type
		,base_tax_before_discount
		,tax_before_discount
		,original_custom_price
		,redirect_url
		,base_cost
		,price_incl_tax
		,base_price_incl_tax
		,row_total_incl_tax
		,base_row_total_incl_tax
		,discount_tax_compensation_amount
		,base_discount_tax_compensation_amount
		,event_id
		,weee_tax_applied
		,weee_tax_applied_amount
		,weee_tax_applied_row_amount
		,weee_tax_disposition
		,weee_tax_row_disposition
		,base_weee_tax_applied_amount
		,base_weee_tax_applied_row_amnt
		,base_weee_tax_disposition
		,base_weee_tax_row_disposition
		,giftregistry_item_id
		,gift_message_id
		,gw_id
		,gw_base_price
		,gw_price
		,gw_base_tax_amount
		,gw_tax_amount
		,free_shipping
		,"load_ts"
	)

	VALUES (
		NEW.item_id
		,NEW.quote_id
		,NEW.created_at
		,NEW.updated_at
		,NEW.product_id
		,NEW.store_id
		,NEW.parent_item_id
		,NEW.is_virtual
		,NEW.sku
		,NEW."name"
		,NEW.description
		,NEW.applied_rule_ids
		,NEW.additional_data
		,NEW.is_qty_decimal
		,NEW.no_discount
		,NEW.weight
		,NEW.qty
		,NEW.price
		,NEW.base_price
		,NEW.custom_price
		,NEW.discount_percent
		,NEW.discount_amount
		,NEW.base_discount_amount
		,NEW.tax_percent
		,NEW.tax_amount
		,NEW.base_tax_amount
		,NEW.row_total
		,NEW.base_row_total
		,NEW.row_total_with_discount
		,NEW.row_weight
		,NEW.product_type
		,NEW.base_tax_before_discount
		,NEW.tax_before_discount
		,NEW.original_custom_price
		,NEW.redirect_url
		,NEW.base_cost
		,NEW.price_incl_tax
		,NEW.base_price_incl_tax
		,NEW.row_total_incl_tax
		,NEW.base_row_total_incl_tax
		,NEW.discount_tax_compensation_amount
		,NEW.base_discount_tax_compensation_amount
		,NEW.event_id
		,NEW.weee_tax_applied
		,NEW.weee_tax_applied_amount
		,NEW.weee_tax_applied_row_amount
		,NEW.weee_tax_disposition
		,NEW.weee_tax_row_disposition
		,NEW.base_weee_tax_applied_amount
		,NEW.base_weee_tax_applied_row_amnt
		,NEW.base_weee_tax_disposition
		,NEW.base_weee_tax_row_disposition
		,NEW.giftregistry_item_id
		,NEW.gift_message_id
		,NEW.gw_id
		,NEW.gw_base_price
		,NEW.gw_price
		,NEW.gw_base_tax_amount
		,NEW.gw_tax_amount
		,NEW.free_shipping
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("item_id") DO UPDATE
	SET
		quote_id = EXCLUDED.quote_id
		,created_at = EXCLUDED.created_at
		,updated_at = EXCLUDED.updated_at
		,product_id = EXCLUDED.product_id
		,store_id = EXCLUDED.store_id
		,parent_item_id = EXCLUDED.parent_item_id
		,is_virtual = EXCLUDED.is_virtual
		,sku = EXCLUDED.sku
		,name = EXCLUDED.name
		,description = EXCLUDED.description
		,applied_rule_ids = EXCLUDED.applied_rule_ids
		,additional_data = EXCLUDED.additional_data
		,is_qty_decimal = EXCLUDED.is_qty_decimal
		,no_discount = EXCLUDED.no_discount
		,weight = EXCLUDED.weight
		,qty = EXCLUDED.qty
		,price = EXCLUDED.price
		,base_price = EXCLUDED.base_price
		,custom_price = EXCLUDED.custom_price
		,discount_percent = EXCLUDED.discount_percent
		,discount_amount = EXCLUDED.discount_amount
		,base_discount_amount = EXCLUDED.base_discount_amount
		,tax_percent = EXCLUDED.tax_percent
		,tax_amount = EXCLUDED.tax_amount
		,base_tax_amount = EXCLUDED.base_tax_amount
		,row_total = EXCLUDED.row_total
		,base_row_total = EXCLUDED.base_row_total
		,row_total_with_discount = EXCLUDED.row_total_with_discount
		,row_weight = EXCLUDED.row_weight
		,product_type = EXCLUDED.product_type
		,base_tax_before_discount = EXCLUDED.base_tax_before_discount
		,tax_before_discount = EXCLUDED.tax_before_discount
		,original_custom_price = EXCLUDED.original_custom_price
		,redirect_url = EXCLUDED.redirect_url
		,base_cost = EXCLUDED.base_cost
		,price_incl_tax = EXCLUDED.price_incl_tax
		,base_price_incl_tax = EXCLUDED.base_price_incl_tax
		,row_total_incl_tax = EXCLUDED.row_total_incl_tax
		,base_row_total_incl_tax = EXCLUDED.base_row_total_incl_tax
		,discount_tax_compensation_amount = EXCLUDED.discount_tax_compensation_amount
		,base_discount_tax_compensation_amount = EXCLUDED.base_discount_tax_compensation_amount
		,event_id = EXCLUDED.event_id
		,weee_tax_applied = EXCLUDED.weee_tax_applied
		,weee_tax_applied_amount = EXCLUDED.weee_tax_applied_amount
		,weee_tax_applied_row_amount = EXCLUDED.weee_tax_applied_row_amount
		,weee_tax_disposition = EXCLUDED.weee_tax_disposition
		,weee_tax_row_disposition = EXCLUDED.weee_tax_row_disposition
		,base_weee_tax_applied_amount = EXCLUDED.base_weee_tax_applied_amount
		,base_weee_tax_applied_row_amnt = EXCLUDED.base_weee_tax_applied_row_amnt
		,base_weee_tax_disposition = EXCLUDED.base_weee_tax_disposition
		,base_weee_tax_row_disposition = EXCLUDED.base_weee_tax_row_disposition
		,giftregistry_item_id = EXCLUDED.giftregistry_item_id
		,gift_message_id = EXCLUDED.gift_message_id
		,gw_id = EXCLUDED.gw_id
		,gw_base_price = EXCLUDED.gw_base_price
		,gw_price = EXCLUDED.gw_price
		,gw_base_tax_amount = EXCLUDED.gw_base_tax_amount
		,gw_tax_amount = EXCLUDED.gw_tax_amount
		,free_shipping = EXCLUDED.free_shipping
		,"load_ts" = CURRENT_TIMESTAMP;
	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD QOUTE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_quote_item ON bronze.magento_prod_quote_item;
CREATE TRIGGER trg_upsert_magento_prod_quote_item
AFTER INSERT OR UPDATE ON bronze.magento_prod_quote_item
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_quote_item();

