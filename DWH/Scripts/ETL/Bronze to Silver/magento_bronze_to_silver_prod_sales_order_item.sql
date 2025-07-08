-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD SALES ORDER ITEM TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_sales_order_item (
	item_id serial4 NOT NULL
	,order_id int4 NOT NULL
	,parent_item_id int4 NULL
	,quote_item_id int4 NULL
	,store_id int2 NULL
	,created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,product_id int4 NULL
	,product_type varchar(255) NULL
	,product_options text NULL
	,weight numeric(12, 4) DEFAULT 0.0000 NULL
	,is_virtual int2 NULL
	,sku varchar(255) NULL
	,"name" varchar(255) NULL
	,description text NULL
	,applied_rule_ids text NULL
	,additional_data text NULL
	,is_qty_decimal int2 NULL
	,no_discount int2 DEFAULT 0 NOT NULL
	,qty_backordered numeric(12, 4) DEFAULT 0.0000 NULL
	,qty_canceled numeric(12, 4) DEFAULT 0.0000 NULL
	,qty_invoiced numeric(12, 4) DEFAULT 0.0000 NULL
	,qty_ordered numeric(12, 4) DEFAULT 0.0000 NULL
	,qty_refunded numeric(12, 4) DEFAULT 0.0000 NULL
	,qty_shipped numeric(12, 4) DEFAULT 0.0000 NULL
	,base_cost numeric(12, 4) DEFAULT 0.0000 NULL
	,price numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,base_price numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,original_price numeric(20, 4) NULL
	,base_original_price numeric(20, 4) NULL
	,tax_percent numeric(12, 4) DEFAULT 0.0000 NULL
	,tax_amount numeric(20, 4) DEFAULT 0.0000 NULL
	,base_tax_amount numeric(20, 4) DEFAULT 0.0000 NULL
	,tax_invoiced numeric(20, 4) DEFAULT 0.0000 NULL
	,base_tax_invoiced numeric(20, 4) DEFAULT 0.0000 NULL
	,discount_percent numeric(12, 4) DEFAULT 0.0000 NULL
	,discount_amount numeric(20, 4) DEFAULT 0.0000 NULL
	,base_discount_amount numeric(20, 4) DEFAULT 0.0000 NULL
	,discount_invoiced numeric(20, 4) DEFAULT 0.0000 NULL
	,base_discount_invoiced numeric(20, 4) DEFAULT 0.0000 NULL
	,amount_refunded numeric(20, 4) DEFAULT 0.0000 NULL
	,base_amount_refunded numeric(20, 4) DEFAULT 0.0000 NULL
	,row_total numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,base_row_total numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,row_invoiced numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,base_row_invoiced numeric(20, 4) DEFAULT 0.0000 NOT NULL
	,row_weight numeric(12, 4) DEFAULT 0.0000 NULL
	,base_tax_before_discount numeric(20, 4) NULL
	,tax_before_discount numeric(20, 4) NULL
	,ext_order_item_id varchar(255) NULL
	,locked_do_invoice int2 NULL
	,locked_do_ship int2 NULL
	,price_incl_tax numeric(20, 4) NULL
	,base_price_incl_tax numeric(20, 4) NULL
	,row_total_incl_tax numeric(20, 4) NULL
	,base_row_total_incl_tax numeric(20, 4) NULL
	,discount_tax_compensation_amount numeric(20, 4) NULL
	,base_discount_tax_compensation_amount numeric(20, 4) NULL
	,discount_tax_compensation_invoiced numeric(20, 4) NULL
	,base_discount_tax_compensation_invoiced numeric(20, 4) NULL
	,discount_tax_compensation_refunded numeric(20, 4) NULL
	,base_discount_tax_compensation_refunded numeric(20, 4) NULL
	,tax_canceled numeric(12, 4) NULL
	,discount_tax_compensation_canceled numeric(20, 4) NULL
	,tax_refunded numeric(20, 4) NULL
	,base_tax_refunded numeric(20, 4) NULL
	,discount_refunded numeric(20, 4) NULL
	,base_discount_refunded numeric(20, 4) NULL
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
	,gift_message_available int4 NULL
	,gw_id int4 NULL
	,gw_base_price numeric(12, 4) NULL
	,gw_price numeric(12, 4) NULL
	,gw_base_tax_amount numeric(12, 4) NULL
	,gw_tax_amount numeric(12, 4) NULL
	,gw_base_price_invoiced numeric(12, 4) NULL
	,gw_price_invoiced numeric(12, 4) NULL
	,gw_base_tax_amount_invoiced numeric(12, 4) NULL
	,gw_tax_amount_invoiced numeric(12, 4) NULL
	,gw_base_price_refunded numeric(12, 4) NULL
	,gw_price_refunded numeric(12, 4) NULL
	,gw_base_tax_amount_refunded numeric(12, 4) NULL
	,gw_tax_amount_refunded numeric(12, 4) NULL
	,free_shipping int2 DEFAULT 0 NOT NULL
	,qty_returned numeric(12, 4) DEFAULT 0.0000 NOT NULL
	,"load_ts" timestamptz NULL
	,CONSTRAINT magento_prod_sales_order_item_item_pkey PRIMARY KEY (item_id)
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_sales_order_item (
	item_id
	,order_id
	,parent_item_id
	,quote_item_id
	,store_id
	,created_at
	,updated_at
	,product_id
	,product_type
	,product_options
	,weight
	,is_virtual
	,sku
	,"name"
	,description
	,applied_rule_ids
	,additional_data
	,is_qty_decimal
	,no_discount
	,qty_backordered
	,qty_canceled
	,qty_invoiced
	,qty_ordered
	,qty_refunded
	,qty_shipped
	,base_cost
	,price
	,base_price
	,original_price
	,base_original_price
	,tax_percent
	,tax_amount
	,base_tax_amount
	,tax_invoiced
	,base_tax_invoiced
	,discount_percent
	,discount_amount
	,base_discount_amount
	,discount_invoiced
	,base_discount_invoiced
	,amount_refunded
	,base_amount_refunded
	,row_total
	,base_row_total
	,row_invoiced
	,base_row_invoiced
	,row_weight
	,base_tax_before_discount
	,tax_before_discount
	,ext_order_item_id
	,locked_do_invoice
	,locked_do_ship
	,price_incl_tax
	,base_price_incl_tax
	,row_total_incl_tax
	,base_row_total_incl_tax
	,discount_tax_compensation_amount
	,base_discount_tax_compensation_amount
	,discount_tax_compensation_invoiced
	,base_discount_tax_compensation_invoiced
	,discount_tax_compensation_refunded
	,base_discount_tax_compensation_refunded
	,tax_canceled
	,discount_tax_compensation_canceled
	,tax_refunded
	,base_tax_refunded
	,discount_refunded
	,base_discount_refunded
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
	,gift_message_available
	,gw_id
	,gw_base_price
	,gw_price
	,gw_base_tax_amount
	,gw_tax_amount
	,gw_base_price_invoiced
	,gw_price_invoiced
	,gw_base_tax_amount_invoiced
	,gw_tax_amount_invoiced
	,gw_base_price_refunded
	,gw_price_refunded
	,gw_base_tax_amount_refunded
	,gw_tax_amount_refunded
	,free_shipping
	,qty_returned
	,"load_ts"

)

SELECT
	mpsoi.item_id
	,mpsoi.order_id
	,mpsoi.parent_item_id
	,mpsoi.quote_item_id
	,mpsoi.store_id
	,mpsoi.created_at
	,mpsoi.updated_at
	,mpsoi.product_id
	,mpsoi.product_type
	,mpsoi.product_options
	,mpsoi.weight
	,mpsoi.is_virtual
	,mpsoi.sku
	,mpsoi."name"
	,mpsoi.description
	,mpsoi.applied_rule_ids
	,mpsoi.additional_data
	,mpsoi.is_qty_decimal
	,mpsoi.no_discount
	,mpsoi.qty_backordered
	,mpsoi.qty_canceled
	,mpsoi.qty_invoiced
	,mpsoi.qty_ordered
	,mpsoi.qty_refunded
	,mpsoi.qty_shipped
	,mpsoi.base_cost
	,mpsoi.price
	,mpsoi.base_price
	,mpsoi.original_price
	,mpsoi.base_original_price
	,mpsoi.tax_percent
	,mpsoi.tax_amount
	,mpsoi.base_tax_amount
	,mpsoi.tax_invoiced
	,mpsoi.base_tax_invoiced
	,mpsoi.discount_percent
	,mpsoi.discount_amount
	,mpsoi.base_discount_amount
	,mpsoi.discount_invoiced
	,mpsoi.base_discount_invoiced
	,mpsoi.amount_refunded
	,mpsoi.base_amount_refunded
	,mpsoi.row_total
	,mpsoi.base_row_total
	,mpsoi.row_invoiced
	,mpsoi.base_row_invoiced
	,mpsoi.row_weight
	,mpsoi.base_tax_before_discount
	,mpsoi.tax_before_discount
	,mpsoi.ext_order_item_id
	,mpsoi.locked_do_invoice
	,mpsoi.locked_do_ship
	,mpsoi.price_incl_tax
	,mpsoi.base_price_incl_tax
	,mpsoi.row_total_incl_tax
	,mpsoi.base_row_total_incl_tax
	,mpsoi.discount_tax_compensation_amount
	,mpsoi.base_discount_tax_compensation_amount
	,mpsoi.discount_tax_compensation_invoiced
	,mpsoi.base_discount_tax_compensation_invoiced
	,mpsoi.discount_tax_compensation_refunded
	,mpsoi.base_discount_tax_compensation_refunded
	,mpsoi.tax_canceled
	,mpsoi.discount_tax_compensation_canceled
	,mpsoi.tax_refunded
	,mpsoi.base_tax_refunded
	,mpsoi.discount_refunded
	,mpsoi.base_discount_refunded
	,mpsoi.event_id
	,mpsoi.weee_tax_applied
	,mpsoi.weee_tax_applied_amount
	,mpsoi.weee_tax_applied_row_amount
	,mpsoi.weee_tax_disposition
	,mpsoi.weee_tax_row_disposition
	,mpsoi.base_weee_tax_applied_amount
	,mpsoi.base_weee_tax_applied_row_amnt
	,mpsoi.base_weee_tax_disposition
	,mpsoi.base_weee_tax_row_disposition
	,mpsoi.giftregistry_item_id
	,mpsoi.gift_message_id
	,mpsoi.gift_message_available
	,mpsoi.gw_id
	,mpsoi.gw_base_price
	,mpsoi.gw_price
	,mpsoi.gw_base_tax_amount
	,mpsoi.gw_tax_amount
	,mpsoi.gw_base_price_invoiced
	,mpsoi.gw_price_invoiced
	,mpsoi.gw_base_tax_amount_invoiced
	,mpsoi.gw_tax_amount_invoiced
	,mpsoi.gw_base_price_refunded
	,mpsoi.gw_price_refunded
	,mpsoi.gw_base_tax_amount_refunded
	,mpsoi.gw_tax_amount_refunded
	,mpsoi.free_shipping
	,mpsoi.qty_returned
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_sales_order_item mpsoi
;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_sales_order_item()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_sales_order_item (
		item_id
		,order_id
		,parent_item_id
		,quote_item_id
		,store_id
		,created_at
		,updated_at
		,product_id
		,product_type
		,product_options
		,weight
		,is_virtual
		,sku
		,"name"
		,description
		,applied_rule_ids
		,additional_data
		,is_qty_decimal
		,no_discount
		,qty_backordered
		,qty_canceled
		,qty_invoiced
		,qty_ordered
		,qty_refunded
		,qty_shipped
		,base_cost
		,price
		,base_price
		,original_price
		,base_original_price
		,tax_percent
		,tax_amount
		,base_tax_amount
		,tax_invoiced
		,base_tax_invoiced
		,discount_percent
		,discount_amount
		,base_discount_amount
		,discount_invoiced
		,base_discount_invoiced
		,amount_refunded
		,base_amount_refunded
		,row_total
		,base_row_total
		,row_invoiced
		,base_row_invoiced
		,row_weight
		,base_tax_before_discount
		,tax_before_discount
		,ext_order_item_id
		,locked_do_invoice
		,locked_do_ship
		,price_incl_tax
		,base_price_incl_tax
		,row_total_incl_tax
		,base_row_total_incl_tax
		,discount_tax_compensation_amount
		,base_discount_tax_compensation_amount
		,discount_tax_compensation_invoiced
		,base_discount_tax_compensation_invoiced
		,discount_tax_compensation_refunded
		,base_discount_tax_compensation_refunded
		,tax_canceled
		,discount_tax_compensation_canceled
		,tax_refunded
		,base_tax_refunded
		,discount_refunded
		,base_discount_refunded
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
		,gift_message_available
		,gw_id
		,gw_base_price
		,gw_price
		,gw_base_tax_amount
		,gw_tax_amount
		,gw_base_price_invoiced
		,gw_price_invoiced
		,gw_base_tax_amount_invoiced
		,gw_tax_amount_invoiced
		,gw_base_price_refunded
		,gw_price_refunded
		,gw_base_tax_amount_refunded
		,gw_tax_amount_refunded
		,free_shipping
		,qty_returned
		,"load_ts"
	)

	VALUES (
		NEW.item_id
		,NEW.order_id
		,NEW.parent_item_id
		,NEW.quote_item_id
		,NEW.store_id
		,NEW.created_at
		,NEW.updated_at
		,NEW.product_id
		,NEW.product_type
		,NEW.product_options
		,NEW.weight
		,NEW.is_virtual
		,NEW.sku
		,NEW."name"
		,NEW.description
		,NEW.applied_rule_ids
		,NEW.additional_data
		,NEW.is_qty_decimal
		,NEW.no_discount
		,NEW.qty_backordered
		,NEW.qty_canceled
		,NEW.qty_invoiced
		,NEW.qty_ordered
		,NEW.qty_refunded
		,NEW.qty_shipped
		,NEW.base_cost
		,NEW.price
		,NEW.base_price
		,NEW.original_price
		,NEW.base_original_price
		,NEW.tax_percent
		,NEW.tax_amount
		,NEW.base_tax_amount
		,NEW.tax_invoiced
		,NEW.base_tax_invoiced
		,NEW.discount_percent
		,NEW.discount_amount
		,NEW.base_discount_amount
		,NEW.discount_invoiced
		,NEW.base_discount_invoiced
		,NEW.amount_refunded
		,NEW.base_amount_refunded
		,NEW.row_total
		,NEW.base_row_total
		,NEW.row_invoiced
		,NEW.base_row_invoiced
		,NEW.row_weight
		,NEW.base_tax_before_discount
		,NEW.tax_before_discount
		,NEW.ext_order_item_id
		,NEW.locked_do_invoice
		,NEW.locked_do_ship
		,NEW.price_incl_tax
		,NEW.base_price_incl_tax
		,NEW.row_total_incl_tax
		,NEW.base_row_total_incl_tax
		,NEW.discount_tax_compensation_amount
		,NEW.base_discount_tax_compensation_amount
		,NEW.discount_tax_compensation_invoiced
		,NEW.base_discount_tax_compensation_invoiced
		,NEW.discount_tax_compensation_refunded
		,NEW.base_discount_tax_compensation_refunded
		,NEW.tax_canceled
		,NEW.discount_tax_compensation_canceled
		,NEW.tax_refunded
		,NEW.base_tax_refunded
		,NEW.discount_refunded
		,NEW.base_discount_refunded
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
		,NEW.gift_message_available
		,NEW.gw_id
		,NEW.gw_base_price
		,NEW.gw_price
		,NEW.gw_base_tax_amount
		,NEW.gw_tax_amount
		,NEW.gw_base_price_invoiced
		,NEW.gw_price_invoiced
		,NEW.gw_base_tax_amount_invoiced
		,NEW.gw_tax_amount_invoiced
		,NEW.gw_base_price_refunded
		,NEW.gw_price_refunded
		,NEW.gw_base_tax_amount_refunded
		,NEW.gw_tax_amount_refunded
		,NEW.free_shipping
		,NEW.qty_returned
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("item_id") DO UPDATE
	SET
		order_id = EXCLUDED.order_id
		,parent_item_id = EXCLUDED.parent_item_id
		,quote_item_id = EXCLUDED.quote_item_id
		,store_id = EXCLUDED.store_id
		,created_at = EXCLUDED.created_at
		,updated_at = EXCLUDED.updated_at
		,product_id = EXCLUDED.product_id
		,product_type = EXCLUDED.product_type
		,product_options = EXCLUDED.product_options
		,weight = EXCLUDED.weight
		,is_virtual = EXCLUDED.is_virtual
		,sku = EXCLUDED.sku
		,"name" = EXCLUDED."name"
		,description = EXCLUDED.description
		,applied_rule_ids = EXCLUDED.applied_rule_ids
		,additional_data = EXCLUDED.additional_data
		,is_qty_decimal = EXCLUDED.is_qty_decimal
		,no_discount = EXCLUDED.no_discount
		,qty_backordered = EXCLUDED.qty_backordered
		,qty_canceled = EXCLUDED.qty_canceled
		,qty_invoiced = EXCLUDED.qty_invoiced
		,qty_ordered = EXCLUDED.qty_ordered
		,qty_refunded = EXCLUDED.qty_refunded
		,qty_shipped = EXCLUDED.qty_shipped
		,base_cost = EXCLUDED.base_cost
		,price = EXCLUDED.price
		,base_price = EXCLUDED.base_price
		,original_price = EXCLUDED.original_price
		,base_original_price = EXCLUDED.base_original_price
		,tax_percent = EXCLUDED.tax_percent
		,tax_amount = EXCLUDED.tax_amount
		,base_tax_amount = EXCLUDED.base_tax_amount
		,tax_invoiced = EXCLUDED.tax_invoiced
		,base_tax_invoiced = EXCLUDED.base_tax_invoiced
		,discount_percent = EXCLUDED.discount_percent
		,discount_amount = EXCLUDED.discount_amount
		,base_discount_amount = EXCLUDED.base_discount_amount
		,discount_invoiced = EXCLUDED.discount_invoiced
		,base_discount_invoiced = EXCLUDED.base_discount_invoiced
		,amount_refunded = EXCLUDED.amount_refunded
		,base_amount_refunded = EXCLUDED.base_amount_refunded
		,row_total = EXCLUDED.row_total
		,base_row_total = EXCLUDED.base_row_total
		,row_invoiced = EXCLUDED.row_invoiced
		,base_row_invoiced = EXCLUDED.base_row_invoiced
		,row_weight = EXCLUDED.row_weight
		,base_tax_before_discount = EXCLUDED.base_tax_before_discount
		,tax_before_discount = EXCLUDED.tax_before_discount
		,ext_order_item_id = EXCLUDED.ext_order_item_id
		,locked_do_invoice = EXCLUDED.locked_do_invoice
		,locked_do_ship = EXCLUDED.locked_do_ship
		,price_incl_tax = EXCLUDED.price_incl_tax
		,base_price_incl_tax = EXCLUDED.base_price_incl_tax
		,row_total_incl_tax = EXCLUDED.row_total_incl_tax
		,base_row_total_incl_tax = EXCLUDED.base_row_total_incl_tax
		,discount_tax_compensation_amount = EXCLUDED.discount_tax_compensation_amount
		,base_discount_tax_compensation_amount = EXCLUDED.base_discount_tax_compensation_amount
		,discount_tax_compensation_invoiced = EXCLUDED.discount_tax_compensation_invoiced
		,base_discount_tax_compensation_invoiced = EXCLUDED.base_discount_tax_compensation_invoiced
		,discount_tax_compensation_refunded = EXCLUDED.discount_tax_compensation_refunded
		,base_discount_tax_compensation_refunded = EXCLUDED.base_discount_tax_compensation_refunded
		,tax_canceled = EXCLUDED.tax_canceled
		,discount_tax_compensation_canceled = EXCLUDED.discount_tax_compensation_canceled
		,tax_refunded = EXCLUDED.tax_refunded
		,base_tax_refunded = EXCLUDED.base_tax_refunded
		,discount_refunded = EXCLUDED.discount_refunded
		,base_discount_refunded = EXCLUDED.base_discount_refunded
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
		,gift_message_available = EXCLUDED.gift_message_available
		,gw_id = EXCLUDED.gw_id
		,gw_base_price = EXCLUDED.gw_base_price
		,gw_price = EXCLUDED.gw_price
		,gw_base_tax_amount = EXCLUDED.gw_base_tax_amount
		,gw_tax_amount = EXCLUDED.gw_tax_amount
		,gw_base_price_invoiced = EXCLUDED.gw_base_price_invoiced
		,gw_price_invoiced = EXCLUDED.gw_price_invoiced
		,gw_base_tax_amount_invoiced = EXCLUDED.gw_base_tax_amount_invoiced
		,gw_tax_amount_invoiced = EXCLUDED.gw_tax_amount_invoiced
		,gw_base_price_refunded = EXCLUDED.gw_base_price_refunded
		,gw_price_refunded = EXCLUDED.gw_price_refunded
		,gw_base_tax_amount_refunded = EXCLUDED.gw_base_tax_amount_refunded
		,gw_tax_amount_refunded = EXCLUDED.gw_tax_amount_refunded
		,free_shipping = EXCLUDED.free_shipping
		,qty_returned = EXCLUDED.qty_returned
		,"load_ts" = CURRENT_TIMESTAMP;
	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD SALES ORDER ITEM
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_sales_order_item ON bronze.magento_prod_sales_order_item;
CREATE TRIGGER trg_upsert_magento_prod_sales_order_item
AFTER INSERT OR UPDATE ON bronze.magento_prod_sales_order_item
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_sales_order_item();

