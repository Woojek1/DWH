-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD QOUTE TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_quote (
	entity_id serial4 NOT NULL
	,store_id int2 DEFAULT 0 NOT NULL
	,created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,converted_at timestamp NULL
	,is_active int2 DEFAULT 1 NULL
	,is_virtual int2 DEFAULT 0 NULL
	,is_multi_shipping int2 DEFAULT 0 NULL
	,items_count int4 DEFAULT 0 NULL
	,items_qty numeric(12, 4) DEFAULT 0.0000 NULL
	,orig_order_id int4 DEFAULT 0 NULL
	,store_to_base_rate numeric(12, 4) DEFAULT 0.0000 NULL
	,store_to_quote_rate numeric(12, 4) DEFAULT 0.0000 NULL
	,base_currency_code varchar(255) NULL
	,store_currency_code varchar(255) NULL
	,quote_currency_code varchar(255) NULL
	,grand_total numeric(20, 4) DEFAULT 0.0000 NULL
	,base_grand_total numeric(20, 4) DEFAULT 0.0000 NULL
	,checkout_method varchar(255) NULL
	,customer_id int4 NULL
	,customer_tax_class_id int4 NULL
	,customer_group_id int4 DEFAULT 0 NULL
	,customer_email varchar(255) NULL
	,customer_prefix varchar(40) NULL
	,customer_firstname varchar(255) NULL
	,customer_middlename varchar(40) NULL
	,customer_lastname varchar(255) NULL
	,customer_suffix varchar(40) NULL
	,customer_dob timestamp NULL
	,customer_note text NULL
	,customer_note_notify int2 DEFAULT 1 NULL
	,customer_is_guest int2 DEFAULT 0 NULL
	,remote_ip varchar(45) NULL
	,applied_rule_ids varchar(255) NULL
	,reserved_order_id varchar(64) NULL
	,password_hash varchar(255) NULL
	,coupon_code varchar(255) NULL
	,global_currency_code varchar(255) NULL
	,base_to_global_rate numeric(20, 4) NULL
	,base_to_quote_rate numeric(20, 4) NULL
	,customer_taxvat varchar(255) NULL
	,customer_gender varchar(255) NULL
	,subtotal numeric(20, 4) NULL
	,base_subtotal numeric(20, 4) NULL
	,subtotal_with_discount numeric(20, 4) NULL
	,base_subtotal_with_discount numeric(20, 4) NULL
	,is_changed int4 NULL
	,trigger_recollect int2 DEFAULT 0 NOT NULL
	,ext_shipping_info text NULL
	,customer_balance_amount_used numeric(20, 4) NULL
	,base_customer_bal_amount_used numeric(20, 4) NULL
	,use_customer_balance int4 NULL
	,gift_cards text NULL
	,gift_cards_amount numeric(20, 4) NULL
	,base_gift_cards_amount numeric(20, 4) NULL
	,gift_cards_amount_used numeric(20, 4) NULL
	,base_gift_cards_amount_used numeric(20, 4) NULL
	,gift_message_id int4 NULL
	,gw_id int4 NULL
	,gw_allow_gift_receipt int4 NULL
	,gw_add_card int4 NULL
	,gw_base_price numeric(12, 4) NULL
	,gw_price numeric(12, 4) NULL
	,gw_items_base_price numeric(12, 4) NULL
	,gw_items_price numeric(12, 4) NULL
	,gw_card_base_price numeric(12, 4) NULL
	,gw_card_price numeric(12, 4) NULL
	,gw_base_tax_amount numeric(12, 4) NULL
	,gw_tax_amount numeric(12, 4) NULL
	,gw_items_base_tax_amount numeric(12, 4) NULL
	,gw_items_tax_amount numeric(12, 4) NULL
	,gw_card_base_tax_amount numeric(12, 4) NULL
	,gw_card_tax_amount numeric(12, 4) NULL
	,gw_base_price_incl_tax numeric(12, 4) NULL
	,gw_price_incl_tax numeric(12, 4) NULL
	,gw_items_base_price_incl_tax numeric(12, 4) NULL
	,gw_items_price_incl_tax numeric(12, 4) NULL
	,gw_card_base_price_incl_tax numeric(12, 4) NULL
	,gw_card_price_incl_tax numeric(12, 4) NULL
	,is_persistent int2 DEFAULT 0 NULL
	,use_reward_points int4 NULL
	,reward_points_balance int4 NULL
	,base_reward_currency_amount numeric(20, 4) NULL
	,reward_currency_amount numeric(20, 4) NULL
	,payment_fee numeric(12, 4) DEFAULT 0.0000 NULL
	,base_payment_fee numeric(12, 4) DEFAULT 0.0000 NULL
	,payment_fee_tax numeric(12, 4) DEFAULT 0.0000 NULL
	,base_payment_fee_tax numeric(12, 4) DEFAULT 0.0000 NULL
	,"load_ts" timestamptz NULL
	,CONSTRAINT magento_prod_quote_pkey PRIMARY KEY (entity_id)
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_quote (
	entity_id
	,store_id
	,created_at
	,updated_at
	,converted_at
	,is_active
	,is_virtual
	,is_multi_shipping
	,items_count
	,items_qty
	,orig_order_id
	,store_to_base_rate
	,store_to_quote_rate
	,base_currency_code
	,store_currency_code
	,quote_currency_code
	,grand_total
	,base_grand_total
	,checkout_method 
	,customer_id
	,customer_tax_class_id
	,customer_group_id
	,customer_email
	,customer_prefix
	,customer_firstname
	,customer_middlename
	,customer_lastname
	,customer_suffix
	,customer_dob
	,customer_note
	,customer_note_notify
	,customer_is_guest
	,remote_ip
	,applied_rule_ids
	,reserved_order_id
	,password_hash
	,coupon_code
	,global_currency_code
	,base_to_global_rate
	,base_to_quote_rate
	,customer_taxvat
	,customer_gender
	,subtotal
	,base_subtotal
	,subtotal_with_discount
	,base_subtotal_with_discount
	,is_changed
	,trigger_recollect
	,ext_shipping_info
	,customer_balance_amount_used
	,base_customer_bal_amount_used
	,use_customer_balance
	,gift_cards
	,gift_cards_amount
	,base_gift_cards_amount
	,gift_cards_amount_used
	,base_gift_cards_amount_used
	,gift_message_id
	,gw_id
	,gw_allow_gift_receipt
	,gw_add_card
	,gw_base_price
	,gw_price
	,gw_items_base_price
	,gw_items_price
	,gw_card_base_price
	,gw_card_price
	,gw_base_tax_amount
	,gw_tax_amount
	,gw_items_base_tax_amount
	,gw_items_tax_amount
	,gw_card_base_tax_amount
	,gw_card_tax_amount
	,gw_base_price_incl_tax
	,gw_price_incl_tax
	,gw_items_base_price_incl_tax
	,gw_items_price_incl_tax
	,gw_card_base_price_incl_tax
	,gw_card_price_incl_tax
	,is_persistent
	,use_reward_points
	,reward_points_balance
	,base_reward_currency_amount
	,reward_currency_amount
	,payment_fee
	,base_payment_fee
	,payment_fee_tax
	,base_payment_fee_tax
	,"load_ts"
)

SELECT
	mpq.entity_id
	,mpq.store_id
	,mpq.created_at
	,mpq.updated_at
	,mpq.converted_at
	,mpq.is_active
	,mpq.is_virtual
	,mpq.is_multi_shipping
	,mpq.items_count
	,mpq.items_qty
	,mpq.orig_order_id
	,mpq.store_to_base_rate
	,mpq.store_to_quote_rate
	,mpq.base_currency_code
	,mpq.store_currency_code
	,mpq.quote_currency_code
	,mpq.grand_total
	,mpq.base_grand_total
	,mpq.checkout_method 
	,mpq.customer_id
	,mpq.customer_tax_class_id
	,mpq.customer_group_id
	,mpq.customer_email
	,mpq.customer_prefix
	,mpq.customer_firstname
	,mpq.customer_middlename
	,mpq.customer_lastname
	,mpq.customer_suffix
	,mpq.customer_dob
	,mpq.customer_note
	,mpq.customer_note_notify
	,mpq.customer_is_guest
	,mpq.remote_ip
	,mpq.applied_rule_ids
	,mpq.reserved_order_id
	,mpq.password_hash
	,mpq.coupon_code
	,mpq.global_currency_code
	,mpq.base_to_global_rate
	,mpq.base_to_quote_rate
	,mpq.customer_taxvat
	,mpq.customer_gender
	,mpq.subtotal
	,mpq.base_subtotal
	,mpq.subtotal_with_discount
	,mpq.base_subtotal_with_discount
	,mpq.is_changed
	,mpq.trigger_recollect
	,mpq.ext_shipping_info
	,mpq.customer_balance_amount_used
	,mpq.base_customer_bal_amount_used
	,mpq.use_customer_balance
	,mpq.gift_cards
	,mpq.gift_cards_amount
	,mpq.base_gift_cards_amount
	,mpq.gift_cards_amount_used
	,mpq.base_gift_cards_amount_used
	,mpq.gift_message_id
	,mpq.gw_id
	,mpq.gw_allow_gift_receipt
	,mpq.gw_add_card
	,mpq.gw_base_price
	,mpq.gw_price
	,mpq.gw_items_base_price
	,mpq.gw_items_price
	,mpq.gw_card_base_price
	,mpq.gw_card_price
	,mpq.gw_base_tax_amount
	,mpq.gw_tax_amount
	,mpq.gw_items_base_tax_amount
	,mpq.gw_items_tax_amount
	,mpq.gw_card_base_tax_amount
	,mpq.gw_card_tax_amount
	,mpq.gw_base_price_incl_tax
	,mpq.gw_price_incl_tax
	,mpq.gw_items_base_price_incl_tax
	,mpq.gw_items_price_incl_tax
	,mpq.gw_card_base_price_incl_tax
	,mpq.gw_card_price_incl_tax
	,mpq.is_persistent
	,mpq.use_reward_points
	,mpq.reward_points_balance
	,mpq.base_reward_currency_amount
	,mpq.reward_currency_amount
	,mpq.payment_fee
	,mpq.base_payment_fee
	,mpq.payment_fee_tax
	,mpq.base_payment_fee_tax
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_quote mpq
;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_quote()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_quote (
		entity_id
		,store_id
		,created_at
		,updated_at
		,converted_at
		,is_active
		,is_virtual
		,is_multi_shipping
		,items_count
		,items_qty
		,orig_order_id
		,store_to_base_rate
		,store_to_quote_rate
		,base_currency_code
		,store_currency_code
		,quote_currency_code
		,grand_total
		,base_grand_total
		,checkout_method 
		,customer_id
		,customer_tax_class_id
		,customer_group_id
		,customer_email
		,customer_prefix
		,customer_firstname
		,customer_middlename
		,customer_lastname
		,customer_suffix
		,customer_dob
		,customer_note
		,customer_note_notify
		,customer_is_guest
		,remote_ip
		,applied_rule_ids
		,reserved_order_id
		,password_hash
		,coupon_code
		,global_currency_code
		,base_to_global_rate
		,base_to_quote_rate
		,customer_taxvat
		,customer_gender
		,subtotal
		,base_subtotal
		,subtotal_with_discount
		,base_subtotal_with_discount
		,is_changed
		,trigger_recollect
		,ext_shipping_info
		,customer_balance_amount_used
		,base_customer_bal_amount_used
		,use_customer_balance
		,gift_cards
		,gift_cards_amount
		,base_gift_cards_amount
		,gift_cards_amount_used
		,base_gift_cards_amount_used
		,gift_message_id
		,gw_id
		,gw_allow_gift_receipt
		,gw_add_card
		,gw_base_price
		,gw_price
		,gw_items_base_price
		,gw_items_price
		,gw_card_base_price
		,gw_card_price
		,gw_base_tax_amount
		,gw_tax_amount
		,gw_items_base_tax_amount
		,gw_items_tax_amount
		,gw_card_base_tax_amount
		,gw_card_tax_amount
		,gw_base_price_incl_tax
		,gw_price_incl_tax
		,gw_items_base_price_incl_tax
		,gw_items_price_incl_tax
		,gw_card_base_price_incl_tax
		,gw_card_price_incl_tax
		,is_persistent
		,use_reward_points
		,reward_points_balance
		,base_reward_currency_amount
		,reward_currency_amount
		,payment_fee
		,base_payment_fee
		,payment_fee_tax
		,base_payment_fee_tax
		,"load_ts"
	)

	VALUES (
		NEW.entity_id
		,NEW.store_id
		,NEW.created_at
		,NEW.updated_at
		,NEW.converted_at
		,NEW.is_active
		,NEW.is_virtual
		,NEW.is_multi_shipping
		,NEW.items_count
		,NEW.items_qty
		,NEW.orig_order_id
		,NEW.store_to_base_rate
		,NEW.store_to_quote_rate
		,NEW.base_currency_code
		,NEW.store_currency_code
		,NEW.quote_currency_code
		,NEW.grand_total
		,NEW.base_grand_total
		,NEW.checkout_method 
		,NEW.customer_id
		,NEW.customer_tax_class_id
		,NEW.customer_group_id
		,NEW.customer_email
		,NEW.customer_prefix
		,NEW.customer_firstname
		,NEW.customer_middlename
		,NEW.customer_lastname
		,NEW.customer_suffix
		,NEW.customer_dob
		,NEW.customer_note
		,NEW.customer_note_notify
		,NEW.customer_is_guest
		,NEW.remote_ip
		,NEW.applied_rule_ids
		,NEW.reserved_order_id
		,NEW.password_hash
		,NEW.coupon_code
		,NEW.global_currency_code
		,NEW.base_to_global_rate
		,NEW.base_to_quote_rate
		,NEW.customer_taxvat
		,NEW.customer_gender
		,NEW.subtotal
		,NEW.base_subtotal
		,NEW.subtotal_with_discount
		,NEW.base_subtotal_with_discount
		,NEW.is_changed
		,NEW.trigger_recollect
		,NEW.ext_shipping_info
		,NEW.customer_balance_amount_used
		,NEW.base_customer_bal_amount_used
		,NEW.use_customer_balance
		,NEW.gift_cards
		,NEW.gift_cards_amount
		,NEW.base_gift_cards_amount
		,NEW.gift_cards_amount_used
		,NEW.base_gift_cards_amount_used
		,NEW.gift_message_id
		,NEW.gw_id
		,NEW.gw_allow_gift_receipt
		,NEW.gw_add_card
		,NEW.gw_base_price
		,NEW.gw_price
		,NEW.gw_items_base_price
		,NEW.gw_items_price
		,NEW.gw_card_base_price
		,NEW.gw_card_price
		,NEW.gw_base_tax_amount
		,NEW.gw_tax_amount
		,NEW.gw_items_base_tax_amount
		,NEW.gw_items_tax_amount
		,NEW.gw_card_base_tax_amount
		,NEW.gw_card_tax_amount
		,NEW.gw_base_price_incl_tax
		,NEW.gw_price_incl_tax
		,NEW.gw_items_base_price_incl_tax
		,NEW.gw_items_price_incl_tax
		,NEW.gw_card_base_price_incl_tax
		,NEW.gw_card_price_incl_tax
		,NEW.is_persistent
		,NEW.use_reward_points
		,NEW.reward_points_balance
		,NEW.base_reward_currency_amount
		,NEW.reward_currency_amount
		,NEW.payment_fee
		,NEW.base_payment_fee
		,NEW.payment_fee_tax
		,NEW.base_payment_fee_tax
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("entity_id") DO UPDATE
	SET
		store_id = EXCLUDED.store_id
		,created_at = EXCLUDED.created_at
		,updated_at = EXCLUDED.updated_at
		,converted_at = EXCLUDED.converted_at
		,is_active = EXCLUDED.is_active
		,is_virtual = EXCLUDED.is_virtual
		,is_multi_shipping = EXCLUDED.is_multi_shipping
		,items_count = EXCLUDED.items_count
		,items_qty = EXCLUDED.items_qty
		,orig_order_id = EXCLUDED.orig_order_id
		,store_to_base_rate = EXCLUDED.store_to_base_rate
		,store_to_quote_rate = EXCLUDED.store_to_quote_rate
		,base_currency_code = EXCLUDED.base_currency_code
		,store_currency_code = EXCLUDED.store_currency_code
		,quote_currency_code = EXCLUDED.quote_currency_code
		,grand_total = EXCLUDED.grand_total
		,base_grand_total = EXCLUDED.base_grand_total
		,checkout_method = EXCLUDED.checkout_method
		,customer_id = EXCLUDED.customer_id
		,customer_tax_class_id = EXCLUDED.customer_tax_class_id
		,customer_group_id = EXCLUDED.customer_group_id
		,customer_email = EXCLUDED.customer_email
		,customer_prefix = EXCLUDED.customer_prefix
		,customer_firstname = EXCLUDED.customer_firstname
		,customer_middlename = EXCLUDED.customer_middlename
		,customer_lastname = EXCLUDED.customer_lastname
		,customer_suffix = EXCLUDED.customer_suffix
		,customer_dob = EXCLUDED.customer_dob
		,customer_note = EXCLUDED.customer_note
		,customer_note_notify = EXCLUDED.customer_note_notify
		,customer_is_guest = EXCLUDED.customer_is_guest
		,remote_ip = EXCLUDED.remote_ip
		,applied_rule_ids = EXCLUDED.applied_rule_ids
		,reserved_order_id = EXCLUDED.reserved_order_id
		,password_hash = EXCLUDED.password_hash
		,coupon_code = EXCLUDED.coupon_code
		,global_currency_code = EXCLUDED.global_currency_code
		,base_to_global_rate = EXCLUDED.base_to_global_rate
		,base_to_quote_rate = EXCLUDED.base_to_quote_rate
		,customer_taxvat = EXCLUDED.customer_taxvat
		,customer_gender = EXCLUDED.customer_gender
		,subtotal = EXCLUDED.subtotal
		,base_subtotal = EXCLUDED.base_subtotal
		,subtotal_with_discount = EXCLUDED.subtotal_with_discount
		,base_subtotal_with_discount = EXCLUDED.base_subtotal_with_discount
		,is_changed = EXCLUDED.is_changed
		,trigger_recollect = EXCLUDED.trigger_recollect
		,ext_shipping_info = EXCLUDED.ext_shipping_info
		,customer_balance_amount_used = EXCLUDED.customer_balance_amount_used
		,base_customer_bal_amount_used = EXCLUDED.base_customer_bal_amount_used
		,use_customer_balance = EXCLUDED.use_customer_balance
		,gift_cards = EXCLUDED.gift_cards
		,gift_cards_amount = EXCLUDED.gift_cards_amount
		,base_gift_cards_amount = EXCLUDED.base_gift_cards_amount
		,gift_cards_amount_used = EXCLUDED.gift_cards_amount_used
		,base_gift_cards_amount_used = EXCLUDED.base_gift_cards_amount_used
		,gift_message_id = EXCLUDED.gift_message_id
		,gw_id = EXCLUDED.gw_id
		,gw_allow_gift_receipt = EXCLUDED.gw_allow_gift_receipt
		,gw_add_card = EXCLUDED.gw_add_card
		,gw_base_price = EXCLUDED.gw_base_price
		,gw_price = EXCLUDED.gw_price
		,gw_items_base_price = EXCLUDED.gw_items_base_price
		,gw_items_price = EXCLUDED.gw_items_price
		,gw_card_base_price = EXCLUDED.gw_card_base_price
		,gw_card_price = EXCLUDED.gw_card_price
		,gw_base_tax_amount = EXCLUDED.gw_base_tax_amount
		,gw_tax_amount = EXCLUDED.gw_tax_amount
		,gw_items_base_tax_amount = EXCLUDED.gw_items_base_tax_amount
		,gw_items_tax_amount = EXCLUDED.gw_items_tax_amount
		,gw_card_base_tax_amount = EXCLUDED.gw_card_base_tax_amount
		,gw_card_tax_amount = EXCLUDED.gw_card_tax_amount
		,gw_base_price_incl_tax = EXCLUDED.gw_base_price_incl_tax
		,gw_price_incl_tax = EXCLUDED.gw_price_incl_tax
		,gw_items_base_price_incl_tax = EXCLUDED.gw_items_base_price_incl_tax
		,gw_items_price_incl_tax = EXCLUDED.gw_items_price_incl_tax
		,gw_card_base_price_incl_tax = EXCLUDED.gw_card_base_price_incl_tax
		,gw_card_price_incl_tax = EXCLUDED.gw_card_price_incl_tax
		,is_persistent = EXCLUDED.is_persistent
		,use_reward_points = EXCLUDED.use_reward_points
		,reward_points_balance = EXCLUDED.reward_points_balance
		,base_reward_currency_amount = EXCLUDED.base_reward_currency_amount
		,reward_currency_amount = EXCLUDED.reward_currency_amount
		,payment_fee = EXCLUDED.payment_fee
		,base_payment_fee = EXCLUDED.base_payment_fee
		,payment_fee_tax = EXCLUDED.payment_fee_tax
		,base_payment_fee_tax = EXCLUDED.base_payment_fee_tax
		,"load_ts" = CURRENT_TIMESTAMP;
	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD QOUTE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_quote ON bronze.magento_prod_quote;
CREATE TRIGGER trg_upsert_magento_prod_quote
AFTER INSERT OR UPDATE ON bronze.magento_prod_quote
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_quote();

