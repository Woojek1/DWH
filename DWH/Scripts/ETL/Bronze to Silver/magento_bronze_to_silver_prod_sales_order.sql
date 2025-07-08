-----------------------------------------------------------------------------
-- CREATING MAGENTO PROD QOUTE TABLES IN SILVER LAYER AND FIRST LOAD
-----------------------------------------------------------------------------


CREATE TABLE IF NOT EXISTS silver.magento_prod_sales_order (
	entity_id serial4 NOT NULL
	,state varchar(32) NULL
	,status varchar(32) NULL
	,coupon_code varchar(255) NULL
	,protect_code varchar(255) NULL
	,shipping_description varchar(255) NULL
	,is_virtual int2 NULL
	,store_id int2 NULL
	,customer_id int4 NULL
	,base_discount_amount numeric(20, 4) NULL
	,base_discount_canceled numeric(20, 4) NULL
	,base_discount_invoiced numeric(20, 4) NULL
	,base_discount_refunded numeric(20, 4) NULL
	,base_grand_total numeric(20, 4) NULL
	,base_shipping_amount numeric(20, 4) NULL
	,base_shipping_canceled numeric(20, 4) NULL
	,base_shipping_invoiced numeric(20, 4) NULL
	,base_shipping_refunded numeric(20, 4) NULL
	,base_shipping_tax_amount numeric(20, 4) NULL
	,base_shipping_tax_refunded numeric(20, 4) NULL
	,base_subtotal numeric(20, 4) NULL
	,base_subtotal_canceled numeric(20, 4) NULL
	,base_subtotal_invoiced numeric(20, 4) NULL
	,base_subtotal_refunded numeric(20, 4) NULL
	,base_tax_amount numeric(20, 4) NULL
	,base_tax_canceled numeric(20, 4) NULL
	,base_tax_invoiced numeric(20, 4) NULL
	,base_tax_refunded numeric(20, 4) NULL
	,base_to_global_rate numeric(20, 4) NULL
	,base_to_order_rate numeric(20, 4) NULL
	,base_total_canceled numeric(20, 4) NULL
	,base_total_invoiced numeric(20, 4) NULL
	,base_total_invoiced_cost numeric(20, 4) NULL
	,base_total_offline_refunded numeric(20, 4) NULL
	,base_total_online_refunded numeric(20, 4) NULL
	,base_total_paid numeric(20, 4) NULL
	,base_total_qty_ordered numeric(12, 4) NULL
	,base_total_refunded numeric(20, 4) NULL
	,discount_amount numeric(20, 4) NULL
	,discount_canceled numeric(20, 4) NULL
	,discount_invoiced numeric(20, 4) NULL
	,discount_refunded numeric(20, 4) NULL
	,grand_total numeric(20, 4) NULL
	,shipping_amount numeric(20, 4) NULL
	,shipping_canceled numeric(20, 4) NULL
	,shipping_invoiced numeric(20, 4) NULL
	,shipping_refunded numeric(20, 4) NULL
	,shipping_tax_amount numeric(20, 4) NULL
	,shipping_tax_refunded numeric(20, 4) NULL
	,store_to_base_rate numeric(12, 4) NULL
	,store_to_order_rate numeric(12, 4) NULL
	,subtotal numeric(20, 4) NULL
	,subtotal_canceled numeric(20, 4) NULL
	,subtotal_invoiced numeric(20, 4) NULL
	,subtotal_refunded numeric(20, 4) NULL
	,tax_amount numeric(20, 4) NULL
	,tax_canceled numeric(20, 4) NULL
	,tax_invoiced numeric(20, 4) NULL
	,tax_refunded numeric(20, 4) NULL
	,total_canceled numeric(20, 4) NULL
	,total_invoiced numeric(20, 4) NULL
	,total_offline_refunded numeric(20, 4) NULL
	,total_online_refunded numeric(20, 4) NULL
	,total_paid numeric(20, 4) NULL
	,total_qty_ordered numeric(12, 4) NULL
	,total_refunded numeric(20, 4) NULL
	,can_ship_partially int2 NULL
	,can_ship_partially_item int2 NULL
	,customer_is_guest int2 NULL
	,customer_note_notify int2 NULL
	,billing_address_id int4 NULL
	,customer_group_id int4 NULL
	,edit_increment int4 NULL
	,email_sent int2 NULL
	,send_email int2 NULL
	,forced_shipment_with_invoice int2 NULL
	,payment_auth_expiration int4 NULL
	,quote_address_id int4 NULL
	,quote_id int4 NULL
	,shipping_address_id int4 NULL
	,adjustment_negative numeric(20, 4) NULL
	,adjustment_positive numeric(20, 4) NULL
	,base_adjustment_negative numeric(20, 4) NULL
	,base_adjustment_positive numeric(20, 4) NULL
	,base_shipping_discount_amount numeric(20, 4) NULL
	,base_subtotal_incl_tax numeric(20, 4) NULL
	,base_total_due numeric(20, 4) NULL
	,payment_authorization_amount numeric(20, 4) NULL
	,shipping_discount_amount numeric(20, 4) NULL
	,subtotal_incl_tax numeric(20, 4) NULL
	,total_due numeric(20, 4) NULL
	,weight numeric(12, 4) NULL
	,customer_dob timestamp NULL
	,increment_id varchar(50) NULL
	,applied_rule_ids varchar(128) NULL
	,base_currency_code varchar(3) NULL
	,customer_email varchar(128) NULL
	,customer_firstname varchar(128) NULL
	,customer_lastname varchar(128) NULL
	,customer_middlename varchar(128) NULL
	,customer_prefix varchar(32) NULL
	,customer_suffix varchar(32) NULL
	,customer_taxvat varchar(32) NULL
	,discount_description varchar(255) NULL
	,ext_customer_id varchar(32) NULL
	,ext_order_id varchar(32) NULL
	,global_currency_code varchar(3) NULL
	,hold_before_state varchar(32) NULL
	,hold_before_status varchar(32) NULL
	,order_currency_code varchar(3) NULL
	,original_increment_id varchar(50) NULL
	,relation_child_id varchar(32) NULL
	,relation_child_real_id varchar(32) NULL
	,relation_parent_id varchar(32) NULL
	,relation_parent_real_id varchar(32) NULL
	,remote_ip varchar(45) NULL
	,shipping_method varchar(120) NULL
	,store_currency_code varchar(3) NULL
	,store_name varchar(255) NULL
	,x_forwarded_for varchar(255) NULL
	,customer_note text NULL
	,created_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,updated_at timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
	,total_item_count int2 DEFAULT 0 NOT NULL
	,customer_gender int4 NULL
	,discount_tax_compensation_amount numeric(20, 4) NULL
	,base_discount_tax_compensation_amount numeric(20, 4) NULL
	,shipping_discount_tax_compensation_amount numeric(20, 4) NULL
	,base_shipping_discount_tax_compensation_amnt numeric(20, 4) NULL
	,discount_tax_compensation_invoiced numeric(20, 4) NULL
	,base_discount_tax_compensation_invoiced numeric(20, 4) NULL
	,discount_tax_compensation_refunded numeric(20, 4) NULL
	,base_discount_tax_compensation_refunded numeric(20, 4) NULL
	,shipping_incl_tax numeric(20, 4) NULL
	,base_shipping_incl_tax numeric(20, 4) NULL
	,coupon_rule_name varchar(255) NULL
	,base_customer_balance_amount numeric(20, 4) NULL
	,customer_balance_amount numeric(20, 4) NULL
	,base_customer_balance_invoiced numeric(20, 4) NULL
	,customer_balance_invoiced numeric(20, 4) NULL
	,base_customer_balance_refunded numeric(20, 4) NULL
	,customer_balance_refunded numeric(20, 4) NULL
	,bs_customer_bal_total_refunded numeric(20, 4) NULL
	,customer_bal_total_refunded numeric(20, 4) NULL
	,gift_cards text NULL
	,base_gift_cards_amount numeric(20, 4) NULL
	,gift_cards_amount numeric(20, 4) NULL
	,base_gift_cards_invoiced numeric(20, 4) NULL
	,gift_cards_invoiced numeric(20, 4) NULL
	,base_gift_cards_refunded numeric(20, 4) NULL
	,gift_cards_refunded numeric(20, 4) NULL
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
	,gw_base_price_invoiced numeric(12, 4) NULL
	,gw_price_invoiced numeric(12, 4) NULL
	,gw_items_base_price_invoiced numeric(12, 4) NULL
	,gw_items_price_invoiced numeric(12, 4) NULL
	,gw_card_base_price_invoiced numeric(12, 4) NULL
	,gw_card_price_invoiced numeric(12, 4) NULL
	,gw_base_tax_amount_invoiced numeric(12, 4) NULL
	,gw_tax_amount_invoiced numeric(12, 4) NULL
	,gw_items_base_tax_invoiced numeric(12, 4) NULL
	,gw_items_tax_invoiced numeric(12, 4) NULL
	,gw_card_base_tax_invoiced numeric(12, 4) NULL
	,gw_card_tax_invoiced numeric(12, 4) NULL
	,gw_base_price_refunded numeric(12, 4) NULL
	,gw_price_refunded numeric(12, 4) NULL
	,gw_items_base_price_refunded numeric(12, 4) NULL
	,gw_items_price_refunded numeric(12, 4) NULL
	,gw_card_base_price_refunded numeric(12, 4) NULL
	,gw_card_price_refunded numeric(12, 4) NULL
	,gw_base_tax_amount_refunded numeric(12, 4) NULL
	,gw_tax_amount_refunded numeric(12, 4) NULL
	,gw_items_base_tax_refunded numeric(12, 4) NULL
	,gw_items_tax_refunded numeric(12, 4) NULL
	,gw_card_base_tax_refunded numeric(12, 4) NULL
	,gw_card_tax_refunded numeric(12, 4) NULL
	,paypal_ipn_customer_notified int4 DEFAULT 0 NULL
	,reward_points_balance int4 NULL
	,base_reward_currency_amount numeric(20, 4) NULL
	,reward_currency_amount numeric(20, 4) NULL
	,base_rwrd_crrncy_amt_invoiced numeric(20, 4) NULL
	,rwrd_currency_amount_invoiced numeric(20, 4) NULL
	,base_rwrd_crrncy_amnt_refnded numeric(20, 4) NULL
	,rwrd_crrncy_amnt_refunded numeric(20, 4) NULL
	,reward_points_balance_refund int4 NULL
	,dispute_status varchar(45) NULL
	,payment_fee numeric(12, 4) DEFAULT 0.0000 NULL
	,base_payment_fee numeric(12, 4) DEFAULT 0.0000 NULL
	,payment_fee_tax numeric(12, 4) DEFAULT 0.0000 NULL
	,base_payment_fee_tax numeric(12, 4) DEFAULT 0.0000 NULL
	,order_source varchar(255) NULL
	,"load_ts" timestamptz NULL
	,CONSTRAINT magento_prod_sales_order_pkey PRIMARY KEY ("entity_id")
);


-- Pierwsze ładowanie danych z bronze do silver

INSERT INTO silver.magento_prod_sales_order (
	entity_id
	,state
	,status
	,coupon_code
	,protect_code
	,shipping_description
	,is_virtual
	,store_id
	,customer_id
	,base_discount_amount
	,base_discount_canceled
	,base_discount_invoiced
	,base_discount_refunded
	,base_grand_total
	,base_shipping_amount
	,base_shipping_canceled
	,base_shipping_invoiced
	,base_shipping_refunded
	,base_shipping_tax_amount
	,base_shipping_tax_refunded
	,base_subtotal
	,base_subtotal_canceled
	,base_subtotal_invoiced
	,base_subtotal_refunded
	,base_tax_amount
	,base_tax_canceled
	,base_tax_invoiced
	,base_tax_refunded
	,base_to_global_rate
	,base_to_order_rate
	,base_total_canceled
	,base_total_invoiced
	,base_total_invoiced_cost
	,base_total_offline_refunded
	,base_total_online_refunded
	,base_total_paid
	,base_total_qty_ordered
	,base_total_refunded
	,discount_amount
	,discount_canceled
	,discount_invoiced
	,discount_refunded
	,grand_total
	,shipping_amount
	,shipping_canceled
	,shipping_invoiced
	,shipping_refunded
	,shipping_tax_amount
	,shipping_tax_refunded
	,store_to_base_rate
	,store_to_order_rate
	,subtotal
	,subtotal_canceled
	,subtotal_invoiced
	,subtotal_refunded
	,tax_amount
	,tax_canceled
	,tax_invoiced
	,tax_refunded
	,total_canceled
	,total_invoiced
	,total_offline_refunded
	,total_online_refunded
	,total_paid
	,total_qty_ordered
	,total_refunded
	,can_ship_partially
	,can_ship_partially_item
	,customer_is_guest
	,customer_note_notify
	,billing_address_id
	,customer_group_id
	,edit_increment
	,email_sent
	,send_email
	,forced_shipment_with_invoice
	,payment_auth_expiration
	,quote_address_id
	,quote_id
	,shipping_address_id
	,adjustment_negative
	,adjustment_positive
	,base_adjustment_negative
	,base_adjustment_positive
	,base_shipping_discount_amount
	,base_subtotal_incl_tax
	,base_total_due
	,payment_authorization_amount
	,shipping_discount_amount
	,subtotal_incl_tax
	,total_due
	,weight
	,customer_dob
	,increment_id
	,applied_rule_ids
	,base_currency_code
	,customer_email
	,customer_firstname
	,customer_lastname
	,customer_middlename
	,customer_prefix
	,customer_suffix
	,customer_taxvat
	,discount_description
	,ext_customer_id
	,ext_order_id
	,global_currency_code
	,hold_before_state
	,hold_before_status
	,order_currency_code
	,original_increment_id
	,relation_child_id
	,relation_child_real_id
	,relation_parent_id
	,relation_parent_real_id
	,remote_ip
	,shipping_method
	,store_currency_code
	,store_name
	,x_forwarded_for
	,customer_note
	,created_at
	,updated_at
	,total_item_count
	,customer_gender
	,discount_tax_compensation_amount
	,base_discount_tax_compensation_amount
	,shipping_discount_tax_compensation_amount
	,base_shipping_discount_tax_compensation_amnt
	,discount_tax_compensation_invoiced
	,base_discount_tax_compensation_invoiced
	,discount_tax_compensation_refunded
	,base_discount_tax_compensation_refunded
	,shipping_incl_tax
	,base_shipping_incl_tax
	,coupon_rule_name
	,base_customer_balance_amount
	,customer_balance_amount
	,base_customer_balance_invoiced
	,customer_balance_invoiced
	,base_customer_balance_refunded
	,customer_balance_refunded
	,bs_customer_bal_total_refunded
	,customer_bal_total_refunded
	,gift_cards
	,base_gift_cards_amount
	,gift_cards_amount
	,base_gift_cards_invoiced
	,gift_cards_invoiced
	,base_gift_cards_refunded
	,gift_cards_refunded
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
	,gw_base_price_invoiced
	,gw_price_invoiced
	,gw_items_base_price_invoiced
	,gw_items_price_invoiced
	,gw_card_base_price_invoiced
	,gw_card_price_invoiced
	,gw_base_tax_amount_invoiced
	,gw_tax_amount_invoiced
	,gw_items_base_tax_invoiced
	,gw_items_tax_invoiced
	,gw_card_base_tax_invoiced
	,gw_card_tax_invoiced
	,gw_base_price_refunded
	,gw_price_refunded
	,gw_items_base_price_refunded
	,gw_items_price_refunded
	,gw_card_base_price_refunded
	,gw_card_price_refunded
	,gw_base_tax_amount_refunded
	,gw_tax_amount_refunded
	,gw_items_base_tax_refunded
	,gw_items_tax_refunded
	,gw_card_base_tax_refunded
	,gw_card_tax_refunded
	,paypal_ipn_customer_notified
	,reward_points_balance
	,base_reward_currency_amount
	,reward_currency_amount
	,base_rwrd_crrncy_amt_invoiced
	,rwrd_currency_amount_invoiced
	,base_rwrd_crrncy_amnt_refnded
	,rwrd_crrncy_amnt_refunded
	,reward_points_balance_refund
	,dispute_status
	,payment_fee
	,base_payment_fee
	,payment_fee_tax
	,base_payment_fee_tax
	,order_source
	,"load_ts"

)

SELECT
	mpso.entity_id
	,mpso.state
	,mpso.status
	,mpso.coupon_code
	,mpso.protect_code
	,mpso.shipping_description
	,mpso.is_virtual
	,mpso.store_id
	,mpso.customer_id
	,mpso.base_discount_amount
	,mpso.base_discount_canceled
	,mpso.base_discount_invoiced
	,mpso.base_discount_refunded
	,mpso.base_grand_total
	,mpso.base_shipping_amount
	,mpso.base_shipping_canceled
	,mpso.base_shipping_invoiced
	,mpso.base_shipping_refunded
	,mpso.base_shipping_tax_amount
	,mpso.base_shipping_tax_refunded
	,mpso.base_subtotal
	,mpso.base_subtotal_canceled
	,mpso.base_subtotal_invoiced
	,mpso.base_subtotal_refunded
	,mpso.base_tax_amount
	,mpso.base_tax_canceled
	,mpso.base_tax_invoiced
	,mpso.base_tax_refunded
	,mpso.base_to_global_rate
	,mpso.base_to_order_rate
	,mpso.base_total_canceled
	,mpso.base_total_invoiced
	,mpso.base_total_invoiced_cost
	,mpso.base_total_offline_refunded
	,mpso.base_total_online_refunded
	,mpso.base_total_paid
	,mpso.base_total_qty_ordered
	,mpso.base_total_refunded
	,mpso.discount_amount
	,mpso.discount_canceled
	,mpso.discount_invoiced
	,mpso.discount_refunded
	,mpso.grand_total
	,mpso.shipping_amount
	,mpso.shipping_canceled
	,mpso.shipping_invoiced
	,mpso.shipping_refunded
	,mpso.shipping_tax_amount
	,mpso.shipping_tax_refunded
	,mpso.store_to_base_rate
	,mpso.store_to_order_rate
	,mpso.subtotal
	,mpso.subtotal_canceled
	,mpso.subtotal_invoiced
	,mpso.subtotal_refunded
	,mpso.tax_amount
	,mpso.tax_canceled
	,mpso.tax_invoiced
	,mpso.tax_refunded
	,mpso.total_canceled
	,mpso.total_invoiced
	,mpso.total_offline_refunded
	,mpso.total_online_refunded
	,mpso.total_paid
	,mpso.total_qty_ordered
	,mpso.total_refunded
	,mpso.can_ship_partially
	,mpso.can_ship_partially_item
	,mpso.customer_is_guest
	,mpso.customer_note_notify
	,mpso.billing_address_id
	,mpso.customer_group_id
	,mpso.edit_increment
	,mpso.email_sent
	,mpso.send_email
	,mpso.forced_shipment_with_invoice
	,mpso.payment_auth_expiration
	,mpso.quote_address_id
	,mpso.quote_id
	,mpso.shipping_address_id
	,mpso.adjustment_negative
	,mpso.adjustment_positive
	,mpso.base_adjustment_negative
	,mpso.base_adjustment_positive
	,mpso.base_shipping_discount_amount
	,mpso.base_subtotal_incl_tax
	,mpso.base_total_due
	,mpso.payment_authorization_amount
	,mpso.shipping_discount_amount
	,mpso.subtotal_incl_tax
	,mpso.total_due
	,mpso.weight
	,mpso.customer_dob
	,mpso.increment_id
	,mpso.applied_rule_ids
	,mpso.base_currency_code
	,mpso.customer_email
	,mpso.customer_firstname
	,mpso.customer_lastname
	,mpso.customer_middlename
	,mpso.customer_prefix
	,mpso.customer_suffix
	,mpso.customer_taxvat
	,mpso.discount_description
	,mpso.ext_customer_id
	,mpso.ext_order_id
	,mpso.global_currency_code
	,mpso.hold_before_state
	,mpso.hold_before_status
	,mpso.order_currency_code
	,mpso.original_increment_id
	,mpso.relation_child_id
	,mpso.relation_child_real_id
	,mpso.relation_parent_id
	,mpso.relation_parent_real_id
	,mpso.remote_ip
	,mpso.shipping_method
	,mpso.store_currency_code
	,mpso.store_name
	,mpso.x_forwarded_for
	,mpso.customer_note
	,mpso.created_at
	,mpso.updated_at
	,mpso.total_item_count
	,mpso.customer_gender
	,mpso.discount_tax_compensation_amount
	,mpso.base_discount_tax_compensation_amount
	,mpso.shipping_discount_tax_compensation_amount
	,mpso.base_shipping_discount_tax_compensation_amnt
	,mpso.discount_tax_compensation_invoiced
	,mpso.base_discount_tax_compensation_invoiced
	,mpso.discount_tax_compensation_refunded
	,mpso.base_discount_tax_compensation_refunded
	,mpso.shipping_incl_tax
	,mpso.base_shipping_incl_tax
	,mpso.coupon_rule_name
	,mpso.base_customer_balance_amount
	,mpso.customer_balance_amount
	,mpso.base_customer_balance_invoiced
	,mpso.customer_balance_invoiced
	,mpso.base_customer_balance_refunded
	,mpso.customer_balance_refunded
	,mpso.bs_customer_bal_total_refunded
	,mpso.customer_bal_total_refunded
	,mpso.gift_cards
	,mpso.base_gift_cards_amount
	,mpso.gift_cards_amount
	,mpso.base_gift_cards_invoiced
	,mpso.gift_cards_invoiced
	,mpso.base_gift_cards_refunded
	,mpso.gift_cards_refunded
	,mpso.gift_message_id
	,mpso.gw_id
	,mpso.gw_allow_gift_receipt
	,mpso.gw_add_card
	,mpso.gw_base_price
	,mpso.gw_price
	,mpso.gw_items_base_price
	,mpso.gw_items_price
	,mpso.gw_card_base_price
	,mpso.gw_card_price
	,mpso.gw_base_tax_amount
	,mpso.gw_tax_amount
	,mpso.gw_items_base_tax_amount
	,mpso.gw_items_tax_amount
	,mpso.gw_card_base_tax_amount
	,mpso.gw_card_tax_amount
	,mpso.gw_base_price_incl_tax
	,mpso.gw_price_incl_tax
	,mpso.gw_items_base_price_incl_tax
	,mpso.gw_items_price_incl_tax
	,mpso.gw_card_base_price_incl_tax
	,mpso.gw_card_price_incl_tax
	,mpso.gw_base_price_invoiced
	,mpso.gw_price_invoiced
	,mpso.gw_items_base_price_invoiced
	,mpso.gw_items_price_invoiced
	,mpso.gw_card_base_price_invoiced
	,mpso.gw_card_price_invoiced
	,mpso.gw_base_tax_amount_invoiced
	,mpso.gw_tax_amount_invoiced
	,mpso.gw_items_base_tax_invoiced
	,mpso.gw_items_tax_invoiced
	,mpso.gw_card_base_tax_invoiced
	,mpso.gw_card_tax_invoiced
	,mpso.gw_base_price_refunded
	,mpso.gw_price_refunded
	,mpso.gw_items_base_price_refunded
	,mpso.gw_items_price_refunded
	,mpso.gw_card_base_price_refunded
	,mpso.gw_card_price_refunded
	,mpso.gw_base_tax_amount_refunded
	,mpso.gw_tax_amount_refunded
	,mpso.gw_items_base_tax_refunded
	,mpso.gw_items_tax_refunded
	,mpso.gw_card_base_tax_refunded
	,mpso.gw_card_tax_refunded
	,mpso.paypal_ipn_customer_notified
	,mpso.reward_points_balance
	,mpso.base_reward_currency_amount
	,mpso.reward_currency_amount
	,mpso.base_rwrd_crrncy_amt_invoiced
	,mpso.rwrd_currency_amount_invoiced
	,mpso.base_rwrd_crrncy_amnt_refnded
	,mpso.rwrd_crrncy_amnt_refunded
	,mpso.reward_points_balance_refund
	,mpso.dispute_status
	,mpso.payment_fee
	,mpso.base_payment_fee
	,mpso.payment_fee_tax
	,mpso.base_payment_fee_tax
	,mpso.order_source
	,CURRENT_TIMESTAMP
FROM bronze.magento_prod_sales_order mpso
;



--------------------------------------------------------------
-- CREATING DATA LOADING FUNCTION FROM BRONZE TO SILVER LAYER
--------------------------------------------------------------


CREATE OR REPLACE FUNCTION bronze.fn_upsert_magento_prod_sales_order()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO silver.magento_prod_sales_order (
		entity_id
		,state
		,status
		,coupon_code
		,protect_code
		,shipping_description
		,is_virtual
		,store_id
		,customer_id
		,base_discount_amount
		,base_discount_canceled
		,base_discount_invoiced
		,base_discount_refunded
		,base_grand_total
		,base_shipping_amount
		,base_shipping_canceled
		,base_shipping_invoiced
		,base_shipping_refunded
		,base_shipping_tax_amount
		,base_shipping_tax_refunded
		,base_subtotal
		,base_subtotal_canceled
		,base_subtotal_invoiced
		,base_subtotal_refunded
		,base_tax_amount
		,base_tax_canceled
		,base_tax_invoiced
		,base_tax_refunded
		,base_to_global_rate
		,base_to_order_rate
		,base_total_canceled
		,base_total_invoiced
		,base_total_invoiced_cost
		,base_total_offline_refunded
		,base_total_online_refunded
		,base_total_paid
		,base_total_qty_ordered
		,base_total_refunded
		,discount_amount
		,discount_canceled
		,discount_invoiced
		,discount_refunded
		,grand_total
		,shipping_amount
		,shipping_canceled
		,shipping_invoiced
		,shipping_refunded
		,shipping_tax_amount
		,shipping_tax_refunded
		,store_to_base_rate
		,store_to_order_rate
		,subtotal
		,subtotal_canceled
		,subtotal_invoiced
		,subtotal_refunded
		,tax_amount
		,tax_canceled
		,tax_invoiced
		,tax_refunded
		,total_canceled
		,total_invoiced
		,total_offline_refunded
		,total_online_refunded
		,total_paid
		,total_qty_ordered
		,total_refunded
		,can_ship_partially
		,can_ship_partially_item
		,customer_is_guest
		,customer_note_notify
		,billing_address_id
		,customer_group_id
		,edit_increment
		,email_sent
		,send_email
		,forced_shipment_with_invoice
		,payment_auth_expiration
		,quote_address_id
		,quote_id
		,shipping_address_id
		,adjustment_negative
		,adjustment_positive
		,base_adjustment_negative
		,base_adjustment_positive
		,base_shipping_discount_amount
		,base_subtotal_incl_tax
		,base_total_due
		,payment_authorization_amount
		,shipping_discount_amount
		,subtotal_incl_tax
		,total_due
		,weight
		,customer_dob
		,increment_id
		,applied_rule_ids
		,base_currency_code
		,customer_email
		,customer_firstname
		,customer_lastname
		,customer_middlename
		,customer_prefix
		,customer_suffix
		,customer_taxvat
		,discount_description
		,ext_customer_id
		,ext_order_id
		,global_currency_code
		,hold_before_state
		,hold_before_status
		,order_currency_code
		,original_increment_id
		,relation_child_id
		,relation_child_real_id
		,relation_parent_id
		,relation_parent_real_id
		,remote_ip
		,shipping_method
		,store_currency_code
		,store_name
		,x_forwarded_for
		,customer_note
		,created_at
		,updated_at
		,total_item_count
		,customer_gender
		,discount_tax_compensation_amount
		,base_discount_tax_compensation_amount
		,shipping_discount_tax_compensation_amount
		,base_shipping_discount_tax_compensation_amnt
		,discount_tax_compensation_invoiced
		,base_discount_tax_compensation_invoiced
		,discount_tax_compensation_refunded
		,base_discount_tax_compensation_refunded
		,shipping_incl_tax
		,base_shipping_incl_tax
		,coupon_rule_name
		,base_customer_balance_amount
		,customer_balance_amount
		,base_customer_balance_invoiced
		,customer_balance_invoiced
		,base_customer_balance_refunded
		,customer_balance_refunded
		,bs_customer_bal_total_refunded
		,customer_bal_total_refunded
		,gift_cards
		,base_gift_cards_amount
		,gift_cards_amount
		,base_gift_cards_invoiced
		,gift_cards_invoiced
		,base_gift_cards_refunded
		,gift_cards_refunded
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
		,gw_base_price_invoiced
		,gw_price_invoiced
		,gw_items_base_price_invoiced
		,gw_items_price_invoiced
		,gw_card_base_price_invoiced
		,gw_card_price_invoiced
		,gw_base_tax_amount_invoiced
		,gw_tax_amount_invoiced
		,gw_items_base_tax_invoiced
		,gw_items_tax_invoiced
		,gw_card_base_tax_invoiced
		,gw_card_tax_invoiced
		,gw_base_price_refunded
		,gw_price_refunded
		,gw_items_base_price_refunded
		,gw_items_price_refunded
		,gw_card_base_price_refunded
		,gw_card_price_refunded
		,gw_base_tax_amount_refunded
		,gw_tax_amount_refunded
		,gw_items_base_tax_refunded
		,gw_items_tax_refunded
		,gw_card_base_tax_refunded
		,gw_card_tax_refunded
		,paypal_ipn_customer_notified
		,reward_points_balance
		,base_reward_currency_amount
		,reward_currency_amount
		,base_rwrd_crrncy_amt_invoiced
		,rwrd_currency_amount_invoiced
		,base_rwrd_crrncy_amnt_refnded
		,rwrd_crrncy_amnt_refunded
		,reward_points_balance_refund
		,dispute_status
		,payment_fee
		,base_payment_fee
		,payment_fee_tax
		,base_payment_fee_tax
		,order_source
		,"load_ts"
	)

	VALUES (
		NEW.entity_id
		,NEW.state
		,NEW.status
		,NEW.coupon_code
		,NEW.protect_code
		,NEW.shipping_description
		,NEW.is_virtual
		,NEW.store_id
		,NEW.customer_id
		,NEW.base_discount_amount
		,NEW.base_discount_canceled
		,NEW.base_discount_invoiced
		,NEW.base_discount_refunded
		,NEW.base_grand_total
		,NEW.base_shipping_amount
		,NEW.base_shipping_canceled
		,NEW.base_shipping_invoiced
		,NEW.base_shipping_refunded
		,NEW.base_shipping_tax_amount
		,NEW.base_shipping_tax_refunded
		,NEW.base_subtotal
		,NEW.base_subtotal_canceled
		,NEW.base_subtotal_invoiced
		,NEW.base_subtotal_refunded
		,NEW.base_tax_amount
		,NEW.base_tax_canceled
		,NEW.base_tax_invoiced
		,NEW.base_tax_refunded
		,NEW.base_to_global_rate
		,NEW.base_to_order_rate
		,NEW.base_total_canceled
		,NEW.base_total_invoiced
		,NEW.base_total_invoiced_cost
		,NEW.base_total_offline_refunded
		,NEW.base_total_online_refunded
		,NEW.base_total_paid
		,NEW.base_total_qty_ordered
		,NEW.base_total_refunded
		,NEW.discount_amount
		,NEW.discount_canceled
		,NEW.discount_invoiced
		,NEW.discount_refunded
		,NEW.grand_total
		,NEW.shipping_amount
		,NEW.shipping_canceled
		,NEW.shipping_invoiced
		,NEW.shipping_refunded
		,NEW.shipping_tax_amount
		,NEW.shipping_tax_refunded
		,NEW.store_to_base_rate
		,NEW.store_to_order_rate
		,NEW.subtotal
		,NEW.subtotal_canceled
		,NEW.subtotal_invoiced
		,NEW.subtotal_refunded
		,NEW.tax_amount
		,NEW.tax_canceled
		,NEW.tax_invoiced
		,NEW.tax_refunded
		,NEW.total_canceled
		,NEW.total_invoiced
		,NEW.total_offline_refunded
		,NEW.total_online_refunded
		,NEW.total_paid
		,NEW.total_qty_ordered
		,NEW.total_refunded
		,NEW.can_ship_partially
		,NEW.can_ship_partially_item
		,NEW.customer_is_guest
		,NEW.customer_note_notify
		,NEW.billing_address_id
		,NEW.customer_group_id
		,NEW.edit_increment
		,NEW.email_sent
		,NEW.send_email
		,NEW.forced_shipment_with_invoice
		,NEW.payment_auth_expiration
		,NEW.quote_address_id
		,NEW.quote_id
		,NEW.shipping_address_id
		,NEW.adjustment_negative
		,NEW.adjustment_positive
		,NEW.base_adjustment_negative
		,NEW.base_adjustment_positive
		,NEW.base_shipping_discount_amount
		,NEW.base_subtotal_incl_tax
		,NEW.base_total_due
		,NEW.payment_authorization_amount
		,NEW.shipping_discount_amount
		,NEW.subtotal_incl_tax
		,NEW.total_due
		,NEW.weight
		,NEW.customer_dob
		,NEW.increment_id
		,NEW.applied_rule_ids
		,NEW.base_currency_code
		,NEW.customer_email
		,NEW.customer_firstname
		,NEW.customer_lastname
		,NEW.customer_middlename
		,NEW.customer_prefix
		,NEW.customer_suffix
		,NEW.customer_taxvat
		,NEW.discount_description
		,NEW.ext_customer_id
		,NEW.ext_order_id
		,NEW.global_currency_code
		,NEW.hold_before_state
		,NEW.hold_before_status
		,NEW.order_currency_code
		,NEW.original_increment_id
		,NEW.relation_child_id
		,NEW.relation_child_real_id
		,NEW.relation_parent_id
		,NEW.relation_parent_real_id
		,NEW.remote_ip
		,NEW.shipping_method
		,NEW.store_currency_code
		,NEW.store_name
		,NEW.x_forwarded_for
		,NEW.customer_note
		,NEW.created_at
		,NEW.updated_at
		,NEW.total_item_count
		,NEW.customer_gender
		,NEW.discount_tax_compensation_amount
		,NEW.base_discount_tax_compensation_amount
		,NEW.shipping_discount_tax_compensation_amount
		,NEW.base_shipping_discount_tax_compensation_amnt
		,NEW.discount_tax_compensation_invoiced
		,NEW.base_discount_tax_compensation_invoiced
		,NEW.discount_tax_compensation_refunded
		,NEW.base_discount_tax_compensation_refunded
		,NEW.shipping_incl_tax
		,NEW.base_shipping_incl_tax
		,NEW.coupon_rule_name
		,NEW.base_customer_balance_amount
		,NEW.customer_balance_amount
		,NEW.base_customer_balance_invoiced
		,NEW.customer_balance_invoiced
		,NEW.base_customer_balance_refunded
		,NEW.customer_balance_refunded
		,NEW.bs_customer_bal_total_refunded
		,NEW.customer_bal_total_refunded
		,NEW.gift_cards
		,NEW.base_gift_cards_amount
		,NEW.gift_cards_amount
		,NEW.base_gift_cards_invoiced
		,NEW.gift_cards_invoiced
		,NEW.base_gift_cards_refunded
		,NEW.gift_cards_refunded
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
		,NEW.gw_base_price_invoiced
		,NEW.gw_price_invoiced
		,NEW.gw_items_base_price_invoiced
		,NEW.gw_items_price_invoiced
		,NEW.gw_card_base_price_invoiced
		,NEW.gw_card_price_invoiced
		,NEW.gw_base_tax_amount_invoiced
		,NEW.gw_tax_amount_invoiced
		,NEW.gw_items_base_tax_invoiced
		,NEW.gw_items_tax_invoiced
		,NEW.gw_card_base_tax_invoiced
		,NEW.gw_card_tax_invoiced
		,NEW.gw_base_price_refunded
		,NEW.gw_price_refunded
		,NEW.gw_items_base_price_refunded
		,NEW.gw_items_price_refunded
		,NEW.gw_card_base_price_refunded
		,NEW.gw_card_price_refunded
		,NEW.gw_base_tax_amount_refunded
		,NEW.gw_tax_amount_refunded
		,NEW.gw_items_base_tax_refunded
		,NEW.gw_items_tax_refunded
		,NEW.gw_card_base_tax_refunded
		,NEW.gw_card_tax_refunded
		,NEW.paypal_ipn_customer_notified
		,NEW.reward_points_balance
		,NEW.base_reward_currency_amount
		,NEW.reward_currency_amount
		,NEW.base_rwrd_crrncy_amt_invoiced
		,NEW.rwrd_currency_amount_invoiced
		,NEW.base_rwrd_crrncy_amnt_refnded
		,NEW.rwrd_crrncy_amnt_refunded
		,NEW.reward_points_balance_refund
		,NEW.dispute_status
		,NEW.payment_fee
		,NEW.base_payment_fee
		,NEW.payment_fee_tax
		,NEW.base_payment_fee_tax
		,NEW.order_source
		,CURRENT_TIMESTAMP
	)
	
	ON CONFLICT ("entity_id") DO UPDATE
	SET
		state = EXCLUDED.state
		,status = EXCLUDED.status
		,coupon_code = EXCLUDED.coupon_code
		,protect_code = EXCLUDED.protect_code
		,shipping_description = EXCLUDED.shipping_description
		,is_virtual = EXCLUDED.is_virtual
		,store_id = EXCLUDED.store_id
		,customer_id = EXCLUDED.customer_id
		,base_discount_amount = EXCLUDED.base_discount_amount
		,base_discount_canceled = EXCLUDED.base_discount_canceled
		,base_discount_invoiced = EXCLUDED.base_discount_invoiced
		,base_discount_refunded = EXCLUDED.base_discount_refunded
		,base_grand_total = EXCLUDED.base_grand_total
		,base_shipping_amount = EXCLUDED.base_shipping_amount
		,base_shipping_canceled = EXCLUDED.base_shipping_canceled
		,base_shipping_invoiced = EXCLUDED.base_shipping_invoiced
		,base_shipping_refunded = EXCLUDED.base_shipping_refunded
		,base_shipping_tax_amount = EXCLUDED.base_shipping_tax_amount
		,base_shipping_tax_refunded = EXCLUDED.base_shipping_tax_refunded
		,base_subtotal = EXCLUDED.base_subtotal
		,base_subtotal_canceled = EXCLUDED.base_subtotal_canceled
		,base_subtotal_invoiced = EXCLUDED.base_subtotal_invoiced
		,base_subtotal_refunded = EXCLUDED.base_subtotal_refunded
		,base_tax_amount = EXCLUDED.base_tax_amount
		,base_tax_canceled = EXCLUDED.base_tax_canceled
		,base_tax_invoiced = EXCLUDED.base_tax_invoiced
		,base_tax_refunded = EXCLUDED.base_tax_refunded
		,base_to_global_rate = EXCLUDED.base_to_global_rate
		,base_to_order_rate = EXCLUDED.base_to_order_rate
		,base_total_canceled = EXCLUDED.base_total_canceled
		,base_total_invoiced = EXCLUDED.base_total_invoiced
		,base_total_invoiced_cost = EXCLUDED.base_total_invoiced_cost
		,base_total_offline_refunded = EXCLUDED.base_total_offline_refunded
		,base_total_online_refunded = EXCLUDED.base_total_online_refunded
		,base_total_paid = EXCLUDED.base_total_paid
		,base_total_qty_ordered = EXCLUDED.base_total_qty_ordered
		,base_total_refunded = EXCLUDED.base_total_refunded
		,discount_amount = EXCLUDED.discount_amount
		,discount_canceled = EXCLUDED.discount_canceled
		,discount_invoiced = EXCLUDED.discount_invoiced
		,discount_refunded = EXCLUDED.discount_refunded
		,grand_total = EXCLUDED.grand_total
		,shipping_amount = EXCLUDED.shipping_amount
		,shipping_canceled = EXCLUDED.shipping_canceled
		,shipping_invoiced = EXCLUDED.shipping_invoiced
		,shipping_refunded = EXCLUDED.shipping_refunded
		,shipping_tax_amount = EXCLUDED.shipping_tax_amount
		,shipping_tax_refunded = EXCLUDED.shipping_tax_refunded
		,store_to_base_rate = EXCLUDED.store_to_base_rate
		,store_to_order_rate = EXCLUDED.store_to_order_rate
		,subtotal = EXCLUDED.subtotal
		,subtotal_canceled = EXCLUDED.subtotal_canceled
		,subtotal_invoiced = EXCLUDED.subtotal_invoiced
		,subtotal_refunded = EXCLUDED.subtotal_refunded
		,tax_amount = EXCLUDED.tax_amount
		,tax_canceled = EXCLUDED.tax_canceled
		,tax_invoiced = EXCLUDED.tax_invoiced
		,tax_refunded = EXCLUDED.tax_refunded
		,total_canceled = EXCLUDED.total_canceled
		,total_invoiced = EXCLUDED.total_invoiced
		,total_offline_refunded = EXCLUDED.total_offline_refunded
		,total_online_refunded = EXCLUDED.total_online_refunded
		,total_paid = EXCLUDED.total_paid
		,total_qty_ordered = EXCLUDED.total_qty_ordered
		,total_refunded = EXCLUDED.total_refunded
		,can_ship_partially = EXCLUDED.can_ship_partially
		,can_ship_partially_item = EXCLUDED.can_ship_partially_item
		,customer_is_guest = EXCLUDED.customer_is_guest
		,customer_note_notify = EXCLUDED.customer_note_notify
		,billing_address_id = EXCLUDED.billing_address_id
		,customer_group_id = EXCLUDED.customer_group_id
		,edit_increment = EXCLUDED.edit_increment
		,email_sent = EXCLUDED.email_sent
		,send_email = EXCLUDED.send_email
		,forced_shipment_with_invoice = EXCLUDED.forced_shipment_with_invoice
		,payment_auth_expiration = EXCLUDED.payment_auth_expiration
		,quote_address_id = EXCLUDED.quote_address_id
		,quote_id = EXCLUDED.quote_id
		,shipping_address_id = EXCLUDED.shipping_address_id
		,adjustment_negative = EXCLUDED.adjustment_negative
		,adjustment_positive = EXCLUDED.adjustment_positive
		,base_adjustment_negative = EXCLUDED.base_adjustment_negative
		,base_adjustment_positive = EXCLUDED.base_adjustment_positive
		,base_shipping_discount_amount = EXCLUDED.base_shipping_discount_amount
		,base_subtotal_incl_tax = EXCLUDED.base_subtotal_incl_tax
		,base_total_due = EXCLUDED.base_total_due
		,payment_authorization_amount = EXCLUDED.payment_authorization_amount
		,shipping_discount_amount = EXCLUDED.shipping_discount_amount
		,subtotal_incl_tax = EXCLUDED.subtotal_incl_tax
		,total_due = EXCLUDED.total_due
		,weight = EXCLUDED.weight
		,customer_dob = EXCLUDED.customer_dob
		,increment_id = EXCLUDED.increment_id
		,applied_rule_ids = EXCLUDED.applied_rule_ids
		,base_currency_code = EXCLUDED.base_currency_code
		,customer_email = EXCLUDED.customer_email
		,customer_firstname = EXCLUDED.customer_firstname
		,customer_lastname = EXCLUDED.customer_lastname
		,customer_middlename = EXCLUDED.customer_middlename
		,customer_prefix = EXCLUDED.customer_prefix
		,customer_suffix = EXCLUDED.customer_suffix
		,customer_taxvat = EXCLUDED.customer_taxvat
		,discount_description = EXCLUDED.discount_description
		,ext_customer_id = EXCLUDED.ext_customer_id
		,ext_order_id = EXCLUDED.ext_order_id
		,global_currency_code = EXCLUDED.global_currency_code
		,hold_before_state = EXCLUDED.hold_before_state
		,hold_before_status = EXCLUDED.hold_before_status
		,order_currency_code = EXCLUDED.order_currency_code
		,original_increment_id = EXCLUDED.original_increment_id
		,relation_child_id = EXCLUDED.relation_child_id
		,relation_child_real_id = EXCLUDED.relation_child_real_id
		,relation_parent_id = EXCLUDED.relation_parent_id
		,relation_parent_real_id = EXCLUDED.relation_parent_real_id
		,remote_ip = EXCLUDED.remote_ip
		,shipping_method = EXCLUDED.shipping_method
		,store_currency_code = EXCLUDED.store_currency_code
		,store_name = EXCLUDED.store_name
		,x_forwarded_for = EXCLUDED.x_forwarded_for
		,customer_note = EXCLUDED.customer_note
		,created_at = EXCLUDED.created_at
		,updated_at = EXCLUDED.updated_at
		,total_item_count = EXCLUDED.total_item_count
		,customer_gender = EXCLUDED.customer_gender
		,discount_tax_compensation_amount = EXCLUDED.discount_tax_compensation_amount
		,base_discount_tax_compensation_amount = EXCLUDED.base_discount_tax_compensation_amount
		,shipping_discount_tax_compensation_amount = EXCLUDED.shipping_discount_tax_compensation_amount
		,base_shipping_discount_tax_compensation_amnt = EXCLUDED.base_shipping_discount_tax_compensation_amnt
		,discount_tax_compensation_invoiced = EXCLUDED.discount_tax_compensation_invoiced
		,base_discount_tax_compensation_invoiced = EXCLUDED.base_discount_tax_compensation_invoiced
		,discount_tax_compensation_refunded = EXCLUDED.discount_tax_compensation_refunded
		,base_discount_tax_compensation_refunded = EXCLUDED.base_discount_tax_compensation_refunded
		,shipping_incl_tax = EXCLUDED.shipping_incl_tax
		,base_shipping_incl_tax = EXCLUDED.base_shipping_incl_tax
		,coupon_rule_name = EXCLUDED.coupon_rule_name
		,base_customer_balance_amount = EXCLUDED.base_customer_balance_amount
		,customer_balance_amount = EXCLUDED.customer_balance_amount
		,base_customer_balance_invoiced = EXCLUDED.base_customer_balance_invoiced
		,customer_balance_invoiced = EXCLUDED.customer_balance_invoiced
		,base_customer_balance_refunded = EXCLUDED.base_customer_balance_refunded
		,customer_balance_refunded = EXCLUDED.customer_balance_refunded
		,bs_customer_bal_total_refunded = EXCLUDED.bs_customer_bal_total_refunded
		,customer_bal_total_refunded = EXCLUDED.customer_bal_total_refunded
		,gift_cards = EXCLUDED.gift_cards
		,base_gift_cards_amount = EXCLUDED.base_gift_cards_amount
		,gift_cards_amount = EXCLUDED.gift_cards_amount
		,base_gift_cards_invoiced = EXCLUDED.base_gift_cards_invoiced
		,gift_cards_invoiced = EXCLUDED.gift_cards_invoiced
		,base_gift_cards_refunded = EXCLUDED.base_gift_cards_refunded
		,gift_cards_refunded = EXCLUDED.gift_cards_refunded
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
		,gw_base_price_invoiced = EXCLUDED.gw_base_price_invoiced
		,gw_price_invoiced = EXCLUDED.gw_price_invoiced
		,gw_items_base_price_invoiced = EXCLUDED.gw_items_base_price_invoiced
		,gw_items_price_invoiced = EXCLUDED.gw_items_price_invoiced
		,gw_card_base_price_invoiced = EXCLUDED.gw_card_base_price_invoiced
		,gw_card_price_invoiced = EXCLUDED.gw_card_price_invoiced
		,gw_base_tax_amount_invoiced = EXCLUDED.gw_base_tax_amount_invoiced
		,gw_tax_amount_invoiced = EXCLUDED.gw_tax_amount_invoiced
		,gw_items_base_tax_invoiced = EXCLUDED.gw_items_base_tax_invoiced
		,gw_items_tax_invoiced = EXCLUDED.gw_items_tax_invoiced
		,gw_card_base_tax_invoiced = EXCLUDED.gw_card_base_tax_invoiced
		,gw_card_tax_invoiced = EXCLUDED.gw_card_tax_invoiced
		,gw_base_price_refunded = EXCLUDED.gw_base_price_refunded
		,gw_price_refunded = EXCLUDED.gw_price_refunded
		,gw_items_base_price_refunded = EXCLUDED.gw_items_base_price_refunded
		,gw_items_price_refunded = EXCLUDED.gw_items_price_refunded
		,gw_card_base_price_refunded = EXCLUDED.gw_card_base_price_refunded
		,gw_card_price_refunded = EXCLUDED.gw_card_price_refunded
		,gw_base_tax_amount_refunded = EXCLUDED.gw_base_tax_amount_refunded
		,gw_tax_amount_refunded = EXCLUDED.gw_tax_amount_refunded
		,gw_items_base_tax_refunded = EXCLUDED.gw_items_base_tax_refunded
		,gw_items_tax_refunded = EXCLUDED.gw_items_tax_refunded
		,gw_card_base_tax_refunded = EXCLUDED.gw_card_base_tax_refunded
		,gw_card_tax_refunded = EXCLUDED.gw_card_tax_refunded
		,paypal_ipn_customer_notified = EXCLUDED.paypal_ipn_customer_notified
		,reward_points_balance = EXCLUDED.reward_points_balance
		,base_reward_currency_amount = EXCLUDED.base_reward_currency_amount
		,reward_currency_amount = EXCLUDED.reward_currency_amount
		,base_rwrd_crrncy_amt_invoiced = EXCLUDED.base_rwrd_crrncy_amt_invoiced
		,rwrd_currency_amount_invoiced = EXCLUDED.rwrd_currency_amount_invoiced
		,base_rwrd_crrncy_amnt_refnded = EXCLUDED.base_rwrd_crrncy_amnt_refnded
		,rwrd_crrncy_amnt_refunded = EXCLUDED.rwrd_crrncy_amnt_refunded
		,reward_points_balance_refund = EXCLUDED.reward_points_balance_refund
		,dispute_status = EXCLUDED.dispute_status
		,payment_fee = EXCLUDED.payment_fee
		,base_payment_fee = EXCLUDED.base_payment_fee
		,payment_fee_tax = EXCLUDED.payment_fee_tax
		,base_payment_fee_tax = EXCLUDED.base_payment_fee_tax
		,order_source = EXCLUDED.order_source
		,"load_ts" = CURRENT_TIMESTAMP;
	RETURN NEW;
END;
$$;




------------------------------------------------------------------------
-- CREATING TRIGGER IN BRONZE LAYER ON MAGENTO PROD QOUTE
------------------------------------------------------------------------


DROP TRIGGER IF EXISTS trg_upsert_magento_prod_sales_order ON bronze.magento_prod_sales_order;
CREATE TRIGGER trg_upsert_magento_prod_sales_order
AFTER INSERT OR UPDATE ON bronze.magento_prod_sales_order
FOR EACH ROW
EXECUTE FUNCTION bronze.fn_upsert_magento_prod_sales_order();

