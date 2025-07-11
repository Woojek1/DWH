CREATE OR REPLACE VIEW gold.v_magento_prod_sales_order AS
SELECT
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
FROM
	silver.magento_prod_sales_order