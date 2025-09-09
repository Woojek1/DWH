CREATE OR REPLACE VIEW gold.v_bc_customers AS
WITH Customers_Aircon AS (
	SELECT 
		c."No" as "NoCustomer"
		,concat(c."Firma", '_', c."No") as "KeyNoCustomer"
		,c."Contact_Type" as "ContactType"
		,c."Name" as "Name"
		,c."Name_2" as "Name2"
		,c."IC_Partner_Code" as "PartnerCode"
		,c."Balance_LCY" as "BalanceLCY"
		,c."Balance_Due_LCY" as "BalanceDueLCY"
		,c."Credit_Limit_LCY" as "CreditLimitLCY"
		,c."Salesperson_Code" as "Salesperson_Code"
		,c."TotalSales2" as "TotalSales"
		,c."VIP" as "VIP"
		,c."Company_Profile" as "CompanyProfile"
		,c."Company_SegmentA" as "CompanySegmentA"
		,c."MagentoID" as "MagentoID"
		,c."EDN_Reckoning_Limit_LCY" as "ReckoningLimitLCY"
		,c."EDN_Used_Limit_LCY" as "UsedLimitLCY"
		,c."EDN_Factoring_Reckoning" as "FactoringReckoning"
		,c."EDN_Insurance_Customer" as "InsuranceCustomer"
		,c."EDN_Limit_Amount_Insur_LCY" as "LimitAmountInsurLCY"
		,c."Country_Region_Code" as "CountryRegionCode"
		,c."Address" as "Address"
		,c."City" as "City"
		,c."County" as "County"
		,c."Post_Code" as "PostCode"
		,c."VAT_Registration_No" as "VATRegistrationNo"
		,c."E_Mail"
		,c."Phone_No"
		,c."Gen_Bus_Posting_Group" as "GenBusPostingGroup"
		,c."Payment_Terms_Code" as "PaymentTermsCode"
		,c."Payment_Method_Code" as "PaymentMethodCode"
		,c."Related_company" as "RelatedCompany"
		,c."EDN_KUKE_Symbol" as "KUKE_Symbol"
		,c."Fgas_UDT_Verify" as "Fgas_UDT_Verify"
		,c."EDN_FGAS_Declaration" as "EDN_FGAS_Declaration"
		,c."Customer_Category" as "Customer_Category"
		,c."Customer_Activity" as "Customer_Activity"
		,c."load_ts" AS "LoadDate"
		,'Aircon' AS "Company"
	FROM
		silver.bc_customers_aircon c
),
Customers_Technab as (
	SELECT 
		c."No" as "NoCustomer"
		,concat(c."Firma", '_', c."No") as "KeyNoCustomer"
		,c."Contact_Type" as "ContactType"
		,c."Name" as "Name"
		,c."Name_2" as "Name2"
		,c."IC_Partner_Code" as "PartnerCode"
		,c."Balance_LCY" as "BalanceLCY"
		,c."Balance_Due_LCY" as "BalanceDueLCY"
		,c."Credit_Limit_LCY" as "CreditLimitLCY"
		,c."Salesperson_Code" as "Salesperson_Code"
		,c."TotalSales2" as "TotalSales"
		,c."VIP" as "VIP"
		,c."Company_Profile" as "CompanyProfile"
		,c."Company_SegmentA" as "CompanySegmentA"
		,c."MagentoID" as "MagentoID"
		,c."EDN_Reckoning_Limit_LCY" as "ReckoningLimitLCY"
		,c."EDN_Used_Limit_LCY" as "UsedLimitLCY"
		,c."EDN_Factoring_Reckoning" as "FactoringReckoning"
		,c."EDN_Insurance_Customer" as "InsuranceCustomer"
		,c."EDN_Limit_Amount_Insur_LCY" as "LimitAmountInsurLCY"
		,c."Country_Region_Code" as "CountryRegionCode"
		,c."Address" as "Address"
		,c."City" as "City"
		,c."County" as "County"
		,c."Post_Code" as "PostCode"
		,c."VAT_Registration_No" as "VATRegistrationNo"
		,c."E_Mail"
		,c."Phone_No"
		,c."Gen_Bus_Posting_Group" as "GenBusPostingGroup"
		,c."Payment_Terms_Code" as "PaymentTermsCode"
		,c."Payment_Method_Code" as "PaymentMethodCode"
		,c."Related_company" as "RelatedCompany"
		,c."EDN_KUKE_Symbol" as "KUKE_Symbol"
		,c."Fgas_UDT_Verify" as "Fgas_UDT_Verify"
		,c."EDN_FGAS_Declaration" as "EDN_FGAS_Declaration"
		,c."Customer_Category" as "Customer_Category"
		,c."Customer_Activity" as "Customer_Activity"
		,c."load_ts" AS "LoadDate"
		,'Technab' AS "Company"
	FROM
		silver.bc_customers_technab c
),
Customers_Zymetric as (
	SELECT 
		c."No" as "NoCustomer"
		,concat(c."Firma", '_', c."No") as "KeyNoCustomer"
		,c."Contact_Type" as "ContactType"
		,c."Name" as "Name"
		,c."Name_2" as "Name2"
		,c."IC_Partner_Code" as "PartnerCode"
		,c."Balance_LCY" as "BalanceLCY"
		,c."Balance_Due_LCY" as "BalanceDueLCY"
		,c."Credit_Limit_LCY" as "CreditLimitLCY"
		,c."Salesperson_Code" as "Salesperson_Code"
		,c."TotalSales2" as "TotalSales"
		,c."VIP" as "VIP"
		,c."Company_Profile" as "CompanyProfile"
		,c."Company_SegmentA" as "CompanySegmentA"
		,c."MagentoID" as "MagentoID"
		,c."EDN_Reckoning_Limit_LCY" as "ReckoningLimitLCY"
		,c."EDN_Used_Limit_LCY" as "UsedLimitLCY"
		,c."EDN_Factoring_Reckoning" as "FactoringReckoning"
		,c."EDN_Insurance_Customer" as "InsuranceCustomer"
		,c."EDN_Limit_Amount_Insur_LCY" as "LimitAmountInsurLCY"
		,c."Country_Region_Code" as "CountryRegionCode"
		,c."Address" as "Address"
		,c."City" as "City"
		,c."County" as "County"
		,c."Post_Code" as "PostCode"
		,c."VAT_Registration_No" as "VATRegistrationNo"
		,c."E_Mail"
		,c."Phone_No"
		,c."Gen_Bus_Posting_Group" as "GenBusPostingGroup"
		,c."Payment_Terms_Code" as "PaymentTermsCode"
		,c."Payment_Method_Code" as "PaymentMethodCode"
		,c."Related_company" as "RelatedCompany"
		,c."EDN_KUKE_Symbol" as "KUKE_Symbol"
		,c."Fgas_UDT_Verify" as "Fgas_UDT_Verify"
		,c."EDN_FGAS_Declaration" as "EDN_FGAS_Declaration"
		,c."Customer_Category" as "Customer_Category"
		,c."Customer_Activity" as "Customer_Activity"
		,c."load_ts" AS "LoadDate"
		,'Zymetric' AS "Company"
	FROM
		silver.bc_customers_zymetric c
)

SELECT *
FROM
	Customers_Aircon
UNION ALL
SELECT *
FROM
	Customers_Technab
UNION ALL
SELECT *
FROM
	Customers_Zymetric
;