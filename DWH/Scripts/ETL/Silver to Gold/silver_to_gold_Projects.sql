create or replace view gold.v_projects AS
with Projects_Aircon as (
	select
		p."No" as "NoProject"
		,concat(p."Firma", '_', p."No") as "KeyNoProject"
		,p."Description" as "ProjectName"
		,p."Description_2" as "ProjectStage"
		,p."Status" as "Status"
		,p."Creation_Date" as "CreationDate"
		,p."Manufacturer_Code" as "Manufacturer_Code"
		,p."City" as "City"
		,p."County" as "County"
		,p."Object_Type" as "ObjectType"
		,p."Project_Source" as "ProjectSource"
		,p."Manufacturer" as "Manufacturer"
		,p."Planned_Delivery_Date" as "PlannedDeliveryDate"
		,p."Project_Account_Manager" as "AccountManager"
		,p."Salesperson_Code" as "Salesperson_Code"
		,p."load_ts" as "LoadDate" 
		,'Aircon' as "Company"
	from
		silver.bc_projects_aircon p
),

Projects_Technab as (
	select
		p."No" as "NoProject"
		,concat(p."Firma", '_', p."No") as "KeyNoProject"
		,p."Description" as "ProjectName"
		,p."Description_2" as "ProjectStage"
		,p."Status" as "Status"
		,p."Creation_Date" as "CreationDate"
		,p."Manufacturer_Code" as "Manufacturer_Code"
		,p."City" as "City"
		,p."County" as "County"
		,p."Object_Type" as "ObjectType"
		,p."Project_Source" as "ProjectSource"
		,p."Manufacturer" as "Manufacturer"
		,p."Planned_Delivery_Date" as "PlannedDeliveryDate"
		,p."Project_Account_Manager" as "AccountManager"
		,p."Salesperson_Code" as "Salesperson_Code"
		,p."load_ts" as "LoadDate" 
		,'Technab' as "Company"
	from
		silver.bc_projects_technab p
),

Projects_Zymetric as (
	select
		p."No" as "NoProject"
		,concat(p."Firma", '_', p."No") as "KeyNoProject"
		,p."Description" as "ProjectName"
		,p."Description_2" as "ProjectStage"
		,p."Status" as "Status"
		,p."Creation_Date" as "CreationDate"
		,p."Manufacturer_Code" as "Manufacturer_Code"
		,p."City" as "City"
		,p."County" as "County"
		,p."Object_Type" as "ObjectType"
		,p."Project_Source" as "ProjectSource"
		,p."Manufacturer" as "Manufacturer"
		,p."Planned_Delivery_Date" as "PlannedDeliveryDate"
		,p."Project_Account_Manager" as "AccountManager"
		,p."Salesperson_Code" as "Salesperson_Code"
		,p."load_ts" as "LoadDate" 
		,'Zymetric' as "Company"
	from
		silver.bc_projects_zymetric p
)

select *
	from Projects_Aircon
union all
select *
	from Projects_Technab
union all
select *
	from Projects_Zymetric
;