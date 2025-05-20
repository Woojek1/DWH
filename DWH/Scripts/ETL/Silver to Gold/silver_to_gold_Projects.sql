select * from silver.bc_projects_aircon

-------------------------------------------------

create or replace view gold.v_Projekty AS
with Projekty_Aircon as (
	select
		concat("Firma", '_', "No") as "Klucz projektu"
		,"No" as "Numer projektu"
		,"Description" as "Nazwa projektu"
		,"Description_2" as "Etap projektu"
		,"Status" as "Status"
		,"Creation_Date" as "Data utworzenia"
		,"Manufacturer_Code" as "Kod Producenta"
		,"City" as "Miasto"
		,"County" as "Wojewodztwo"
		,"Object_Type" as "Typ obiektu"
		,"Project_Source" as "Zrodlo projektu"
		,"Manufacturer" as "Producent"
		,"Planned_Delivery_Date" as "Planowana data dostawy"
		,"Project_Account_Manager" as "Account Manager"
		,"Salesperson_Code" as "Kod sprzedawcy"
		,'Aircon' AS "Firma"
	from
		silver.bc_projects_aircon
),

Projekty_Technab as (
	select
		concat("Firma", '_', "No") as "Klucz projektu"
		,"No" as "Numer projektu"
		,"Description" as "Nazwa projektu"
		,"Description_2" as "Etap projektu"
		,"Status" as "Status"
		,"Creation_Date" as "Data utworzenia"
		,"Manufacturer_Code" as "Kod Producenta"
		,"City" as "Miasto"
		,"County" as "Wojewodztwo"
		,"Object_Type" as "Typ obiektu"
		,"Project_Source" as "Zrodlo projektu"
		,"Manufacturer" as "Producent"
		,"Planned_Delivery_Date" as "Planowana data dostawy"
		,"Project_Account_Manager" as "Account Manager"
		,"Salesperson_Code" as "Kod sprzedawcy"
		,'Technab' AS "Firma"
	from
		silver.bc_projects_technab
),

Projekty_Zymetric as (
	select
		concat("Firma", '_', "No") as "Klucz projektu"
		,"No" as "Numer projektu"
		,"Description" as "Nazwa projektu"
		,"Description_2" as "Etap projektu"
		,"Status" as "Status"
		,"Creation_Date" as "Data utworzenia"
		,"Manufacturer_Code" as "Kod Producenta"
		,"City" as "Miasto"
		,"County" as "Wojewodztwo"
		,"Object_Type" as "Typ obiektu"
		,"Project_Source" as "Zrodlo projektu"
		,"Manufacturer" as "Producent"
		,"Planned_Delivery_Date" as "Planowana data dostawy"
		,"Project_Account_Manager" as "Account Manager"
		,"Salesperson_Code" as "Kod sprzedawcy"
		,'Zymetric' AS "Firma"
	from
		silver.bc_projects_zymetric
)

select *
	from Projekty_Aircon
union all
select *
	from Projekty_Technab
union all
select *
	from Projekty_Zymetric
;