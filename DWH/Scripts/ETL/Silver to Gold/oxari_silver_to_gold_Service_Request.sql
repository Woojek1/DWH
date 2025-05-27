CREATE OR REPLACE VIEW gold.v_oxari_service_request as(
SELECT *
FROM silver.oxari_service_request
order by 
	"load_ts" DESC
)