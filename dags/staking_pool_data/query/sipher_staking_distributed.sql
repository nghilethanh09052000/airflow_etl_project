CREATE OR REPLACE TABLE `{{ params.bigquery_project}}.raw_staking.sipher_staking_distributed_{{ ds_nodash }}`
PARTITION BY act_date
AS
WITH distributed_tx_hash AS(
   SELECT  
    DISTINCT transaction_hash
   FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
   WHERE DATE(block_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND ( 
         LOWER(from_address) = LOWER('0x7776C65E112475Cebd4A2fC72E685f35641DB3dA')
         OR LOWER(to_address) = LOWER('0x7776C65E112475Cebd4A2fC72E685f35641DB3dA')
        )
   )

   ,filtered_distributed_tx AS(
      SELECT
         DATE(block_timestamp) AS act_date,
         token_address,
         CASE
                  WHEN LOWER(token_address) = LOWER('0x9F52c8ecbEe10e00D9faaAc5Ee9Ba0fF6550F511') THEN 'SIPHER Token'
                  WHEN LOWER(token_address) = LOWER('0xb2d1464ae4cc86856474a34d112b4a2efa326ed9') THEN 'Escrowed SIPHER Token'
                  ELSE 'N/A'
            END AS token_name,
          CAST(value AS NUMERIC) AS sipher_value,
         * EXCEPT(value, token_address)
      FROM `bigquery-public-data.crypto_ethereum.token_transfers`
      WHERE transaction_hash IN (SELECT * FROM distributed_tx_hash)
   )

   SELECT * FROM filtered_distributed_tx
   