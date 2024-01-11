{{- config(
    materialized='table',
    partition='act_date'
)-}}

WITH distributed_tx_hash AS(
   SELECT  
    DISTINCT transaction_hash
   FROM `bigquery-public-data.crypto_ethereum.token_transfers` 
   WHERE DATE(block_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
      AND ( 
         LOWER(from_address) = '{{ var("sipher_staking_manager")}}'
         OR LOWER(to_address) = '{{ var("sipher_staking_manager")}}'
        )
   )

   ,filtered_distributed_tx AS(
      SELECT
         DATE(block_timestamp) AS act_date,
         token_address,
         CASE
                  WHEN LOWER(token_address) = '{{ var("sipher_token_wallet_address")}}' THEN 'SIPHER Token'
                  WHEN LOWER(token_address) = '{{ var("escrowed_sipher_address")}}' THEN 'Escrowed SIPHER Token'
                  ELSE 'N/A'
            END AS token_name,
          CAST(value AS NUMERIC) AS sipher_value,
         * EXCEPT(value, token_address)
      FROM `bigquery-public-data.crypto_ethereum.token_transfers`
      WHERE transaction_hash IN (SELECT * FROM distributed_tx_hash)
   )

   SELECT * FROM filtered_distributed_tx
   