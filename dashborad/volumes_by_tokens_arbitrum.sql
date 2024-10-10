WITH unique_events AS (
  SELECT DISTINCT evt_tx_hash, evt_block_number
  FROM (
    SELECT evt_tx_hash, evt_block_number FROM raindex_arbitrum.OrderBook_evt_TakeOrder
    UNION ALL
    SELECT evt_tx_hash, evt_block_number FROM raindex_arbitrum.OrderBook_evt_TakeOrderV2
  ) AS events
),
arbitrum_token_volume AS (
  SELECT
    t.block_date,
    t.token_bought_symbol AS token_symbol,
    SUM(t.amount_usd) AS usd_volume
  FROM dex.trades AS t
  INNER JOIN unique_events AS ue
    ON t.tx_hash = ue.evt_tx_hash AND t.block_number = ue.evt_block_number
  WHERE
    t.block_date > TRY_CAST('2023-09-01' AS DATE)
  GROUP BY
    t.block_date,
    t.token_bought_symbol
)
SELECT
  block_date,
  token_symbol AS token,
  usd_volume
FROM arbitrum_token_volume
ORDER BY
  block_date,
  token_symbol;
