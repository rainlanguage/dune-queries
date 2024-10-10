WITH eth_raw_data AS (
  SELECT
    t.block_date,
    SUM(t.amount_usd) AS usd_volume,
    COUNT(DISTINCT t.tx_hash) AS trade_count
  FROM dex.trades AS t
  INNER JOIN raindex_ethereum.OrderBook_evt_TakeOrder AS eth_evt
    ON t.tx_hash = eth_evt.evt_tx_hash AND t.block_number = eth_evt.evt_block_number
  WHERE
    t.block_date >= TRY_CAST('2023-09-01' AS DATE)
  GROUP BY t.block_date
),
polygon_raw_data AS (
  SELECT
    t.block_date,
    SUM(t.amount_usd) AS usd_volume,
    COUNT(DISTINCT t.tx_hash) AS trade_count
  FROM dex.trades AS t
  INNER JOIN (
    SELECT evt_tx_hash, evt_block_number FROM raindex_polygon.OrderBook_evt_TakeOrder
    UNION ALL
    SELECT evt_tx_hash, evt_block_number FROM raindex_polygon.OrderBook_evt_TakeOrderV2
  ) AS poly_evt
    ON t.tx_hash = poly_evt.evt_tx_hash AND t.block_number = poly_evt.evt_block_number
  WHERE
    t.block_date >= TRY_CAST('2023-09-01' AS DATE)
  GROUP BY t.block_date
),
bnb_raw_data AS (
  SELECT
    t.block_date,
    SUM(t.amount_usd) AS usd_volume,
    COUNT(DISTINCT t.tx_hash) AS trade_count
  FROM dex.trades AS t
  INNER JOIN (
    SELECT evt_tx_hash, evt_block_number FROM raindex_bnb.OrderBook_evt_TakeOrder
    UNION ALL
    SELECT evt_tx_hash, evt_block_number FROM raindex_bnb.OrderBook_evt_TakeOrderV2
  ) AS bnb_evt
    ON t.tx_hash = bnb_evt.evt_tx_hash AND t.block_number = bnb_evt.evt_block_number
  WHERE
    t.block_date >= TRY_CAST('2023-09-01' AS DATE)
  GROUP BY t.block_date
),
base_raw_data AS (
  SELECT
    t.block_date,
    SUM(t.amount_usd) AS usd_volume,
    COUNT(DISTINCT t.tx_hash) AS trade_count
  FROM dex.trades AS t
  INNER JOIN (
    SELECT evt_tx_hash, evt_block_number FROM raindex_base.OrderBook_evt_TakeOrder
    UNION ALL
    SELECT evt_tx_hash, evt_block_number FROM raindex_base.OrderBook_evt_TakeOrderV2
  ) AS base_evt
    ON t.tx_hash = base_evt.evt_tx_hash AND t.block_number = base_evt.evt_block_number
  WHERE
    t.block_date >= TRY_CAST('2023-09-01' AS DATE)
  GROUP BY t.block_date
),
arbitrum_raw_data AS (
  SELECT
    t.block_date,
    SUM(t.amount_usd) AS usd_volume,
    COUNT(DISTINCT t.tx_hash) AS trade_count
  FROM dex.trades AS t
  INNER JOIN (
    SELECT evt_tx_hash, evt_block_number FROM raindex_arbitrum.OrderBook_evt_TakeOrder
    UNION ALL
    SELECT evt_tx_hash, evt_block_number FROM raindex_arbitrum.OrderBook_evt_TakeOrderV2
  ) AS arb_evt
    ON t.tx_hash = arb_evt.evt_tx_hash AND t.block_number = arb_evt.evt_block_number
  WHERE
    t.block_date >= TRY_CAST('2023-09-01' AS DATE)
  GROUP BY t.block_date
),
combined_raw_data AS (
  SELECT
    block_date,
    SUM(usd_volume) AS usd_volume,
    SUM(trade_count) AS trade_count
  FROM (
    SELECT * FROM eth_raw_data
    UNION ALL
    SELECT * FROM polygon_raw_data
    UNION ALL
    SELECT * FROM bnb_raw_data
    UNION ALL
    SELECT * FROM base_raw_data
    UNION ALL
    SELECT * FROM arbitrum_raw_data
  ) AS combined
  GROUP BY
    block_date
),
time_series AS (
  SELECT
    gen_date
  FROM UNNEST(SEQUENCE(
    TRY_CAST('2023-09-01' AS DATE),
    CURRENT_DATE,
    INTERVAL '1' DAY
  )) AS tbl(gen_date)
),
cumulative_data AS (
  SELECT
    ts.gen_date AS day,
    SUM(COALESCE(rd.usd_volume, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_volume,
    SUM(COALESCE(rd.trade_count, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_trade_count,
    SUM(COALESCE(er.usd_volume, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_eth_volume,
    SUM(COALESCE(er.trade_count, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_eth_trade_count,
    SUM(COALESCE(pr.usd_volume, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_polygon_volume,
    SUM(COALESCE(pr.trade_count, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_polygon_trade_count,
    SUM(COALESCE(br.usd_volume, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_bnb_volume,
    SUM(COALESCE(br.trade_count, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_bnb_trade_count,
    SUM(COALESCE(ba.usd_volume, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_base_volume,
    SUM(COALESCE(ba.trade_count, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_base_trade_count,
    SUM(COALESCE(ar.usd_volume, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_arbitrum_volume,
    SUM(COALESCE(ar.trade_count, 0)) OVER (ORDER BY ts.gen_date) AS cumulative_arbitrum_trade_count
  FROM time_series AS ts
  LEFT JOIN combined_raw_data AS rd
    ON ts.gen_date = rd.block_date
  LEFT JOIN eth_raw_data AS er
    ON ts.gen_date = er.block_date
  LEFT JOIN polygon_raw_data AS pr
    ON ts.gen_date = pr.block_date
  LEFT JOIN bnb_raw_data AS br
    ON ts.gen_date = br.block_date
  LEFT JOIN base_raw_data AS ba
    ON ts.gen_date = ba.block_date
  LEFT JOIN arbitrum_raw_data AS ar
    ON ts.gen_date = ar.block_date
)
SELECT
  day,
  cumulative_volume,
  cumulative_trade_count,
  cumulative_eth_volume,
  cumulative_eth_trade_count,
  cumulative_polygon_volume,
  cumulative_polygon_trade_count,
  cumulative_bnb_volume,
  cumulative_bnb_trade_count,
  cumulative_base_volume,
  cumulative_base_trade_count,
  cumulative_arbitrum_volume,
  cumulative_arbitrum_trade_count
FROM cumulative_data
ORDER BY
  day DESC;
