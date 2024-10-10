WITH eth_unique_events AS (
  SELECT DISTINCT evt_tx_hash, evt_block_number
  FROM raindex_ethereum.OrderBook_evt_TakeOrder
),
eth_trade_flows AS (
  SELECT
    t.block_date,
    'Ethereum' AS network,
    t.token_bought_symbol AS token_symbol,
    t.amount_usd AS usd_volume,
    'inflow' AS flow_direction,
    t.tx_hash
  FROM dex.trades AS t
  INNER JOIN eth_unique_events AS ue
    ON t.tx_hash = ue.evt_tx_hash AND t.block_number = ue.evt_block_number
  WHERE t.block_date >= TRY_CAST('2023-09-01' AS DATE)
),
eth_raw_data AS (
  SELECT
    block_date,
    network,
    token_symbol,
    SUM(usd_volume) AS usd_volume,
    COUNT(DISTINCT tx_hash) AS trade_count
  FROM eth_trade_flows
  GROUP BY
    block_date,
    network,
    token_symbol
),

-- Repeat similar steps for Polygon
polygon_unique_events AS (
  SELECT DISTINCT evt_tx_hash, evt_block_number
  FROM (
    SELECT evt_tx_hash, evt_block_number FROM raindex_polygon.OrderBook_evt_TakeOrder
    UNION ALL
    SELECT evt_tx_hash, evt_block_number FROM raindex_polygon.OrderBook_evt_TakeOrderV2
  ) AS events
),
polygon_trade_flows AS (
  SELECT
    t.block_date,
    'Polygon' AS network,
    t.token_bought_symbol AS token_symbol,
    t.amount_usd AS usd_volume,
    'inflow' AS flow_direction,
    t.tx_hash
  FROM dex.trades AS t
  INNER JOIN polygon_unique_events AS ue
    ON t.tx_hash = ue.evt_tx_hash AND t.block_number = ue.evt_block_number
  WHERE t.block_date >= TRY_CAST('2023-09-01' AS DATE)
),
polygon_raw_data AS (
  SELECT
    block_date,
    network,
    token_symbol,
    SUM(usd_volume) AS usd_volume,
    COUNT(DISTINCT tx_hash) AS trade_count
  FROM polygon_trade_flows
  GROUP BY
    block_date,
    network,
    token_symbol
),


-- Repeat similar steps for BNB
bnb_unique_events AS (
  SELECT DISTINCT evt_tx_hash, evt_block_number
  FROM (
    SELECT evt_tx_hash, evt_block_number FROM raindex_bnb.OrderBook_evt_TakeOrder
    UNION ALL
    SELECT evt_tx_hash, evt_block_number FROM raindex_bnb.OrderBook_evt_TakeOrderV2
  ) AS events
),
bnb_trade_flows AS (
  SELECT
    t.block_date,
    'BNB' AS network,
    t.token_bought_symbol AS token_symbol,
    t.amount_usd AS usd_volume,
    'inflow' AS flow_direction,
    t.tx_hash
  FROM dex.trades AS t
  INNER JOIN bnb_unique_events AS ue
    ON t.tx_hash = ue.evt_tx_hash AND t.block_number = ue.evt_block_number
  WHERE t.block_date >= TRY_CAST('2023-09-01' AS DATE)
),
bnb_raw_data AS (
  SELECT
    block_date,
    network,
    token_symbol,
    SUM(usd_volume) AS usd_volume,
    COUNT(DISTINCT tx_hash) AS trade_count
  FROM bnb_trade_flows
  GROUP BY
    block_date,
    network,
    token_symbol
),

-- Repeat similar steps for Base
base_unique_events AS (
  SELECT DISTINCT evt_tx_hash, evt_block_number
  FROM (
    SELECT evt_tx_hash, evt_block_number FROM raindex_base.OrderBook_evt_TakeOrder
    UNION ALL
    SELECT evt_tx_hash, evt_block_number FROM raindex_base.OrderBook_evt_TakeOrderV2
  ) AS events
),
base_trade_flows AS (
  SELECT
    t.block_date,
    'Base' AS network,
    t.token_bought_symbol AS token_symbol,
    t.amount_usd AS usd_volume,
    'inflow' AS flow_direction,
    t.tx_hash
  FROM dex.trades AS t
  INNER JOIN base_unique_events AS ue
    ON t.tx_hash = ue.evt_tx_hash AND t.block_number = ue.evt_block_number
  WHERE t.block_date >= TRY_CAST('2023-09-01' AS DATE)
),
base_raw_data AS (
  SELECT
    block_date,
    network,
    token_symbol,
    SUM(usd_volume) AS usd_volume,
    COUNT(DISTINCT tx_hash) AS trade_count
  FROM base_trade_flows
  GROUP BY
    block_date,
    network,
    token_symbol
),

-- Repeat similar steps for Arbitrum
arbitrum_unique_events AS (
  SELECT DISTINCT evt_tx_hash, evt_block_number
  FROM (
    SELECT evt_tx_hash, evt_block_number FROM raindex_arbitrum.OrderBook_evt_TakeOrder
    UNION ALL
    SELECT evt_tx_hash, evt_block_number FROM raindex_arbitrum.OrderBook_evt_TakeOrderV2
  ) AS events
),

arbitrum_trade_flows AS (
  SELECT
    t.block_date,
    'Arbitrum' AS network,
    t.token_bought_symbol AS token_symbol,
    t.amount_usd AS usd_volume,
    'inflow' AS flow_direction,
    t.tx_hash
  FROM dex.trades AS t
  INNER JOIN arbitrum_unique_events AS ue
    ON t.tx_hash = ue.evt_tx_hash AND t.block_number = ue.evt_block_number
  WHERE t.block_date >= TRY_CAST('2023-09-01' AS DATE)
),
arbitrum_raw_data AS (
  SELECT
    block_date,
    network,
    token_symbol,
    SUM(usd_volume) AS usd_volume,
    COUNT(DISTINCT tx_hash) AS trade_count
  FROM arbitrum_trade_flows
  GROUP BY
    block_date,
    network,
    token_symbol
),

-- Combine all networks
combined_raw_data AS (
  SELECT * FROM eth_raw_data
  UNION ALL
  SELECT * FROM polygon_raw_data
  UNION ALL
  SELECT * FROM bnb_raw_data
  UNION ALL
  SELECT * FROM base_raw_data
  UNION ALL
  SELECT * FROM arbitrum_raw_data
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
    networks.network,
    COALESCE(network_data.daily_volume, 0) AS daily_volume,
    SUM(COALESCE(network_data.daily_volume, 0)) OVER (PARTITION BY networks.network ORDER BY ts.gen_date) AS cumulative_volume,
    COALESCE(network_data.daily_trade_count, 0) AS daily_trade_count,
    SUM(COALESCE(network_data.daily_trade_count, 0)) OVER (PARTITION BY networks.network ORDER BY ts.gen_date) AS cumulative_trade_count
  FROM time_series AS ts
  CROSS JOIN (SELECT DISTINCT network FROM combined_raw_data) AS networks
  LEFT JOIN (
    SELECT
      block_date,
      network,
      SUM(usd_volume) AS daily_volume,
      SUM(trade_count) AS daily_trade_count
    FROM combined_raw_data
    GROUP BY
      block_date,
      network
  ) AS network_data
    ON ts.gen_date = network_data.block_date AND networks.network = network_data.network
  WHERE ts.gen_date <= CURRENT_DATE
)

SELECT
  day,
  network,
  daily_volume,
  cumulative_volume,
  daily_trade_count,
  cumulative_trade_count
FROM cumulative_data
ORDER BY
  day,
  network;
