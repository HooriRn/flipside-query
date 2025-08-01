// Change outbound asset here
const outbound_asset = 'ETH.ETH'

const sql = `
WITH
true_memos AS (
  SELECT DISTINCT
    TX_ID,
    FIRST_VALUE(MEMO) OVER (PARTITION BY TX_ID ORDER BY EVENT_ID) AS MEMO
  FROM thorchain.defi.fact_swaps_events
  WHERE TRUE
    AND UPPER(SPLIT_PART(MEMO, ':', 1)) IN ('SWAP', 'S', '=')
),
attempted_swaps AS (
    SELECT
      s.TX_ID,
      CASE UPPER(SPLIT_PART(REGEXP_REPLACE(SPLIT_PART(m.MEMO, ':', 2), '[~/]{1}', '.'), '-', 1))
        WHEN 'A' THEN 'AVAX.AVAX'
        WHEN 'B' THEN 'BTC.BTC'
        WHEN 'C' THEN 'BCH.BCH'
        WHEN 'N' THEN 'BNB.BNB'
        WHEN 'S' THEN 'BSC.BNB'
        WHEN 'D' THEN 'DOGE.DOGE'
        WHEN 'E' THEN 'ETH.ETH'
        WHEN 'G' THEN 'GAIA.ATOM'
        WHEN 'L' THEN 'LTC.LTC'
        WHEN 'F' THEN 'BASE.ETH'
        WHEN 'X' THEN 'XRP.XRP'
        WHEN 'R' THEN 'THOR.RUNE'
        ELSE UPPER(SPLIT_PART(REGEXP_REPLACE(SPLIT_PART(m.MEMO, ':', 2), '[~/]{1}', '.'), '-', 1))
      END AS OUTBOUND_ASSET,
      MAX(s.STREAMING_QUANTITY) AS QUANTITY,
      COUNT(DISTINCT POOL_NAME) AS POOL_COUNT,
      MIN(UPPER(SPLIT_PART(POOL_NAME, '-',1))) AS _ASSET_1,
      MAX(UPPER(SPLIT_PART(POOL_NAME, '-',1))) AS _ASSET_2,
      CASE
        WHEN POOL_COUNT = 2 AND OUTBOUND_ASSET = _ASSET_1 THEN _ASSET_2
        WHEN POOL_COUNT = 2 AND OUTBOUND_ASSET <> _ASSET_1 THEN _ASSET_1
        WHEN POOL_COUNT = 1 AND OUTBOUND_ASSET = 'THOR.RUNE' THEN _ASSET_1
        ELSE 'THOR.RUNE'
      END AS INBOUND_ASSET,
      CASE
        WHEN POOL_COUNT = 2 THEN CEIL(COUNT(1)/2)
        ELSE COUNT(1)
      END AS SUB_SWAPS,
      MIN(m.MEMO) AS MEMO
    FROM thorchain.defi.fact_swaps AS s
    JOIN true_memos AS m
      ON s.TX_ID = m.TX_ID
    WHERE TRUE
      AND NOT (FROM_ASSET LIKE '%~%' OR TO_ASSET LIKE '%~%')
      AND NOT (FROM_ASSET LIKE '%/%' OR TO_ASSET LIKE '%/%')
      AND UPPER(SPLIT_PART(m.MEMO, ':', 1)) IN ('SWAP', 'S', '=')
    GROUP BY 1,2
)
, successful_swaps AS (
  SELECT * FROM attempted_swaps AS a
  WHERE NOT EXISTS (
      SELECT TX_ID
      FROM thorchain.defi.fact_refund_events
      WHERE TX_ID = a.TX_ID
  ) AND a.SUB_SWAPS >= a.QUANTITY
)
, first_swap_out AS (
    SELECT 
      ROW_NUMBER() OVER (PARTITION BY b.TX_ID ORDER BY b.TO_AMOUNT * QUANTITY DESC) AS NUM,
      b.BLOCK_TIMESTAMP::DATE AS DATE,
      b.TX_ID,
      s.MEMO,
      INBOUND_ASSET,
      OUTBOUND_ASSET,
      CASE 
        WHEN s.OUTBOUND_ASSET = 'THOR.RUNE' 
          THEN (b.TO_AMOUNT * QUANTITY - 0.02)
        WHEN b.BLOCK_TIMESTAMP > TIMESTAMP '2024-12-13 01:08:24+00'
          THEN (b.TO_AMOUNT * QUANTITY) * (1 - b.AFFILIATE_FEE_BASIS_POINTS / 10000.0)
        ELSE (b.TO_AMOUNT * QUANTITY)
      END AS EST_SWAP,
      QUANTITY AS QUANTITY,
      b.BLOCK_TIMESTAMP AS BLOCK_TIMESTAMP,
      b.TO_AMOUNT_USD * QUANTITY AS INBOUND_EST_USD,
      b.ASSET_USD AS OUT_ASSET_USD,
      b.AFFILIATE_FEE_BASIS_POINTS AS AFFILIATE_FEE_BPS
    FROM thorchain.defi.fact_swaps AS b
    JOIN successful_swaps AS s
      ON b.TX_ID = s.TX_ID
    WHERE TRUE
      AND UPPER(SPLIT_PART(REGEXP_REPLACE(b.TO_ASSET, '[~/]{1}', '.'), '-', 1)) = s.OUTBOUND_ASSET
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.TX_ID ORDER BY b.TO_AMOUNT * QUANTITY DESC) = 1
)
, outbound AS (
    SELECT
        IN_TX AS TX_ID,
        UPPER(SPLIT_PART(REGEXP_REPLACE(ASSET, '[~/]{1}', '.'), '-', 1)) AS OUTBOUND_ASSET,
        CASE
          WHEN UPPER(SPLIT_PART(REGEXP_REPLACE(ASSET, '[~/]{1}', '.'), '-', 1)) = 'THOR.RUNE'
              THEN MAX(ASSET_E8) / 1e8
          ELSE SUM(ASSET_E8) / 1e8
        END AS AMOUNT,
        TRUE AS HAS_OUTBOUND
    FROM thorchain.defi.fact_outbound_events
    WHERE IN_TX IS NOT NULL
    GROUP BY 1, 2
    UNION ALL
    SELECT
      TX_ID,
      UPPER(SPLIT_PART(REGEXP_REPLACE(ASSET, '[~/]{1}', '.'), '-', 1)) AS OUTBOUND_ASSET,
      SUM(ASSET_E8)/1e8 AS AMOUNT,
      FALSE AS HAS_OUTBOUND
    FROM thorchain.defi.fact_fee_events
    WHERE ASSET <> 'THOR.RUNE'
    GROUP BY 1, 2
)
, outbound_totals AS (
    SELECT 
        TX_ID,
        OUTBOUND_ASSET,
        SUM(AMOUNT) as TOTAL_AMOUNT,
        MAX(HAS_OUTBOUND) as HAS_OUTBOUND
    FROM outbound
    GROUP BY 1, 2
)
, base AS (
  SELECT
    DATE,
    b.BLOCK_ID AS BLOCK_HEIGHT,
    i.TX_ID as TX_ID,
    (o.TOTAL_AMOUNT) as OUT_AMOUNT,
    i.OUT_ASSET_USD * (o.TOTAL_AMOUNT) as OUT_AMOUNT_USD,
    i.EST_SWAP as EST_SWAP,
    i.INBOUND_EST_USD AS EST_USD,
    i.INBOUND_ASSET as INBOUND_ASSET,
    i.OUTBOUND_ASSET as OUTBOUND_ASSET,
    i.QUANTITY as QUANTITY,
    ss.INTERVAL as INTERVAL,
    i.BLOCK_TIMESTAMP as BTIMESTAMP,
    i.AFFILIATE_FEE_BPS as AFFILIATE_FEE_BPS
  FROM first_swap_out AS i
  JOIN outbound_totals AS o ON
     i.TX_ID = o.TX_ID AND
     o.OUTBOUND_ASSET = i.OUTBOUND_ASSET
  JOIN thorchain.core.dim_block AS b
    ON i.BLOCK_TIMESTAMP = b.BLOCK_TIMESTAMP
  JOIN thorchain.defi.fact_streamling_swap_details_events AS ss
    ON i.TX_ID = ss.TX_ID
  WHERE o.HAS_OUTBOUND = TRUE
  ORDER BY i.BLOCK_TIMESTAMP 
)
, qt as (
  SELECT
    INBOUND_ASSET,
    OUTBOUND_ASSET,
    INTERVAL,
    (OUT_AMOUNT_USD) AS OUT_USD,
    EST_SWAP AS ESTIMATED_OUTPUT,
    OUT_AMOUNT AS ACTUAL_OUTPUT,
    (OUT_AMOUNT / EST_SWAP) AS EXC_QUALITY,
    BTIMESTAMP,
    TX_ID,
    QUANTITY,
    AFFILIATE_FEE_BPS
  FROM base
  WHERE QUANTITY > 0
)

SELECT DISTINCT
  *
FROM qt
WHERE OUTBOUND_ASSET = '${outbound_asset}'
LIMIT 10
;
`

export default sql
