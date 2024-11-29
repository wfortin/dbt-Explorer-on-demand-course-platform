WITH fct_order_items AS (
  SELECT
    ORDER_ITEM_ID,
    ORDER_ID,
    PRODUCT_ID,
    LOCATION_ID,
    CUSTOMER_ID,
    ORDER_TOTAL,
    TAX_PAID,
    ORDERED_AT,
    CUSTOMER_NAME,
    LOCATION_NAME,
    TAX_RATE,
    LOCATION_OPENED_AT
  FROM {{ ref('fct_order_items') }}
), fct_orders AS (
  SELECT
    ORDER_ID,
    LOCATION_ID,
    CUSTOMER_ID,
    ORDER_TOTAL,
    TAX_PAID,
    ORDERED_AT,
    CUSTOMER_NAME,
    LOCATION_NAME,
    TAX_RATE,
    LOCATION_OPENED_AT,
    ORDERED_MONTH,
    ORDERED_DAY,
    ORDERED_YEAR
  FROM {{ ref('fct_orders', v=1) }}
), join_1 AS (
  SELECT
    fct_orders.ORDER_ID,
    fct_orders.LOCATION_ID,
    fct_orders.CUSTOMER_ID,
    fct_orders.ORDER_TOTAL,
    fct_orders.TAX_PAID,
    fct_orders.ORDERED_AT,
    fct_orders.CUSTOMER_NAME,
    fct_orders.LOCATION_NAME,
    fct_orders.TAX_RATE,
    fct_orders.LOCATION_OPENED_AT,
    fct_orders.ORDERED_MONTH,
    fct_orders.ORDERED_DAY,
    fct_orders.ORDERED_YEAR,
    fct_order_items.ORDER_ITEM_ID,
    fct_order_items.ORDER_ID AS ORDER_ID_1,
    fct_order_items.PRODUCT_ID,
    fct_order_items.LOCATION_ID AS LOCATION_ID_1,
    fct_order_items.CUSTOMER_ID AS CUSTOMER_ID_1,
    fct_order_items.ORDER_TOTAL AS ORDER_TOTAL_1,
    fct_order_items.TAX_PAID AS TAX_PAID_1,
    fct_order_items.ORDERED_AT AS ORDERED_AT_1,
    fct_order_items.CUSTOMER_NAME AS CUSTOMER_NAME_1,
    fct_order_items.LOCATION_NAME AS LOCATION_NAME_1,
    fct_order_items.TAX_RATE AS TAX_RATE_1,
    fct_order_items.LOCATION_OPENED_AT AS LOCATION_OPENED_AT_1
  FROM fct_orders
  JOIN fct_order_items
    ON fct_orders.ORDER_ID = fct_order_items.ORDER_ID
)
SELECT
  *
FROM join_1