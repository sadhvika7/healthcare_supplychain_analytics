# KPI Definitions

## 1. Inventory Fill Rate
**Formula:** `(Total SKUs − Stockout SKUs) / Total SKUs × 100`  
**Target:** ≥ 95% | **Alert threshold:** < 90%  
**Source:** `fact_inventory` | **Refresh:** Daily

## 2. Shipment SLA %
**Formula:** `On-Time Deliveries / Total Completed Deliveries × 100`  
**Target:** ≥ 90% | **Segments:** priority, carrier, hospital  
**Source:** `fact_shipments` (actual_delivery_date not null only)

## 3. Delayed Shipment Count
**Formula:** `COUNT WHERE delivery_delay_days > 0`  
**Target:** Trending down month-over-month

## 4. Average Delivery Time
**Formula:** `AVG(actual_transit_days)`  
**Target:** ≤ 5 days STANDARD, ≤ 2 days CRITICAL

## 5. Critical Stock Shortage Count
**Formula:** `COUNT WHERE stock_status IN ('STOCKOUT','CRITICAL_LOW')`  
**Target:** 0 for life-critical items (is_critical = TRUE)

## 6. Supplier Reliability Score
**Formula:** `(on_time_rate × 0.70) + (max(0, 100 − avg_delay × 10) × 0.30)`  
**Tiers:** PLATINUM ≥ 90 | GOLD 75–89 | SILVER 60–74 | WATCH_LIST < 60  
**Target:** Average ≥ 85

## 7. Inventory Turnover Ratio
**Formula:** `Total COGS / Average Inventory Value`  
**Cadence:** Monthly | Compare against category benchmarks

## 8. Procurement Cost Variance
**Formula:** `(Actual Cost − Budget Amount) / Budget Amount × 100`  
**Target:** ≤ ±5% | Alert: > 10% overage

## 9. Stockout Percentage
**Formula:** `Stockout SKU Count / Total SKU Count × 100`  
**Target:** ≤ 2% overall; 0% for is_critical items

## 10. Operational Health Score
**Formula:** `(Inventory Fill Rate × 0.40) + (Shipment SLA % × 0.60)`  
**Target:** ≥ 88
