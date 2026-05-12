# Power BI Dashboard Design Guide

---

## Dashboard 1: Executive Overview
**Audience:** VP / C-Suite

**Visuals:**
- 4 KPI cards: Inventory Fill Rate | Shipment SLA % | Critical Shortages | Operational Health Score
- 6-month trend line for Fill Rate and SLA %
- Donut: Inventory value by category
- Stacked bar: Supplier tier distribution
- Slicers: Month, Hospital Region

**Key DAX:**
```dax
Inventory Fill Rate % =
DIVIDE(
    COUNTROWS(FILTER(fact_inventory, fact_inventory[stock_status] <> "STOCKOUT")),
    COUNTROWS(fact_inventory)
) * 100

Operational Health Score =
[Inventory Fill Rate %] * 0.40 + [Shipment SLA %] * 0.60
```

---

## Dashboard 2: Inventory Analytics
**Audience:** Supply Chain Operations Team

**Visuals:**
- Matrix: Hospital × Category with red/amber/green conditional formatting
- Bar: Top 10 SKUs by Restock Urgency Score
- Trend: 30-day daily fill rate line chart
- Alert table: All items where is_critical_shortage = TRUE
- Gauge: Current Stockout % vs 2% target

---

## Dashboard 3: Shipment Analytics
**Audience:** Logistics Team

**Visuals:**
- KPI cards: Total Shipments | SLA % | Delayed Count | Avg Delivery Days
- Bar: Delayed shipments by carrier (worst → best)
- Matrix: Priority × Month SLA % heat map
- Line: Monthly SLA % trend by priority

```dax
Delayed Shipments =
COUNTROWS(FILTER(fact_shipments,
    fact_shipments[is_sla_met] = FALSE() &&
    NOT ISBLANK(fact_shipments[actual_delivery_date])))
```

---

## Dashboard 4: Supplier Performance
**Audience:** Procurement Team

**Visuals:**
- KPI cards: Active Suppliers | Avg Reliability Score | WATCH_LIST Count
- Ranked bar: Top/Bottom 10 suppliers by reliability score
- Donut: PLATINUM / GOLD / SILVER / WATCH_LIST distribution
- Conditional table: WATCH_LIST suppliers highlighted red

---

## Design Standards

| Element | Specification |
|---|---|
| Primary color | #0078D4 |
| Alert red | #D13438 |
| Warning amber | #FF8C00 |
| Success green | #107C10 |
| Background | #F3F2F1 |
| Font | Segoe UI |
| KPI value size | 28pt |
| Refresh schedule | Daily 06:00 local |
| Row-level security | By hospital_id for operational users |

## Row-Level Security
```dax
-- HospitalOpsUser role (sees own hospital only)
[hospital_id] = USERPRINCIPALNAME()

-- ExecutiveViewer role: no filter applied
```
