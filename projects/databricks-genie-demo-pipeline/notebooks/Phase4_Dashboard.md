# Phase 4: Dashboard Generation with Genie Code

## Objective
Test Genie Code's ability to generate production-ready AI/BI dashboards.

## How to Test

### Option A: From a Notebook
1. Open a new notebook
2. Open Genie Code > Agent mode
3. Use this prompt:

```
Create an AI/BI Dashboard for retail analytics using:
- dev_sandbox.genie_code_demo.gold_daily_sales
- dev_sandbox.genie_code_demo.gold_customer_summary

The dashboard should include:
1. Revenue KPI card showing total revenue and month-over-month change
2. Daily revenue trend line chart
3. Revenue by country bar chart (top 10)
4. Customer segments distribution pie chart
5. Customer lifetime value distribution histogram
6. A filter for country and date range

Make it clean and professional, suitable for executive reporting.
```

### Option B: From Dashboard Editor
1. Go to SQL > Dashboards > Create Dashboard
2. Open Genie Code panel
3. Use the same prompt above

## What to Observe

- Did it create the correct chart types?
- How did it handle the layout (multi-page, single page)?
- Did it configure filters correctly?
- Did it use semantic definitions?
- Did it pick up the right metrics from Unity Catalog metadata?
- Is the dashboard actually usable by a business stakeholder?

## Advanced Test: Sketch to Dashboard

If you want to push it further, try:

```
Here's a rough sketch of what I want:
[Top row] 3 KPI cards: Total Revenue, Total Customers, Avg Order Value
[Middle] Line chart showing daily revenue trend on the left,
         bar chart of top countries on the right
[Bottom] Table of top 20 products by revenue with sparklines

Use the gold tables in dev_sandbox.genie_code_demo.
```

## Scoring

| Aspect | Rating | Notes |
|--------|--------|-------|
| Chart type selection | /10 | |
| Layout quality | /10 | |
| Filter configuration | /10 | |
| Data accuracy | /10 | |
| Executive readiness | /10 | |

**Overall Phase 4 Rating: /10**
