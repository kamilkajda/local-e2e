# Power BI Master Measures Blueprint

This document defines the standards for creating DAX measures and the analytical business logic within the Polish Economic Analysis project.

## 1. Global Standards & UI Logic

### Naming Convention
* **Avg [Metric]**: Used for rates, ratios, and per capita values (e.g., Avg Gross Wage).
* **Total [Metric]**: Used for absolute counts and sums (e.g., Total Population, Total GDP).
* **[Metric] (Poland)**: National benchmark using `level_type = "Country"`.
* **[Metric] YoY %**: Year-over-Year growth for the selected region.
* **[Metric] (Poland) YoY %**: Year-over-Year growth for the national benchmark.
* **[Metric] vs Poland Gap %**: Relative difference between selection and benchmark.

### UI Color Logic (Status Colors)
* **Higher is Better (1% Threshold):** Grey zone between -1% and +1%. Used for Wages, GDP, Income, Investment, Entities, Housing Prices, Sales, Revenue.
* **Lower is Better (1% Threshold):** Grey zone between -1% and +1%. Used for Unemployment Rate, Expenditures, Budget Expenditure.
* **Population (Zero Tolerance):** Green for any growth (>0), Red for any decline (<0).

---

## 2. Global Measures (Navigation & Header)

### [A] Dynamic Report Title
```dax
-- Get currently selected year from Slicer
Selected Year = SELECTEDVALUE('Dim_Calendar'[year])

-- Get currently selected region from Slicer (Defaults to NATIONAL)
Selected Region = SELECTEDVALUE('Dim_Region'[unit_name_en], "NATIONAL")

-- Final Dynamic Title for the Header Card
Report Title Dynamic = 
"POLAND: ECONOMIC PULSE - " & [Selected Region] & " (" & [Selected Year] & ")"
```

---

## 3. Metric Inventory (Variable Mapping)

| Display Folder | Metric Name | GUS ID | Aggregation | Color Logic |
| :--- | :--- | :--- | :--- | :--- |
| **Labor Market** | Avg Gross Wage | 64428 | AVERAGE | Higher is Better |
| **Labor Market** | Unemployment Rate | 60270 | AVERAGE | Lower is Better |
| **Living Standards** | Avg Disposable Income | 216968 | AVERAGE | Higher is Better |
| **Living Standards** | Avg Expenditures | 7737 | AVERAGE | Lower is Better |
| **Economy** | Total GDP | 458271 | SUM | Higher is Better |
| **Economy** | GDP per Capita | 458421 | AVERAGE | Higher is Better |
| **Economy** | Investment per Capita | 60520 | AVERAGE | Higher is Better |
| **Business & Innovation**| Entities per 10k Pop | 60530 | AVERAGE | Higher is Better |
| **Housing Market** | Price per m2 (Residential)| 633692 | AVERAGE | Higher is Better |
| **Housing Market** | Dwellings per 1k Pop | 747060 | AVERAGE | Higher is Better |
| **Housing Market** | Completed Dwellings | 748601 | SUM | Higher is Better |
| **Housing Market** | Total Dwellings Sold | 633101 | SUM | Higher is Better |
| **Housing Market** | Market Dwellings Sold | 633617 | SUM | Higher is Better |
| **Public Finance** | Budget Revenue | 60508 | AVERAGE | Higher is Better |
| **Public Finance** | Budget Expenditure | 60518 | AVERAGE | Lower is Better |
| **Demographics** | Total Population | 72305 | SUM | Population (ZT) |

---

## 4. Master DAX Blueprints

### 4.1 The 5-Measure Pattern (Example: Total GDP)
```dax
-- 1. Base Metric
Total GDP = CALCULATE(SUM(Fact_Economics[value]), Fact_Economics[variable_id] = 458271)

-- 2. National Benchmark
Total GDP (Poland) = CALCULATE([Total GDP], ALL(Dim_Region), Dim_Region[level_type] = "Country")

-- 3. Regional YoY %
Total GDP YoY % = 
VAR _MaxYear = MAX('Dim_Calendar'[year])
VAR _Current = [Total GDP]
VAR _Prev = CALCULATE([Total GDP], 'Dim_Calendar'[year] = _MaxYear - 1)
RETURN DIVIDE(_Current - _Prev, _Prev)

-- 4. National YoY %
Total GDP (Poland) YoY % = 
VAR _MaxYear = MAX('Dim_Calendar'[year])
VAR _Current = [Total GDP (Poland)]
VAR _Prev = CALCULATE([Total GDP (Poland)], 'Dim_Calendar'[year] = _MaxYear - 1)
RETURN DIVIDE(_Current - _Prev, _Prev)

-- 5. Performance Gap %
Total GDP vs Poland Gap % = DIVIDE([Total GDP] - [Total GDP (Poland)], [Total GDP (Poland)])
```

### 4.2 Unemployment Rate Percentage Fix
```dax
Avg Unemployment Rate = 
DIVIDE(
    CALCULATE(AVERAGE(Fact_Economics[value]), Fact_Economics[variable_id] = 60270),
    100
)
```

---

## 5. UI Status Colors (Conditional Formatting)

### A. UI Color - Higher is Better (1% Threshold)
```dax
UI Color - Higher is Better = 
VAR _Trend = [Target YoY %] -- Replace with specific YoY measure
RETURN 
    SWITCH( TRUE(),
        _Trend >= 0.01,  "#00C805", -- Green (Growth >= 1%)
        _Trend <= -0.01, "#FF0000", -- Red (Decline <= -1%)
        "#6B7280"                   -- Grey (Neutral)
    )
```

### B. UI Color - Lower is Better (1% Threshold)
```dax
UI Color - Lower is Better = 
VAR _Trend = [Target YoY %] -- Replace with specific YoY measure
RETURN 
    SWITCH( TRUE(),
        _Trend <= -0.01, "#00C805", -- Green (Drop is good)
        _Trend >= 0.01,  "#FF0000", -- Red (Increase is bad)
        "#6B7280"                   -- Grey (Neutral)
    )
```

### C. UI Color - Population (Zero Tolerance)
```dax
UI Color - Population = 
VAR _Trend = [Total Population YoY %]
RETURN 
    IF(_Trend > 0, "#00C805", "#FF0000")
```
