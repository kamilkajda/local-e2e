# Power BI Master Measures & Design Blueprint

This document serves as the Single Source of Truth (SSoT) for the analytical layer of the Polish Economic Analysis project. It defines the business logic, measure structure, and visual standards of the report.

## 1. Metric Inventory (GUS BDL Mapping)

The model incorporates 16 key economic variables. Each serves as the foundation for the standardized **5-Measure Pattern**.

| Display Folder | Metric Name (Base) | GUS ID | Aggregation | Color Logic |
| :--- | :--- | :--- | :--- | :--- |
| **Labor Market** | Avg Gross Wage | 64428 | AVERAGE | Higher is Better |
| **Labor Market** | Unemployment Rate | 60270 | AVERAGE | Lower is Better |
| **Living Standards** | Avg Disposable Income | 216968 | AVERAGE | Higher is Better |
| **Living Standards** | Avg Expenditures | 7737 | AVERAGE | Lower is Better |
| **Economy** | Total GDP | 458271 | SUM | Higher is Better |
| **Economy** | GDP per Capita | 458421 | AVERAGE | Higher is Better |
| **Economy** | Investment per Capita | 60520 | AVERAGE | Higher is Better |
| **Business & Innovation** | Entities per 10k Pop | 60530 | AVERAGE | Higher is Better |
| **Housing Market** | Price per m2 (Residential) | 633692 | AVERAGE | Higher is Better |
| **Housing Market** | Dwellings per 1k Pop | 747060 | AVERAGE | Higher is Better |
| **Housing Market** | Completed Dwellings | 748601 | SUM | Higher is Better |
| **Housing Market** | Total Dwellings Sold | 633101 | SUM | Higher is Better |
| **Housing Market** | Market Dwellings Sold | 633617 | SUM | Higher is Better |
| **Public Finance** | Budget Revenue | 60508 | AVERAGE | Higher is Better |
| **Public Finance** | Budget Expenditure | 60518 | AVERAGE | Lower is Better |
| **Demographics** | Total Population | 72305 | SUM | Neutral |

## 2. Analytical Pattern: The 5-Measure Framework

For each variable listed above (e.g., Avg Gross Wage), five derivative measures are implemented to provide full analytical context (national benchmark and YoY dynamics).

| Measure Type | DAX Logic Example (for Avg Gross Wage) |
| :--- | :--- |
| **1. Base** | `Avg Gross Wage = AVERAGE(Fact_Economics[value])` (Context: Filtered by Region) |
| **2. National** | `Avg Gross Wage (Poland) = CALCULATE([Avg Gross Wage], ALL(Dim_Region), Dim_Region[region] = "NATIONAL")` |
| **3. YoY %** | `Avg Gross Wage YoY % = DIVIDE([Avg Gross Wage] - [Avg Gross Wage (LY)], [Avg Gross Wage (LY)])` |
| **4. Nat. YoY %** | `Avg Gross Wage (Poland) YoY % = CALCULATE([Avg Gross Wage YoY %], ALL(Dim_Region), Dim_Region[region] = "NATIONAL")` |
| **5. Gap %** | `Avg Gross Wage vs Avg Poland Gap % = DIVIDE([Avg Gross Wage], [Avg Gross Wage (Poland)]) - 1` |

## 3. Advanced UI Mapping (Conditional Formatting)

The **UI Mapping** folder contains measures controlling the visual layer (chart marker colors, icons, labels).

### [DAX] Color Status Logic
Each color status measure (e.g., `Color Status Wages`) is based on the variance from the national average (`Gap %`) and the metric's optimization characteristic.

```dax
Color Status Wages = 
VAR _Gap = [Avg Gross Wage vs Avg Poland Gap %]
RETURN
    SWITCH( TRUE(),
        _Gap > 0, "#4E56DE", -- Indigo (Above National)
        _Gap < 0, "#FF4D4D", -- Red (Below National)
        "#A0A0A0"            -- Gray (Neutral/No Data)
    )
```

## 4. Executive Summary & Imputation Awareness

The primary analytical component generates a dynamic description of the region's economic status, including an **(ESTIMATED)** flag for data points interpolated during ETL.

### [DAX] Executive Summary Text
```dax
Executive Summary Text = 
VAR _Region = [Selected Region]
VAR _Year = [Selected Year Number]

-- Base Data
VAR _Wage = [Avg Gross Wage]
VAR _WageYoY = [Avg Gross Wages YoY %]
VAR _Unemployment = [Avg Unemployment Rate]
VAR _UnemploymentYoY = [Avg Unemployment YoY %]
VAR _Gap = [Avg Gross Wage vs Avg Poland Gap %]
VAR _Income = [Avg Household Disposable Income]
VAR _Expenditures = [Avg Household Expenditures]

-- Derived Business Logic
VAR _SavingsRate = DIVIDE(_Income - _Expenditures, _Income)
VAR _IsEstimated = _Region = "OPOLSKIE" && _Year = 2023

-- Dynamic Narrative Fragments
VAR _Header = UPPER(_Region) & " (" & _Year & ") - ECONOMIC SUMMARY" & IF(_IsEstimated, " (ESTIMATED)", "")
VAR _LineWages = "• Wages: Average gross pay reached " & FORMAT(_Wage, "#,0 PLN") & " (" & FORMAT(_WageYoY, "+0.0%;-0.0%") & " YoY)."
VAR _LineBench = "• Benchmark: The region is currently " & FORMAT(ABS(_Gap), "0.0%") & IF(_Gap >= 0, " above ", " below ") & "the National average."
VAR _LineLabor = "• Labor: Unemployment rate " & IF(_UnemploymentYoY < 0, "improved to ", "stood at ") & FORMAT(_Unemployment, "0.0%") & "."
VAR _LineBudget = "• Budget: Disposable income is " & FORMAT(_Income, "#,0 PLN") & " with a " & FORMAT(_SavingsRate, "0%") & " savings buffer."

RETURN
    _Header & UNICHAR(10) & 
    _LineWages & UNICHAR(10) & 
    _LineBench & UNICHAR(10) & 
    _LineLabor & UNICHAR(10) & 
    _LineBudget
```

## 5. Visual Standards (Bento Grid)

The report utilizes a **Bento Grid** architecture for high information density while maintaining readability.

* **Indigo Header**: Dynamic page title (`Report Title Dynamic`) on a `#4E56DE` background.
* **KPI Cards**: Key indicators (GDP, Wages, Unemployment, Income, Population) with integrated 10-year trend sparklines.
* **Scatter Chart (Wages vs Unemployment)**: Features a `Play Axis` for temporal movement and dynamic region labels. Axis ranges are locked (Fixed) to ensure consistency.
* **Purchasing Power Chart**: Compares wage dynamics (`Avg Gross Wage`) with property prices (`Avg Residential Price per m2`) to analyze purchasing power.

## 6. Model Relationships (Star Schema)

Based on the `.tmdl` definition, the model follows a star schema centered on the fact table:

* **Fact_Economics** ↔ **Dim_Calendar** (Key: `year`)
* **Fact_Economics** ↔ **Dim_Region** (Key: `region_id`)
* **Fact_Economics** ↔ **Dim_Metrics** (Key: `metric_id`)
* **Fact_Economics** ↔ **Chart_Axis_Years** (Key: `year`) - dedicated axis for chart animations.

## 7. Configuration & Parameters

* **AzuriteBaseUrl**: Parameter controlling the data source (localhost vs. LAN IP), facilitating team collaboration or cross-device presentations.
* **Selected Region/Year**: Helper measures extracting slicer values for use in dynamic titles and narratives.
