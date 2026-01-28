# Power BI Master Measures & Design Blueprint

This document defines the analytical business logic, data transparency standards, and full metric mapping for the Polish Economic Analysis project.

---

## 1. Metric Inventory (Variable Mapping)

The following table maps GUS BDL variable IDs to Power BI measures, including their intended aggregation and UI behavior.

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

## 2. Advanced Dynamic Narrative (Executive Summary)

The primary narrative visual uses DAX to generate a multi-line economic summary. It dynamically adapts to the selected region and year.

### [DAX] Executive Summary Text
```dax
Executive Summary Text = 
VAR _Region = [Selected Region]
VAR _Year = [Selected Year Number]

-- Base Measures
VAR _Wage = [Avg Gross Wage]
VAR _WageYoY = [Avg Gross Wages YoY %]
VAR _Unemployment = [Avg Unemployment Rate]
VAR _UnemploymentYoY = [Avg Unemployment YoY %]
VAR _Gap = [Avg Gross Wage vs Avg Poland Gap %]
VAR _Income = [Avg Household Disposable Income]
VAR _Expenditures = [Avg Household Expenditures]

-- Derived Logic
VAR _SavingsRate = DIVIDE(_Income - _Expenditures, _Income)

-- Imputation Flag (Transparency for Opolskie 2023)
VAR _IsEstimated = _Region = "OPOLSKIE" && _Year = 2023

-- Dynamic Vocabulary for Labor Market
VAR _LaborStatus = 
    SWITCH(TRUE(),
        ISBLANK(_UnemploymentYoY), "stands at ",
        _UnemploymentYoY < -0.01, "improved to ",
        _UnemploymentYoY > 0.01, "increased to ",
        "remains stable at "
    )

-- Formatting Parts
VAR _Header = UPPER(_Region) & " (" & _Year & ") - ECONOMIC SUMMARY" & IF(_IsEstimated, " (ESTIMATED)", "")

VAR _LineWages = "• Wages: Average gross pay reached " & FORMAT(_Wage, "#,0 PLN") & " (" & FORMAT(_WageYoY, "+0.0%;-0.0%") & " YoY)."
VAR _LineBenchmark = "• Benchmark: The region is currently " & FORMAT(ABS(_Gap), "0.0%") & IF(_Gap >= 0, " above ", " below ") & "the National average."
VAR _LineLabor = "• Labor: Unemployment rate " & _LaborStatus & FORMAT(_Unemployment, "0.0%") & "."
VAR _LineBudget = "• Budget: Disposable income is " & FORMAT(_Income, "#,0 PLN") & " with a " & FORMAT(_SavingsRate, "0%") & " savings buffer."

RETURN
    _Header & UNICHAR(10) & 
    _LineWages & UNICHAR(10) & 
    _LineBenchmark & UNICHAR(10) & 
    _LineLabor & UNICHAR(10) & 
    _LineBudget
```

---

## 3. Data Imputation Awareness (The "Estimated" Flag)

To maintain analytical integrity, we explicitly flag data that was imputed during the ETL process.

### Implementation:
* **Visual Titles:** Use the `(ESTIMATED)` suffix in dynamic titles when Opolskie + 2023 are selected.
* **Narrative:** The `Executive Summary Text` measure includes a logic check to append the estimation notice to the header line.
* **Goal:** Full transparency regarding data gaps and the automated interpolation handled in PySpark.

---

## 4. Page Navigator & Sync Slicers Logic

The report behaves like a web application using built-in navigation and synchronized states.

### Page Navigator:
* **Visual:** `Insert -> Buttons -> Navigator -> Page navigator`.
* **Behavior:** Automatically updates buttons based on report page names.
* **Styling:** Rounded corners (12px) with Indigo/White toggle states to match the Bento Grid UI.

### Sync Slicers:
* **Configuration:** Go to `View -> Sync slicers`.
* **Logic:** The "Year" slicer is synced across all pages. 
* **Benefit:** Selecting a year once persists the context during navigation.

---

## 5. Scatter Chart: Play Axis Animation

The "Labor Market: Wages vs Unemployment" chart uses a temporal dimension to show market maturation.

### Configuration:
* **X-Axis:** `Avg Unemployment Rate` (Fixed Range: 0% to 25%).
* **Y-Axis:** `Avg Gross Wage` (Fixed Range: 0 to 10k PLN).
* **Play Axis:** `Dim_Calendar[year]`.
* **Visual Watermark:** Power BI displays the year as a background watermark during animation.

---

## 6. Standard Formatting Summary

| Metric Category | Format String | Logic |
| :--- | :--- | :--- |
| **Financials** | `#,0 "PLN"` | PLN Suffix, Thousands separator |
| **Growth/Rates** | `+0.0%;-0.0%;0.0%` | Signed percentages for YoY changes |
| **Population** | `#,0` | Whole numbers |
