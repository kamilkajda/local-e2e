# Power BI Master Measures Blueprint & Development Process

This document defines the standards for creating DAX measures and the analytical business logic within the Polish Economic Analysis project.

## 1. Metric Development Workflow

To maintain report consistency, every new data point follows this process:
1. **GUS Discovery:** Identify the `variable_id` manually (e.g., via the BDL portal) and verify the unit of measure.
2. **ETL Integration:** Add the ID and metadata to `configs/gus_metrics.json` and run `scripts/run_etl_dev.ps1`.
3. **Semantic Layer:** Implement the standardized 5-measure pattern in Power BI within the appropriate Display Folder.
4. **UI Branding:** Apply conditional formatting using dedicated Status Color measures in the `UI Mapping` folder.

---

## 2. Standard 5-Measure Pattern (Example: Gross Wages)
**Display Folder:** `Labor Market`

### [A] Base Metric (Current Value)
```dax
Avg Gross Wage = 
CALCULATE(
    AVERAGE('Fact_Economics'[value]),
    'Fact_Economics'[variable_id] = 64428
)
```

### [B] National Benchmark (Poland Total)
```dax
Avg Gross Wage (Poland) = 
CALCULATE(
    [Avg Gross Wage],
    ALL('Dim_Region'),
    'Dim_Region'[level_type] = "Country"
)
```

### [C] Regional Growth (YoY %)
```dax
Avg Gross Wages YoY % = 
VAR _CurrentYear = SELECTEDVALUE('Dim_Calendar'[year])
VAR _CurrentVal = [Avg Gross Wage]
VAR _PrevVal = 
    CALCULATE(
        [Avg Gross Wage], 
        'Dim_Calendar'[year] = _CurrentYear - 1
    )
RETURN 
    DIVIDE(_CurrentVal - _PrevVal, _PrevVal)
```

### [D] National Growth (YoY %)
```dax
Avg Gross Wages (Poland) YoY % = 
VAR _CurrentYear = SELECTEDVALUE('Dim_Calendar'[year])
VAR _CurrentVal = [Avg Gross Wage (Poland)]
VAR _PrevVal = 
    CALCULATE(
        [Avg Gross Wage (Poland)], 
        'Dim_Calendar'[year] = _CurrentYear - 1
    )
RETURN 
    DIVIDE(_CurrentVal - _PrevVal, _PrevVal)
```

### [E] Performance Gap vs. National Average
```dax
Wages vs Poland Gap % = 
DIVIDE(
    [Avg Gross Wage] - [Avg Gross Wage (Poland)], 
    [Avg Gross Wage (Poland)]
)
```

---

## 3. Standard 5-Measure Pattern (Example: Unemployment)
**Display Folder:** `Labor Market`

### [A] Base Metric
```dax
Unemployment Rate = 
CALCULATE(
    AVERAGE('Fact_Economics'[value]),
    'Fact_Economics'[variable_id] = 60270
)
```

### [B] National Benchmark
```dax
Unemployment Rate (Poland) = 
CALCULATE(
    [Unemployment Rate],
    ALL('Dim_Region'),
    'Dim_Region'[level_type] = "Country"
)
```

### [C] Regional Growth (YoY %)
```dax
Unemployment YoY % = 
VAR _CurrentYear = SELECTEDVALUE('Dim_Calendar'[year])
VAR _CurrentVal = [Unemployment Rate]
VAR _PrevVal = 
    CALCULATE(
        [Unemployment Rate], 
        'Dim_Calendar'[year] = _CurrentYear - 1
    )
RETURN 
    DIVIDE(_CurrentVal - _PrevVal, _PrevVal)
```

### [D] National Growth (YoY %)
```dax
Unemployment (Poland) YoY % = 
VAR _CurrentYear = SELECTEDVALUE('Dim_Calendar'[year])
VAR _CurrentVal = [Unemployment Rate (Poland)]
VAR _PrevVal = 
    CALCULATE(
        [Unemployment Rate (Poland)], 
        'Dim_Calendar'[year] = _CurrentYear - 1
    )
RETURN 
    DIVIDE(_CurrentVal - _PrevVal, _PrevVal)
```

### [E] Performance Gap vs. National Average
```dax
Unemployment vs Poland Gap % = 
DIVIDE(
    [Unemployment Rate] - [Unemployment Rate (Poland)], 
    [Unemployment Rate (Poland)]
)
```

---

## 4. UI Formatting Logic (Folder: UI Mapping)

These measures are used for "Conditional Formatting" -> "Field Value" to drive the visual experience.

### Status Color Wages (Higher is Better)
```dax
Color Status Wages = 
VAR _Trend = [Avg Gross Wages YoY %]
RETURN 
    SWITCH( TRUE(),
        _Trend > 0, "#00C805", -- Green
        _Trend < 0, "#FF0000", -- Red
        "#808080"              -- Grey
    )
```

### Status Color Unemployment (Lower is Better)
```dax
Color Status Unemployment = 
VAR _Trend = [Unemployment YoY %]
RETURN 
    SWITCH( TRUE(),
        _Trend < 0, "#00C805", -- Green (Drop is good)
        _Trend > 0, "#FF0000", -- Red (Increase is bad)
        "#808080"              -- Grey
    )
```

---

## 5. Business Logic: Real Estate Market

In real estate analysis, it is critical to distinguish between "Total Sales" and "Market Transactions."

* **Total Dwellings Sold (633101):** Includes all ownership changes, such as subsidized municipal housing buyouts by tenants or donations.
* **Market Dwellings Sold (633617):** Includes only arms-length transactions (buying/selling on the open market).
* **Market Share %:** The measure `DIVIDE([Market Dwellings Sold], [Total Dwellings Sold])` indicates the level of market commercialization. A high ratio suggests a region is attractive for private investment and developers.

---

## 6. Data Source & Folder Mapping

Below is the complete inventory of GUS Source Data used in the project and their assigned Power BI Display Folders.

| Display Folder | Metric Name | Variable ID | Calculation |
| :--- | :--- | :--- | :--- |
| **Labor Market** | Avg Gross Wage | 64428 | AVERAGE |
| **Labor Market** | Unemployment Rate | 60270 | AVERAGE |
| **Living Standards** | Avg Disposable Income | 216968 | AVERAGE |
| **Living Standards** | Avg Expenditures | 7737 | AVERAGE |
| **Economy** | GDP per Capita | 458421 | AVERAGE |
| **Economy** | Investment per Capita | 60520 | AVERAGE |
| **Business & Innovation** | Entities per 10k Population | 60530 | AVERAGE |
| **Housing Market** | Price per m2 (Residential) | 633692 | AVERAGE |
| **Housing Market** | Dwellings per 1k Population | 747060 | AVERAGE |
| **Housing Market** | Completed Dwellings (Total) | 748601 | SUM |
| **Housing Market** | Total Dwellings Sold | 633101 | SUM |
| **Housing Market** | Market Dwellings Sold | 633617 | SUM |
| **Public Finance** | Voivodship Budget Revenue | 60508 | AVERAGE |
| **Public Finance** | Voivodship Budget Expenditure | 60518 | AVERAGE |
| **Demographics** | Total Population | 72305 | SUM |

