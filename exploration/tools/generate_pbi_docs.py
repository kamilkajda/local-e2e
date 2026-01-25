import os

def generate_documentation():
    """
    Generates the master documentation for Power BI measures (Blueprint).
    Ensures naming consistency and clear logic for the entire project.
    """
    # 1. Path resolution (handles the project structure)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up 3 levels: exploration/tools/ -> exploration/ -> project_root/
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
    docs_dir = os.path.join(project_root, "docs")
    file_path = os.path.join(docs_dir, "pbi_advanced_measures.md")

    # 2. Create docs directory if it doesn't exist
    if not os.path.exists(docs_dir):
        os.makedirs(docs_dir)
        print(f"Created directory: {docs_dir}")

    # 3. Define content (Professional English Documentation)
    content = """# Power BI Master Measures Blueprint & Development Process

This document defines the standards for creating DAX measures and the analytical business logic within the Polish Economic Analysis project.

## 1. Metric Development Workflow

To maintain report consistency, every new data point follows this process:
1. **GUS Discovery:** Identify the `variable_id` manually (e.g., via the BDL portal) and verify the unit of measure.
2. **ETL Integration:** Add the ID and metadata to `configs/gus_metrics.json` and run `scripts/run_etl_dev.ps1`.
3. **Semantic Layer:** Implement the standardized 5-measure pattern in Power BI within the appropriate Display Folder.
4. **UI Branding:** Apply conditional formatting using dedicated Status Color measures in the `UI Mapping` folder.

---

## 2. Global Naming Standards

* **Avg [Metric Name]**: Used for rates, ratios, and averages (e.g., Price per m2, Unemployment).
* **Total [Metric Name]**: Used for absolute counts and sums (e.g., Population, Dwellings Sold).
* **[Metric] (Poland)**: National benchmark (Level 0).
* **[Metric] YoY %**: Regional Year-over-Year growth.
* **[Metric] (Poland) YoY %**: National Year-over-Year growth.
* **[Metric] vs Avg Poland Gap %**: Relative difference between region and national benchmark.

---

## 3. Standard 5-Measure Pattern (Example: Gross Wages)
**Display Folder:** `Labor Market`

### [A] Base Metric (ID: 64428)
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
Avg Gross Wage YoY % = 
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
Avg Gross Wage (Poland) YoY % = 
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
Avg Gross Wage vs Avg Poland Gap % = 
DIVIDE(
    [Avg Gross Wage] - [Avg Gross Wage (Poland)], 
    [Avg Gross Wage (Poland)]
)
```

---

## 4. UI Formatting Logic (Folder: UI Mapping)

### Status Colors (Higher is Better)
*Used for Wages, Household Income, GDP, etc.*
```dax
Color Status Positive = 
VAR _Trend = [Selected Metric YoY %]
RETURN 
    SWITCH( TRUE(),
        _Trend > 0, "#00C805", -- Green
        _Trend < 0, "#FF0000", -- Red
        "#808080"              -- Grey
    )
```

### Status Colors (Lower is Better)
*Used for Unemployment Rate.*
```dax
Color Status Unemployment = 
VAR _Trend = [Avg Unemployment Rate YoY %]
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

* **Total Dwellings Sold (633101):** Includes all ownership changes (subsidized buyouts, donations, etc.).
* **Total Market Dwellings Sold (633617):** Includes only arms-length transactions on the open market.
* **Total Market Share %:** `DIVIDE([Total Market Dwellings Sold], [Total Dwellings Sold])`. A high ratio suggests a region is attractive for private investment and developers.

---

## 6. Data Source & Inventory

| Display Folder | Professional Metric Name | Variable ID | Calculation |
| :--- | :--- | :--- | :--- |
| **Labor Market** | Avg Gross Wage | 64428 | AVERAGE |
| **Labor Market** | Avg Unemployment Rate | 60270 | AVERAGE |
| **Living Standards** | Avg Household Disposable Income | 216968 | AVERAGE |
| **Living Standards** | Avg Household Expenditures | 7737 | AVERAGE |
| **Economy** | Avg GDP per Capita | 458421 | AVERAGE |
| **Economy** | Avg Investment per Capita | 60520 | AVERAGE |
| **Business & Innovation** | Avg Business Entities per 10k | 60530 | AVERAGE |
| **Housing Market** | Avg Residential Price per m2 | 633692 | AVERAGE |
| **Housing Market** | Avg Dwellings Completed per 1k | 747060 | AVERAGE |
| **Housing Market** | Total Dwellings Completed | 748601 | SUM |
| **Housing Market** | Total Dwellings Sold | 633101 | SUM |
| **Housing Market** | Total Market Dwellings Sold | 633617 | SUM |
| **Public Finance** | Avg Budget Revenue per capita | 60508 | AVERAGE |
| **Public Finance** | Avg Budget Expenditure per capita | 60518 | AVERAGE |
| **Demographics** | Total Population | 72305 | SUM |
"""

    # 4. Write to file with UTF-8 encoding
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Document updated successfully: {file_path}")
    except Exception as e:
        print(f"Error writing file: {e}")

if __name__ == "__main__":
    generate_documentation()