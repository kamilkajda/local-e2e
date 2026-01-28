import os

def generate_pbi_documentation():
    """
    Orchestrates the generation of the full Power BI documentation.
    Ensures SSoT for 16 variables and the 5-Measure Pattern.
    """
    
    docs_dir = 'docs'
    if not os.path.exists(docs_dir):
        os.makedirs(docs_dir)
        
    file_path = os.path.join(docs_dir, 'pbi_advanced_measures.md')
    
    lines = []
    
    # 0. Header
    lines.append("# Power BI Master Measures & Design Blueprint\n")
    lines.append("This document serves as the Single Source of Truth (SSoT) for the analytical layer of the Polish Economic Analysis project. It defines the business logic, measure structure, and visual standards of the report.\n")
    
    # 1. Metric Inventory
    lines.append("## 1. Metric Inventory (GUS BDL Mapping)\n")
    lines.append("The model incorporates 16 key economic variables. Each serves as the foundation for the standardized **5-Measure Pattern**.\n")
    lines.append("| Display Folder | Metric Name (Base) | GUS ID | Aggregation | Color Logic |")
    lines.append("| :--- | :--- | :--- | :--- | :--- |")
    lines.append("| **Labor Market** | Avg Gross Wage | 64428 | AVERAGE | Higher is Better |")
    lines.append("| **Labor Market** | Unemployment Rate | 60270 | AVERAGE | Lower is Better |")
    lines.append("| **Living Standards** | Avg Disposable Income | 216968 | AVERAGE | Higher is Better |")
    lines.append("| **Living Standards** | Avg Expenditures | 7737 | AVERAGE | Lower is Better |")
    lines.append("| **Economy** | Total GDP | 458271 | SUM | Higher is Better |")
    lines.append("| **Economy** | GDP per Capita | 458421 | AVERAGE | Higher is Better |")
    lines.append("| **Economy** | Investment per Capita | 60520 | AVERAGE | Higher is Better |")
    lines.append("| **Business & Innovation** | Entities per 10k Pop | 60530 | AVERAGE | Higher is Better |")
    lines.append("| **Housing Market** | Price per m2 (Residential) | 633692 | AVERAGE | Higher is Better |")
    lines.append("| **Housing Market** | Dwellings per 1k Pop | 747060 | AVERAGE | Higher is Better |")
    lines.append("| **Housing Market** | Completed Dwellings | 748601 | SUM | Higher is Better |")
    lines.append("| **Housing Market** | Total Dwellings Sold | 633101 | SUM | Higher is Better |")
    lines.append("| **Housing Market** | Market Dwellings Sold | 633617 | SUM | Higher is Better |")
    lines.append("| **Public Finance** | Budget Revenue | 60508 | AVERAGE | Higher is Better |")
    lines.append("| **Public Finance** | Budget Expenditure | 60518 | AVERAGE | Lower is Better |")
    lines.append("| **Demographics** | Total Population | 72305 | SUM | Neutral |\n")
    
    # 2. Analytical Pattern
    lines.append("## 2. Analytical Pattern: The 5-Measure Framework\n")
    lines.append("For each variable listed above (e.g., Avg Gross Wage), five derivative measures are implemented to provide full analytical context (national benchmark and YoY dynamics).\n")
    lines.append("| Measure Type | DAX Logic Example (for Avg Gross Wage) |")
    lines.append("| :--- | :--- |")
    lines.append("| **1. Base** | `Avg Gross Wage = AVERAGE(Fact_Economics[value])` (Context: Filtered by Region) |")
    lines.append("| **2. National** | `Avg Gross Wage (Poland) = CALCULATE([Avg Gross Wage], ALL(Dim_Region), Dim_Region[region] = \"NATIONAL\")` |")
    lines.append("| **3. YoY %** | `Avg Gross Wage YoY % = DIVIDE([Avg Gross Wage] - [Avg Gross Wage (LY)], [Avg Gross Wage (LY)])` |")
    lines.append("| **4. Nat. YoY %** | `Avg Gross Wage (Poland) YoY % = CALCULATE([Avg Gross Wage YoY %], ALL(Dim_Region), Dim_Region[region] = \"NATIONAL\")` |")
    lines.append("| **5. Gap %** | `Avg Gross Wage vs Avg Poland Gap % = DIVIDE([Avg Gross Wage], [Avg Gross Wage (Poland)]) - 1` |\n")
    
    # 3. Advanced UI Mapping
    lines.append("## 3. Advanced UI Mapping (Conditional Formatting)\n")
    lines.append("The **UI Mapping** folder contains measures controlling the visual layer (chart marker colors, icons, labels).\n")
    lines.append("### [DAX] Color Status Logic")
    lines.append("Each color status measure (e.g., `Color Status Wages`) is based on the variance from the national average (`Gap %`) and the metric's optimization characteristic.\n")
    lines.append("```dax")
    lines.append("Color Status Wages = ")
    lines.append("VAR _Gap = [Avg Gross Wage vs Avg Poland Gap %]")
    lines.append("RETURN")
    lines.append("    SWITCH( TRUE(),")
    lines.append("        _Gap > 0, \"#4E56DE\", -- Indigo (Above National)")
    lines.append("        _Gap < 0, \"#FF4D4D\", -- Red (Below National)")
    lines.append("        \"#A0A0A0\"            -- Gray (Neutral/No Data)")
    lines.append("    )")
    lines.append("```\n")
    
    # 4. Executive Summary
    lines.append("## 4. Executive Summary & Imputation Awareness\n")
    lines.append("The primary analytical component generates a dynamic description of the region's economic status, including an **(ESTIMATED)** flag for data points interpolated during ETL.\n")
    lines.append("### [DAX] Executive Summary Text")
    lines.append("```dax")
    lines.append("Executive Summary Text = ")
    lines.append("VAR _Region = [Selected Region]")
    lines.append("VAR _Year = [Selected Year Number]\n")
    lines.append("-- Base Data")
    lines.append("VAR _Wage = [Avg Gross Wage]")
    lines.append("VAR _WageYoY = [Avg Gross Wages YoY %]")
    lines.append("VAR _Unemployment = [Avg Unemployment Rate]")
    lines.append("VAR _UnemploymentYoY = [Avg Unemployment YoY %]")
    lines.append("VAR _Gap = [Avg Gross Wage vs Avg Poland Gap %]")
    lines.append("VAR _Income = [Avg Household Disposable Income]")
    lines.append("VAR _Expenditures = [Avg Household Expenditures]\n")
    lines.append("-- Derived Business Logic")
    lines.append("VAR _SavingsRate = DIVIDE(_Income - _Expenditures, _Income)")
    lines.append("VAR _IsEstimated = _Region = \"OPOLSKIE\" && _Year = 2023\n")
    lines.append("-- Dynamic Narrative Fragments")
    lines.append("VAR _Header = UPPER(_Region) & \" (\" & _Year & \") - ECONOMIC SUMMARY\" & IF(_IsEstimated, \" (ESTIMATED)\", \"\")")
    lines.append("VAR _LineWages = \"• Wages: Average gross pay reached \" & FORMAT(_Wage, \"#,0 PLN\") & \" (\" & FORMAT(_WageYoY, \"+0.0%;-0.0%\") & \" YoY).\"")
    lines.append("VAR _LineBench = \"• Benchmark: The region is currently \" & FORMAT(ABS(_Gap), \"0.0%\") & IF(_Gap >= 0, \" above \", \" below \") & \"the National average.\"")
    lines.append("VAR _LineLabor = \"• Labor: Unemployment rate \" & IF(_UnemploymentYoY < 0, \"improved to \", \"stood at \") & FORMAT(_Unemployment, \"0.0%\") & \".\"")
    lines.append("VAR _LineBudget = \"• Budget: Disposable income is \" & FORMAT(_Income, \"#,0 PLN\") & \" with a \" & FORMAT(_SavingsRate, \"0%\") & \" savings buffer.\"\n")
    lines.append("RETURN")
    lines.append("    _Header & UNICHAR(10) & ")
    lines.append("    _LineWages & UNICHAR(10) & ")
    lines.append("    _LineBench & UNICHAR(10) & ")
    lines.append("    _LineLabor & UNICHAR(10) & ")
    lines.append("    _LineBudget")
    lines.append("```\n")
    
    # 5. Visual Standards
    lines.append("## 5. Visual Standards (Bento Grid)\n")
    lines.append("The report utilizes a **Bento Grid** architecture for high information density while maintaining readability.\n")
    lines.append("* **Indigo Header**: Dynamic page title (`Report Title Dynamic`) on a `#4E56DE` background.")
    lines.append("* **KPI Cards**: Key indicators (GDP, Wages, Unemployment, Income, Population) with integrated 10-year trend sparklines.")
    lines.append("* **Scatter Chart (Wages vs Unemployment)**: Features a `Play Axis` for temporal movement and dynamic region labels. Axis ranges are locked (Fixed) to ensure consistency.")
    lines.append("* **Purchasing Power Chart**: Compares wage dynamics (`Avg Gross Wage`) with property prices (`Avg Residential Price per m2`) to analyze purchasing power.\n")
    
    # 6. Model Relationships
    lines.append("## 6. Model Relationships (Star Schema)\n")
    lines.append("Based on the `.tmdl` definition, the model follows a star schema centered on the fact table:\n")
    lines.append("* **Fact_Economics** ↔ **Dim_Calendar** (Key: `year`)")
    lines.append("* **Fact_Economics** ↔ **Dim_Region** (Key: `region_id`)")
    lines.append("* **Fact_Economics** ↔ **Dim_Metrics** (Key: `metric_id`)")
    lines.append("* **Fact_Economics** ↔ **Chart_Axis_Years** (Key: `year`) - dedicated axis for chart animations.\n")
    
    # 7. Configuration
    lines.append("## 7. Configuration & Parameters\n")
    lines.append("* **AzuriteBaseUrl**: Parameter controlling the data source (localhost vs. LAN IP), facilitating team collaboration or cross-device presentations.")
    lines.append("* **Selected Region/Year**: Helper measures extracting slicer values for use in dynamic titles and narratives.\n")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines('\n'.join(lines))
    
    print(f"Documentation successfully generated: {file_path}")

if __name__ == "__main__":
    generate_pbi_documentation()