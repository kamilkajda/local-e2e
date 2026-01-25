POWER BI MASTER MEASURES BLUEPRINT

METRIC DEVELOPMENT WORKFLOW

Discovery: Find ID via exploration tools.

Integration: Add to configs/gus_metrics.json.

Base Measure: Filter fact table by variable_id.

Benchmarking: Compare with level_type = "Country".

Time Intelligence: YoY growth.

STANDARD MEASURE PATTERN (Example: Gross Wages)

Avg Gross Wage =
CALCULATE(
AVERAGE(Fact_Economics[value]),
Fact_Economics[variable_id] = 64428
)

Avg Gross Wage (Poland) =
CALCULATE(
[Avg Gross Wage],
ALL(Dim_Region),
Dim_Region[level_type] = "Country"
)

Wages YoY % =
VAR CurrentYear = SELECTEDVALUE('Dim_Calendar'[year])
VAR CurrentValue = [Avg Gross Wage]
VAR PreviousValue =
CALCULATE(
[Avg Gross Wage],
'Dim_Calendar'[year] = CurrentYear - 1
)
RETURN DIVIDE(CurrentValue - PreviousValue, PreviousValue)

Wages vs Poland % =
DIVIDE([Avg Gross Wage] - [Avg Gross Wage (Poland)], [Avg Gross Wage (Poland)])

VARIABLE ID REFERENCE

Avg Gross Wage: 64428 (AVG)

Unemployment Rate: 60270 (AVG)

Avg Disposable Income: 216968 (AVG)

Avg Expenditures: 7737 (AVG)

Avg GDP per Capita: 458421 (AVG)

Avg Investment per Capita: 60520 (AVG)

Entities per 10k: 60530 (AVG)

Price per m2: 633692 (AVG)

Dwellings per 1k: 747060 (AVG)

Voivodship Revenue: 60508 (AVG)

Voivodship Expenditure: 60518 (AVG)

Total Population: 72305 (SUM)

Total Dwellings Sold: 633101 (SUM)

Market Dwellings Sold: 633617 (SUM)