"""Prompt templates for Hive to BigQuery SQL conversion."""

HIVE_VALIDATION_PROMPT = """You are a Hive SQL syntax expert. Validate if the following SQL is valid Hive SQL syntax.

```sql
{hive_sql}
```

Respond in JSON format only:
{{
    "is_valid": true/false,
    "error": "error message if invalid, null if valid"
}}

Hive SQL features to consider:
- Data types: STRING, INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP, DATE, ARRAY, MAP, STRUCT
- Functions: date_format, datediff, date_add, date_sub, from_unixtime, unix_timestamp, nvl, concat_ws, collect_list, collect_set, get_json_object
- Syntax: LATERAL VIEW, EXPLODE, POSEXPLODE, DISTRIBUTE BY, CLUSTER BY, SORT BY, GROUPING SETS
- DDL: CREATE TABLE, ALTER VIEW, PARTITIONED BY, STORED AS, ROW FORMAT, SERDE, TBLPROPERTIES
- DML: INSERT OVERWRITE TABLE, INSERT INTO

**IMPORTANT - Scheduling System Parameters:**
The SQL may contain scheduling system macros/variables. These are VALID and should NOT be treated as errors:
- `set hivevar:var_name=${{...}};` - Variable definition statements
- `${{zdt.format("yyyy-MM-dd")}}` - Date formatting macro
- `${{zdt.addDay(-1).format("yyyyMMdd")}}` - Date calculation macro
- `${{zdt.add(10,-1).format("HH")}}` - Time calculation macro
- `${{zdt.addMonth(-1).format("yyyy-MM")}}` - Month calculation macro
- `${{hivevar:var_name}}` - Variable reference
- `${{var_name}}` - Simple variable reference
- String concatenation in variable values like `${{...}}_suffix`

These macros are runtime placeholders from the scheduling system. Treat them as valid string literals.

Be strict on syntax, permissive on semantics (don't check if tables exist).
"""

HIVE_TO_BIGQUERY_PROMPT = """You are an expert SQL translator. Convert Hive SQL to functionally equivalent BigQuery SQL.

## Input Hive SQL:
```sql
{hive_sql}
```

{table_mapping_info}

---

## Conversion Rules:

### 1. Data Types
| Hive | BigQuery |
|------|----------|
| STRING | STRING |
| INT, SMALLINT, TINYINT | INT64 |
| BIGINT | INT64 |
| FLOAT | FLOAT64 |
| DOUBLE | FLOAT64 |
| BOOLEAN | BOOL |
| TIMESTAMP | TIMESTAMP |
| DATE | DATE |
| DECIMAL(p,s) | NUMERIC or BIGNUMERIC |
| ARRAY<T> | ARRAY<T> |
| MAP<K,V> | JSON or STRUCT |
| STRUCT<...> | STRUCT<...> |

### 2. Date/Time Functions
| Hive | BigQuery |
|------|----------|
| date_format(date, 'yyyy-MM-dd') | FORMAT_DATE('%Y-%m-%d', date) |
| date_format(date, 'yyyy-MM-dd HH:mm:ss') | FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', ts) |
| datediff(end, start) | DATE_DIFF(end, start, DAY) |
| date_add(date, n) | DATE_ADD(date, INTERVAL n DAY) |
| date_sub(date, n) | DATE_SUB(date, INTERVAL n DAY) |
| add_months(date, n) | DATE_ADD(date, INTERVAL n MONTH) |
| from_unixtime(ts) | TIMESTAMP_SECONDS(CAST(ts AS INT64)) |
| from_unixtime(ts, 'yyyy-MM-dd') | FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_SECONDS(CAST(ts AS INT64))) |
| unix_timestamp() | UNIX_SECONDS(CURRENT_TIMESTAMP()) |
| unix_timestamp(ts) | UNIX_SECONDS(TIMESTAMP(ts)) |
| unix_timestamp(str, fmt) | UNIX_SECONDS(PARSE_TIMESTAMP(fmt, str)) |
| to_date(ts) | DATE(ts) |
| current_date() | CURRENT_DATE() |
| current_timestamp() | CURRENT_TIMESTAMP() |
| year(date) | EXTRACT(YEAR FROM date) |
| month(date) | EXTRACT(MONTH FROM date) |
| day(date) | EXTRACT(DAY FROM date) |
| hour(ts) | EXTRACT(HOUR FROM ts) |
| minute(ts) | EXTRACT(MINUTE FROM ts) |
| second(ts) | EXTRACT(SECOND FROM ts) |
| weekofyear(date) | EXTRACT(WEEK FROM date) |
| dayofweek(date) | EXTRACT(DAYOFWEEK FROM date) |
| last_day(date) | LAST_DAY(date) |
| trunc(date, 'MM') | DATE_TRUNC(date, MONTH) |
| trunc(date, 'YYYY') | DATE_TRUNC(date, YEAR) |

### 3. String Functions
| Hive | BigQuery |
|------|----------|
| concat(a, b, ...) | CONCAT(a, b, ...) |
| concat_ws(sep, a, b, ...) | ARRAY_TO_STRING([a, b, ...], sep) |
| substr(str, pos, len) | SUBSTR(str, pos, len) |
| substring(str, pos, len) | SUBSTR(str, pos, len) |
| length(str) | LENGTH(str) |
| upper(str) | UPPER(str) |
| lower(str) | LOWER(str) |
| trim(str) | TRIM(str) |
| ltrim(str) | LTRIM(str) |
| rtrim(str) | RTRIM(str) |
| lpad(str, len, pad) | LPAD(str, len, pad) |
| rpad(str, len, pad) | RPAD(str, len, pad) |
| instr(str, substr) | STRPOS(str, substr) |
| locate(substr, str) | STRPOS(str, substr) |
| locate(substr, str, pos) | STRPOS(SUBSTR(str, pos), substr) + pos - 1 |
| replace(str, search, replace) | REPLACE(str, search, replace) |
| reverse(str) | REVERSE(str) |
| split(str, delim) | SPLIT(str, delim) |
| regexp_extract(str, pattern, idx) | REGEXP_EXTRACT(str, pattern) |
| regexp_replace(str, pattern, repl) | REGEXP_REPLACE(str, pattern, repl) |
| parse_url(url, 'HOST') | NET.HOST(url) |
| get_json_object(json, '$.key') | JSON_EXTRACT_SCALAR(json, '$.key') |
| json_tuple(json, 'k1', 'k2') | JSON_EXTRACT_SCALAR(json, '$.k1'), JSON_EXTRACT_SCALAR(json, '$.k2') |

### 4. Aggregate Functions
| Hive | BigQuery |
|------|----------|
| count(*) | COUNT(*) |
| count(distinct col) | COUNT(DISTINCT col) |
| sum(col) | SUM(col) |
| avg(col) | AVG(col) |
| min(col) | MIN(col) |
| max(col) | MAX(col) |
| collect_list(col) | ARRAY_AGG(col IGNORE NULLS) |
| collect_set(col) | ARRAY_AGG(DISTINCT col IGNORE NULLS) |
| percentile_approx(col, 0.5) | APPROX_QUANTILES(col, 100)[OFFSET(50)] |
| percentile_approx(col, 0.95) | APPROX_QUANTILES(col, 100)[OFFSET(95)] |
| var_pop(col) | VAR_POP(col) |
| var_samp(col) | VAR_SAMP(col) |
| stddev_pop(col) | STDDEV_POP(col) |
| stddev_samp(col) | STDDEV_SAMP(col) |

### 5. Conditional & NULL Functions
| Hive | BigQuery |
|------|----------|
| nvl(a, b) | IFNULL(a, b) or COALESCE(a, b) |
| nvl2(expr, val1, val2) | IF(expr IS NOT NULL, val1, val2) |
| coalesce(a, b, ...) | COALESCE(a, b, ...) |
| if(cond, then, else) | IF(cond, then, else) |
| case when ... end | CASE WHEN ... END |
| nullif(a, b) | NULLIF(a, b) |
| isnull(a) | a IS NULL |
| isnotnull(a) | a IS NOT NULL |

### 6. Math Functions
| Hive | BigQuery |
|------|----------|
| abs(x) | ABS(x) |
| ceil(x) / ceiling(x) | CEIL(x) |
| floor(x) | FLOOR(x) |
| round(x, d) | ROUND(x, d) |
| pow(x, y) / power(x, y) | POW(x, y) |
| sqrt(x) | SQRT(x) |
| exp(x) | EXP(x) |
| ln(x) | LN(x) |
| log(base, x) | LOG(x, base) |
| log10(x) | LOG10(x) |
| log2(x) | LOG(x, 2) |
| rand() | RAND() |
| mod(a, b) | MOD(a, b) |
| greatest(a, b, ...) | GREATEST(a, b, ...) |
| least(a, b, ...) | LEAST(a, b, ...) |
| sign(x) | SIGN(x) |

### 7. Array Functions
| Hive | BigQuery |
|------|----------|
| size(array) | ARRAY_LENGTH(array) |
| array_contains(arr, val) | val IN UNNEST(arr) |
| sort_array(arr) | (SELECT ARRAY_AGG(x ORDER BY x) FROM UNNEST(arr) x) |
| array(a, b, c) | [a, b, c] |
| explode(arr) | UNNEST(arr) |
| posexplode(arr) | UNNEST(arr) WITH OFFSET |

### 8. Map Functions
| Hive | BigQuery |
|------|----------|
| map('k1', v1, 'k2', v2) | JSON_OBJECT('k1', v1, 'k2', v2) or STRUCT(v1 AS k1, v2 AS k2) |
| map_keys(map) | (extract keys from JSON/STRUCT) |
| map_values(map) | (extract values from JSON/STRUCT) |

### 9. LATERAL VIEW / EXPLODE
```sql
-- Hive:
SELECT id, item FROM t LATERAL VIEW explode(items) tmp AS item

-- BigQuery:
SELECT id, item FROM t CROSS JOIN UNNEST(items) AS item
```

```sql
-- Hive (with position):
SELECT id, pos, item FROM t LATERAL VIEW posexplode(items) tmp AS pos, item

-- BigQuery:
SELECT id, pos, item FROM t CROSS JOIN UNNEST(items) AS item WITH OFFSET AS pos
```

### 10. GROUPING SETS / CUBE / ROLLUP
```sql
-- Hive:
GROUP BY a, b GROUPING SETS ((a, b), (a), ())

-- BigQuery (same syntax supported):
GROUP BY GROUPING SETS ((a, b), (a), ())
-- or
GROUP BY ROLLUP(a, b)
-- or  
GROUP BY CUBE(a, b)
```

### 11. DDL Conversions

**CRITICAL: BigQuery DDL does NOT support wildcard `*` in table names!**
- `CREATE TABLE table_*` is INVALID
- `CREATE OR REPLACE TABLE table_*` is INVALID  
- Wildcard `*` is ONLY valid in SELECT queries for reading data

```sql
-- Hive INSERT OVERWRITE:
INSERT OVERWRITE TABLE target_table SELECT ...

-- BigQuery (use CREATE OR REPLACE):
CREATE OR REPLACE TABLE `target_table` AS SELECT ...
```

```sql
-- Hive ALTER VIEW:
ALTER VIEW view_name AS SELECT ...

-- BigQuery:
CREATE OR REPLACE VIEW `view_name` AS SELECT ...
```

```sql
-- Hive CREATE TABLE with partitioning:
CREATE TABLE t (...) PARTITIONED BY (dt STRING) STORED AS PARQUET

-- BigQuery:
CREATE TABLE `t` (...) PARTITION BY dt
-- Note: Remove STORED AS, ROW FORMAT, SERDE, TBLPROPERTIES
```

#### 11.1 DDL with Dynamic Table Names (MUST use EXECUTE IMMEDIATE)
When DDL target table name contains variables, you MUST use EXECUTE IMMEDIATE:
```sql
-- Hive:
INSERT OVERWRITE TABLE db.result_${{hivevar:date_suffix}} SELECT ...

-- BigQuery (WRONG - wildcard not allowed in DDL):
CREATE OR REPLACE TABLE `project.dataset.result_*` AS SELECT ...  -- ❌ INVALID!

-- BigQuery (CORRECT - use EXECUTE IMMEDIATE):
DECLARE date_suffix STRING DEFAULT FORMAT_DATE('%Y%m%d', CURRENT_DATE());
EXECUTE IMMEDIATE FORMAT('''
  CREATE OR REPLACE TABLE `project.dataset.result_%s` AS
  SELECT * FROM source_table
''', date_suffix);
```

#### 11.2 Reading from Dynamic Tables (Wildcard OK in SELECT only)
```sql
-- BigQuery: Wildcard is ONLY valid for reading data in SELECT
SELECT * FROM `project.dataset.events_*`
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
```

### 12. Hive-Specific Syntax to Remove/Convert
| Hive | BigQuery |
|------|----------|
| DISTRIBUTE BY col | (remove - BQ handles automatically) |
| CLUSTER BY col | (remove or use ORDER BY) |
| SORT BY col | ORDER BY col |
| STORED AS format | (remove) |
| ROW FORMAT ... | (remove) |
| SERDE ... | (remove) |
| TBLPROPERTIES (...) | (remove or use OPTIONS) |
| /*+ HINT */ | (remove hints) |

### 13. Table References
- Use backticks for BigQuery table names: `project.dataset.table`
- Apply the table mapping provided above to replace Hive table names

### 14. Hive Variable Conversion to BigQuery Scripting - CRITICAL

#### 14.1 SET hivevar Statements
Convert Hive variable definitions to BigQuery DECLARE/SET:
```sql
-- Hive:
set hivevar:start_date=${{zdt.addDay(-7).format("yyyy-MM-dd")}};
set hivevar:end_date=${{zdt.format("yyyy-MM-dd")}};
set hivevar:table_suffix=${{zdt.addDay(-1).format("yyyyMMdd")}};
set hivevar:date_app=${{zdt.addDay(-1).format("yyyyMMdd")}}_test;  -- with suffix
set hivevar:month_start=${{zdt.format("yyyy-MM")}}-01;  -- month start date

-- BigQuery:
DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
DECLARE end_date DATE DEFAULT CURRENT_DATE();
DECLARE table_suffix STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));
DECLARE date_app STRING DEFAULT CONCAT(FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)), '_test');
DECLARE month_start DATE DEFAULT DATE_TRUNC(CURRENT_DATE(), MONTH);  -- first day of month
```

#### 14.2 Scheduling Parameter Mappings
| Hive Scheduling Param | BigQuery Equivalent |
|----------------------|---------------------|
| `${{zdt.format("yyyy-MM-dd")}}` | `CURRENT_DATE()` |
| `${{zdt.format("yyyyMMdd")}}` | `FORMAT_DATE('%Y%m%d', CURRENT_DATE())` |
| `${{zdt.addDay(N).format("yyyy-MM-dd")}}` | `DATE_ADD(CURRENT_DATE(), INTERVAL N DAY)` |
| `${{zdt.addDay(-N).format("yyyy-MM-dd")}}` | `DATE_SUB(CURRENT_DATE(), INTERVAL N DAY)` |
| `${{zdt.add(10,-1).format("yyyy-MM-dd")}}` | `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` |
| `${{zdt.add(10,-1).format("HH")}}` | `FORMAT_TIMESTAMP('%H', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))` |
| `${{zdt.addMonth(-1).format("yyyy-MM")}}` | `FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))` |
| `${{zdt.addMonth(N).format(...)}}` | `DATE_ADD(CURRENT_DATE(), INTERVAL N MONTH)` |
| `${{zdt.format("yyyy-MM")}}-01` (month start) | `DATE_TRUNC(CURRENT_DATE(), MONTH)` |
| `${{...}}_suffix` (with suffix) | `CONCAT(FORMAT_DATE(...), '_suffix')` |
| String concatenation `a + b` | `CONCAT(a, b)` or `a \|\| b` |
| Hive `add_months(date, N)` | `DATE_ADD(date, INTERVAL N MONTH)` |

#### 14.3 Using Variables in WHERE Clauses
When `${{var}}` is used for filtering values, replace with variable name directly (no quotes):
```sql
-- Hive:
WHERE dt = '${{hivevar:start_date}}'
WHERE d = '${{zdt.format("yyyy-MM-dd")}}'

-- BigQuery:
WHERE dt = start_date  -- (if declared as DATE variable)
WHERE d = CURRENT_DATE()  -- (inline the function if no variable)
```

#### 14.4 Using Variables in FROM Clause (Dynamic Table Names)
When `${{var}}` constructs table names dynamically:

**IMPORTANT: Choose the right approach based on statement type:**
- **SELECT (reading data)**: Can use wildcard `table_*` with `_TABLE_SUFFIX`
- **DDL (CREATE/INSERT)**: MUST use `EXECUTE IMMEDIATE` (wildcard NOT allowed!)

**Option A: Wildcard Tables (SELECT only, NOT for DDL!)**
```sql
-- Hive:
SELECT * FROM db.table_${{zdt.format("yyyyMMdd")}}

-- BigQuery (OK for SELECT):
SELECT * FROM `project.dataset.table_*`
WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE())
```

**Option B: EXECUTE IMMEDIATE (Required for DDL with dynamic table names)**
```sql
-- BigQuery:
DECLARE table_name STRING DEFAULT CONCAT('project.dataset.table_', FORMAT_DATE('%Y%m%d', CURRENT_DATE()));
DECLARE query STRING;
SET query = FORMAT('SELECT * FROM `%s` WHERE dt = @dt', table_name);
EXECUTE IMMEDIATE query USING DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS dt;
```

#### 14.5 Complete Conversion Example
```sql
-- Hive:
set hivevar:dt=${{zdt.addDay(-1).format("yyyy-MM-dd")}};
set hivevar:suffix=${{zdt.addDay(-1).format("yyyyMMdd")}};
SELECT * FROM db.events_${{hivevar:suffix}} WHERE dt = '${{hivevar:dt}}';

-- BigQuery:
DECLARE dt DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
DECLARE suffix STRING DEFAULT FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY));

SELECT * FROM `project.dataset.events_*`
WHERE _TABLE_SUFFIX = suffix AND dt = dt;
```

#### 14.6 DDL with Dynamic Table Names (MUST use EXECUTE IMMEDIATE)
**Note: DDL statements (CREATE TABLE, INSERT INTO) do NOT support wildcard `*`!**
```sql
-- Hive:
set hivevar:date_app=${{zdt.addDay(-1).format("yyyyMMdd")}}_test;
INSERT OVERWRITE TABLE db.result_${{hivevar:date_app}}
SELECT * FROM db.source WHERE dt = '${{hivevar:date_app}}';

-- BigQuery (WRONG - this will fail!):
CREATE OR REPLACE TABLE `project.dataset.result_*` AS ...  -- ❌ INVALID!

-- BigQuery (CORRECT - use EXECUTE IMMEDIATE for dynamic DDL):
DECLARE date_app STRING DEFAULT CONCAT(FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)), '_test');
DECLARE target_table STRING DEFAULT CONCAT('project.dataset.result_', date_app);
DECLARE query STRING;

SET query = FORMAT('''
  CREATE OR REPLACE TABLE `%s` AS
  SELECT * FROM `project.dataset.source` WHERE dt = @date_app
''', target_table);

EXECUTE IMMEDIATE query USING date_app AS date_app;
```

### 15. Template Variables Preservation (When NOT Converting)
- If instructed to preserve template variables, keep `${{zdt.format(...)}}` as-is
- Do NOT wrap them with PARSE_DATE(), DATE(), CAST() or any functions
- They are runtime placeholders and should remain as string literals
- Correct: `WHERE d = '${{zdt.format("yyyy-MM-dd")}}'`
- WRONG: `WHERE d = DATE('${{zdt.format("yyyy-MM-dd")}}')`

### 16. UDF Functions
- Custom UDF calls like `db.function_name(...)` should be preserved as-is
- The UDF will be migrated separately

---

## Output Requirements:
1. Return ONLY the converted BigQuery SQL
2. No explanations, no markdown formatting, no code blocks
3. Preserve the query structure and logic
4. Ensure all table names are mapped correctly
5. Keep template variables unchanged
"""

BIGQUERY_VALIDATION_PROMPT = """You are a BigQuery SQL syntax expert. Validate if the following SQL is valid BigQuery syntax.

```sql
{bigquery_sql}
```

Respond in JSON format only:
{{
    "is_valid": true/false,
    "error": "detailed error message if invalid, null if valid"
}}

Check for:
1. Valid function names and argument counts
2. Correct data types (INT64, FLOAT64, BOOL, STRING, etc.)
3. Proper UNNEST / CROSS JOIN syntax
4. Valid table references with backticks
5. Correct GROUP BY with aggregates
6. Valid window function syntax
7. Proper GROUPING SETS / ROLLUP / CUBE syntax

Be permissive on: table existence, column names, custom UDFs.
"""

FIX_BIGQUERY_PROMPT = """You are an expert BigQuery SQL debugger. Fix the BigQuery SQL based on the error.

## Original Hive SQL:
```sql
{hive_sql}
```

## Current BigQuery SQL (has error):
```sql
{bigquery_sql}
```

## BigQuery Error:
```
{error_message}
```

## Previous Attempts:
{conversion_history}

---

## Common Fixes:

### Data Type Errors
- Use INT64 instead of INT/INTEGER
- Use FLOAT64 instead of FLOAT/DOUBLE  
- Use BOOL instead of BOOLEAN
- Add CAST() for type conversions: `CAST(col AS INT64)`

### Function Errors
- date_format → FORMAT_DATE or FORMAT_TIMESTAMP
- datediff → DATE_DIFF(end, start, DAY)
- nvl → IFNULL or COALESCE
- collect_list → ARRAY_AGG
- size(arr) → ARRAY_LENGTH(arr)
- instr/locate → STRPOS

### LATERAL VIEW / EXPLODE Errors
```sql
-- Wrong:
LATERAL VIEW explode(arr) t AS item

-- Correct:
CROSS JOIN UNNEST(arr) AS item
```

### String Concatenation
```sql
-- Wrong: 
concat_ws("_", a, b, c)

-- Correct:
ARRAY_TO_STRING([a, b, c], "_")
```

### Reserved Keywords
- Use backticks for reserved words: `select`, `from`, `table`, `group`, `order`, `language`, etc.

### Variable & Scheduling Parameter Errors
- If error mentions "Could not cast literal '${{...}}'", convert to BigQuery scripting
- Replace `${{zdt.format(...)}}` with appropriate BigQuery date functions:
  - `${{zdt.format("yyyy-MM-dd")}}` → `CURRENT_DATE()`
  - `${{zdt.addDay(-1).format("yyyy-MM-dd")}}` → `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`
- For dynamic table names with variables, use:
  - Wildcard tables: `FROM table_* WHERE _TABLE_SUFFIX = ...`
  - Or EXECUTE IMMEDIATE for fully dynamic queries
- If preserving variables is required, keep `${{...}}` as-is without wrappers

### GROUP BY with Non-Aggregated Columns
- Ensure all non-aggregated SELECT columns are in GROUP BY
- For GROUPING SETS, include all columns used in any grouping set

---

## Output:
Return ONLY the corrected BigQuery SQL. No explanations, no markdown.
"""
