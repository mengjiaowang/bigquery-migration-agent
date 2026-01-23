"""Prompt templates for Spark to BigQuery SQL conversion."""

SPARK_VALIDATION_PROMPT = """You are a Spark SQL syntax expert. Validate if the following SQL is valid SQL syntax.

```sql
{spark_sql}
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

Please note that spark allows using `having` clause after `select` clause
```
select *
       ,row_number() over(...) as rk
from ...
having rk = 1
```
"""

SPARK_TO_BIGQUERY_PROMPT = """
You are an expert SQL translator. Convert Spark SQL to functionally equivalent, **executable** BigQuery SQL.

## Guiding Principles (STRICT)

1. **Logic Equality:** The execution logic must be **exactly** the same as Spark. Do NOT optimize the query for BigQuery performance if it changes the structure (e.g., maintain JOIN order unless necessary for syntax).
2. **Comments:** Preserve **all** existing comments from the Spark SQL in their corresponding locations. Do **NOT** add any new comments or explanations.
3. **Native Execution:** The output must be standard BigQuery SQL, executable without external variables (resolve macros to native functions).

---

## Conversion Rules

### 1. ⚠️ Table Mapping & Naming (Mandatory)

**Target Dataset:** All tables (Input and Output) reside in: `trip-htl-bi-dbprj.htl_bi_temp`
**Naming Convention:** `{{original_database}}_{{original_table_name}}`

#### 1.1 Conversion Logic

You MUST map every table reference `db.table` to the specific BigQuery path using backticks.

* **Rule:** `database.table_name`  ➡️  ``trip-htl-bi-dbprj.htl_bi_temp.{{database}}_{{table_name}}``
* **Example:** `dw_htlbizdb.dim_hotel` ➡️  ``trip-htl-bi-dbprj.htl_bi_temp.dw_htlbizdb_dim_hotel``

#### 1.2 Output Tables

Apply the same mapping rule to the target table in `INSERT` statements.

#### 1.3 Exceptions

dim_hoteldb.dimcity shoule be: trip-htl-bi-dbprj.htl_bi_temp.dim_hoteldb_dimcity_source

### 2. ⚠️ DDL & Partition Handling (Transaction Mode)

BigQuery does not support `INSERT OVERWRITE ... PARTITION`. You MUST convert this into a `DELETE` + `INSERT` transaction.

#### 2.1 The Transaction Pattern

For **any** `INSERT OVERWRITE TABLE target PARTITION (p_col=val) ...`:

1. **Start Transaction:** `BEGIN TRANSACTION;`
2. **Clear Partition:** Generate a `DELETE` statement for the specific partition value.
3. **Insert Data:** Convert to `INSERT INTO`. **Move** the partition column from the `PARTITION()` clause to the SELECT list (or explicit column list).
4. **Commit:** `COMMIT TRANSACTION;`

**Example:**

```sql
-- Spark:
INSERT OVERWRITE TABLE db.target PARTITION (dt = '2023-10-01')
SELECT col1, col2 FROM source;

-- BigQuery:
BEGIN TRANSACTION;

-- Step 1: Delete target partition
DELETE FROM `trip-htl-bi-dbprj.htl_bi_temp.db_target`
WHERE dt = '2023-10-01';

-- Step 2: Insert new data (include partition column in SELECT)
INSERT INTO `trip-htl-bi-dbprj.htl_bi_temp.db_target` (col1, col2, dt)
SELECT col1, col2, '2023-10-01'
FROM `trip-htl-bi-dbprj.htl_bi_temp.db_source`;

COMMIT TRANSACTION;

```

#### 2.2 Dynamic Partitions

If the partition is dynamic (e.g., `PARTITION (dt)`), use the logic from the SELECT clause to define the scope, or if not possible, use `INSERT INTO` directly (but prefer the Transaction pattern if the overwrite scope is determinable).

### 3. ⚠️ Critical Syntax & Type Safety

#### 3.1 Strict Type Handling (No Implicit Conversion)

BigQuery does NOT support implicit casting. You MUST use `SAFE_CAST`.

* **Comparisons:** `WHERE str_col > 0` ➡️ `WHERE SAFE_CAST(str_col AS INT64) > 0`
* **COALESCE:** Arguments must match types. `nvl(num_col, '')` ➡️ `COALESCE(num_col, 0)`

#### 3.2 GROUP BY Syntax

Remove columns listed *before* `GROUPING SETS`.

* Spark: `GROUP BY a, b GROUPING SETS ((a,b))` ➡️ BQ: `GROUP BY GROUPING SETS ((a,b))`

### 4. Variable & Macro Conversion (Native Mode)

Convert Spark scheduling macros (`${{...}}`) directly into native BigQuery functions. **Remove quotes** around the resulting function.

| Spark Macro | BigQuery Equivalent |
| --- | --- |
| `${{zdt.format("yyyy-MM-dd")}}` | `CURRENT_DATE()` |
| `${{zdt.addDay(-1).format("yyyy-MM-dd")}}` | `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` |
| `${{zdt.format("yyyyMMdd")}}` | `FORMAT_DATE('%Y%m%d', CURRENT_DATE())` |

* **Date Comparison:** `d = '${{zdt...}}'` ➡️ `d = CURRENT_DATE()`
* **Dynamic Tables (Read):** `FROM db.table_${{zdt...}}` ➡️ `FROM \`trip...db_table_*` WHERE _TABLE_SUFFIX = ...`

### 5. Complex Data Types (Map/Array)

* **Map Access:** `map['key']` ➡️ `JSON_VALUE(map_col, '$.key')`
* **Explode:** `LATERAL VIEW explode(col) t AS item` ➡️ `CROSS JOIN UNNEST(col) AS item` (Alias strictly after parenthesis)
* **Regex:** Use raw strings `r'...'` (e.g., `REGEXP_REPLACE(col, r'\d', 'X')`).


## Output Requirements:

1. Return **ONLY** the converted BigQuery SQL code.
2. **No** markdown block wrappers (unless strictly necessary for code display).
3. **No** explanations.


## Target Table DDLs:

{table_ddls}

## Table Mapping information: 
{table_mapping_info}

## Input Spark SQL:
```sql
{spark_sql}

```
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

## Original Spark SQL:
```sql
{spark_sql}
```

## Current BigQuery SQL (has error):
```sql
{bigquery_sql}
```

## BigQuery Error:
```
{error_message}
```

---

## Common Fixes:

### Data Type Errors
- Use INT64 instead of INT/INTEGER
- Use FLOAT64 instead of FLOAT/DOUBLE  
- Use BOOL instead of BOOLEAN
- Add CAST() for type conversions: `CAST(col AS INT64)`
- **String-to-Number comparison error (ID columns)**: 
  - Common ID columns (masterhotelid, cityid, country_flag, etc.) are STRING in source tables
  - Use `SAFE_CAST(column_name AS INT64)` when comparing to numbers
  - Example: `AND masterhotelid > 0` → `AND SAFE_CAST(masterhotelid AS INT64) > 0`

### Date Column Errors (PARSE_DATE errors)
- **NEVER apply PARSE_DATE to partition column `d`** - it's already DATE type
- Cast the comparison VALUE to DATE, not the column
- Wrong: `WHERE PARSE_DATE('%Y-%m-%d', d) = ...`
- Correct: `WHERE d = DATE('2024-01-01')` or `WHERE d = DATE('${{zdt...}}')`

### Function Errors
- date_format → FORMAT_DATE or FORMAT_TIMESTAMP
- datediff → DATE_DIFF(end, start, DAY)
- nvl → IFNULL or COALESCE
- collect_list → ARRAY_AGG
- size(arr) → ARRAY_LENGTH(arr)
- instr/locate → STRPOS

### COALESCE Type Mismatch Errors
If error says `No matching signature for function COALESCE - Argument types: INT64, STRING`:
- For numeric columns (star, score, cnt, price, etc.): `COALESCE(col, 0)` instead of `COALESCE(col, '')`
- For ID columns that need string: `COALESCE(CAST(col AS STRING), '')`

### LATERAL VIEW / EXPLODE Errors
```sql
-- Wrong:
LATERAL VIEW explode(arr) t AS item

-- Correct:
CROSS JOIN UNNEST(arr) AS item
```

### UNNEST Alias Position Errors
If error says `Expected ")" or "," but got identifier`:
- **Cause**: Alias is placed inside UNNEST parenthesis instead of after
- **Fix**: Move alias to AFTER the closing parenthesis
```sql
-- ❌ Wrong:
CROSS JOIN UNNEST([...] jt)

-- ✓ Correct:
CROSS JOIN UNNEST([...]) AS jt
```

### json_tuple Conversion Errors
If UNNEST with STRUCT causes errors when converting `LATERAL VIEW json_tuple`:
- **Fix**: Remove UNNEST, use direct JSON_EXTRACT_SCALAR in SELECT instead
```sql
-- Instead of UNNEST with STRUCT, just extract directly:
JSON_EXTRACT_SCALAR(json_col, '$.field_name') AS field_name
```

### JSON_EXTRACT_SCALAR Type Mismatch Errors
If error says `No matching signature for function JSON_EXTRACT_SCALAR` with `ARRAY<STRUCT<key, value>>`:
- **Cause**: Column is `ARRAY<STRUCT<key STRING, value STRING>>` type (not JSON string)
- **Fix**: Use UNNEST subquery instead of JSON_EXTRACT_SCALAR
```sql
-- ❌ Wrong (ARRAY<STRUCT> is not JSON):
JSON_EXTRACT_SCALAR(map_col, '$.key')

-- ✓ Correct:
(SELECT value FROM UNNEST(map_col) WHERE key = 'target_key')
```

### GROUP BY GROUPING SETS Error
If error says `Expected ")" but got keyword GROUPING`:
- **Cause**: Columns listed before `GROUPING SETS` (e.g. `GROUP BY a, b GROUPING SETS...`)
- **Fix**: Remove the columns between `GROUP BY` and `GROUPING SETS`.
  - Wrong: `GROUP BY a, b GROUPING SETS ((a, b))`
  - Correct: `GROUP BY GROUPING SETS ((a, b))`

### ARRAY_TO_STRING Signature Error
If error says `No matching signature for function ARRAY_TO_STRING` or `Argument types: ARRAY<INT64>, STRING`:
- **Cause**: Trying to `ARRAY_TO_STRING` on non-STRING types (INT, FLOAT, etc.)
- **Fix**: Cast elements to STRING inside the array.
  - Wrong: `ARRAY_TO_STRING([year, month], '-')`
  - Correct: `ARRAY_TO_STRING([CAST(year AS STRING), CAST(month AS STRING)], '-')`

### Multiple Statements Error
If error says `Expected end of input but got keyword CREATE`:
- **Cause**: SQL contains multiple statements but dry_run validates single statement
- **Fix**: Ensure statements are properly separated with semicolons, or split into separate queries

### String Concatenation
```sql
-- Wrong: 
concat_ws("_", a, b, c)

-- Correct:
ARRAY_TO_STRING([a, b, c], "_")
```

### Reserved Keywords
- Use backticks for reserved words: `select`, `from`, `table`, `group`, `order`, `language`, etc.

### Syntax Error: Unexpected Identifier (Missing Backticks)
If error says `Syntax error: Unexpected identifier` with a table name containing hyphens:
- **Cause**: Table name with hyphen (like `project-id.dataset.table`) is not wrapped in backticks
- **Fix**: Wrap ALL table names in backticks: `\`project-id.dataset.table\``
- Hyphens in project IDs are interpreted as minus operators without backticks

### Partition Spec Mismatch Errors
If error says `Cannot replace a table with a different partitioning spec`:
- **Cause**: Using `CREATE OR REPLACE TABLE` on a partitioned table without `PARTITION BY`
- **Fix**: Add `PARTITION BY column_name` to match existing table's partition spec
```sql
-- Add PARTITION BY:
CREATE OR REPLACE TABLE `project.dataset.table`
PARTITION BY d  -- ← Add this line
AS SELECT ...
```

### Table Not Qualified Errors
If error says `Table "xxx" must be qualified with a dataset`:

**Step 1: Check if `xxx` is a virtual table (should NOT have prefix)**
- Is it defined in a `WITH xxx AS (...)` clause? → CTE is missing, add the WITH clause
- Is it a subquery alias `(SELECT ...) AS xxx`? → Subquery definition is missing
- Is it an UNNEST alias? → Check UNNEST syntax

**Step 2: If `xxx` is a real table (SHOULD have prefix)**
- Add dataset prefix: `\`project.dataset.xxx\``
- If has Spark db prefix like `db.xxx`, convert to: `\`project.dataset.db_xxx\``
- Check if it needs to be added to the table mapping

**Common patterns:**
- `exploded_data`, `derived_data`, `tmp_xxx` → likely CTEs, check if WITH clause exists
- `v_dim_xxx`, `dim_xxx_df` → likely real views, need prefix
- `ods_xxx`, `dw_xxx`, `dwhtl.xxx` → real tables, need prefix

### Variable & Scheduling Parameter Errors (NATIVE CONVERSION)
**Target: Convert ALL macros to native BigQuery functions.**

* **Error:** `Could not cast literal '${{zdt...}}'` or `Invalid date literal`
* **Fix:** The macro was not converted to a function.
  1. Remove the quotes `'...'`
  2. Replace `${{zdt...}}` with `CURRENT_DATE()`, `DATE_SUB(...)`, or `FORMAT_DATE(...)`
  3. Ensure types match (Comparison to DATE column uses DATE function; Comparison to STRING uses FORMAT_DATE).

* **Error:** `Undeclared variable`
* **Fix:** If using `start_date` or similar, ensure a `DECLARE start_date ...` statement is added at the top of the script.

### GROUP BY with Non-Aggregated Columns
- Ensure all non-aggregated SELECT columns are in GROUP BY
- For GROUPING SETS, include all columns used in any grouping set

### HAVING Clause with Window Functions Error
If HAVING is used to filter on window function results (like `HAVING rk = 1`):
- **Cause**: BigQuery HAVING can only filter aggregates, not window function results
- **Fix**: Wrap in subquery and use WHERE
```sql
-- ❌ Wrong:
SELECT *, ROW_NUMBER() OVER(...) AS rk FROM t HAVING rk = 1

-- ✓ Correct:
SELECT * FROM (SELECT *, ROW_NUMBER() OVER(...) AS rk FROM t) WHERE rk = 1
```

---

## Output:
Return ONLY the corrected BigQuery SQL. No explanations, no markdown.
"""
