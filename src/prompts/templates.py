"""Prompt templates for Spark to BigQuery SQL conversion."""

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


### 6. Handling Type Mismatch (STRING vs INT64)

* **BigQuery is strongly typed and does not support implicit conversion between STRING and INT64 (e.g., `Operator = for argument types: STRING, INT64` error). 
* **You must use the table DDL to check the data type of the column and apply the following rules:

(1) Numeric Literals:
If a STRING column is compared to a numeric literal, wrap the literal in quotes.
- Example: `WHERE country_flag = 1` → `WHERE country_flag = '1'` (if country_flag is STRING).

(2) JOIN Keys & Column Comparisons:
If comparing a STRING column with an INT64 column, explicitly cast the types to match.
- Priority: Use `SAFE_CAST(string_column AS INT64)` to convert the string side.
- Reason: `SAFE_CAST` returns NULL instead of failing if the string contains non-numeric characters.
- Example: `ON t.id = m.id` (if t.id is STRING) → `ON SAFE_CAST(t.id AS INT64) = m.id`.

(3) Inequality Logic (>, <):
If a STRING column is involved in a numerical range check (e.g., `country > 1`), you must cast it to ensure numerical comparison rather than dictionary (lexicographical) order.
- Example: `WHERE country > 1` → `WHERE SAFE_CAST(country AS INT64) > 1`.

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

FIX_BIGQUERY_PROMPT = """
You are a BigQuery SQL Expert and Debugger. Your goal is to fix the provided "Current BigQuery SQL" based on the specific "BigQuery Error" and "Original Spark SQL" logic.

## Context Data
### 1. Original Spark SQL (Reference Logic):
```sql
{spark_sql}

```

### 2. Current BigQuery SQL (Has Error):

```sql
{bigquery_sql}

```

### 3. BigQuery Error Message:

```
{error_message}

```

### 4. Target Table DDLs:

{table_ddls}

---

## Debugging & Fixing Guidelines

Apply the following rules to resolve errors. Prioritize the specific error message provided.

### A. Syntax & Structure Fixes

1. **Backticks:** Wrap ALL table names in backticks (e.g., `project-id.dataset.table`), especially if they contain hyphens.
2. **Unnest Syntax:**
* Replace `LATERAL VIEW explode(col)` with `CROSS JOIN UNNEST(col) AS alias`.
* **Crucial:** Place the alias *after* the closing parenthesis of UNNEST.
* *Correct:* `CROSS JOIN UNNEST(my_array) AS item`
* *Wrong:* `CROSS JOIN UNNEST(my_array item)`


3. **Grouping Sets:** Ensure no columns are listed between `GROUP BY` and `GROUPING SETS`.
* *Correct:* `GROUP BY GROUPING SETS ((a, b), (a))`


4. **Multi-statement:** Ensure statements are separated by semicolons `;`.
5. **Window Functions:** If `HAVING` filters a window function result, wrap the query in a subquery and use `WHERE`.

### B. Data Type & Function Mapping

1. **Strict Types:** Use `INT64` (not INT), `FLOAT64` (not DOUBLE), `BOOL` (not BOOLEAN).
2. **ID Columns (String vs Int):** If comparing a String ID (e.g., `hotelid`, `cityid`) to a number, use `SAFE_CAST(col AS INT64)`.
3. **Coalesce/IfNull:** Ensure arguments have matching types.
* Numeric: `COALESCE(num_col, 0)`
* String: `COALESCE(str_col, '')` or `COALESCE(CAST(id_col AS STRING), '')`


4. **Arrays:**
* `collect_list` → `ARRAY_AGG`
* `size(arr)` → `ARRAY_LENGTH(arr)`
* `concat_ws` → `ARRAY_TO_STRING([a, b], separator)`. Ensure elements are CAST to STRING first.


5. **JSON vs Structs:**
* If column is `ARRAY<STRUCT>`, use `UNNEST`. Do NOT use `JSON_EXTRACT` functions.
* If column is JSON String, use `JSON_EXTRACT_SCALAR`.


### C. Date & Partition Handling

1. **Partition Columns:** NEVER use `PARSE_DATE` on a partition column that is already a DATE. Instead, cast the *value* you are comparing against.
* *Correct:* `d = DATE('2024-01-01')`


2. **Spark Macros:** Convert `${{zdt...}}` macros to native BigQuery functions:
* `'${{zdt.add(0).format("yyyy-MM-dd")}}'` → `CURRENT_DATE()`
* `'${{zdt.add(-1).format("yyyy-MM-dd")}}'` → `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`
* Remove quotes around the resulting function calls.

---

## Output Requirement

* Return **ONLY** the corrected BigQuery SQL code block.
* Do NOT include markdown like "Here is the fixed code" or explanations.
* Do NOT output JSON.
"""
