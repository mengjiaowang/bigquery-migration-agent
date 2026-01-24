"""Prompt templates for Spark to BigQuery SQL conversion."""

SHARED_SQL_CONVERSION_RULES = """
## Guiding Principles (STRICT)

1. **Logic Equality:** The execution logic must be **exactly** the same as Spark. Do NOT optimize the query for BigQuery performance if it changes the structure (e.g., maintain JOIN order unless necessary for syntax).
2. **Comments:** Preserve **all** existing comments from the Spark SQL in their corresponding locations. Do **NOT** add any new comments or explanations.
3. **Native Execution:** The output must be standard BigQuery SQL, executable without external variables (resolve macros to native functions).

---

## Conversion Rules

### 1. JSON Functions

#### **Rule 1: Extracting Fields (The "Universal Extraction" Rule)**

When extracting fields from the json object:

* **Requirement:** Handle both scalars and non-scalars dynamically.
* **Formula:** Use `COALESCE(JSON_VALUE(t.json_value, '$.field1'), CAST(JSON_EXTRACT(t.json_value, '$.field1') AS STRING))`.
* **Behavior:** This automatically removes quotes for strings/numbers but returns a JSON-formatted string for objects/arrays, perfectly matching Spark's `get_json_object` and `json_tuple` behavior.

#### **Rule 2: Converting `explode` to `UNNEST**`

- For `LATERAL VIEW explode(...)`: Spark SQL drops rows with invalid JSON, you **MUST** apply `WHERE COALESCE(rm_json,'') <> ''` in the query to ensure strict row-count parity with Spark’s inner-join behavior
- For `LATERAL VIEW OUTER explode(...)`: Use `LEFT JOIN UNNEST(JSON_QUERY_ARRAY(json_obj.path))

#### **Rule 3: Apply filter to `LATERAL VIEW json_tuple()`

lateral view json_tuple

##### Example 1: convert a single LATERAL VIEW json_tuple

Spark SQL: 
```sql
SELECT
  field1, field2, field3
FROM table t 
LATERAL VIEW json_tuple(data, 'field1', 'field2', 'field3') jt as field1, field2, field3
```

The BigQuery SQL:
```sql
SELECT
  *,
  COALESCE(JSON_VALUE(data, '$.field1'), CAST(JSON_EXTRACT(data, '$.field1') AS STRING)) as field1
  COALESCE(JSON_VALUE(data, '$.field2'), CAST(JSON_EXTRACT(data, '$.field2') AS STRING)) as field2
  COALESCE(JSON_VALUE(data, '$.field3'), CAST(JSON_EXTRACT(data, '$.field3') AS STRING)) as field3
FROM table t
WHERE COALESCE(data,'') <> '' -- You have to apply this filter to ensure strict row-count parity with Spark’s inner-join behavior         
```

##### Example 2: convert LATERAL VIEW OUTER EXPLODE followed by LATERAL VIEW json_tuple

Spark SQL: 

```sql
SELECT
  field1, field2, field3
FROM table t 
LATERAL VIEW OUTER EXPLODE(udf.json_split(t.json_obj_list)) as json_data
LATERAL VIEW json_tuple(json_data, 'field1', 'field2', 'field3') jt AS  field1, field2, field3
```

The BigQuery SQL:

```sql
SELECT
  COALESCE(JSON_VALUE(t_base.json_value, '$.field1'), CAST(JSON_EXTRACT(t_base.json_value, '$.field1') AS STRING)) as field1,
  COALESCE(JSON_VALUE(t_base.json_value, '$.field2'), CAST(JSON_EXTRACT(t_base.json_value, '$.field2') AS STRING)) as field2,
  COALESCE(JSON_VALUE(t_base.json_value, '$.field3'), CAST(JSON_EXTRACT(t_base.json_value, '$.field3') AS STRING)) as field3
FROM table t 
LEFT JOIN UNNEST(JSON_QUERY_ARRAY(t.json_obj_list)) as json_data
WHERE COALESCE(json_data,'') <> '' -- You have to apply this filter to ensure strict row-count parity with Spark’s inner-join behavior  
```

### 2. ⚠️ DDL & Partition Handling (Transaction Mode)

BigQuery does not support `INSERT OVERWRITE ... PARTITION`. You MUST convert this into a `DELETE` + `INSERT` transaction.

#### 2.1 The Transaction Pattern

For **any** `INSERT OVERWRITE TABLE target PARTITION (p_col=val) ...`:

1. **Start Transaction:** `BEGIN TRANSACTION;`
2. **Clear Partition:** Generate a `DELETE` statement for the specific partition value.
3. **Insert Data:** Convert to `INSERT INTO`. **Move** the partition column from the `PARTITION()` clause to the SELECT list (or explicit column list).
4. **Commit:** `COMMIT TRANSACTION;`

**Example:**
Spark:
```sql
INSERT OVERWRITE TABLE db.target PARTITION (dt = '2023-10-01')
SELECT col1, col2 FROM source;
```
BigQuery:
```sql
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

### 3. ⚠️ Critical Syntax & Type Safety

#### 3.1 Strict Type Handling (No Implicit Conversion)

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

Date Comparison:** `d = '${{zdt...}}'` ➡️ `d = CURRENT_DATE()`
Dynamic Tables (Read):** `FROM db.table_${{zdt...}}` ➡️ `FROM \`trip...db_table_*` WHERE _TABLE_SUFFIX = ...`

### 5. Regular Expression

Use raw strings `r'...'` (e.g., `REGEXP_REPLACE(col, r'\d', 'X')`).

"""

SPARK_TO_BIGQUERY_PROMPT = """
You are an expert SQL translator. Convert Spark SQL to functionally equivalent, **executable** BigQuery SQL.

## Input Spark SQL:
```sql
{spark_sql}
```

## Target Table DDLs:
{table_ddls}

## Table Mapping information: 
{table_mapping_info}

""" + SHARED_SQL_CONVERSION_RULES + """

## Output Requirements:

1. Return **ONLY** the converted BigQuery SQL code.
2. **No** markdown block wrappers (unless strictly necessary for code display).
3. **No** explanations.

```
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

""" + SHARED_SQL_CONVERSION_RULES + """

## Detailed Fix Guidelines (Supplementary)

If the error persists or is not covered by the main rules, check these specific strict requirements:

### A. Syntax Checks
1. **Backticks:** Wrap ALL table names in backticks (e.g., `project-id.dataset.table`), especially if they contain hyphens.
2. **Multi-statement:** Ensure statements are separated by semicolons `;`.
3. **Window Functions:** If `HAVING` filters a window function result, wrap the query in a subquery and use `WHERE`.

### B. Common Fix Patterns
1. **Unnest Alias:** `CROSS JOIN UNNEST(col) AS alias`. The AS alias must be **outside** the parentheses.
2. **Grouping Sets:** `GROUP BY GROUPING SETS ((a, b), (a))`. No columns before `GROUPING SETS`.

## Output Requirement

* Return **ONLY** the corrected BigQuery SQL code block.
* Do NOT include markdown like "Here is the fixed code" or explanations.
* Do NOT output JSON.
"""

LLM_SQL_CHECK_PROMPT = """
# Role
You are a BigQuery SQL Validator. Your task is to check if the converted BigQuery SQL follows the following rules:

""" + SHARED_SQL_CONVERSION_RULES + """

## Context Data
### 1. Original Spark SQL:
```sql
{spark_sql}
```

### 2. Converted BigQuery SQL (To Verify):
```sql
{bigquery_sql}
```

### 3. Target Table DDLs:
{table_ddls}

---

## Output Format
Return a JSON object with the following structure:
{{
    "is_valid": boolean,
    "error": "string" // Error message if is_valid is false, null otherwise
}}

If valid, set "is_valid" to true and "error" to null.
If invalid, set "is_valid" to false and provide a concise, actionable error message in "error".

Example Output:
{{
    "is_valid": true,
    "error": null
}}
"""
