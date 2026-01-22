"""Prompt templates for Hive to BigQuery SQL conversion."""

SPARK_VALIDATION_PROMPT = """You are a Hive SQL syntax expert. Validate if the following SQL is valid Hive SQL syntax.

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
"""

SPARK_TO_BIGQUERY_PROMPT = """You are an expert SQL translator. Convert Hive SQL to functionally equivalent BigQuery SQL.

## Input Hive SQL:
```sql
{spark_sql}
```

{table_mapping_info}

## Target Table DDLs:
{table_ddls}

---

## Conversion Rules:

### ⚠️ Critical Syntax Differences (MUST READ)

#### 1. GROUP BY GROUPING SETS - STRICT SYNTAX
Hive allows listing columns before GROUPING SETS, but BigQuery FORBIDS it. You MUST remove the columns between GROUP BY and GROUPING SETS.

* ❌ Hive Style (Invalid in BQ): `GROUP BY a, b GROUPING SETS ((a, b), (a))`
* ✅ BigQuery Style (Correct): `GROUP BY GROUPING SETS ((a, b), (a))`

### ⚠️ Variable Handling Strategy (MODE: NATIVE CONVERSION)
**Goal: Convert ALL Hive scheduling macros into native BigQuery dynamic functions.**

You must translate Hive macros (like `${{zdt...}}`) into functionally equivalent BigQuery logic (`CURRENT_DATE()`, `DATE_SUB`, etc.).

#### 1. Macro Translation Rules (CRITICAL)
**Rule A: Remove Quotes**
When converting a macro inside quotes, you MUST **remove the surrounding quotes**.
* ❌ Wrong: `WHERE d = 'DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)'` (String)
* ✅ Correct: `WHERE d = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` (Function)

**Rule B: Common Mapping Table**
| Hive Macro Pattern | BigQuery Equivalent | Result Type |
|-------------------|---------------------|-------------|
| `${{zdt.format("yyyy-MM-dd")}}` | `CURRENT_DATE()` | DATE |
| `${{zdt.addDay(-1).format("yyyy-MM-dd")}}` | `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` | DATE |
| `${{zdt.addDay(N).format("yyyy-MM-dd")}}` | `DATE_ADD(CURRENT_DATE(), INTERVAL N DAY)` | DATE |
| `${{zdt.format("yyyyMMdd")}}` | `FORMAT_DATE('%Y%m%d', CURRENT_DATE())` | STRING |
| `${{zdt.addDay(-1).format("yyyyMMdd")}}` | `FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))` | STRING |
| `${{zdt.format("yyyy-MM-dd HH:mm:ss")}}` | `CURRENT_TIMESTAMP()` | TIMESTAMP |
| `${{zdt.addMonth(-1).format("yyyy-MM")}}` | `FORMAT_DATE('%Y-%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))` | STRING |
| `${{zdt.add(10, N).format("yyyy-MM-dd")}}` | `DATE(TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL N HOUR))` | DATE |
| `${{zdt.add(10, N).format("HH")}}` | `FORMAT_TIMESTAMP('%H', TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL N HOUR))` | STRING |

#### 2. Comparison Context Handling
**Scenario 1: Comparing to DATE Column (`d`, `dt`, `partition_date`)**
If the macro represents a Date (yyyy-MM-dd), use the DATE-returning function directly.
* Hive: `WHERE d = '${{zdt.format("yyyy-MM-dd")}}'`
* BigQuery: `WHERE d = CURRENT_DATE()`

* Hive: `WHERE d = '${{zdt.addDay(-1).format("yyyy-MM-dd")}}'`
* BigQuery: `WHERE d = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`

**Scenario 2: Comparing to STRING Column**
If the macro represents a formatted String (e.g. yyyyMMdd), use `FORMAT_DATE`.
* Hive: `WHERE str_col = '${{zdt.format("yyyyMMdd")}}'`
* BigQuery: `WHERE str_col = FORMAT_DATE('%Y%m%d', CURRENT_DATE())`

#### 3. Handling `SET hivevar` Variables
Convert Hive variables to BigQuery Scripting (`DECLARE` / `SET`).

**Hive Input:**
```sql
set hivevar:start_date=${{zdt.addDay(-7).format("yyyy-MM-dd")}};
SELECT * FROM t WHERE d >= '${{hivevar:start_date}}';
```
**BigQuery Output:**
```sql
DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
SELECT * FROM t WHERE d >= start_date; -- Note: No quotes around variable
```

#### 4. Dynamic Table Names (Read vs Write)
**Reading (SELECT): Use Wildcard**
* Hive: `FROM table_${{zdt.format("yyyyMMdd")}}`
* BigQuery: `FROM \`project.dataset.table_*\` WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', CURRENT_DATE())`

**Writing (INSERT/CREATE): Use EXECUTE IMMEDIATE**
* Hive: `INSERT OVERWRITE TABLE table_${{zdt.format("yyyyMMdd")}} ...`
* BigQuery:
```sql
DECLARE suffix STRING DEFAULT FORMAT_DATE('%Y%m%d', CURRENT_DATE());
EXECUTE IMMEDIATE FORMAT('''
  CREATE OR REPLACE TABLE \`project.dataset.table_%s\` AS ...
''', suffix);
```

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

#### 1.1 Implicit Type Conversion - CRITICAL for String-to-Number Comparisons
Hive allows implicit type conversion (string compared to number), but BigQuery does NOT.
You MUST explicitly convert using SAFE_CAST:

| Hive Pattern | BigQuery Pattern |
|--------------|------------------|
| `string_col > 0` | `SAFE_CAST(string_col AS INT64) > 0` |
| `string_col >= 100` | `SAFE_CAST(string_col AS INT64) >= 100` |
| `string_col = 1` | `SAFE_CAST(string_col AS INT64) = 1` |
| `string_col < 10.5` | `SAFE_CAST(string_col AS FLOAT64) < 10.5` |
| `string_col BETWEEN 1 AND 10` | `SAFE_CAST(string_col AS INT64) BETWEEN 1 AND 10` |

**Common ID Columns that are STRING in Hive but compared to numbers:**
- `masterhotelid`, `hotel`, `hotelid`
- `cityid`, `city`, `countryid`, `country`
- `country_flag`, `status`, `type`
- Any column ending with `_id`, `_flag`, `_type`

Example:
```sql
-- Hive (allows implicit conversion):
SELECT * FROM hotels 
WHERE masterhotelid > 0 
  AND cityid = 1 
  AND country_flag >= 0

-- BigQuery (MUST use SAFE_CAST for STRING columns):
SELECT * FROM `project.dataset.hotels` 
WHERE SAFE_CAST(masterhotelid AS INT64) > 0 
  AND SAFE_CAST(cityid AS INT64) = 1 
  AND SAFE_CAST(country_flag AS INT64) >= 0
```

**IMPORTANT**: Use `SAFE_CAST` instead of `CAST` to avoid errors when conversion fails (returns NULL instead of error).

#### 1.2 Date/Timestamp Column Type Handling - CRITICAL for Partition/Time Columns
**Assume the partition column `d` in BigQuery is strictly of type DATE.**
**Assume `updatedt` and other timestamp columns are strictly of type TIMESTAMP.**

**Rules:**
1. NEVER apply `PARSE_DATE()` or `STR_TO_DATE()` to the column `d` itself
2. Cast the comparison VALUE (string/variable) to DATE, not the column
3. When comparing to DATE columns with Hive variables like `${{zdt...}}`, wrap the variable in `DATE()`
4. When comparing to TIMESTAMP columns (like `updatedt`) with Hive variables, wrap the variable in `TIMESTAMP()` or `SAFE_CAST(... AS TIMESTAMP)`

| Hive Pattern | BigQuery Pattern |
|--------------|------------------|
| `WHERE d = '2024-01-01'` | `WHERE d = DATE('2024-01-01')` |
| `WHERE d = '${{zdt.format("yyyy-MM-dd")}}'` | `WHERE d = DATE('${{zdt.format("yyyy-MM-dd")}}')` |
| `WHERE d >= '${{zdt.addDay(-7).format("yyyy-MM-dd")}}'` | `WHERE d >= DATE('${{zdt.addDay(-7).format("yyyy-MM-dd")}}')` |
| `WHERE updatedt >= '${{zdt.format("yyyy-MM-dd")}}'` | `WHERE updatedt >= TIMESTAMP('${{zdt.format("yyyy-MM-dd")}}')` |
| `WHERE ts_col >= '${{zdt.format("yyyy-MM-dd")}}'` | `WHERE ts_col >= TIMESTAMP('${{zdt.format("yyyy-MM-dd")}}')` |

**WRONG (do NOT do this):**
```sql
-- ❌ Applying PARSE_DATE to the column d is WRONG:
WHERE PARSE_DATE('%Y-%m-%d', d) = '2024-01-01'

-- ❌ Comparing TIMESTAMP column with string literal without casting:
WHERE updatedt >= '${{zdt.format("yyyy-MM-dd")}}'
```

**CORRECT:**
```sql
-- ✓ Cast the comparison value to DATE:
WHERE d = DATE('2024-01-01')
WHERE d = DATE('${{zdt.format("yyyy-MM-dd")}}')

-- ✓ Cast the comparison value to TIMESTAMP:
WHERE updatedt >= TIMESTAMP('${{zdt.format("yyyy-MM-dd")}}')
```

### 2. Date/Time Functions
| Hive | BigQuery |
|------|----------|
| date_format(date, 'yyyy-MM-dd') | FORMAT_DATE('%Y-%m-%d', date) |
| date_format(date, 'yyyy-MM-dd HH:mm:ss') | FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', ts) |
| datediff(end, start) | DATE_DIFF(end, start, DAY) |
| datediff(to_date(end), to_date(start)) | DATE_DIFF(DATE(end), DATE(start), DAY) |
| date_add(date, n) | DATE_ADD(date, INTERVAL n DAY) |
| date_add(date, -n) | DATE_SUB(date, INTERVAL n DAY) |
| date_add(to_date(ts), -1) | DATE_SUB(DATE(ts), INTERVAL 1 DAY) |
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
| `regexp_replace(col, '\\d', ...)` | `REGEXP_REPLACE(col, r'\d', ...)` (use raw string r-prefix) |
| parse_url(url, 'HOST') | NET.HOST(url) |
| get_json_object(json, '$.key') | JSON_EXTRACT_SCALAR(json, '$.key') |
| json_tuple(json, 'k1', 'k2') | JSON_EXTRACT_SCALAR(json, '$.k1'), JSON_EXTRACT_SCALAR(json, '$.k2') |

#### 3.1 Regular Expression Pattern Handling - CRITICAL
**BigQuery requires raw string (r-prefix) for regex patterns with escape sequences!**

```sql
-- Hive (escaped backslash):
regexp_replace(col, '\\d', 'X')
regexp_replace(col, '\\s+', ' ')

-- BigQuery (use raw string r-prefix):
REGEXP_REPLACE(col, r'\d', 'X')
REGEXP_REPLACE(col, r'\s+', ' ')

-- Hive:
regexp_extract(str, '\\d{{4}}', 0)

-- BigQuery:
REGEXP_EXTRACT(str, r'\d{{4}}')
```

**Rule**: When converting regex patterns with escape sequences (`\d`, `\s`, `\w`, etc.), use `r'pattern'` (raw string) instead of `'pattern'`.

#### 3.2 ARRAY_TO_STRING / CONCAT_WS Type Handling - CRITICAL
**BigQuery's `ARRAY_TO_STRING` (Hive `concat_ws`) requires ALL array elements to be STRING.**
If you are concatenating numbers, you MUST cast them to STRING inside the array!

* Hive: `concat_ws('-', year, month, day)` (year/month/day are INT)
* BigQuery: `ARRAY_TO_STRING([CAST(year AS STRING), CAST(month AS STRING), CAST(day AS STRING)], '-')`

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

#### 5.1 NVL/COALESCE Type Matching - CRITICAL
**BigQuery's COALESCE/IFNULL requires ALL arguments to have the SAME type!**

Hive allows `nvl(numeric_col, '')` (numeric with empty string fallback), but BigQuery will throw:
`No matching signature for function COALESCE - Argument types: INT64, STRING`

**Rules:**
1. If the column is **Numeric** (star, score, rank, price, cnt, amount, count, num, qty, etc.), use **0** as default:
   - `nvl(star, '')` → `COALESCE(star, 0)`
   - `nvl(price, '')` → `COALESCE(price, 0)`
   - `nvl(room_cnt, '')` → `COALESCE(room_cnt, 0)`

2. If the column is **ID or code** that should be STRING, cast the column:
   - `nvl(hotel_id, '')` → `COALESCE(CAST(hotel_id AS STRING), '')`
   - `nvl(city_code, '')` → `COALESCE(CAST(city_code AS STRING), '')`

3. If column is already STRING, keep empty string:
   - `nvl(name, '')` → `COALESCE(name, '')`

**Common Numeric Column Patterns (use 0 as default):**
- Columns ending with: `_cnt`, `_num`, `_qty`, `_count`, `_amount`, `_price`, `_rate`, `_score`, `_rank`
- Columns like: `star`, `level`, `grade`, `rating`, `quantity`, `total`, `sum`, `avg`

Example:
```sql
-- Hive:
SELECT nvl(star, ''), nvl(rating_score, ''), nvl(room_cnt, ''), nvl(hotel_name, '')

-- BigQuery:
SELECT COALESCE(star, 0), COALESCE(rating_score, 0), COALESCE(room_cnt, 0), COALESCE(hotel_name, '')
```

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

### 8. Map and JSON Functions - CRITICAL for Complex DML

#### 8.1 Map Construction (TO_JSON)
Hive `map(k, v)` usually converts to a JSON object string in BigQuery.
Use `TO_JSON(JSON_OBJECT(k, v))` to match Hive's string serialization of maps.

| Hive | BigQuery |
|------|----------|
| `map('k1', v1, 'k2', v2)` | `TO_JSON(JSON_OBJECT('k1', v1, 'k2', v2))` |
| `map_keys(map)` | Extract via JSON functions |
| `map_values(map)` | Extract via JSON functions |

#### 8.2 Map Access (CRITICAL - Common Error Source)
| Hive | BigQuery |
|------|----------|
| `map_col['key']` | `JSON_VALUE(map_col, '$.key')` |
| `map_col['0.05']` | `JSON_VALUE(map_col, '$."0.05"')` |
| `coalesce(map_col['key'], default)` | `COALESCE(JSON_VALUE(map_col, '$.key'), default)` |

Example:
```sql
-- Hive:
case when t.price <= coalesce(city_price_map['0.05'],'') then 1 ... end

-- BigQuery:
CASE WHEN t.price <= COALESCE(JSON_VALUE(city_price_map, '$."0.05"'), '') THEN 1 ... END
```

#### 8.3 ARRAY<STRUCT<key, value>> Type (MAP converted to STRUCT array)
**When Hive MAP is stored as `ARRAY<STRUCT<key STRING, value STRING>>` in BigQuery:**

**DO NOT use JSON_EXTRACT_SCALAR!** Use UNNEST with subquery:

**Known Columns that are ARRAY<STRUCT<key STRING, value STRING>>:**
- `data` (e.g. `t.data`)
- `extra_user_info`
- `srmlist`
- `map_col`
- Any column that was a MAP in Hive and migrated to BigQuery

```sql
-- ❌ WRONG (ARRAY<STRUCT> is not JSON):
JSON_EXTRACT_SCALAR(map_col, '$.target_key')

-- ✓ CORRECT (use subquery with UNNEST):
(SELECT value FROM UNNEST(map_col) WHERE key = 'target_key')
```

**Example with multiple key extractions:**
```sql
-- Hive (accessing map):
SELECT map_col['key1'], map_col['key2'], map_col['key3']

-- BigQuery (when map_col is ARRAY<STRUCT<key, value>>):
SELECT 
    (SELECT value FROM UNNEST(map_col) WHERE key = 'key1') AS key1,
    (SELECT value FROM UNNEST(map_col) WHERE key = 'key2') AS key2,
    (SELECT value FROM UNNEST(map_col) WHERE key = 'key3') AS key3
```

**Handling COALESCE with ARRAY<STRUCT>:**
If you see `coalesce(map_col, '{{}}')` or `nvl(map_col, '{{}}')`, and `map_col` is ARRAY<STRUCT>:
- If the target is STRING (e.g. JSON output): `COALESCE(TO_JSON_STRING(map_col), '{{}}')`
- If keeping as ARRAY: `COALESCE(map_col, [])`

**Example:**
```sql
-- Hive:
coalesce(extra_user_info, '{{}}')

-- BigQuery (if extra_user_info is ARRAY<STRUCT>):
COALESCE(TO_JSON_STRING(extra_user_info), '{{}}')
```

**How to identify ARRAY<STRUCT> vs JSON:**
- If column type is `ARRAY<STRUCT<key STRING, value STRING>>` → use UNNEST
- If column type is `STRING` or `JSON` → use `JSON_VALUE` or `JSON_EXTRACT_SCALAR`

#### 8.4 from_json / to_json Functions
| Hive | BigQuery |
|------|----------|
| `from_json(col, 'map<string,float>')` | `PARSE_JSON(col)` then use JSON_VALUE to access |
| `from_json(col, 'struct<...>')` | `JSON_QUERY(col, '$')` |
| `to_json(struct_col)` | `TO_JSON(struct_col)` |
| `udf.to_json(map(...))` | `TO_JSON(JSON_OBJECT(...))` |

#### 8.5 UDF to_json with map() - Complex Pattern
```sql
-- Hive:
udf.to_json(map(
    'key1', value1,
    'key2', value2,
    'key3', value3
))

-- BigQuery:
TO_JSON(JSON_OBJECT(
    'key1', value1,
    'key2', value2,
    'key3', value3
))
```

#### 8.6 Nested JSON Access for Quantile Maps
```sql
-- Hive (accessing map with numeric string keys):
case when price <= coalesce(price_map['0.05'],'') then 1
     when price <= coalesce(price_map['0.10'],'') then 2
     ...
end

-- BigQuery:
CASE WHEN price <= COALESCE(CAST(JSON_VALUE(price_map, '$."0.05"') AS FLOAT64), 0) THEN 1
     WHEN price <= COALESCE(CAST(JSON_VALUE(price_map, '$."0.10"') AS FLOAT64), 0) THEN 2
     ...
END
```

### 9. LATERAL VIEW / EXPLODE

#### 9.1 Basic EXPLODE
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

#### 9.1.1 OUTER EXPLODE - CRITICAL
**Hive's `OUTER` keyword preserves rows even when array is NULL or empty!**

```sql
-- Hive (OUTER preserves rows with NULL/empty arrays):
SELECT id, item FROM t LATERAL VIEW OUTER explode(items) tmp AS item

-- BigQuery (use LEFT JOIN, NOT CROSS JOIN):
SELECT id, item FROM t LEFT JOIN UNNEST(items) AS item

-- ❌ WRONG (CROSS JOIN removes rows with NULL/empty arrays):
-- SELECT id, item FROM t CROSS JOIN UNNEST(items) AS item
```

**Rule**: 
- `LATERAL VIEW explode(...)` → `CROSS JOIN UNNEST(...)`
- `LATERAL VIEW OUTER explode(...)` → `LEFT JOIN UNNEST(...)`

#### 9.2 UNNEST Alias Placement - CRITICAL
**The alias MUST be placed AFTER the UNNEST() parenthesis, not inside!**

```sql
-- ❌ WRONG (alias inside parenthesis):
CROSS JOIN UNNEST([...] jt)
CROSS JOIN UNNEST(items jt)

-- ✓ CORRECT (alias after parenthesis):
CROSS JOIN UNNEST([...]) AS jt
CROSS JOIN UNNEST(items) AS jt
```

#### 9.3 LATERAL VIEW json_tuple - DO NOT use UNNEST
**For `LATERAL VIEW json_tuple()`, use direct JSON_EXTRACT_SCALAR in SELECT, NOT UNNEST!**

```sql
-- Hive:
SELECT t.id, jt.name, jt.age
FROM table t
LATERAL VIEW json_tuple(t.json_col, 'name', 'age') jt AS name, age

-- BigQuery (✓ CORRECT - direct extraction, no UNNEST needed):
SELECT 
    t.id,
    JSON_EXTRACT_SCALAR(t.json_col, '$.name') AS name,
    JSON_EXTRACT_SCALAR(t.json_col, '$.age') AS age
FROM `table` t

-- BigQuery (❌ WRONG - do NOT use UNNEST for json_tuple):
-- CROSS JOIN UNNEST([STRUCT(JSON_EXTRACT_SCALAR(...) AS name, ...)]) AS jt
```

#### 9.4 Complex JSON Extraction
```sql
-- Hive (extracting many fields):
LATERAL VIEW json_tuple(data, 'field1', 'field2', 'field3', ...) jt 
    AS field1, field2, field3, ...

-- BigQuery (extract each field directly in SELECT):
SELECT 
    JSON_EXTRACT_SCALAR(data, '$.field1') AS field1,
    JSON_EXTRACT_SCALAR(data, '$.field2') AS field2,
    JSON_EXTRACT_SCALAR(data, '$.field3') AS field3,
    ...
FROM table
```

### 10. GROUPING SETS / CUBE / ROLLUP - STRICT SYNTAX
**BigQuery does NOT allow listing columns before `GROUPING SETS`!**

```sql
-- ❌ WRONG (Hive style):
GROUP BY a, b GROUPING SETS ((a, b), (a), ())

-- ✅ CORRECT (BigQuery style - remove 'a, b' after GROUP BY):
GROUP BY GROUPING SETS ((a, b), (a), ())
-- or
GROUP BY ROLLUP(a, b)
-- or  
GROUP BY CUBE(a, b)
```

### 10.1 Window Functions
Window functions are mostly the same, but note these differences:

| Hive | BigQuery |
|------|----------|
| `ROW_NUMBER() OVER (...)` | `ROW_NUMBER() OVER (...)` (same) |
| `RANK() OVER (...)` | `RANK() OVER (...)` (same) |
| `DENSE_RANK() OVER (...)` | `DENSE_RANK() OVER (...)` (same) |
| `LAG(col, n) OVER (...)` | `LAG(col, n) OVER (...)` (same) |
| `LEAD(col, n) OVER (...)` | `LEAD(col, n) OVER (...)` (same) |
| `FIRST_VALUE(col) OVER (...)` | `FIRST_VALUE(col) OVER (...)` (same) |
| `LAST_VALUE(col) OVER (...)` | `LAST_VALUE(col) OVER (...)` (same) |
| `SUM(col) OVER (...)` | `SUM(col) OVER (...)` (same) |

### 10.2 HAVING Clause Limitation - CRITICAL
**BigQuery's HAVING can ONLY filter on aggregate functions, NOT on window function results!**

```sql
-- ❌ WRONG (HAVING cannot filter on window function result):
SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS rk
FROM table
GROUP BY ...
HAVING rk = 1  -- ❌ Invalid in BigQuery!

-- ✓ CORRECT (use subquery to filter window function result):
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS rk
    FROM table
) 
WHERE rk = 1  -- ✓ Filter in outer WHERE clause
```

**Common pattern to fix:**
```sql
-- Hive (sometimes allows HAVING on non-aggregates):
SELECT col1, col2, rank() over(order by version desc) as rk
FROM table
HAVING rk = 1

-- BigQuery (MUST wrap in subquery):
SELECT * FROM (
    SELECT col1, col2, RANK() OVER (ORDER BY version DESC) AS rk
    FROM table
)
WHERE rk = 1
```

### 10.3 LEFT ANTI JOIN / LEFT SEMI JOIN (Hive-specific)
**BigQuery does NOT support LEFT ANTI JOIN or LEFT SEMI JOIN syntax!**

```sql
-- Hive LEFT ANTI JOIN (returns rows from left that have NO match in right):
SELECT t0.*
FROM table_a t0
LEFT ANTI JOIN table_b t1 ON t0.id = t1.id

-- BigQuery (use LEFT JOIN + WHERE IS NULL):
SELECT t0.*
FROM `table_a` t0
LEFT JOIN `table_b` t1 ON t0.id = t1.id
WHERE t1.id IS NULL

-- Alternative BigQuery (use NOT EXISTS):
SELECT t0.*
FROM `table_a` t0
WHERE NOT EXISTS (
    SELECT 1 FROM `table_b` t1 WHERE t0.id = t1.id
)
```

```sql
-- Hive LEFT SEMI JOIN (returns rows from left that HAVE match in right):
SELECT t0.*
FROM table_a t0
LEFT SEMI JOIN table_b t1 ON t0.id = t1.id

-- BigQuery (use EXISTS):
SELECT t0.*
FROM `table_a` t0
WHERE EXISTS (
    SELECT 1 FROM `table_b` t1 WHERE t0.id = t1.id
)

-- Alternative BigQuery (use INNER JOIN with DISTINCT if needed):
SELECT DISTINCT t0.*
FROM `table_a` t0
INNER JOIN `table_b` t1 ON t0.id = t1.id
```

### 10.4 Handling Very Long SQL (200+ columns)
For very long SQL with many columns:
1. **Preserve column order exactly** - do not reorder columns
2. **Keep column aliases** - maintain all `AS alias` names
3. **Preserve CAST expressions** - keep type conversions like `CAST(x AS DECIMAL(18,4))`
4. **Keep comments** - preserve SQL comments for documentation
5. **Handle UNION ALL** - each branch must have same columns in same order

### 10.5 Complex DML Conversion Patterns

#### Pattern 1: USE + INSERT OVERWRITE
```sql
-- Hive:
use dw_htlbizdb;
insert overwrite table dw_htlbizdb.target partition (d = '${{zdt.format("yyyy-MM-dd")}}')
select col1, col2, ... from source;

-- BigQuery (remove USE, convert INSERT):

BEGIN TRANSACTION;

-- 1. 删除目标分区的数据
DELETE FROM `your_project.dataset.target_table`
WHERE date_column = '2023-10-01'; -- 指定要覆盖的分区条件

-- 2. 插入新数据到该分区
INSERT INTO `your_project.dataset.target_table` (col1, col2, date_column)
SELECT col1, col2, date_column
FROM `your_project.dataset.source_table`
WHERE date_column = '2023-10-01';

COMMIT TRANSACTION;
```

#### Pattern 2: UDF to_json with Large Map
```sql
-- Hive:
select udf.to_json(map(
    'key1', t.col1,
    'key2', t.col2,
    'nested_key', other_table.value
)) as json_col
from t left join other_table ...

-- BigQuery:
SELECT TO_JSON(JSON_OBJECT(
    'key1', t.col1,
    'key2', t.col2,
    'nested_key', other_table.value
)) AS json_col
FROM `t` LEFT JOIN `other_table` ...
```

#### Pattern 3: Map Access in CASE WHEN (Quantile Calculation)
```sql
-- Hive:
case when price <= coalesce(price_map['0.05'],'') then 1
     when price <= coalesce(price_map['0.10'],'') then 2
     else 0 end

-- BigQuery:
CASE WHEN price <= COALESCE(SAFE_CAST(JSON_VALUE(price_map, '$."0.05"') AS FLOAT64), 0) THEN 1
     WHEN price <= COALESCE(SAFE_CAST(JSON_VALUE(price_map, '$."0.10"') AS FLOAT64), 0) THEN 2
     ELSE 0 END
```

#### Pattern 4: from_json for Map Type
```sql
-- Hive:
select from_json(col, 'map<string,float>') as parsed_map from t

-- BigQuery:
SELECT PARSE_JSON(col) AS parsed_map FROM `t`
-- Then access values with JSON_VALUE(parsed_map, '$.key')
```

#### Pattern 5: date_add with Negative Value
```sql
-- Hive:
date_add(to_date(t.starttime), -1)

-- BigQuery:
DATE_SUB(DATE(t.starttime), INTERVAL 1 DAY)
```

#### Pattern 6: Multiple UNION ALL with Same Structure
When converting multiple UNION ALL branches:
- Each branch MUST output the same columns in the same order
- Convert each branch following the same rules
- Preserve column aliases exactly

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

#### 11.3 INSERT OVERWRITE with PARTITION - CRITICAL
**Hive vs BigQuery Partition Handling:**
- Hive: Partition column is specified in `PARTITION(col=val)` clause, NOT in SELECT list
- BigQuery: Partition column MUST be included at the END of SELECT list

**Rule: When converting `INSERT OVERWRITE TABLE ... PARTITION(p_col=val) SELECT cols...`:**
1. Remove the PARTITION clause from the statement
2. Add the partition column with its value to the END of the SELECT list
3. Convert to `CREATE OR REPLACE TABLE` syntax
4. **IMPORTANT: Add `PARTITION BY partition_col` to preserve partitioning!**

**WARNING - Partition Spec Must Match:**
If replacing an existing partitioned table, you MUST include `PARTITION BY`:
```sql
-- ❌ WRONG (will fail if table already has partition):
CREATE OR REPLACE TABLE `project.dataset.table` AS SELECT ...

-- ✓ CORRECT (preserves partition spec):
CREATE OR REPLACE TABLE `project.dataset.table` 
PARTITION BY d
AS SELECT ...
```

```sql
-- Hive (partition column 'd' NOT in SELECT):
INSERT OVERWRITE TABLE db.target_table PARTITION (d='${{zdt.format("yyyy-MM-dd")}}')
SELECT 
    user_id,
    order_amount,
    order_count
FROM source_table
WHERE dt = '${{zdt.format("yyyy-MM-dd")}}';

-- BigQuery (partition column 'd' MUST be at END of SELECT):
DECLARE partition_date DATE DEFAULT CURRENT_DATE();

CREATE OR REPLACE TABLE `project.dataset.target_table` AS
SELECT 
    user_id,
    order_amount,
    order_count,
    partition_date AS d  -- ✓ Partition column added to SELECT
FROM `project.dataset.source_table`
WHERE dt = partition_date;
```

**Another Example with static partition value:**
```sql
-- Hive:
INSERT OVERWRITE TABLE result PARTITION (country='US', dt='2024-01-01')
SELECT id, name, amount FROM orders;

-- BigQuery:
CREATE OR REPLACE TABLE `project.dataset.result` AS
SELECT 
    id, 
    name, 
    amount,
    'US' AS country,      -- ✓ Added partition columns
    DATE '2024-01-01' AS dt
FROM `project.dataset.orders`;
```

**With Dynamic Partition (multiple partition values from data):**
```sql
-- Hive (dynamic partition):
INSERT OVERWRITE TABLE result PARTITION (dt)
SELECT id, name, amount, order_date AS dt FROM orders;

-- BigQuery (partition column already in SELECT, just convert syntax):
CREATE OR REPLACE TABLE `project.dataset.result` 
PARTITION BY dt
AS SELECT id, name, amount, order_date AS dt FROM `project.dataset.orders`;
```

### 12. Hive-Specific Syntax to Remove/Convert
| Hive | BigQuery |
|------|----------|
| `USE database;` | (remove entirely - not needed in BQ) |
| `USE db_name;` | (remove - BQ uses fully qualified table names) |
| DISTRIBUTE BY col | (remove - BQ handles automatically) |
| CLUSTER BY col | (remove or use ORDER BY) |
| SORT BY col | ORDER BY col |
| STORED AS format | (remove) |
| ROW FORMAT ... | (remove) |
| SERDE ... | (remove) |
| TBLPROPERTIES (...) | (remove or use OPTIONS) |
| /*+ HINT */ | (remove hints) |
| `-- comment` | Keep comments (optional) |

#### 12.1 Subquery with MAX(d) Pattern
```sql
-- Hive:
WHERE d = (SELECT max(d) FROM some_table)

-- BigQuery (same syntax works):
WHERE d = (SELECT MAX(d) FROM `project.dataset.some_table`)
```

### 13. Table References - CRITICAL

#### 13.1 Identifying Table Types - What Needs Dataset Prefix

**DO NOT add dataset prefix to these (virtual/derived tables):**

| Type | Pattern | Example | Action |
|------|---------|---------|--------|
| **CTE** | Defined in `WITH name AS (...)` | `WITH exploded_data AS (...)` then `FROM exploded_data` | Keep as-is |
| **Subquery alias** | `(SELECT ...) AS alias` | `(SELECT * FROM t) AS sub` | Keep as-is |
| **Table alias** | `FROM real_table alias` | `FROM orders t` → `t` is alias | Keep as-is |
| **UNNEST alias** | `CROSS JOIN UNNEST(...) AS alias` | `UNNEST(arr) AS item` | Keep as-is |
| **LATERAL VIEW result** | After `LATERAL VIEW EXPLODE` | `LATERAL VIEW explode(x) tmp AS y` | Convert to UNNEST |
| **Inline derived** | Name only exists within same query | Used only as intermediate result | Keep as-is |

**MUST add dataset prefix to these (real tables/views):**

| Type | Pattern | Example | Action |
|------|---------|---------|--------|
| **Database.Table** | `db_name.table_name` | `dw_htlbizdb.orders` | Map or add prefix |
| **Standalone table** | Just `table_name` in FROM/JOIN | `FROM orders` (not defined in WITH) | Add prefix |
| **Views** | `v_xxx` or `xxx_view` naming | `v_dim_room` or `dim_room_view` | Treat as real table, add prefix |

#### 13.2 How to Identify Virtual vs Real Tables

**Step 1: Scan the ENTIRE query first to find all CTEs and subquery definitions**
```sql
-- These define virtual tables:
WITH cte_name AS (...)           -- cte_name is virtual
, another_cte AS (...)           -- another_cte is virtual
FROM (SELECT ...) AS subq        -- subq is virtual
CROSS JOIN UNNEST(...) AS arr    -- arr is virtual
```

**Step 2: Any table NOT defined above is a REAL table → needs prefix**

Example Analysis:
```sql
WITH exploded_data AS (
    SELECT * FROM dw_htlbizdb.source_table  -- source_table is REAL
)
SELECT * 
FROM exploded_data t              -- exploded_data is CTE (virtual)
LEFT JOIN dim_hoteldb.dimroom r   -- dimroom is REAL
    ON t.roomid = r.roomid
```

#### 13.3 Conversion Rules

**Real tables (need prefix):**
```sql
-- Hive:
FROM dw_htlbizdb.orders
FROM dim_hoteldb.v_dim_room_df

-- BigQuery:
FROM `project.dataset.dw_htlbizdb_orders`
FROM `project.dataset.dim_hoteldb_v_dim_room_df`
```

**Virtual tables (NO prefix):**
```sql
-- CTEs stay as-is:
WITH my_cte AS (...) SELECT * FROM my_cte  -- ✓ No prefix on my_cte

-- Subquery aliases stay as-is:
FROM (SELECT * FROM real_table) AS derived  -- ✓ No prefix on derived

-- UNNEST aliases stay as-is:
CROSS JOIN UNNEST(items) AS item  -- ✓ No prefix on item
```

#### 13.4 Table Name Format - CRITICAL: Backticks Required!

**ALL BigQuery table names MUST be wrapped in backticks!**

This is especially critical when project ID contains hyphens (`-`):
```sql
-- ❌ WRONG (hyphen is interpreted as minus operator):
FROM trip-htl-bi-dbprj.htl_bi_temp.table_name
-- BigQuery sees: trip - htl - bi - dbprj.htl_bi_temp.table_name (math expression!)

-- ✓ CORRECT (backticks protect the identifier):
FROM `trip-htl-bi-dbprj.htl_bi_temp.table_name`
```

**Rules:**
1. **Always use backticks** for real table names: `\`project.dataset.table\``
2. Apply the table mapping to replace Hive table names
3. For unmapped real tables, use pattern: `\`project.dataset.db_tablename\``
4. **Never output table names without backticks** - even in CREATE, INSERT, JOIN, FROM, etc.

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

### 15. Template Variables Handling - CRITICAL
Hive variables like `${{zdt...}}` are runtime string placeholders from the scheduling system.

**Rules for Variable Handling:**
1. Treat Hive variables as **String literals**
2. When comparing to a DATE column (like partition column `d`), wrap the variable in `DATE()`
3. Keep the variable syntax exactly as-is inside the `DATE()` function

**Correct Examples:**
```sql
-- Comparing to DATE column d:
WHERE d = DATE('${{zdt.format("yyyy-MM-dd")}}')
WHERE d >= DATE('${{zdt.addDay(-7).format("yyyy-MM-dd")}}')
WHERE d BETWEEN DATE('${{zdt.addDay(-30).format("yyyy-MM-dd")}}') AND DATE('${{zdt.format("yyyy-MM-dd")}}')

-- Comparing to STRING column:
WHERE date_str = '${{zdt.format("yyyy-MM-dd")}}'
WHERE name LIKE CONCAT('%', '${{hivevar:suffix}}', '%')
```

**WRONG Examples:**
```sql
-- ❌ Never use PARSE_DATE on the column:
WHERE PARSE_DATE('%Y-%m-%d', d) = '${{zdt.format("yyyy-MM-dd")}}'

-- ❌ Never use STR_TO_DATE:
WHERE STR_TO_DATE(d, '%Y-%m-%d') = ...
```

### 16. UDF Functions

#### 16.1 Custom UDF Mapping
Some common custom UDFs can be mapped to BigQuery native functions:

| Hive UDF | BigQuery Native Function |
|----------|-------------------------|
| `udf.json_split(col)` | `JSON_EXTRACT_ARRAY(col)` |

Example:
```sql
-- Hive:
SELECT udf.json_split(json_col) AS items FROM table

-- BigQuery:
SELECT JSON_EXTRACT_ARRAY(json_col) AS items FROM `table`
```

#### 16.2 Unmapped UDFs
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

## Previous Attempts:
{conversion_history}

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
- If has Hive db prefix like `db.xxx`, convert to: `\`project.dataset.db_xxx\``
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
