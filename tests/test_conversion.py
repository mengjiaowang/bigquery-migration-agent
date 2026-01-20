"""
Hive to BigQuery SQL Conversion Test Script

This script tests the conversion capability with various complex Hive SQL scenarios.
Run with: python -m tests.test_conversion
"""

import json
import os
import sys
from dataclasses import dataclass
from typing import Optional

import requests

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@dataclass
class TestCase:
    """Test case definition."""
    name: str
    description: str
    hive_sql: str
    expected_keywords: list[str]  # Keywords expected in the converted BigQuery SQL


# Define comprehensive test cases
TEST_CASES = [
    # ============================================================
    # 1. 基础查询和数据类型
    # ============================================================
    TestCase(
        name="basic_select",
        description="基础 SELECT 查询",
        hive_sql="""
            SELECT 
                id,
                name,
                age,
                salary
            FROM employees
            WHERE age > 30
            ORDER BY salary DESC
            LIMIT 100
        """,
        expected_keywords=["SELECT", "FROM", "WHERE", "ORDER BY", "LIMIT"]
    ),
    
    TestCase(
        name="data_type_cast",
        description="数据类型转换",
        hive_sql="""
            SELECT 
                CAST(id AS STRING) as id_str,
                CAST(amount AS DOUBLE) as amount_double,
                CAST(count AS BIGINT) as count_big,
                CAST(flag AS BOOLEAN) as flag_bool
            FROM transactions
        """,
        expected_keywords=["CAST", "STRING", "FLOAT64", "INT64", "BOOL"]
    ),
    
    # ============================================================
    # 2. 日期时间函数
    # ============================================================
    TestCase(
        name="date_functions",
        description="日期函数转换",
        hive_sql="""
            SELECT 
                date_format(create_time, 'yyyy-MM-dd') as formatted_date,
                date_format(create_time, 'yyyy-MM-dd HH:mm:ss') as formatted_datetime,
                datediff(end_date, start_date) as days_diff,
                date_add(create_date, 7) as week_later,
                date_sub(create_date, 30) as month_ago,
                to_date(create_time) as date_only
            FROM orders
        """,
        expected_keywords=["FORMAT_DATE", "DATE_DIFF", "DATE_ADD", "DATE_SUB", "DATE"]
    ),
    
    TestCase(
        name="unix_timestamp_functions",
        description="Unix 时间戳函数",
        hive_sql="""
            SELECT 
                from_unixtime(ts) as timestamp_from_unix,
                from_unixtime(ts, 'yyyy-MM-dd') as date_from_unix,
                unix_timestamp(create_time) as unix_ts,
                unix_timestamp('2024-01-01 00:00:00') as fixed_unix_ts
            FROM events
        """,
        expected_keywords=["TIMESTAMP_SECONDS", "UNIX_SECONDS"]
    ),
    
    # ============================================================
    # 3. 字符串函数
    # ============================================================
    TestCase(
        name="string_functions",
        description="字符串函数转换",
        hive_sql="""
            SELECT 
                concat(first_name, ' ', last_name) as full_name,
                concat_ws(',', col1, col2, col3) as combined,
                substr(description, 1, 100) as short_desc,
                length(name) as name_length,
                upper(name) as upper_name,
                lower(name) as lower_name,
                trim(name) as trimmed_name,
                ltrim(name) as left_trimmed,
                rtrim(name) as right_trimmed,
                instr(name, 'test') as position,
                locate('test', name) as position2
            FROM users
        """,
        expected_keywords=["CONCAT", "SUBSTR", "LENGTH", "UPPER", "LOWER", "TRIM", "STRPOS"]
    ),
    
    TestCase(
        name="regex_functions",
        description="正则表达式函数",
        hive_sql="""
            SELECT 
                regexp_extract(url, 'https?://([^/]+)', 1) as domain,
                regexp_replace(phone, '[^0-9]', '') as clean_phone,
                split(tags, ',') as tag_array
            FROM web_logs
        """,
        expected_keywords=["REGEXP_EXTRACT", "REGEXP_REPLACE", "SPLIT"]
    ),
    
    # ============================================================
    # 4. 聚合函数
    # ============================================================
    TestCase(
        name="aggregate_functions",
        description="聚合函数转换",
        hive_sql="""
            SELECT 
                department,
                COUNT(*) as total_count,
                COUNT(DISTINCT employee_id) as unique_employees,
                SUM(salary) as total_salary,
                AVG(salary) as avg_salary,
                MAX(salary) as max_salary,
                MIN(salary) as min_salary,
                collect_list(name) as all_names,
                collect_set(skill) as unique_skills
            FROM employees
            GROUP BY department
        """,
        expected_keywords=["COUNT", "SUM", "AVG", "MAX", "MIN", "ARRAY_AGG", "DISTINCT"]
    ),
    
    TestCase(
        name="percentile_functions",
        description="百分位数函数",
        hive_sql="""
            SELECT 
                category,
                percentile_approx(price, 0.5) as median_price,
                percentile_approx(price, 0.95) as p95_price
            FROM products
            GROUP BY category
        """,
        expected_keywords=["APPROX_QUANTILES"]
    ),
    
    # ============================================================
    # 5. 窗口函数
    # ============================================================
    TestCase(
        name="window_functions",
        description="窗口函数转换",
        hive_sql="""
            SELECT 
                employee_id,
                department,
                salary,
                ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
                RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dense_rank,
                LAG(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) as prev_salary,
                LEAD(salary, 1) OVER (PARTITION BY department ORDER BY hire_date) as next_salary,
                SUM(salary) OVER (PARTITION BY department) as dept_total,
                AVG(salary) OVER (PARTITION BY department) as dept_avg
            FROM employees
        """,
        expected_keywords=["ROW_NUMBER", "RANK", "LAG", "LEAD", "OVER", "PARTITION BY"]
    ),
    
    # ============================================================
    # 6. LATERAL VIEW 和 EXPLODE
    # ============================================================
    TestCase(
        name="lateral_view_explode",
        description="LATERAL VIEW EXPLODE 转换",
        hive_sql="""
            SELECT 
                order_id,
                item
            FROM orders
            LATERAL VIEW explode(items) t AS item
        """,
        expected_keywords=["CROSS JOIN", "UNNEST"]
    ),
    
    TestCase(
        name="lateral_view_posexplode",
        description="LATERAL VIEW POSEXPLODE 转换",
        hive_sql="""
            SELECT 
                order_id,
                pos,
                item
            FROM orders
            LATERAL VIEW posexplode(items) t AS pos, item
        """,
        expected_keywords=["CROSS JOIN", "UNNEST", "WITH OFFSET"]
    ),
    
    TestCase(
        name="multiple_lateral_view",
        description="多重 LATERAL VIEW",
        hive_sql="""
            SELECT 
                user_id,
                tag,
                category
            FROM user_profiles
            LATERAL VIEW explode(tags) t1 AS tag
            LATERAL VIEW explode(categories) t2 AS category
        """,
        expected_keywords=["CROSS JOIN", "UNNEST"]
    ),
    
    # ============================================================
    # 7. JSON 处理
    # ============================================================
    TestCase(
        name="json_functions",
        description="JSON 函数转换",
        hive_sql="""
            SELECT 
                get_json_object(json_data, '$.name') as name,
                get_json_object(json_data, '$.address.city') as city,
                get_json_object(json_data, '$.items[0].id') as first_item_id
            FROM events
        """,
        expected_keywords=["JSON_EXTRACT", "JSON_VALUE"]
    ),
    
    # ============================================================
    # 8. NULL 处理
    # ============================================================
    TestCase(
        name="null_handling",
        description="NULL 值处理函数",
        hive_sql="""
            SELECT 
                nvl(name, 'Unknown') as name_with_default,
                coalesce(phone, mobile, 'N/A') as contact,
                if(age IS NULL, 0, age) as age_safe,
                CASE 
                    WHEN status IS NULL THEN 'pending'
                    WHEN status = 1 THEN 'active'
                    ELSE 'inactive'
                END as status_text
            FROM users
        """,
        expected_keywords=["IFNULL", "COALESCE", "IF", "CASE", "WHEN"]
    ),
    
    # ============================================================
    # 9. 复杂 JOIN
    # ============================================================
    TestCase(
        name="complex_joins",
        description="复杂 JOIN 操作",
        hive_sql="""
            SELECT 
                o.order_id,
                o.order_date,
                c.customer_name,
                p.product_name,
                oi.quantity,
                oi.unit_price
            FROM orders o
            INNER JOIN customers c ON o.customer_id = c.customer_id
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            LEFT JOIN products p ON oi.product_id = p.product_id
            WHERE o.order_date >= '2024-01-01'
        """,
        expected_keywords=["INNER JOIN", "LEFT JOIN", "ON"]
    ),
    
    # ============================================================
    # 10. 子查询和 CTE
    # ============================================================
    TestCase(
        name="subquery",
        description="子查询",
        hive_sql="""
            SELECT 
                department,
                avg_salary
            FROM (
                SELECT 
                    department,
                    AVG(salary) as avg_salary
                FROM employees
                GROUP BY department
            ) t
            WHERE avg_salary > 50000
        """,
        expected_keywords=["SELECT", "FROM", "AVG", "GROUP BY", "WHERE"]
    ),
    
    TestCase(
        name="cte_query",
        description="CTE (WITH 子句)",
        hive_sql="""
            WITH dept_stats AS (
                SELECT 
                    department,
                    COUNT(*) as emp_count,
                    AVG(salary) as avg_salary
                FROM employees
                GROUP BY department
            ),
            high_salary_depts AS (
                SELECT department
                FROM dept_stats
                WHERE avg_salary > 60000
            )
            SELECT 
                e.employee_id,
                e.name,
                e.department,
                e.salary
            FROM employees e
            INNER JOIN high_salary_depts h ON e.department = h.department
        """,
        expected_keywords=["WITH", "AS", "SELECT", "JOIN"]
    ),
    
    # ============================================================
    # 11. UNION 操作
    # ============================================================
    TestCase(
        name="union_operations",
        description="UNION 操作",
        hive_sql="""
            SELECT id, name, 'employee' as type FROM employees
            UNION ALL
            SELECT id, name, 'contractor' as type FROM contractors
            UNION
            SELECT id, name, 'intern' as type FROM interns
        """,
        expected_keywords=["UNION ALL", "UNION"]
    ),
    
    # ============================================================
    # 12. 数学函数
    # ============================================================
    TestCase(
        name="math_functions",
        description="数学函数转换",
        hive_sql="""
            SELECT 
                abs(amount) as abs_amount,
                ceil(price) as ceil_price,
                floor(price) as floor_price,
                round(price, 2) as rounded_price,
                pow(base, exponent) as power_result,
                sqrt(value) as sqrt_value,
                log(value) as log_value,
                log10(value) as log10_value,
                log2(value) as log2_value,
                rand() as random_value,
                greatest(a, b, c) as max_val,
                least(a, b, c) as min_val
            FROM numbers
        """,
        expected_keywords=["ABS", "CEIL", "FLOOR", "ROUND", "POW", "SQRT", "LN", "LOG", "RAND", "GREATEST", "LEAST"]
    ),
    
    # ============================================================
    # 13. 数组操作
    # ============================================================
    TestCase(
        name="array_functions",
        description="数组函数转换",
        hive_sql="""
            SELECT 
                size(items) as item_count,
                array_contains(tags, 'featured') as is_featured,
                sort_array(scores) as sorted_scores
            FROM products
        """,
        expected_keywords=["ARRAY_LENGTH"]
    ),
    
    # ============================================================
    # 14. Hive 特有语法
    # ============================================================
    TestCase(
        name="hive_specific_syntax",
        description="Hive 特有语法（应被移除或转换）",
        hive_sql="""
            SELECT 
                user_id,
                event_type,
                COUNT(*) as event_count
            FROM events
            WHERE dt = '2024-01-01'
            GROUP BY user_id, event_type
            DISTRIBUTE BY user_id
            SORT BY event_count DESC
        """,
        expected_keywords=["SELECT", "GROUP BY", "ORDER BY"]
    ),
    
    # ============================================================
    # 15. 综合复杂查询
    # ============================================================
    TestCase(
        name="complex_analytics_query",
        description="复杂分析查询",
        hive_sql="""
            WITH daily_sales AS (
                SELECT 
                    to_date(order_time) as order_date,
                    product_category,
                    SUM(amount) as daily_amount,
                    COUNT(DISTINCT customer_id) as unique_customers
                FROM orders
                WHERE order_time >= date_sub(current_date(), 30)
                GROUP BY to_date(order_time), product_category
            ),
            category_stats AS (
                SELECT 
                    product_category,
                    AVG(daily_amount) as avg_daily_sales,
                    SUM(daily_amount) as total_sales,
                    AVG(unique_customers) as avg_daily_customers
                FROM daily_sales
                GROUP BY product_category
            )
            SELECT 
                cs.product_category,
                cs.total_sales,
                cs.avg_daily_sales,
                cs.avg_daily_customers,
                ROW_NUMBER() OVER (ORDER BY cs.total_sales DESC) as sales_rank,
                ROUND(cs.total_sales * 100.0 / SUM(cs.total_sales) OVER (), 2) as sales_percentage
            FROM category_stats cs
            ORDER BY cs.total_sales DESC
        """,
        expected_keywords=["WITH", "DATE", "DATE_SUB", "SUM", "COUNT", "DISTINCT", "AVG", "ROW_NUMBER", "OVER", "ORDER BY"]
    ),
    
    TestCase(
        name="complex_user_behavior_analysis",
        description="用户行为分析复杂查询",
        hive_sql="""
            SELECT 
                user_id,
                session_id,
                event_type,
                event_time,
                page_url,
                LAG(event_time) OVER (PARTITION BY user_id, session_id ORDER BY event_time) as prev_event_time,
                LEAD(event_time) OVER (PARTITION BY user_id, session_id ORDER BY event_time) as next_event_time,
                datediff(
                    from_unixtime(unix_timestamp(event_time)),
                    from_unixtime(unix_timestamp(LAG(event_time) OVER (PARTITION BY user_id, session_id ORDER BY event_time)))
                ) as time_diff_seconds,
                FIRST_VALUE(page_url) OVER (PARTITION BY user_id, session_id ORDER BY event_time) as landing_page,
                LAST_VALUE(page_url) OVER (PARTITION BY user_id, session_id ORDER BY event_time 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as exit_page
            FROM user_events
            WHERE dt >= date_format(date_sub(current_date(), 7), 'yyyy-MM-dd')
        """,
        expected_keywords=["LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "OVER", "PARTITION BY", "DATE_DIFF", "TIMESTAMP_SECONDS"]
    ),
]


def run_test(base_url: str, test_case: TestCase) -> dict:
    """Run a single test case."""
    print(f"\n{'='*60}")
    print(f"Test: {test_case.name}")
    print(f"Description: {test_case.description}")
    print(f"{'='*60}")
    
    print("\n[Input Hive SQL]")
    print(test_case.hive_sql.strip())
    
    try:
        response = requests.post(
            f"{base_url}/convert",
            json={"hive_sql": test_case.hive_sql},
            timeout=120
        )
        result = response.json()
        
        print(f"\n[Conversion Result]")
        print(f"Success: {result.get('success')}")
        print(f"Hive Valid: {result.get('hive_valid')}")
        print(f"Validation Success: {result.get('validation_success')}")
        print(f"Validation Mode: {result.get('validation_mode')}")
        print(f"Retry Count: {result.get('retry_count')}")
        
        if result.get('hive_error'):
            print(f"\n[Hive Error]")
            print(result['hive_error'])
        
        if result.get('bigquery_sql'):
            print(f"\n[Converted BigQuery SQL]")
            print(result['bigquery_sql'])
            
            # Check expected keywords
            bigquery_sql_upper = result['bigquery_sql'].upper()
            missing_keywords = []
            found_keywords = []
            
            for keyword in test_case.expected_keywords:
                if keyword.upper() in bigquery_sql_upper:
                    found_keywords.append(keyword)
                else:
                    missing_keywords.append(keyword)
            
            print(f"\n[Keyword Check]")
            print(f"Found: {', '.join(found_keywords) if found_keywords else 'None'}")
            if missing_keywords:
                print(f"Missing: {', '.join(missing_keywords)}")
        
        if result.get('validation_error'):
            print(f"\n[Validation Error]")
            print(result['validation_error'])
        
        if result.get('warning'):
            print(f"\n[Warning]")
            print(result['warning'])
        
        return {
            "name": test_case.name,
            "success": result.get('success', False),
            "hive_valid": result.get('hive_valid', False),
            "validation_success": result.get('validation_success', False),
            "retry_count": result.get('retry_count', 0),
            "error": result.get('validation_error') or result.get('hive_error'),
        }
        
    except requests.exceptions.RequestException as e:
        print(f"\n[Request Error]")
        print(str(e))
        return {
            "name": test_case.name,
            "success": False,
            "error": str(e),
        }
    except Exception as e:
        print(f"\n[Error]")
        print(str(e))
        return {
            "name": test_case.name,
            "success": False,
            "error": str(e),
        }


def main():
    """Run all test cases."""
    base_url = os.getenv("API_URL", "http://localhost:8000")
    
    print("=" * 60)
    print("Hive to BigQuery SQL Conversion Test Suite")
    print("=" * 60)
    print(f"API URL: {base_url}")
    print(f"Total Test Cases: {len(TEST_CASES)}")
    
    # Check if service is running
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code != 200:
            print(f"\nError: Service is not healthy (status: {response.status_code})")
            sys.exit(1)
        print("Service Status: Healthy")
    except requests.exceptions.RequestException as e:
        print(f"\nError: Cannot connect to service at {base_url}")
        print(f"Please make sure the service is running: python -m src.main")
        sys.exit(1)
    
    # Run all tests
    results = []
    for test_case in TEST_CASES:
        result = run_test(base_url, test_case)
        results.append(result)
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    total = len(results)
    successful = sum(1 for r in results if r.get('success'))
    hive_valid = sum(1 for r in results if r.get('hive_valid'))
    validation_passed = sum(1 for r in results if r.get('validation_success'))
    
    print(f"\nTotal Tests: {total}")
    print(f"Hive SQL Valid: {hive_valid}/{total} ({hive_valid/total*100:.1f}%)")
    print(f"Validation Passed: {validation_passed}/{total} ({validation_passed/total*100:.1f}%)")
    print(f"Fully Successful: {successful}/{total} ({successful/total*100:.1f}%)")
    
    print("\n[Test Results by Case]")
    print("-" * 60)
    for r in results:
        status = "✓" if r.get('success') else "✗"
        hive_status = "✓" if r.get('hive_valid') else "✗"
        val_status = "✓" if r.get('validation_success') else "✗"
        retry = r.get('retry_count', 0)
        
        print(f"{status} {r['name']:<40} Hive:{hive_status} Val:{val_status} Retry:{retry}")
    
    # List failed tests
    failed = [r for r in results if not r.get('success')]
    if failed:
        print("\n[Failed Tests]")
        print("-" * 60)
        for r in failed:
            print(f"- {r['name']}: {r.get('error', 'Unknown error')[:100]}")
    
    print("\n" + "=" * 60)
    
    # Return exit code
    return 0 if successful == total else 1


if __name__ == "__main__":
    sys.exit(main())
