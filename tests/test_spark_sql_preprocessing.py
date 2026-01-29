import pytest
from src.agent.nodes.spark_sql_validate import preprocess_spark_sql

def test_preprocess_spark_sql_removes_hivevar():
    sql = """
    set hivevar:date_app=${zdt.addDay(-1).format("yyyyMMdd")}_test;
    -- set hivevar:commented=${zdt};
    SELECT * FROM table_${date_app}
    """
    
    processed = preprocess_spark_sql(sql)
    
    # Check that the active hivevar line is removed (empty or replaced)
    # The line "set hivevar:date_app..." should be gone
    assert "set hivevar:date_app" not in processed.lower()
    
    # Check that the commented hivevar remains (optional, but good for stability)
    assert "-- set hivevar:commented" in processed
    
    # Check that the macro usage is replaced
    # ${date_app} should become dummy_var or similar if not found in variables
    # Wait, the logic is:
    # 1. Extract raw variables
    # 2. If 'date_app' value is complex (contains ${ or (), it gets placeholder_date_app
    # 3. Then substitution happens.
    
    # In this case:
    # key=date_app, value=${zdt...}_test
    # value contains ${, so verified[key] = placeholder_date_app
    # Then ${date_app} in SQL is replaced by placeholder_date_app
    
    assert "placeholder_date_app" in processed
    assert "${date_app}" not in processed

def test_preprocess_spark_sql_replaces_macros():
    sql = "SELECT * FROM table_${zdt.addDay(-1)}"
    processed = preprocess_spark_sql(sql)
    # Should replace ${...} with dummy_var
    assert "dummy_var" in processed
    assert "${" not in processed

def test_preprocess_spark_sql_simple_hivevar():
    sql = """
    set hivevar:my_table=users;
    SELECT * FROM ${my_table}
    """
    processed = preprocess_spark_sql(sql)
    
    # Simple value 'users' should be substituted directly
    assert "SELECT * FROM users" in processed.strip()
