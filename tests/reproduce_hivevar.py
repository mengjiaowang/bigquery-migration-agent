
import logging
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agent.nodes.spark_sql_validate import spark_sql_validate

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_hivevar_validation():
    # User provided failing SQL
    sql = """
set hivevar:date_app=${zdt.addDay(-1).format("yyyyMMdd")}_test;

drop table if exists tmp_htl_dsjobdb.tmp_ibu_overall_base_${date_app};
    """
    
    print("Testing SQL validation with Hive variables...")
    print("-" * 50)
    print(sql.strip())
    print("-" * 50)
    
    state = {"spark_sql": sql}
    result = spark_sql_validate(state)
    
    print(f"\nValidation Result: {result['spark_valid']}")
    if result.get('spark_error'):
        print(f"Error: {result['spark_error']}")
        
    if result['spark_valid']:
        print("\nSUCCESS: SQL passed validation (FIXED)")
    else:
        print("\nFAILURE: SQL failed validation (REPRODUCED)")

if __name__ == "__main__":
    test_hivevar_validation()
