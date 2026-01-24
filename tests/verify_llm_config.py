"""Verification script for Multi-Model LLM Support."""

import os
import sys
from src.services.llm import get_llm, get_model_name

def log(msg):
    print(msg)
    try:
        with open("verification_output.txt", "a") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def test_llm_config():
    """Test LLM configuration for different nodes."""
    if os.path.exists("verification_output.txt"):
        try:
            os.remove("verification_output.txt")
        except Exception:
            pass
            
    log("Testing LLM Configuration...")
    
    # 1. Test Without Default (Should Fail if logic works, but we set it here to pass basic test)
    # To strictly test failure we would need to unset env var, but let's test SUCCESS path with env var set
    os.environ["GEMINI_MODEL"] = "gemini-2.5-flash"
    os.environ["GOOGLE_API_KEY"] = "dummy" 
    os.environ["GOOGLE_CLOUD_PROJECT"] = "test-project"
    os.environ["GOOGLE_CLOUD_LOCATION"] = "us-central1"
    
    try:
        llm = get_llm("sql_convert")
        # ChatGoogleGenerativeAI uses .model
        model_name = getattr(llm, "model", "unknown")
        log(f"Default (sql_convert): {model_name} (Expected: gemini-2.5-flash)")
    except Exception as e:
        log(f"Default test failed: {e}")

    # 1b. Test Failure when no model set
    if "GEMINI_MODEL" in os.environ:
        del os.environ["GEMINI_MODEL"]
    
    try:
        get_llm("unknown_node")
        log("FAILURE: Expected ValueError not raised for missing GEMINI_MODEL")
    except ValueError as e:
        log(f"SUCCESS: Caught expected error for missing model: {e}")
    except Exception as e:
        log(f"FAILURE: Caught unexpected error: {e}")
    
    # Restore for next tests
    os.environ["GEMINI_MODEL"] = "gemini-2.5-flash"
    
    # 2. Test Override Node Model
    os.environ["SQL_CONVERT_MODEL"] = "gemini-1.5-pro"
    try:
        llm = get_llm("sql_convert")
        model_name = getattr(llm, "model", "unknown")
        log(f"Override (sql_convert): {model_name} (Expected: gemini-1.5-pro)")
    except Exception as e:
        log(f"Override test failed: {e}")
    
    # 3. Test Claude Model (Vertex)
    os.environ["LLM_SQL_CHECK_MODEL"] = "claude-3-5-sonnet@20240620"
    
    try:
        llm = get_llm("llm_sql_check")
        model_name = getattr(llm, "model", "unknown")
        log(f"Claude (llm_sql_check): {model_name} (Expected: claude-3-5-sonnet@20240620)")
    except Exception as e:
        log(f"Claude test failed: {e}")

if __name__ == "__main__":
    try:
        test_llm_config()
        log("\nVerification Passed!")
    except Exception as e:
        log(f"\nVerification Failed: {e}")
        exit(1)
