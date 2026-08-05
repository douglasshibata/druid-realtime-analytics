import os
import json
import re
import sys

def validate_json_file(filepath):
    print(f"[*] Validating JSON file: {filepath}")
    if not os.path.exists(filepath):
        print(f"[ERROR] File does not exist: {filepath}")
        return False

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"[SUCCESS] {filepath} is valid JSON.")
        return data
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON in {filepath}: {e}")
        return None

def validate_streaming_spec(data):
    print("[*] Validating Streaming Spec schema correctness...")
    if not data:
        return False

    # Assert top-level type is kafka
    if data.get("type") != "kafka":
        print("[ERROR] Streaming spec must have top-level type 'kafka'")
        return False

    spec = data.get("spec")
    if not spec:
        print("[ERROR] Streaming spec is missing 'spec' node")
        return False

    io_config = spec.get("ioConfig")
    if not io_config or io_config.get("type") != "kafka":
        print("[ERROR] ioConfig must be present and of type 'kafka'")
        return False

    if not io_config.get("topic"):
        print("[ERROR] ioConfig must specify a 'topic'")
        return False

    data_schema = spec.get("dataSchema")
    if not data_schema:
        print("[ERROR] dataSchema must be present")
        return False

    if not data_schema.get("dataSource"):
        print("[ERROR] dataSchema must specify a 'dataSource'")
        return False

    # Check for thetaSketch metric aggregation
    metrics = data_schema.get("metricsSpec", [])
    has_theta = any(m.get("type") == "thetaSketch" for m in metrics)
    if not has_theta:
        print("[ERROR] dataSchema metricsSpec must contain at least one 'thetaSketch'")
        return False

    print("[SUCCESS] Streaming spec matches schema requirements.")
    return True

def validate_batch_spec(data):
    print("[*] Validating Batch Spec schema correctness...")
    if not data:
        return False

    if data.get("type") != "index_parallel":
        print("[ERROR] Batch spec must have top-level type 'index_parallel'")
        return False

    spec = data.get("spec")
    if not spec:
        print("[ERROR] Batch spec is missing 'spec' node")
        return False

    io_config = spec.get("ioConfig")
    if not io_config or io_config.get("type") != "index_parallel":
        print("[ERROR] ioConfig must be present and of type 'index_parallel'")
        return False

    input_source = io_config.get("inputSource")
    if not input_source or input_source.get("type") != "s3":
        print("[ERROR] inputSource must be present and of type 's3'")
        return False

    print("[SUCCESS] Batch spec matches schema requirements.")
    return True

def validate_sql_queries(filepath):
    print(f"[*] Validating SQL syntax in: {filepath}")
    if not os.path.exists(filepath):
        print(f"[ERROR] SQL file does not exist: {filepath}")
        return False

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Perform basic checks (ensure keywords exist, sketches functions are named correctly)
        required_keywords = ["SELECT", "FROM", "WHERE", "GROUP BY"]
        for keyword in required_keywords:
            if keyword not in content.upper():
                print(f"[ERROR] Missing standard SQL keyword: '{keyword}' in {filepath}")
                return False

        required_funcs = ["THETA_SKETCH_ESTIMATE", "DS_THETA"]
        for func in required_funcs:
            if func not in content:
                print(f"[ERROR] Missing essential Druid sketches function: '{func}' in {filepath}")
                return False

        print(f"[SUCCESS] {filepath} contains valid SQL structures and Druid functions.")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to validate SQL: {e}")
        return False

def main():
    print("=== STARTING DRUID ECOSYSTEM CONFIGURATION VALIDATION ===")

    streaming_file = "druid-realtime-analytics/ingestion/streaming-supervisor-spec.json"
    batch_file = "druid-realtime-analytics/ingestion/batch-ingestion-spec.json"
    sql_file = "druid-realtime-analytics/queries/analytical_queries.sql"

    # 1. Validate Streaming Spec JSON & Schema
    streaming_data = validate_json_file(streaming_file)
    if not streaming_data or not validate_streaming_spec(streaming_data):
        print("[FAILURE] Streaming Ingestion Spec validation failed.")
        sys.exit(1)

    # 2. Validate Batch Spec JSON & Schema
    batch_data = validate_json_file(batch_file)
    if not batch_data or not validate_batch_spec(batch_data):
        print("[FAILURE] Batch Ingestion Spec validation failed.")
        sys.exit(1)

    # 3. Validate SQL Queries File
    if not validate_sql_queries(sql_file):
        print("[FAILURE] Analytical SQL queries validation failed.")
        sys.exit(1)

    print("=== ALL VALIDATIONS PASSED SUCCESSFULLY ===")
    sys.exit(0)

if __name__ == "__main__":
    main()
