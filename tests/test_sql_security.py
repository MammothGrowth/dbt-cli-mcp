"""
Tests for the SQL security features in the tools module.
"""

import pytest
from src.tools import is_inline_sql_query, contains_mutation_risk


class TestSQLDetection:
    """Test cases for the is_inline_sql_query function."""

    def test_simple_model_names(self):
        """Test that simple model names are not detected as SQL."""
        model_names = [
            "customers",
            "orders",
            "stg_customers",
            "fct_orders",
            "dim_customers"
        ]
        
        for name in model_names:
            is_sql, _ = is_inline_sql_query(name)
            assert not is_sql, f"'{name}' was incorrectly identified as SQL"

    def test_select_queries(self):
        """Test that SELECT queries are correctly identified."""
        queries = [
            "SELECT * FROM customers",
            "select id, name from customers",
            "SELECT c.id FROM customers c",
            "SELECT * FROM {{ ref('customers') }}",
            "  SELECT  *  FROM  customers  "  # Extra whitespace
        ]
        
        for query in queries:
            is_sql, sql_type = is_inline_sql_query(query)
            assert is_sql, f"'{query}' was not identified as SQL"
            assert sql_type == "SELECT", f"'{query}' was not identified as SELECT"

    def test_with_queries(self):
        """Test that WITH queries are correctly identified."""
        queries = [
            "WITH cte AS (SELECT id FROM customers) SELECT * FROM cte",
            "with orders as (select * from orders) select * from orders",
            "WITH customer_orders AS (SELECT customer_id, COUNT(*) FROM orders GROUP BY 1) SELECT * FROM customer_orders"
        ]
        
        for query in queries:
            is_sql, sql_type = is_inline_sql_query(query)
            assert is_sql, f"'{query}' was not identified as SQL"
            assert sql_type in ["WITH", "SQL_SYNTAX"], f"'{query}' was not identified correctly"

    def test_snowflake_commands(self):
        """Test that Snowflake commands are correctly identified."""
        queries = [
            "SHOW TABLES",
            "show tables",
            "SHOW TABLES IN SCHEMA public",
            "DESCRIBE TABLE customers"
        ]
        
        for query in queries:
            is_sql, sql_type = is_inline_sql_query(query)
            assert is_sql, f"'{query}' was not identified as SQL"

    def test_commented_sql(self):
        """Test that SQL with comments is correctly identified."""
        queries = [
            "-- This is a comment\nSELECT * FROM customers",
            "/* Multi-line\ncomment */\nSELECT * FROM customers",
            "-- Comment only\n-- Another comment"
        ]
        
        for query in queries:
            is_sql, sql_type = is_inline_sql_query(query)
            assert is_sql, f"'{query}' was not identified as SQL"

    def test_complex_cases(self):
        """Test complex edge cases."""
        # These should be identified as SQL
        sql_cases = [
            "SELECT\n*\nFROM\ncustomers",  # Multi-line
            "{{ ref('customers') }}",  # Just a dbt ref
            "select case when amount > 100 then 'high' else 'low' end as amount_category from orders"  # CASE statement
        ]
        
        for query in sql_cases:
            is_sql, _ = is_inline_sql_query(query)
            assert is_sql, f"'{query}' was not identified as SQL"
        
        # These should not be identified as SQL
        non_sql_cases = [
            "customers_",
            "customers+",
            "tag:nightly",
            "path:models/staging"
        ]
        
        for query in non_sql_cases:
            is_sql, _ = is_inline_sql_query(query)
            assert not is_sql, f"'{query}' was incorrectly identified as SQL"


class TestSecurityValidation:
    """Test cases for the contains_mutation_risk function."""

    def test_safe_queries(self):
        """Test that safe queries pass validation."""
        safe_queries = [
            "SELECT * FROM customers",
            "SELECT id, name FROM customers WHERE status = 'active'",
            "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte",
            "SELECT * FROM {{ ref('customers') }} WHERE id = 1",
            "SELECT COUNT(*) FROM orders GROUP BY customer_id",
            "SELECT * FROM customers c JOIN orders o ON c.id = o.customer_id"
        ]
        
        for query in safe_queries:
            has_risk, reason = contains_mutation_risk(query)
            assert not has_risk, f"Safe query incorrectly flagged: {reason}"

    def test_dangerous_queries(self):
        """Test that dangerous queries are correctly flagged."""
        dangerous_queries = [
            ("DROP TABLE customers", "DROP TABLE"),
            ("DELETE FROM customers", "DELETE"),
            ("TRUNCATE TABLE customers", "TRUNCATE"),
            ("INSERT INTO customers VALUES (1, 'test')", "INSERT"),
            ("UPDATE customers SET status = 'inactive' WHERE id = 1", "UPDATE"),
            ("CREATE TABLE new_table (id INT)", "CREATE TABLE"),
            ("ALTER TABLE customers ADD COLUMN email VARCHAR", "ALTER TABLE"),
            ("GRANT SELECT ON customers TO user", "GRANT"),
            ("CREATE OR REPLACE TABLE customers AS SELECT * FROM staging", "CREATE OR REPLACE"),
            ("MERGE INTO customers USING staging ON customers.id = staging.id", "MERGE")
        ]
        
        for query, expected_pattern in dangerous_queries:
            has_risk, reason = contains_mutation_risk(query)
            assert has_risk, f"Dangerous query not flagged: {query}"
            assert expected_pattern.lower() in reason.lower(), f"Incorrect reason: {reason}"

    def test_sql_injection_attempts(self):
        """Test that SQL injection attempts are caught."""
        injection_attempts = [
            "SELECT * FROM customers; DROP TABLE orders",
            "SELECT * FROM customers; DELETE FROM orders",
            "SELECT * FROM customers; -- Comment\nDROP TABLE orders",
            "SELECT * FROM customers /* Comment */ ; DROP TABLE orders"
        ]
        
        for query in injection_attempts:
            has_risk, reason = contains_mutation_risk(query)
            assert has_risk, f"SQL injection not caught: {query}"
            assert "multiple" in reason.lower(), f"Incorrect reason: {reason}"

    def test_comment_evasion_attempts(self):
        """Test that attempts to hide dangerous operations in comments are caught."""
        evasion_attempts = [
            "SELECT * FROM customers /* DROP TABLE orders */",
            "-- DELETE FROM orders\nSELECT * FROM customers",
            "/* This is a\nDROP TABLE orders\nmulti-line comment */\nSELECT * FROM customers"
        ]
        
        # These should be safe because the dangerous operations are in comments
        for query in evasion_attempts:
            has_risk, reason = contains_mutation_risk(query)
            assert not has_risk, f"Comment-enclosed operation incorrectly flagged: {reason}"
        
        # But these should be caught because they have actual operations
        actual_operations = [
            "/* Comment */ DROP TABLE orders",
            "SELECT * FROM customers; /* Comment */ DROP TABLE orders",
            "-- Comment\nDELETE FROM orders"
        ]
        
        for query in actual_operations:
            has_risk, reason = contains_mutation_risk(query)
            assert has_risk, f"Dangerous operation not caught: {query}"

    def test_snowflake_specific_operations(self):
        """Test Snowflake-specific operations."""
        snowflake_operations = [
            ("COPY INTO customers FROM 's3://bucket/data.csv'", "COPY INTO"),
            ("PUT file:///tmp/data.csv @mystage", "PUT"),
            ("REMOVE @mystage/data.csv", "REMOVE"),
            ("UNLOAD TO 's3://bucket/data.csv'", "UNLOAD")
        ]
        
        for query, expected_pattern in snowflake_operations:
            has_risk, reason = contains_mutation_risk(query)
            assert has_risk, f"Snowflake operation not flagged: {query}"
            assert expected_pattern.lower() in reason.lower(), f"Incorrect reason: {reason}"