"""
Analytics Executor for Cognix V2.

Executes analytics queries using DuckDB on parquet files.
Generates SQL from structured intent and returns pandas DataFrames.
"""

import duckdb
import pandas as pd
from typing import Optional
from loguru import logger

from config import settings
from schemas import AnalyticsIntent


class AnalyticsExecutor:
    """
    Executes analytics queries based on structured intent.
    
    Features:
    - Converts intent to optimized SQL
    - Uses DuckDB for fast in-process analytics
    - Supports aggregations, grouping, filtering, sorting
    - Handles time-based analysis with date truncation
    """
    
    def __init__(self, data_path: Optional[str] = None):
        self.data_path = data_path or str(settings.data_path)
        logger.info(f"AnalyticsExecutor initialized with data: {self.data_path}")
    
    def _build_sql(self, intent: AnalyticsIntent) -> str:
        """
        Build SQL query from analytics intent.
        
        This is a sophisticated SQL builder that handles:
        - Multiple metrics with aggregation
        - Multiple dimensions for grouping
        - Time-based grouping with date truncation
        - Filters with various operators
        - Sorting and limits
        """
        selects = []
        group_bys = []
        where_clauses = []
        order_by = []
        
        # Handle time grain (time-based analysis)
        if intent.time_grain:
            time_expr = f"date_trunc('{intent.time_grain}', order_date)"
            selects.append(f"{time_expr} AS time")
            group_bys.append("time")
        
        # Handle dimensions (categorical grouping)
        for dim in intent.dimensions:
            selects.append(dim)
            group_bys.append(dim)
        
        # Handle metrics (aggregations)
        for metric in intent.metrics:
            selects.append(f"SUM({metric}) AS {metric}")
        
        # If no selects yet (shouldn't happen), add count
        if not selects:
            selects.append("COUNT(*) AS count")
        
        # Handle filters
        for f in intent.filters:
            if f.operator == "=":
                where_clauses.append(f"{f.field} = '{f.value}'")
            elif f.operator == "!=":
                where_clauses.append(f"{f.field} != '{f.value}'")
            elif f.operator == ">":
                where_clauses.append(f"{f.field} > {f.value}")
            elif f.operator == "<":
                where_clauses.append(f"{f.field} < {f.value}")
            elif f.operator == ">=":
                where_clauses.append(f"{f.field} >= {f.value}")
            elif f.operator == "<=":
                where_clauses.append(f"{f.field} <= {f.value}")
            elif f.operator == "in":
                values = ", ".join([f"'{v}'" for v in f.value])
                where_clauses.append(f"{f.field} IN ({values})")
            elif f.operator == "not_in":
                values = ", ".join([f"'{v}'" for v in f.value])
                where_clauses.append(f"{f.field} NOT IN ({values})")
        
        # Handle sorting
        if intent.sort:
            order_by.append(f"{intent.sort.field} {intent.sort.order.upper()}")
        elif intent.metrics:
            # Default sort by first metric descending
            order_by.append(f"{intent.metrics[0]} DESC")
        
        # Build the SQL query
        select_clause = ", ".join(selects)
        
        sql = f"SELECT {select_clause}\nFROM '{self.data_path}'"
        
        if where_clauses:
            sql += f"\nWHERE {' AND '.join(where_clauses)}"
        
        if group_bys:
            sql += f"\nGROUP BY {', '.join(group_bys)}"
        
        if order_by:
            sql += f"\nORDER BY {', '.join(order_by)}"
        
        if intent.limit:
            sql += f"\nLIMIT {intent.limit}"
        
        return sql
    
    def execute(self, intent: AnalyticsIntent) -> tuple[pd.DataFrame, str]:
        """
        Execute analytics based on intent.
        
        Args:
            intent: Structured analytics intent
            
        Returns:
            Tuple of (result_dataframe, sql_query)
        """
        sql = self._build_sql(intent)
        logger.info(f"Executing SQL:\n{sql}")
        
        try:
            con = duckdb.connect()
            df = con.execute(sql).df()
            con.close()
            
            # Normalize column names to lowercase
            df.columns = [c.lower() for c in df.columns]
            
            logger.info(f"Query returned {len(df)} rows")
            return df, sql
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise RuntimeError(f"Analytics execution failed: {str(e)}")
    
    def get_data_summary(self) -> dict:
        """Get summary statistics about the data."""
        try:
            con = duckdb.connect()
            
            # Get row count
            count = con.execute(f"SELECT COUNT(*) FROM '{self.data_path}'").fetchone()[0]
            
            # Get column info
            columns = con.execute(f"DESCRIBE SELECT * FROM '{self.data_path}'").df()
            
            # Get date range
            date_range = con.execute(f"""
                SELECT 
                    MIN(order_date) as min_date,
                    MAX(order_date) as max_date
                FROM '{self.data_path}'
            """).fetchone()
            
            # Get unique values for dimensions
            dimensions_info = {}
            for dim in settings.available_dimensions:
                try:
                    unique_count = con.execute(f"""
                        SELECT COUNT(DISTINCT {dim}) FROM '{self.data_path}'
                    """).fetchone()[0]
                    dimensions_info[dim] = unique_count
                except:
                    pass
            
            con.close()
            
            return {
                "total_rows": count,
                "columns": columns.to_dict(orient="records"),
                "date_range": {
                    "min": str(date_range[0]) if date_range else None,
                    "max": str(date_range[1]) if date_range else None
                },
                "dimensions": dimensions_info
            }
            
        except Exception as e:
            logger.error(f"Failed to get data summary: {e}")
            return {"error": str(e)}


# Singleton instance
_executor: Optional[AnalyticsExecutor] = None


def get_executor() -> AnalyticsExecutor:
    """Get or create the analytics executor singleton."""
    global _executor
    if _executor is None:
        _executor = AnalyticsExecutor()
    return _executor


def execute_intent(intent: AnalyticsIntent) -> tuple[pd.DataFrame, str]:
    """Convenience function to execute analytics from intent."""
    executor = get_executor()
    return executor.execute(intent)

