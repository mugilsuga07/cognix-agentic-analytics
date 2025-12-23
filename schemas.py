"""
Pydantic schemas for request/response validation.
Provides type safety and automatic documentation.
"""

from typing import Optional, Any
from pydantic import BaseModel, Field


class FilterSpec(BaseModel):
    """Specification for a data filter."""
    field: str = Field(..., description="Field to filter on")
    operator: str = Field(..., description="Comparison operator (=, !=, >, <, >=, <=, in, not_in)")
    value: Any = Field(..., description="Value to compare against")


class SortSpec(BaseModel):
    """Specification for sorting."""
    field: str = Field(..., description="Field to sort by")
    order: str = Field("desc", description="Sort order: asc or desc")


class AnalyticsIntent(BaseModel):
    """
    Structured intent extracted from natural language query.
    This is the core data structure that drives the analytics pipeline.
    """
    metrics: list[str] = Field(
        default_factory=list,
        description="Numeric fields to aggregate (e.g., sales, profit)"
    )
    dimensions: list[str] = Field(
        default_factory=list,
        description="Categorical fields to group by (e.g., region, category)"
    )
    time_grain: Optional[str] = Field(
        None,
        description="Time granularity for trend analysis (day, week, month, quarter, year)"
    )
    filters: list[FilterSpec] = Field(
        default_factory=list,
        description="Filters to apply to the data"
    )
    sort: Optional[SortSpec] = Field(
        None,
        description="Sorting specification"
    )
    limit: Optional[int] = Field(
        None,
        description="Maximum number of results to return"
    )
    
    # Metadata
    confidence: float = Field(
        1.0,
        description="LLM confidence in the intent extraction (0-1)"
    )
    reasoning: Optional[str] = Field(
        None,
        description="LLM's reasoning for the extraction"
    )


class QueryRequest(BaseModel):
    """API request for analytics query."""
    question: str = Field(..., description="Natural language analytics question")


class QueryResponse(BaseModel):
    """API response with analytics results."""
    question: str = Field(..., description="Original question")
    intent: AnalyticsIntent = Field(..., description="Extracted intent")
    answer: str = Field(..., description="Natural language answer")
    visualization: str = Field(..., description="Recommended chart type")
    visualization_reason: str = Field(..., description="Why this visualization was chosen")
    data: list[dict] = Field(..., description="Analytics results as records")
    sql_query: str = Field(..., description="Generated SQL query")
    artifact_path: Optional[str] = Field(None, description="Path to saved artifact")
    
    class Config:
        json_schema_extra = {
            "example": {
                "question": "Show monthly sales by region",
                "intent": {
                    "metrics": ["sales"],
                    "dimensions": ["region"],
                    "time_grain": "month"
                },
                "answer": "Sales show an upward trend...",
                "visualization": "line",
                "data": [{"time": "2024-01", "region": "West", "sales": 10000}]
            }
        }


class ErrorResponse(BaseModel):
    """API error response."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    suggestion: Optional[str] = Field(None, description="Suggestion for fixing the error")

