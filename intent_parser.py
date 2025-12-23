"""
LLM-Powered Intent Parser for Cognix V2.

This module uses OpenAI to extract structured analytics intent from natural language questions.
This is the core differentiator from the original Person M implementation which used keyword matching.
"""

import json
from typing import Optional
from openai import OpenAI
from loguru import logger

from config import settings
from schemas import AnalyticsIntent, FilterSpec, SortSpec


class IntentParser:
    """
    Extracts structured analytics intent from natural language using LLM.
    
    Unlike simple keyword matching, this can understand:
    - Complex queries with multiple dimensions
    - Implicit time references ("last month", "Q4")
    - Comparative queries ("compare X vs Y")
    - Filtering conditions ("only in West region")
    - Ranking queries ("top 5", "bottom 10")
    """
    
    def __init__(self):
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model
        
        # Build schema context for the LLM
        self.schema_context = self._build_schema_context()
        
        logger.info(f"IntentParser initialized with model: {self.model}")
    
    def _build_schema_context(self) -> str:
        """Build a description of the available data schema for the LLM."""
        return f"""
Available Data Schema:

METRICS (numeric fields that can be summed/averaged):
{json.dumps(settings.available_metrics, indent=2)}

DIMENSIONS (categorical fields to group by):
{json.dumps(settings.available_dimensions, indent=2)}

TIME GRAINS (for time-based analysis):
{json.dumps(settings.available_time_grains, indent=2)}

DATA CONTEXT:
- This is retail/superstore sales data
- Date field is called "order_date"
- All metrics can be aggregated (SUM, AVG, COUNT)
- Dimensions can be used for grouping and filtering
"""
    
    def _get_system_prompt(self) -> str:
        """Generate the system prompt for intent extraction."""
        return f"""You are an expert analytics intent parser. Your job is to convert natural language questions into structured analytics intents.

{self.schema_context}

RULES:
1. Only use metrics, dimensions, and time_grains from the available schema
2. If a metric is not specified, default to "sales"
3. If "trend" or "over time" is mentioned, set an appropriate time_grain
4. If "total" or single aggregate is requested, leave dimensions empty
5. Extract any filters mentioned (e.g., "in West region" → filter on region = "West")
6. Detect sorting needs (e.g., "top 5" → sort desc + limit 5)
7. Set confidence based on how clear the question is (0.0 to 1.0)
8. Provide brief reasoning for your extraction

OUTPUT FORMAT (JSON only):
{{
    "metrics": ["sales"],
    "dimensions": ["region"],
    "time_grain": "month" or null,
    "filters": [{{"field": "region", "operator": "=", "value": "West"}}] or [],
    "sort": {{"field": "sales", "order": "desc"}} or null,
    "limit": 5 or null,
    "confidence": 0.95,
    "reasoning": "User wants monthly sales breakdown by region"
}}

EXAMPLES:

Question: "Show total sales"
→ {{"metrics": ["sales"], "dimensions": [], "time_grain": null, "filters": [], "sort": null, "limit": null, "confidence": 0.99, "reasoning": "Simple total aggregate"}}

Question: "Monthly sales trend by region"
→ {{"metrics": ["sales"], "dimensions": ["region"], "time_grain": "month", "filters": [], "sort": null, "limit": null, "confidence": 0.95, "reasoning": "Time series with regional breakdown"}}

Question: "Top 5 categories by profit"
→ {{"metrics": ["profit"], "dimensions": ["category"], "time_grain": null, "filters": [], "sort": {{"field": "profit", "order": "desc"}}, "limit": 5, "confidence": 0.98, "reasoning": "Ranked categories by profit"}}

Question: "Sales in West region last month"
→ {{"metrics": ["sales"], "dimensions": [], "time_grain": "month", "filters": [{{"field": "region", "operator": "=", "value": "West"}}], "sort": null, "limit": null, "confidence": 0.90, "reasoning": "Filtered aggregate with time context"}}

If the question cannot be answered with the available schema, still try your best but set confidence low and explain in reasoning.
"""
    
    def parse(self, question: str) -> AnalyticsIntent:
        """
        Parse a natural language question into structured analytics intent.
        
        Args:
            question: Natural language analytics question
            
        Returns:
            AnalyticsIntent: Structured intent object
            
        Raises:
            ValueError: If parsing fails completely
        """
        logger.info(f"Parsing question: {question}")
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self._get_system_prompt()},
                    {"role": "user", "content": f"Parse this analytics question:\n\n{question}"}
                ],
                temperature=0,  # Deterministic output
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            logger.debug(f"LLM response: {content}")
            
            # Parse the JSON response
            parsed = json.loads(content)
            
            # Convert to AnalyticsIntent with proper nested objects
            filters = [
                FilterSpec(**f) if isinstance(f, dict) else f 
                for f in parsed.get("filters", [])
            ]
            
            sort = None
            if parsed.get("sort"):
                sort = SortSpec(**parsed["sort"])
            
            intent = AnalyticsIntent(
                metrics=parsed.get("metrics", ["sales"]),
                dimensions=parsed.get("dimensions", []),
                time_grain=parsed.get("time_grain"),
                filters=filters,
                sort=sort,
                limit=parsed.get("limit"),
                confidence=parsed.get("confidence", 1.0),
                reasoning=parsed.get("reasoning")
            )
            
            logger.info(f"Extracted intent: metrics={intent.metrics}, dims={intent.dimensions}, time={intent.time_grain}, confidence={intent.confidence}")
            
            return intent
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            raise ValueError(f"Intent parsing failed: Invalid JSON response from LLM")
            
        except Exception as e:
            logger.error(f"Intent parsing error: {e}")
            raise ValueError(f"Intent parsing failed: {str(e)}")
    
    def validate_intent(self, intent: AnalyticsIntent) -> tuple[bool, Optional[str]]:
        """
        Validate that the extracted intent uses valid schema elements.
        
        Args:
            intent: The extracted intent to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        errors = []
        
        # Validate metrics
        for metric in intent.metrics:
            if metric not in settings.available_metrics:
                errors.append(f"Unknown metric: {metric}")
        
        # Validate dimensions
        for dim in intent.dimensions:
            if dim not in settings.available_dimensions:
                errors.append(f"Unknown dimension: {dim}")
        
        # Validate time grain
        if intent.time_grain and intent.time_grain not in settings.available_time_grains:
            errors.append(f"Unknown time grain: {intent.time_grain}")
        
        # Validate filters
        valid_fields = settings.available_metrics + settings.available_dimensions + ["order_date"]
        for f in intent.filters:
            if f.field not in valid_fields:
                errors.append(f"Unknown filter field: {f.field}")
        
        if errors:
            return False, "; ".join(errors)
        
        return True, None


# Singleton instance for easy import
_parser: Optional[IntentParser] = None


def get_intent_parser() -> IntentParser:
    """Get or create the intent parser singleton."""
    global _parser
    if _parser is None:
        _parser = IntentParser()
    return _parser


def extract_intent(question: str) -> AnalyticsIntent:
    """
    Convenience function to extract intent from a question.
    
    This is the main entry point for intent parsing.
    """
    parser = get_intent_parser()
    return parser.parse(question)

