"""
Natural Language Response Generator for Cognix V2.

Uses LLM to generate human-readable insights from analytics results.
This transforms raw data into meaningful business narratives.
"""

import json
import pandas as pd
from typing import Optional
from openai import OpenAI
from loguru import logger

from config import settings
from schemas import AnalyticsIntent


class ResponseGenerator:
    """
    Generates natural language responses from analytics results.
    
    Features:
    - Summarizes key findings
    - Highlights trends and anomalies
    - Provides business context
    - Adapts language to the question type
    """
    
    def __init__(self):
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model
        logger.info(f"ResponseGenerator initialized with model: {self.model}")
    
    def _prepare_data_context(self, df: pd.DataFrame, intent: AnalyticsIntent) -> str:
        """Prepare data context for the LLM."""
        context = {
            "row_count": len(df),
            "columns": list(df.columns),
        }
        
        # Add statistics for each metric
        for metric in intent.metrics:
            if metric in df.columns:
                context[f"{metric}_total"] = float(df[metric].sum())
                context[f"{metric}_mean"] = float(df[metric].mean())
                context[f"{metric}_min"] = float(df[metric].min())
                context[f"{metric}_max"] = float(df[metric].max())
        
        # Add sample data (top rows)
        context["sample_data"] = df.head(10).to_dict(orient="records")
        
        # If there's a time dimension, add trend info
        if intent.time_grain and "time" in df.columns and len(df) > 1:
            df_sorted = df.sort_values("time")
            first_value = df_sorted[intent.metrics[0]].iloc[0]
            last_value = df_sorted[intent.metrics[0]].iloc[-1]
            if first_value > 0:
                change_pct = ((last_value - first_value) / first_value) * 100
                context["trend_change_pct"] = round(change_pct, 2)
                context["trend_direction"] = "increasing" if change_pct > 0 else "decreasing"
        
        return json.dumps(context, indent=2, default=str)
    
    def generate(
        self, 
        question: str, 
        df: pd.DataFrame, 
        intent: AnalyticsIntent,
        sql_query: str
    ) -> str:
        """
        Generate a natural language response for the analytics results.
        
        Args:
            question: Original user question
            df: Analytics results DataFrame
            intent: Extracted intent
            sql_query: The SQL query that was executed
            
        Returns:
            Natural language insight/answer
        """
        logger.info(f"Generating response for: {question}")
        
        data_context = self._prepare_data_context(df, intent)
        
        system_prompt = """You are a business analytics expert. Generate a clear, concise insight based on the analytics results.

GUIDELINES:
1. Directly answer the user's question first
2. Include specific numbers (formatted nicely: $1.2M, 45.3%, etc.)
3. Highlight the most important finding
4. If there's a trend, describe it
5. Keep response to 2-4 sentences
6. Be confident and actionable
7. Don't mention SQL, queries, or technical details
8. Don't say "based on the data" - just state the findings

EXAMPLES:
- "Total sales reached $2.4M, with the West region contributing 35% of revenue."
- "Sales have grown 23% month-over-month, with Technology category leading at $890K."
- "The top 3 categories account for 78% of profit, led by Office Supplies at $340K."
"""

        user_prompt = f"""Question: {question}

Intent:
- Metrics: {intent.metrics}
- Dimensions: {intent.dimensions}
- Time grain: {intent.time_grain}

Data Summary:
{data_context}

Provide a clear, insightful answer:"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,  # Slightly creative but mostly factual
                max_tokens=200
            )
            
            answer = response.choices[0].message.content.strip()
            logger.info(f"Generated response: {answer[:100]}...")
            return answer
            
        except Exception as e:
            logger.error(f"Response generation failed: {e}")
            # Fallback to basic response
            return self._generate_fallback_response(df, intent)
    
    def _generate_fallback_response(self, df: pd.DataFrame, intent: AnalyticsIntent) -> str:
        """Generate a basic response without LLM (fallback)."""
        if df.empty:
            return "No data found for this query."
        
        parts = []
        
        for metric in intent.metrics:
            if metric in df.columns:
                total = df[metric].sum()
                parts.append(f"Total {metric}: ${total:,.2f}")
        
        if intent.dimensions and len(df) > 0:
            parts.append(f"Across {len(df)} {intent.dimensions[0]} groups")
        
        return ". ".join(parts) + "."


# Singleton instance
_generator: Optional[ResponseGenerator] = None


def get_response_generator() -> ResponseGenerator:
    """Get or create the response generator singleton."""
    global _generator
    if _generator is None:
        _generator = ResponseGenerator()
    return _generator


def generate_response(
    question: str, 
    df: pd.DataFrame, 
    intent: AnalyticsIntent,
    sql_query: str = ""
) -> str:
    """Convenience function to generate response."""
    generator = get_response_generator()
    return generator.generate(question, df, intent, sql_query)

