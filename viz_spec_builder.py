"""
Visualization Specification Builder for Cognix V2.

Intelligently selects and configures the best visualization for the data.
Uses LLM for complex decisions, rule-based logic for simple cases.
"""

import os
import json
import hashlib
from typing import Optional
import pandas as pd
from openai import OpenAI
from loguru import logger

from config import settings
from schemas import AnalyticsIntent


class VizSpecBuilder:
    """
    Builds visualization specifications based on data and intent.
    
    Supports chart types:
    - metric: Single KPI card
    - bar: Categorical comparison
    - line: Time series / trends
    - pie: Part-to-whole relationships
    - horizontal_bar: Ranked items
    - scatter: Correlation (future)
    - heatmap: Two-dimensional comparison (future)
    """
    
    CHART_TYPES = ["metric", "bar", "line", "pie", "horizontal_bar", "scatter", "none"]
    
    def __init__(self):
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model
        logger.info("VizSpecBuilder initialized")
    
    def infer_chart_type(self, df: pd.DataFrame, intent: AnalyticsIntent) -> tuple[str, str]:
        """
        Infer the best chart type based on data and intent.
        
        Returns:
            Tuple of (chart_type, reason)
        """
        num_rows = len(df)
        has_time = intent.time_grain is not None or "time" in df.columns
        has_dimensions = len(intent.dimensions) > 0
        num_dimensions = len(intent.dimensions)
        
        # Rule-based inference (fast path)
        
        # Single value → Metric card
        if num_rows == 1 and not has_dimensions and not has_time:
            return "metric", "Single aggregate value displayed as a KPI metric card"
        
        # Time series without dimensions → Line chart
        if has_time and not has_dimensions:
            return "line", "Time series data best shown as a line chart to display trends"
        
        # Time series WITH dimensions → Multi-line chart
        if has_time and has_dimensions:
            if num_dimensions == 1:
                return "line", "Time series with categorical breakdown shown as multi-line chart"
            else:
                # Too complex for line, use bar
                return "bar", "Complex time series simplified to bar chart for clarity"
        
        # Categorical data with ranking intent → Horizontal bar
        if has_dimensions and intent.sort and intent.limit and intent.limit <= 10:
            return "horizontal_bar", "Ranked data displayed as horizontal bar for easy comparison"
        
        # Few categories → Bar chart
        if has_dimensions and num_rows <= 12:
            return "bar", "Categorical comparison displayed as bar chart"
        
        # Many categories → Horizontal bar (easier to read)
        if has_dimensions and num_rows > 12:
            return "horizontal_bar", "Many categories displayed as horizontal bar for readability"
        
        # Part-to-whole (small number of categories)
        if has_dimensions and num_rows <= 6 and num_dimensions == 1:
            # Check if this looks like a proportion
            if intent.metrics:
                total = df[intent.metrics[0]].sum() if intent.metrics[0] in df.columns else 0
                if total > 0:
                    return "pie", "Distribution across categories shown as pie chart"
        
        # Default fallback
        return "bar", "Default visualization for grouped data"
    
    def build_spec(self, df: pd.DataFrame, intent: AnalyticsIntent) -> dict:
        """
        Build a complete visualization specification.
        
        Args:
            df: Analytics results
            intent: The analytics intent
            
        Returns:
            Visualization specification dictionary
        """
        chart_type, reason = self.infer_chart_type(df, intent)
        
        spec = {
            "chart_type": chart_type,
            "reason": reason,
            "metrics": intent.metrics,
            "dimensions": intent.dimensions,
            "time_grain": intent.time_grain,
            "row_count": len(df),
            "config": self._get_chart_config(chart_type, df, intent)
        }
        
        logger.info(f"Built viz spec: {chart_type} - {reason}")
        return spec
    
    def _get_chart_config(self, chart_type: str, df: pd.DataFrame, intent: AnalyticsIntent) -> dict:
        """Get chart-specific configuration."""
        config = {}
        
        if chart_type == "metric":
            # Single metric card
            if intent.metrics and intent.metrics[0] in df.columns:
                config["value"] = float(df[intent.metrics[0]].iloc[0])
                config["label"] = intent.metrics[0].replace("_", " ").title()
                config["format"] = "currency" if intent.metrics[0] in ["sales", "profit"] else "number"
        
        elif chart_type == "line":
            config["x_axis"] = "time" if "time" in df.columns else intent.dimensions[0] if intent.dimensions else None
            config["y_axis"] = intent.metrics[0] if intent.metrics else None
            config["color_by"] = intent.dimensions[0] if intent.dimensions else None
        
        elif chart_type == "bar":
            config["x_axis"] = intent.dimensions[0] if intent.dimensions else None
            config["y_axis"] = intent.metrics[0] if intent.metrics else None
            config["orientation"] = "vertical"
        
        elif chart_type == "horizontal_bar":
            config["x_axis"] = intent.metrics[0] if intent.metrics else None
            config["y_axis"] = intent.dimensions[0] if intent.dimensions else None
            config["orientation"] = "horizontal"
        
        elif chart_type == "pie":
            config["values"] = intent.metrics[0] if intent.metrics else None
            config["names"] = intent.dimensions[0] if intent.dimensions else None
        
        return config
    
    def save_spec(self, spec: dict, base_dir: Optional[str] = None) -> str:
        """
        Save visualization specification to file.
        
        Args:
            spec: Visualization specification
            base_dir: Directory to save to (default: settings.viz_specs_dir)
            
        Returns:
            Path to saved specification file
        """
        base_dir = base_dir or str(settings.viz_specs_dir)
        os.makedirs(base_dir, exist_ok=True)
        
        # Generate unique ID based on spec content
        spec_id = hashlib.md5(
            json.dumps(spec, sort_keys=True, default=str).encode()
        ).hexdigest()
        
        path = os.path.join(base_dir, f"{spec_id}.json")
        
        with open(path, "w") as f:
            json.dump(spec, f, indent=2, default=str)
        
        logger.info(f"Saved viz spec: {path}")
        return path


# Singleton instance
_builder: Optional[VizSpecBuilder] = None


def get_viz_builder() -> VizSpecBuilder:
    """Get or create the viz spec builder singleton."""
    global _builder
    if _builder is None:
        _builder = VizSpecBuilder()
    return _builder


def infer_chart_type(intent: AnalyticsIntent) -> str:
    """Simple inference for backward compatibility."""
    if intent.dimensions and intent.time_grain:
        return "line"
    if intent.dimensions:
        return "bar"
    return "metric"

