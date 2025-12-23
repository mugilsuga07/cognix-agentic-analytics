"""
Streamlit Frontend for Cognix Analytics Engine.

Interactive analytics dashboard with natural language query support.
Directly integrates with Python backend - no separate API server needed.
"""

import os
import streamlit as st
import pandas as pd
import altair as alt
from typing import Optional
import openai

from workflow import run_analytics
from config import settings

# Configure OpenAI API key (supports both local .env and Streamlit Cloud secrets)
try:
    openai.api_key = st.secrets["OPENAI_API_KEY"]
except Exception:
    openai.api_key = os.getenv("OPENAI_API_KEY")

# Page configuration
st.set_page_config(
    page_title="Cognix Analytics Engine",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for clean white background with black text
st.markdown("""
<style>
    /* Plain white background */
    .stApp {
        background-color: #ffffff;
    }
    
    /* Main header - black text */
    .main-header {
        font-size: 2.8rem;
        font-weight: 700;
        color: #000000;
        margin-bottom: 0.5rem;
        text-align: center;
    }
    
    /* Caption - black text, single line */
    .main-caption {
        font-size: 1rem;
        color: #000000;
        text-align: center;
        max-width: 100%;
        margin: 0 auto 2rem auto;
        line-height: 1.4;
        white-space: nowrap;
    }
    
    /* All text elements - black */
    h1, h2, h3, h4, h5, h6, p, span, label, .stMarkdown {
        color: #000000 !important;
    }
    
    /* Metric card styling - black theme */
    .metric-card {
        background: #000000;
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        text-align: center;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #ffffff;
    }
    .metric-label {
        font-size: 1rem;
        color: #ffffff;
        opacity: 0.9;
    }
    
    /* Insight box styling - white with black border */
    .insight-box {
        background: #ffffff;
        padding: 1.5rem;
        border-radius: 0.5rem;
        border-left: 4px solid #000000;
        color: #000000;
    }
    
    /* Button styling - black */
    .stButton > button {
        background-color: #000000 !important;
        color: #ffffff !important;
        border: none !important;
    }
    .stButton > button:hover {
        background-color: #333333 !important;
        color: #ffffff !important;
    }
    
    /* Primary button - black */
    .stButton > button[kind="primary"] {
        background-color: #000000 !important;
        color: #ffffff !important;
    }
    
    /* Input field styling */
    .stTextInput > div > div > input {
        font-size: 1.1rem;
        background-color: #ffffff;
        border: 1px solid #000000;
        color: #000000;
    }
    
    /* Sidebar styling - white background */
    [data-testid="stSidebar"] {
        background-color: #ffffff;
    }
    
    /* Sidebar text - black */
    [data-testid="stSidebar"] * {
        color: #000000 !important;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab"] {
        color: #000000 !important;
    }
    
    /* Download button - black */
    .stDownloadButton > button {
        background-color: #000000 !important;
        color: #ffffff !important;
    }
    
    /* JSON viewer in Intent tab - dark background with white text */
    [data-testid="stJson"] {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
        border-radius: 0.5rem;
        padding: 1rem;
    }
    [data-testid="stJson"] * {
        color: #ffffff !important;
    }
    
    /* Code block in SQL tab - dark background with white text */
    [data-testid="stCode"] {
        background-color: #1a1a1a !important;
    }
    [data-testid="stCode"] * {
        color: #ffffff !important;
        background-color: #1a1a1a !important;
    }
    .stCode, .stCode pre, .stCode code {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
    }
    .stCodeBlock, .stCodeBlock pre, .stCodeBlock code {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
    }
    .stCodeBlock pre code span {
        color: #ffffff !important;
    }
    pre {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
    }
    pre * {
        color: #ffffff !important;
    }
    code {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
    }
    .hljs, .hljs * {
        color: #ffffff !important;
        background-color: #1a1a1a !important;
    }
    
    /* Force all button text to be white */
    .stButton > button {
        color: #ffffff !important;
    }
    .stButton > button * {
        color: #ffffff !important;
    }
    .stButton > button p {
        color: #ffffff !important;
    }
    .stButton > button span {
        color: #ffffff !important;
    }
    button[kind="primary"] {
        color: #ffffff !important;
    }
    button[kind="primary"] p {
        color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)

# Check if OpenAI API key is configured
if not openai.api_key:
    st.error("OpenAI API key not configured. Please add OPENAI_API_KEY to Streamlit secrets or .env file.")
    st.stop()


def execute_query(question: str) -> Optional[dict]:
    """Execute analytics query directly using Python workflow."""
    try:
        # Check if OpenAI key is configured
        if not settings.validate_openai_key():
            st.error("OpenAI API key not configured. Please set OPENAI_API_KEY in .env file.")
            return None
        
        # Run the analytics workflow directly
        response = run_analytics(question)
        
        # Convert response to dict format
        return {
            "question": response.question,
            "intent": response.intent.model_dump() if response.intent else {},
            "answer": response.answer,
            "visualization": response.visualization,
            "visualization_reason": response.visualization_reason,
            "data": response.data,
            "sql_query": response.sql_query,
            "artifact_path": response.artifact_path
        }
        
    except Exception as e:
        st.error(f"Error processing query: {str(e)}")
        return None


def render_metric_card(value: float, label: str, format_type: str = "currency"):
    """Render a single metric as a styled card."""
    if format_type == "currency":
        formatted = f"${value:,.2f}"
    elif format_type == "percent":
        formatted = f"{value:.1f}%"
    else:
        formatted = f"{value:,.0f}"
    
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{formatted}</div>
        <div class="metric-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)


def render_chart(data: list, viz_type: str, intent: dict):
    """Render the appropriate chart based on visualization type."""
    if not data:
        st.warning("No data to visualize")
        return
    
    df = pd.DataFrame(data)
    
    metrics = intent.get("metrics", ["sales"])
    dimensions = intent.get("dimensions", [])
    
    metric = metrics[0] if metrics else "sales"
    
    # Handle different visualization types
    if viz_type == "metric":
        # Single value metric card
        if metric in df.columns:
            value = df[metric].iloc[0] if len(df) > 0 else 0
            render_metric_card(value, metric.replace("_", " ").title())
    
    elif viz_type == "line":
        # Line chart for time series
        x_field = "time" if "time" in df.columns else dimensions[0] if dimensions else None
        if x_field and metric in df.columns:
            if dimensions and dimensions[0] in df.columns:
                # Multi-line chart
                chart = alt.Chart(df).mark_line(point=True).encode(
                    x=alt.X(f"{x_field}:T" if x_field == "time" else f"{x_field}:N"),
                    y=alt.Y(f"{metric}:Q", title=metric.title()),
                    color=alt.Color(f"{dimensions[0]}:N"),
                    tooltip=[x_field, dimensions[0], metric]
                ).properties(height=400).interactive()
            else:
                # Single line chart
                chart = alt.Chart(df).mark_line(point=True).encode(
                    x=alt.X(f"{x_field}:T" if x_field == "time" else f"{x_field}:N"),
                    y=alt.Y(f"{metric}:Q", title=metric.title()),
                    tooltip=[x_field, metric]
                ).properties(height=400).interactive()
            st.altair_chart(chart, use_container_width=True)
    
    elif viz_type == "bar":
        # Vertical bar chart
        dim = dimensions[0] if dimensions else None
        if dim and dim in df.columns and metric in df.columns:
            chart = alt.Chart(df).mark_bar().encode(
                x=alt.X(f"{dim}:N", sort="-y"),
                y=alt.Y(f"{metric}:Q", title=metric.title()),
                color=alt.Color(f"{dim}:N", legend=None),
                tooltip=[dim, metric]
            ).properties(height=400).interactive()
            st.altair_chart(chart, use_container_width=True)
    
    elif viz_type == "horizontal_bar":
        # Horizontal bar chart
        dim = dimensions[0] if dimensions else None
        if dim and dim in df.columns and metric in df.columns:
            chart = alt.Chart(df).mark_bar().encode(
                y=alt.Y(f"{dim}:N", sort="-x"),
                x=alt.X(f"{metric}:Q", title=metric.title()),
                color=alt.Color(f"{dim}:N", legend=None),
                tooltip=[dim, metric]
            ).properties(height=max(300, len(df) * 30)).interactive()
            st.altair_chart(chart, use_container_width=True)
    
    elif viz_type == "pie":
        # Pie chart
        dim = dimensions[0] if dimensions else None
        if dim and dim in df.columns and metric in df.columns:
            chart = alt.Chart(df).mark_arc().encode(
                theta=alt.Theta(f"{metric}:Q"),
                color=alt.Color(f"{dim}:N"),
                tooltip=[dim, metric]
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)
    
    else:
        # Default: show as bar chart
        if len(df.columns) >= 2:
            st.bar_chart(df.set_index(df.columns[0]))


def main():
    """Main Streamlit app."""
    
    # Header
    st.markdown('<h1 class="main-header">Cognix Analytics Engine</h1>', unsafe_allow_html=True)
    st.markdown('<p class="main-caption">Cognix is an agentic analytics platform that allows users to ask business questions in plain language and automatically generates the corresponding analytics, artifacts, and visualizations.</p>', unsafe_allow_html=True)
    
    # Sidebar with examples
    with st.sidebar:
        st.header("Example Questions")
        
        example_questions = [
            "Show total sales",
            "Sales by region",
            "Monthly sales trend",
            "Monthly sales by region",
            "Top 5 categories by profit",
            "Profit by sub-category",
            "Quarterly sales by category"
        ]
        
        st.markdown("Click to try:")
        for q in example_questions:
            if st.button(q, key=f"example_{q}", use_container_width=True):
                st.session_state.question = q
        
        st.divider()
        
        st.header("About")
        st.markdown("""
        **Cognix** uses:
        - OpenAI for intent parsing
        - LangGraph for workflow
        - DuckDB for analytics
        - Altair for visualization
        """)
        
        st.divider()
        
        # Show schema
        st.header("Available Fields")
        st.markdown("""
        **Metrics:** sales, profit, quantity
        
        **Dimensions:** region, category, sub_category
        
        **Time Grains:** day, week, month, quarter, year
        """)
    
    # Main input area
    col1, col2 = st.columns([4, 1])
    
    with col1:
        question = st.text_input(
            "Ask your analytics question",
            value=st.session_state.get("question", ""),
            placeholder="e.g., Show monthly sales by region",
            label_visibility="collapsed"
        )
    
    with col2:
        run_button = st.button("Analyze", type="primary", use_container_width=True)
    
    # Execute query
    if run_button and question.strip():
        with st.spinner("Processing your question..."):
            result = execute_query(question)
        
        if result:
            # Display insight
            st.markdown("---")
            st.markdown("### Insight")
            st.markdown(f'<div class="insight-box">{result["answer"]}</div>', unsafe_allow_html=True)
            
            # Create tabs for different views
            tab1, tab2, tab3, tab4 = st.tabs(["Visualization", "Data", "Intent", "SQL"])
            
            with tab1:
                st.markdown(f"**Chart Type:** {result['visualization'].replace('_', ' ').title()}")
                st.caption(result.get("visualization_reason", ""))
                render_chart(
                    result["data"],
                    result["visualization"],
                    result["intent"]
                )
            
            with tab2:
                df = pd.DataFrame(result["data"])
                st.dataframe(df, use_container_width=True, height=400)
                
                # Download button
                csv = df.to_csv(index=False)
                st.download_button(
                    "Download CSV",
                    csv,
                    "analytics_result.csv",
                    "text/csv",
                    use_container_width=True
                )
            
            with tab3:
                st.json(result["intent"])
            
            with tab4:
                st.code(result.get("sql_query", "No SQL generated"), language="sql")
            
            # Show artifact path if saved
            if result.get("artifact_path"):
                st.caption(f"Saved to: {result['artifact_path']}")
    
    elif run_button:
        st.warning("Please enter a question")


if __name__ == "__main__":
    main()
