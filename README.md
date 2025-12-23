# Cognix Analytics Engine

An agentic analytics platform that allows users to ask business questions in plain language and automatically generates the corresponding analytics, artifacts, and visualizations.

## Features

- **Natural Language Interface**: Ask questions about your data in plain English
- **LLM-Powered Intent Parsing**: Automatically understands query intent using OpenAI
- **Agentic Workflow**: Uses LangGraph for intelligent workflow orchestration
- **Dynamic Visualization**: Presents results with appropriate charts and visualizations
- **Artifact Storage**: Persists analytics results for audit and reuse

## Tech Stack

### Backend
- Python 3.10+
- LangGraph (for workflow orchestration)
- LangChain (for LLM integration)
- OpenAI (for natural language processing)
- DuckDB (for analytics execution)

### Frontend
- Streamlit
- Altair (for data visualization)

## Prerequisites

- Python 3.10+
- OpenAI API key

## Installation

1. Clone the repository
   ```bash
   git clone <repository-url>
   cd COGNIX_V2
   ```

2. Create a virtual environment
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

4. Create a `.env` file with your API keys:
   ```
   OPENAI_API_KEY=your_openai_api_key
   ```

5. Prepare the data (if not already done)
   ```bash
   python data_loader.py
   ```

6. Start the application
   ```bash
   streamlit run app.py
   ```

7. Open http://localhost:8501 in your browser

## How It Works

1. User inputs a question about their data
2. The LLM parses the question to understand the intent
3. A SQL query is generated based on the intent
4. The query is executed against the data using DuckDB
5. An appropriate visualization type is selected based on the data
6. A natural language response is generated
7. The frontend renders the visualization and displays the results

## Project Structure

```
COGNIX_V2/
├── app.py                 # Streamlit application (frontend + backend)
├── workflow.py            # LangGraph workflow orchestration
├── intent_parser.py       # LLM-powered intent extraction
├── analytics_executor.py  # DuckDB query execution
├── response_generator.py  # Natural language responses
├── viz_spec_builder.py    # Visualization selection
├── artifact_store.py      # Result persistence
├── schemas.py             # Pydantic data models
├── config.py              # Configuration management
├── data_loader.py         # Data preparation script
├── requirements.txt       # Python dependencies
├── data/
│   └── raw.parquet        # Analytics data
├── artifacts/             # Saved query results
└── viz/                   # Visualization specs
```

## Example Queries

- "Show total sales"
- "Sales by region"
- "Monthly sales trend"
- "Monthly sales by region"
- "Top 5 categories by profit"
- "Profit by sub-category"
- "Quarterly sales by category"

## Configuration

Environment variables (`.env` file):

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENAI_API_KEY` | Your OpenAI API key | Required |
| `OPENAI_MODEL` | Model to use | gpt-4o-mini |
| `DEBUG` | Debug mode | false |
