"""
LangGraph Workflow Orchestration for Cognix V2.

This module implements an agentic workflow using LangGraph,
providing the same level of sophistication as Person A's implementation.
"""

from typing import TypedDict, Optional, Annotated
import operator
import pandas as pd
from langgraph.graph import StateGraph, END
from loguru import logger

from schemas import AnalyticsIntent, QueryResponse
from intent_parser import extract_intent, get_intent_parser
from analytics_executor import execute_intent, get_executor
from response_generator import generate_response, get_response_generator
from viz_spec_builder import get_viz_builder
from artifact_store import save_artifact


class WorkflowState(TypedDict):
    """State that flows through the workflow."""
    # Input
    question: str
    
    # Intent parsing
    intent: Optional[AnalyticsIntent]
    intent_valid: bool
    intent_error: Optional[str]
    
    # Query execution
    sql_query: Optional[str]
    results: Optional[list[dict]]
    results_df: Optional[pd.DataFrame]
    execution_error: Optional[str]
    
    # Response generation
    answer: Optional[str]
    
    # Visualization
    visualization: Optional[str]
    visualization_reason: Optional[str]
    viz_spec: Optional[dict]
    
    # Artifact
    artifact_path: Optional[str]
    
    # Final output
    success: bool
    error: Optional[str]


class AnalyticsWorkflow:
    """
    Orchestrates the analytics pipeline using LangGraph.
    
    Workflow:
    1. parse_intent - Extract structured intent from natural language
    2. validate_intent - Validate intent against schema
    3. execute_query - Run analytics on data
    4. generate_response - Create natural language answer
    5. build_visualization - Determine best chart type
    6. save_artifact - Persist results
    
    Each node can trigger error handling if something goes wrong.
    """
    
    def __init__(self):
        self.intent_parser = get_intent_parser()
        self.executor = get_executor()
        self.response_generator = get_response_generator()
        self.viz_builder = get_viz_builder()
        
        # Build the workflow graph
        self.graph = self._build_graph()
        self.app = self.graph.compile()
        
        logger.info("AnalyticsWorkflow initialized")
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow."""
        workflow = StateGraph(WorkflowState)
        
        # Add nodes
        workflow.add_node("parse_intent", self._parse_intent)
        workflow.add_node("validate_intent", self._validate_intent)
        workflow.add_node("execute_query", self._execute_query)
        workflow.add_node("generate_response", self._generate_response)
        workflow.add_node("build_visualization", self._build_visualization)
        workflow.add_node("save_artifact", self._save_artifact)
        workflow.add_node("handle_error", self._handle_error)
        
        # Define edges
        workflow.set_entry_point("parse_intent")
        
        workflow.add_edge("parse_intent", "validate_intent")
        
        # Conditional: if intent is valid, execute; else error
        workflow.add_conditional_edges(
            "validate_intent",
            self._should_execute,
            {
                "execute": "execute_query",
                "error": "handle_error"
            }
        )
        
        # Conditional: if query succeeded, continue; else error
        workflow.add_conditional_edges(
            "execute_query",
            self._should_continue,
            {
                "continue": "generate_response",
                "error": "handle_error"
            }
        )
        
        workflow.add_edge("generate_response", "build_visualization")
        workflow.add_edge("build_visualization", "save_artifact")
        workflow.add_edge("save_artifact", END)
        workflow.add_edge("handle_error", END)
        
        return workflow
    
    # ===== Workflow Nodes =====
    
    def _parse_intent(self, state: WorkflowState) -> dict:
        """Parse the question into structured intent."""
        logger.info(f"[parse_intent] Processing: {state['question']}")
        
        try:
            intent = self.intent_parser.parse(state["question"])
            return {
                "intent": intent,
                "intent_error": None
            }
        except Exception as e:
            logger.error(f"[parse_intent] Error: {e}")
            return {
                "intent": None,
                "intent_error": str(e)
            }
    
    def _validate_intent(self, state: WorkflowState) -> dict:
        """Validate the extracted intent."""
        logger.info("[validate_intent] Validating intent")
        
        if state.get("intent_error") or not state.get("intent"):
            return {
                "intent_valid": False,
                "intent_error": state.get("intent_error", "No intent extracted")
            }
        
        is_valid, error = self.intent_parser.validate_intent(state["intent"])
        
        return {
            "intent_valid": is_valid,
            "intent_error": error
        }
    
    def _execute_query(self, state: WorkflowState) -> dict:
        """Execute the analytics query."""
        logger.info("[execute_query] Executing analytics")
        
        try:
            df, sql = self.executor.execute(state["intent"])
            
            return {
                "sql_query": sql,
                "results_df": df,
                "results": df.to_dict(orient="records"),
                "execution_error": None
            }
        except Exception as e:
            logger.error(f"[execute_query] Error: {e}")
            return {
                "sql_query": None,
                "results_df": None,
                "results": [],
                "execution_error": str(e)
            }
    
    def _generate_response(self, state: WorkflowState) -> dict:
        """Generate natural language response."""
        logger.info("[generate_response] Generating answer")
        
        try:
            answer = self.response_generator.generate(
                question=state["question"],
                df=state["results_df"],
                intent=state["intent"],
                sql_query=state.get("sql_query", "")
            )
            return {"answer": answer}
        except Exception as e:
            logger.error(f"[generate_response] Error: {e}")
            # Fallback answer
            return {"answer": f"Query returned {len(state.get('results', []))} results."}
    
    def _build_visualization(self, state: WorkflowState) -> dict:
        """Build visualization specification."""
        logger.info("[build_visualization] Determining chart type")
        
        try:
            spec = self.viz_builder.build_spec(state["results_df"], state["intent"])
            
            return {
                "visualization": spec["chart_type"],
                "visualization_reason": spec["reason"],
                "viz_spec": spec
            }
        except Exception as e:
            logger.error(f"[build_visualization] Error: {e}")
            return {
                "visualization": "bar",
                "visualization_reason": "Default visualization",
                "viz_spec": None
            }
    
    def _save_artifact(self, state: WorkflowState) -> dict:
        """Save results as artifact."""
        logger.info("[save_artifact] Saving artifact")
        
        try:
            if state.get("results_df") is not None and len(state["results_df"]) > 0:
                path = save_artifact(
                    state["results_df"],
                    metadata={
                        "question": state["question"],
                        "intent": state["intent"].model_dump() if state.get("intent") else None,
                        "sql_query": state.get("sql_query")
                    }
                )
                return {"artifact_path": path, "success": True, "error": None}
            return {"artifact_path": None, "success": True, "error": None}
        except Exception as e:
            logger.error(f"[save_artifact] Error: {e}")
            return {"artifact_path": None, "success": True, "error": None}  # Non-critical
    
    def _handle_error(self, state: WorkflowState) -> dict:
        """Handle errors in the workflow."""
        error = state.get("intent_error") or state.get("execution_error") or "Unknown error"
        logger.error(f"[handle_error] Workflow error: {error}")
        
        return {
            "success": False,
            "error": error,
            "answer": f"Sorry, I couldn't process your question: {error}"
        }
    
    # ===== Conditional Edge Functions =====
    
    def _should_execute(self, state: WorkflowState) -> str:
        """Decide whether to execute query or handle error."""
        if state.get("intent_valid"):
            return "execute"
        return "error"
    
    def _should_continue(self, state: WorkflowState) -> str:
        """Decide whether to continue or handle error."""
        if state.get("execution_error"):
            return "error"
        if state.get("results") is not None:
            return "continue"
        return "error"
    
    # ===== Public API =====
    
    def run(self, question: str) -> QueryResponse:
        """
        Run the complete analytics workflow.
        
        Args:
            question: Natural language question
            
        Returns:
            QueryResponse with all results
        """
        logger.info(f"Starting workflow for: {question}")
        
        # Initialize state
        initial_state: WorkflowState = {
            "question": question,
            "intent": None,
            "intent_valid": False,
            "intent_error": None,
            "sql_query": None,
            "results": None,
            "results_df": None,
            "execution_error": None,
            "answer": None,
            "visualization": None,
            "visualization_reason": None,
            "viz_spec": None,
            "artifact_path": None,
            "success": False,
            "error": None
        }
        
        # Run the workflow
        final_state = self.app.invoke(initial_state)
        
        # Convert to response
        return QueryResponse(
            question=question,
            intent=final_state.get("intent") or AnalyticsIntent(),
            answer=final_state.get("answer", "No answer generated"),
            visualization=final_state.get("visualization", "none"),
            visualization_reason=final_state.get("visualization_reason", ""),
            data=final_state.get("results", []),
            sql_query=final_state.get("sql_query", ""),
            artifact_path=final_state.get("artifact_path")
        )
    
    def get_graph(self):
        """Return the compiled graph for LangGraph Cloud deployment."""
        return self.app


# Singleton workflow instance
_workflow: Optional[AnalyticsWorkflow] = None


def get_workflow() -> AnalyticsWorkflow:
    """Get or create the workflow singleton."""
    global _workflow
    if _workflow is None:
        _workflow = AnalyticsWorkflow()
    return _workflow


def run_analytics(question: str) -> QueryResponse:
    """Convenience function to run analytics."""
    workflow = get_workflow()
    return workflow.run(question)


# For LangGraph Cloud deployment
graph = None

def get_graph_for_deployment():
    """Initialize and return graph for deployment."""
    global graph
    if graph is None:
        workflow = AnalyticsWorkflow()
        graph = workflow.get_graph()
    return graph

