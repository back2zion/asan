"""
Text2SQL Pydantic Models
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Any
from enum import Enum


class EnhancementType(str, Enum):
    MEDICAL = "medical"
    GENERAL = "general"


# Request Models
class Text2SQLRequest(BaseModel):
    """Basic Text2SQL generation request"""
    question: str = Field(..., description="Natural language question")
    include_explanation: bool = Field(default=True, description="Include SQL explanation")


class EnhancedText2SQLRequest(BaseModel):
    """Enhanced Text2SQL generation request with medical terminology support"""
    question: str = Field(..., description="Natural language question")
    enhancement_type: EnhancementType = Field(
        default=EnhancementType.MEDICAL,
        description="Type of enhancement to apply"
    )
    include_explanation: bool = Field(default=True, description="Include SQL explanation")
    auto_execute: bool = Field(default=False, description="Automatically execute the generated SQL")


# Response Models
class Text2SQLResponse(BaseModel):
    """Basic Text2SQL generation response"""
    sql: str = Field(..., description="Generated SQL query")
    explanation: Optional[str] = Field(None, description="SQL explanation")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score")


class ExecutionResult(BaseModel):
    """SQL execution result"""
    results: List[Any] = Field(default_factory=list, description="Query results")
    row_count: int = Field(default=0, description="Number of rows returned")
    columns: List[str] = Field(default_factory=list, description="Column names")
    execution_time_ms: float = Field(default=0.0, description="Execution time in milliseconds")
    natural_language_explanation: Optional[str] = Field(None, description="Natural language explanation of results")


class EnhancedText2SQLResponse(BaseModel):
    """Enhanced Text2SQL generation response"""
    original_question: str = Field(..., description="Original user question")
    enhanced_question: str = Field(..., description="Enhanced question with resolved terminology")
    enhancements_applied: List[str] = Field(default_factory=list, description="List of enhancements applied")
    enhancement_confidence: float = Field(..., ge=0.0, le=1.0, description="Enhancement confidence score")
    sql: str = Field(..., description="Generated SQL query")
    sql_explanation: Optional[str] = Field(None, description="SQL explanation")
    sql_confidence: float = Field(..., ge=0.0, le=1.0, description="SQL generation confidence score")
    execution_result: Optional[ExecutionResult] = Field(None, description="SQL execution result if auto_execute is True")


# Internal Models for Pipeline
class IntentResult(BaseModel):
    """Intent extraction result from LLM"""
    action: str = Field(..., description="Detected action (COUNT, SUM, LIST, etc.)")
    entities: List[str] = Field(default_factory=list, description="Detected entities")
    time_range: Optional[str] = Field(None, description="Detected time range")
    filters: List[str] = Field(default_factory=list, description="Detected filters")
    confidence: float = Field(..., ge=0.0, le=1.0)


class TermResolution(BaseModel):
    """BizMeta term resolution result"""
    original_term: str
    resolved_term: str
    term_type: str  # "icd_code", "synonym", "standard_term"
    confidence: float = Field(..., ge=0.0, le=1.0)


class SchemaContext(BaseModel):
    """ITMeta schema context"""
    tables: List[str] = Field(default_factory=list)
    columns: List[dict] = Field(default_factory=list)
    relationships: List[dict] = Field(default_factory=list)
    ddl_context: str = Field(default="")
