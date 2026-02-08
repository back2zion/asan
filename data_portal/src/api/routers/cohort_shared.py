"""
코호트 빌더 공통 모듈 — Pydantic 모델 + SQL 생성 엔진

SQL Injection 방지:
  - concept_code: 숫자만 허용 (regex)
  - date: ISO 형식만 (regex)
  - gender: Literal['M','F']
  - visit_concept_id: Literal[9201,9202,9203]
  - 모든 숫자값: Pydantic strict validation
"""
import re
from typing import List, Optional, Literal, Union
from pydantic import BaseModel, Field, field_validator


# ── Criterion 모델 ────────────────────────────────────────

class AgeRangeCriterion(BaseModel):
    type: Literal["age_range"]
    label: str = "연령"
    min_age: int = Field(0, ge=0, le=150)
    max_age: int = Field(150, ge=0, le=150)


class GenderCriterion(BaseModel):
    type: Literal["gender"]
    label: str = "성별"
    gender: Literal["M", "F"]


class ConditionCriterion(BaseModel):
    type: Literal["condition"]
    label: str = "진단"
    concept_code: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None

    @field_validator("concept_code")
    @classmethod
    def validate_concept_code(cls, v: str) -> str:
        if not re.fullmatch(r"\d{1,20}", v):
            raise ValueError("concept_code must be numeric")
        return v

    @field_validator("date_from", "date_to")
    @classmethod
    def validate_date(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError("date must be YYYY-MM-DD format")
        return v


class DrugCriterion(BaseModel):
    type: Literal["drug"]
    label: str = "약물"
    concept_code: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None

    @field_validator("concept_code")
    @classmethod
    def validate_concept_code(cls, v: str) -> str:
        if not re.fullmatch(r"\d{1,20}", v):
            raise ValueError("concept_code must be numeric")
        return v

    @field_validator("date_from", "date_to")
    @classmethod
    def validate_date(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError("date must be YYYY-MM-DD format")
        return v


class ProcedureCriterion(BaseModel):
    type: Literal["procedure"]
    label: str = "시술"
    concept_code: str
    date_from: Optional[str] = None
    date_to: Optional[str] = None

    @field_validator("concept_code")
    @classmethod
    def validate_concept_code(cls, v: str) -> str:
        if not re.fullmatch(r"\d{1,20}", v):
            raise ValueError("concept_code must be numeric")
        return v

    @field_validator("date_from", "date_to")
    @classmethod
    def validate_date(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError("date must be YYYY-MM-DD format")
        return v


class MeasurementCriterion(BaseModel):
    type: Literal["measurement"]
    label: str = "검사"
    concept_code: str
    value_min: Optional[float] = None
    value_max: Optional[float] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None

    @field_validator("concept_code")
    @classmethod
    def validate_concept_code(cls, v: str) -> str:
        if not re.fullmatch(r"\d{1,20}", v):
            raise ValueError("concept_code must be numeric")
        return v

    @field_validator("date_from", "date_to")
    @classmethod
    def validate_date(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError("date must be YYYY-MM-DD format")
        return v


class VisitTypeCriterion(BaseModel):
    type: Literal["visit_type"]
    label: str = "방문유형"
    visit_concept_id: Literal[9201, 9202, 9203]
    date_from: Optional[str] = None
    date_to: Optional[str] = None

    @field_validator("date_from", "date_to")
    @classmethod
    def validate_date(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError("date must be YYYY-MM-DD format")
        return v


Criterion = Union[
    AgeRangeCriterion,
    GenderCriterion,
    ConditionCriterion,
    DrugCriterion,
    ProcedureCriterion,
    MeasurementCriterion,
    VisitTypeCriterion,
]


# ── Flow Step / Request 모델 ─────────────────────────────

class FlowStep(BaseModel):
    step_type: Literal["inclusion", "exclusion"]
    criterion: Criterion = Field(..., discriminator="type")
    label: Optional[str] = None


class CountRequest(BaseModel):
    criterion: Criterion = Field(..., discriminator="type")


class ExecuteFlowRequest(BaseModel):
    steps: List[FlowStep]


class SetOperationRequest(BaseModel):
    group_a: List[Criterion]
    group_b: List[Criterion]
    operation: Literal["intersection", "union", "difference"]


class DrillDownRequest(BaseModel):
    criteria: List[Criterion]
    limit: int = Field(50, ge=1, le=500)
    offset: int = Field(0, ge=0)


class SummaryStatsRequest(BaseModel):
    criteria: List[Criterion]


# ── SQL 생성 엔진 ────────────────────────────────────────


def criterion_to_subquery(c: Criterion) -> str:
    """단일 criterion → SELECT DISTINCT person_id 서브쿼리 생성"""

    if c.type == "age_range":
        return (
            f"SELECT DISTINCT person_id FROM person "
            f"WHERE (2026 - year_of_birth) BETWEEN {c.min_age} AND {c.max_age}"
        )

    if c.type == "gender":
        return (
            f"SELECT DISTINCT person_id FROM person "
            f"WHERE gender_source_value = '{c.gender}'"
        )

    if c.type == "condition":
        sql = (
            f"SELECT DISTINCT person_id FROM condition_occurrence "
            f"WHERE condition_source_value = '{c.concept_code}'"
        )
        if c.date_from:
            sql += f" AND condition_start_date >= '{c.date_from}'"
        if c.date_to:
            sql += f" AND condition_start_date <= '{c.date_to}'"
        return sql

    if c.type == "drug":
        sql = (
            f"SELECT DISTINCT person_id FROM drug_exposure "
            f"WHERE drug_source_value = '{c.concept_code}'"
        )
        if c.date_from:
            sql += f" AND drug_exposure_start_date >= '{c.date_from}'"
        if c.date_to:
            sql += f" AND drug_exposure_start_date <= '{c.date_to}'"
        return sql

    if c.type == "procedure":
        sql = (
            f"SELECT DISTINCT person_id FROM procedure_occurrence "
            f"WHERE procedure_source_value = '{c.concept_code}'"
        )
        if c.date_from:
            sql += f" AND procedure_date >= '{c.date_from}'"
        if c.date_to:
            sql += f" AND procedure_date <= '{c.date_to}'"
        return sql

    if c.type == "measurement":
        sql = (
            f"SELECT DISTINCT person_id FROM measurement "
            f"WHERE measurement_source_value = '{c.concept_code}'"
        )
        if c.value_min is not None:
            sql += f" AND value_as_number >= {c.value_min}"
        if c.value_max is not None:
            sql += f" AND value_as_number <= {c.value_max}"
        if c.date_from:
            sql += f" AND measurement_date >= '{c.date_from}'"
        if c.date_to:
            sql += f" AND measurement_date <= '{c.date_to}'"
        return sql

    if c.type == "visit_type":
        sql = (
            f"SELECT DISTINCT person_id FROM visit_occurrence "
            f"WHERE visit_concept_id = {c.visit_concept_id}"
        )
        if c.date_from:
            sql += f" AND visit_start_date >= '{c.date_from}'"
        if c.date_to:
            sql += f" AND visit_start_date <= '{c.date_to}'"
        return sql

    raise ValueError(f"Unknown criterion type: {c.type}")


def criteria_to_person_sql(criteria: List[Criterion]) -> str:
    """여러 criteria를 intersect하여 person_id 집합 반환하는 SQL 생성"""
    if not criteria:
        return "SELECT DISTINCT person_id FROM person"

    if len(criteria) == 1:
        return criterion_to_subquery(criteria[0])

    # 첫 번째 criterion 기준, 나머지는 IN 절로 intersect
    base = criterion_to_subquery(criteria[0])
    for c in criteria[1:]:
        sub = criterion_to_subquery(c)
        base = f"SELECT person_id FROM ({base}) t WHERE person_id IN ({sub})"
    return base
