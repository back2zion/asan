"""
DPR-005: 포털 운영 관리 공유 모듈
DB 테이블, Pydantic 모델, 시드 데이터
"""
import os
import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from fastapi import HTTPException
from pydantic import BaseModel, Field
import asyncpg

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}


async def get_connection():
    try:
        return await asyncpg.connect(**OMOP_DB_CONFIG)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")


# ── Pydantic Models ──

# Monitoring
class AccessLogEntry(BaseModel):
    user_id: str = Field(..., max_length=50)
    user_name: str = Field(default="", max_length=100)
    action: str = Field(..., max_length=50)  # login, page_view, data_download, query_execute, export
    resource: str = Field(default="", max_length=200)
    ip_address: str = Field(default="", max_length=45)
    user_agent: str = Field(default="", max_length=500)
    duration_ms: int = Field(default=0)
    details: Dict[str, Any] = Field(default_factory=dict)

class AlertCreate(BaseModel):
    severity: str = Field(..., pattern=r"^(info|warning|error|critical)$")
    source: str = Field(..., max_length=100)
    message: str = Field(..., max_length=1000)
    details: Dict[str, Any] = Field(default_factory=dict)

class AlertUpdate(BaseModel):
    status: str = Field(..., pattern=r"^(active|acknowledged|resolved)$")
    resolved_by: Optional[str] = Field(None, max_length=50)

# Announcements
class AnnouncementCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    content: str = Field(..., min_length=1, max_length=5000)
    ann_type: str = Field(default="notice", pattern=r"^(notice|maintenance|banner|popup)$")
    priority: str = Field(default="normal", pattern=r"^(low|normal|high|urgent)$")
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    is_pinned: bool = False
    creator: str = Field(default="admin")

class AnnouncementUpdate(BaseModel):
    title: Optional[str] = Field(None, max_length=200)
    content: Optional[str] = Field(None, max_length=5000)
    ann_type: Optional[str] = Field(None, pattern=r"^(notice|maintenance|banner|popup)$")
    priority: Optional[str] = Field(None, pattern=r"^(low|normal|high|urgent)$")
    status: Optional[str] = Field(None, pattern=r"^(draft|published|archived)$")
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    is_pinned: Optional[bool] = None

# Menu Management
class MenuItemCreate(BaseModel):
    menu_key: str = Field(..., max_length=50)
    label: str = Field(..., max_length=100)
    icon: str = Field(default="", max_length=50)
    path: str = Field(default="", max_length=200)
    parent_key: Optional[str] = Field(None, max_length=50)
    sort_order: int = Field(default=0)
    visible: bool = True
    roles: List[str] = Field(default_factory=lambda: ["admin", "researcher", "staff", "developer"])

class MenuItemUpdate(BaseModel):
    label: Optional[str] = Field(None, max_length=100)
    icon: Optional[str] = Field(None, max_length=50)
    path: Optional[str] = Field(None, max_length=200)
    sort_order: Optional[int] = None
    visible: Optional[bool] = None
    roles: Optional[List[str]] = None

# Data Quality
class QualityRuleCreate(BaseModel):
    table_name: str = Field(..., max_length=100)
    column_name: Optional[str] = Field(None, max_length=100)
    rule_type: str = Field(..., pattern=r"^(completeness|uniqueness|validity|freshness|consistency)$")
    rule_expr: str = Field(..., max_length=500)
    threshold: float = Field(default=95.0, ge=0, le=100)
    description: Optional[str] = Field(None, max_length=500)


# ── Table Setup ──

async def _ensure_portal_ops_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS po_access_log (
            log_id SERIAL PRIMARY KEY,
            user_id VARCHAR(50) NOT NULL,
            user_name VARCHAR(100),
            action VARCHAR(50) NOT NULL,
            resource VARCHAR(200),
            ip_address VARCHAR(45),
            user_agent VARCHAR(500),
            duration_ms INTEGER DEFAULT 0,
            details JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_po_access_log_user ON po_access_log(user_id);
        CREATE INDEX IF NOT EXISTS idx_po_access_log_time ON po_access_log(created_at);

        CREATE TABLE IF NOT EXISTS po_alert (
            alert_id SERIAL PRIMARY KEY,
            severity VARCHAR(20) NOT NULL,
            source VARCHAR(100) NOT NULL,
            message TEXT NOT NULL,
            details JSONB DEFAULT '{}',
            status VARCHAR(20) DEFAULT 'active',
            resolved_by VARCHAR(50),
            resolved_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS po_announcement (
            ann_id SERIAL PRIMARY KEY,
            title VARCHAR(200) NOT NULL,
            content TEXT NOT NULL,
            ann_type VARCHAR(20) DEFAULT 'notice',
            priority VARCHAR(20) DEFAULT 'normal',
            status VARCHAR(20) DEFAULT 'draft',
            start_date TIMESTAMPTZ,
            end_date TIMESTAMPTZ,
            is_pinned BOOLEAN DEFAULT FALSE,
            view_count INTEGER DEFAULT 0,
            creator VARCHAR(50) DEFAULT 'admin',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS po_menu_item (
            item_id SERIAL PRIMARY KEY,
            menu_key VARCHAR(50) UNIQUE NOT NULL,
            label VARCHAR(100) NOT NULL,
            icon VARCHAR(50),
            path VARCHAR(200),
            parent_key VARCHAR(50),
            sort_order INTEGER DEFAULT 0,
            visible BOOLEAN DEFAULT TRUE,
            roles JSONB DEFAULT '["admin","researcher","staff","developer"]',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS po_quality_rule (
            rule_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            column_name VARCHAR(100),
            rule_type VARCHAR(30) NOT NULL,
            rule_expr VARCHAR(500) NOT NULL,
            threshold DOUBLE PRECISION DEFAULT 95.0,
            description TEXT,
            last_score DOUBLE PRECISION,
            last_checked TIMESTAMPTZ,
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS po_quality_history (
            history_id SERIAL PRIMARY KEY,
            rule_id INTEGER REFERENCES po_quality_rule(rule_id) ON DELETE CASCADE,
            score DOUBLE PRECISION NOT NULL,
            total_rows BIGINT DEFAULT 0,
            failed_rows BIGINT DEFAULT 0,
            checked_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS po_system_setting (
            setting_key VARCHAR(100) PRIMARY KEY,
            setting_value JSONB NOT NULL,
            description VARCHAR(500),
            updated_by VARCHAR(50) DEFAULT 'admin',
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)


# ── Seed Data ──

_PO_SEEDED = False

async def _ensure_portal_ops_seed(conn):
    global _PO_SEEDED
    if _PO_SEEDED:
        return
    cnt = await conn.fetchval("SELECT COUNT(*) FROM po_announcement")
    if cnt > 0:
        _PO_SEEDED = True
        return

    # Announcements
    anns = [
        ("시스템 정기 점검 안내", "2월 15일 02:00~06:00 시스템 정기 점검이 진행됩니다.", "maintenance", "high", "published", True),
        ("OMOP CDM 데이터 적재 완료", "Synthea 합성 데이터 92,260,027건 적재가 완료되었습니다. 분석에 활용 가능합니다.", "notice", "normal", "published", False),
        ("BI 셀프서비스 기능 오픈", "No-code 차트 빌더, SQL Editor, 대시보드 기능이 추가되었습니다.", "notice", "normal", "published", True),
        ("데이터 반출 승인 절차 변경", "IRB 승인 없는 데이터 반출은 불가합니다. 반드시 승인 후 다운로드하세요.", "notice", "urgent", "published", False),
        ("신규 사용자 교육 안내", "데이터 포털 사용법 교육이 매주 수요일 14:00에 진행됩니다.", "notice", "low", "draft", False),
    ]
    for a in anns:
        await conn.execute(
            "INSERT INTO po_announcement (title, content, ann_type, priority, status, is_pinned) VALUES ($1,$2,$3,$4,$5,$6)",
            *a,
        )

    # Access logs (demo)
    actions = [
        ("admin", "관리자", "login", "/", "192.168.1.10"),
        ("researcher01", "김연구", "page_view", "/bi", "192.168.1.20"),
        ("researcher01", "김연구", "query_execute", "/bi/sql-editor", "192.168.1.20"),
        ("staff01", "박행정", "data_download", "/data-catalog/export", "192.168.1.30"),
        ("admin", "관리자", "page_view", "/governance", "192.168.1.10"),
        ("researcher02", "이분석", "login", "/", "192.168.1.40"),
        ("researcher02", "이분석", "query_execute", "/text2sql", "192.168.1.40"),
        ("developer01", "최개발", "login", "/", "192.168.1.50"),
        ("developer01", "최개발", "page_view", "/etl", "192.168.1.50"),
        ("staff02", "정간호", "page_view", "/cohort", "192.168.1.60"),
    ]
    for a in actions:
        await conn.execute(
            "INSERT INTO po_access_log (user_id, user_name, action, resource, ip_address, duration_ms) "
            "VALUES ($1,$2,$3,$4,$5, (random()*5000)::int)",
            *a,
        )

    # Alerts
    alerts = [
        ("warning", "ETL", "drug_exposure ETL 배치 실행 시간 초과 (>30분)"),
        ("info", "System", "OMOP DB 커넥션 풀 사용률 75%"),
        ("error", "SSH Tunnel", "GPU 서버 SSH 터널 연결 끊김 (28888 포트)"),
        ("info", "Storage", "디스크 사용률 65% — 정상 범위"),
        ("critical", "DB", "measurement 테이블 풀스캔 감지 (>2분 소요)"),
    ]
    for a in alerts:
        await conn.execute(
            "INSERT INTO po_alert (severity, source, message) VALUES ($1,$2,$3)", *a,
        )

    # Menu items
    menus = [
        ("dashboard", "대시보드", "DashboardOutlined", "/", None, 0),
        ("ai-assistant", "AI 어시스턴트", "RobotOutlined", "/ai-assistant", None, 1),
        ("data-catalog", "데이터 카탈로그", "BookOutlined", "/data-catalog", None, 2),
        ("bi", "셀프서비스 BI", "BarChartOutlined", "/bi", None, 3),
        ("text2sql", "Text-to-SQL", "CodeOutlined", "/text2sql", None, 4),
        ("cohort", "코호트 빌더", "TeamOutlined", "/cohort", None, 5),
        ("governance", "데이터 거버넌스", "SafetyOutlined", "/governance", None, 6),
        ("etl", "ETL 관리", "SwapOutlined", "/etl", None, 7),
        ("portal-ops", "포털 운영 관리", "SettingOutlined", "/portal-ops", None, 8),
    ]
    for m in menus:
        await conn.execute(
            "INSERT INTO po_menu_item (menu_key, label, icon, path, parent_key, sort_order) "
            "VALUES ($1,$2,$3,$4,$5,$6)",
            *m,
        )

    # Quality rules
    rules = [
        ("person", "person_id", "uniqueness", "COUNT(DISTINCT person_id) = COUNT(person_id)", 100.0, "환자 ID 유일성"),
        ("person", "year_of_birth", "completeness", "COUNT(year_of_birth) * 100.0 / COUNT(*)", 99.0, "생년 결측률"),
        ("person", "gender_source_value", "validity", "COUNT(CASE WHEN gender_source_value IN ('M','F') THEN 1 END) * 100.0 / COUNT(*)", 100.0, "성별 유효값"),
        ("visit_occurrence", "visit_start_date", "freshness", "EXTRACT(DAY FROM NOW() - MAX(visit_start_date))", 365.0, "방문 데이터 최신성 (일)"),
        ("condition_occurrence", "condition_source_value", "completeness", "COUNT(condition_source_value) * 100.0 / COUNT(*)", 95.0, "진단코드 결측률"),
        ("measurement", "value_as_number", "completeness", "COUNT(value_as_number) * 100.0 / COUNT(*)", 80.0, "검사 수치 결측률"),
        ("drug_exposure", "drug_source_value", "completeness", "COUNT(drug_source_value) * 100.0 / COUNT(*)", 98.0, "약물코드 결측률"),
    ]
    for r in rules:
        await conn.execute(
            "INSERT INTO po_quality_rule (table_name, column_name, rule_type, rule_expr, threshold, description) "
            "VALUES ($1,$2,$3,$4,$5,$6)",
            *r,
        )

    # System settings
    settings = [
        ("portal.name", '"서울아산병원 통합 데이터 플랫폼"', "포털 이름"),
        ("portal.logo_url", '"/assets/logo.png"', "포털 로고 URL"),
        ("session.timeout_minutes", '30', "세션 타임아웃 (분)"),
        ("export.require_approval", 'true', "데이터 반출 승인 필수 여부"),
        ("export.max_rows", '100000', "데이터 반출 최대 행 수"),
        ("deident.auto_mask", 'true', "자동 비식별화 활성화"),
        ("notification.email_enabled", 'false', "이메일 알림 활성화"),
        ("quality.auto_check_interval", '"daily"', "품질 자동 검사 주기"),
    ]
    for s in settings:
        await conn.execute(
            "INSERT INTO po_system_setting (setting_key, setting_value, description) VALUES ($1,$2::jsonb,$3)", *s,
        )

    _PO_SEEDED = True


async def portal_ops_init(conn):
    await _ensure_portal_ops_tables(conn)
    await _ensure_portal_ops_seed(conn)
