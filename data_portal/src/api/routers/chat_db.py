"""
AI Assistant Chat — DB persistence, schema discovery, and query utilities

Extracted from chat_core.py to keep module sizes manageable.
This module has NO router; it provides helper functions only.
"""
import asyncio
import json
import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# ===== Chat DB persistence =====
import asyncpg as _asyncpg

_CHAT_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}
_chat_tables_ready = False


async def _ensure_chat_tables():
    global _chat_tables_ready
    if _chat_tables_ready:
        return
    conn = await _asyncpg.connect(**_CHAT_DB_CONFIG)
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_session (
                session_id VARCHAR(200) PRIMARY KEY,
                user_id VARCHAR(64) NOT NULL DEFAULT 'anonymous',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS chat_message (
                message_id VARCHAR(200) PRIMARY KEY,
                session_id VARCHAR(200) NOT NULL REFERENCES chat_session(session_id) ON DELETE CASCADE,
                role VARCHAR(16) NOT NULL,
                content TEXT NOT NULL,
                enhanced_content TEXT,
                tool_results JSONB DEFAULT '[]',
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_chat_msg_session ON chat_message(session_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_chat_session_user ON chat_session(user_id, updated_at DESC);
        """)
        _chat_tables_ready = True
    finally:
        await conn.close()


async def save_chat_message(
    session_id: str,
    user_id: str,
    message_id: str,
    role: str,
    content: str,
    enhanced_content: str = None,
    tool_results: list = None,
):
    """채팅 메시지를 DB에 영속화 (fire-and-forget)"""
    try:
        await _ensure_chat_tables()
        conn = await _asyncpg.connect(**_CHAT_DB_CONFIG)
        try:
            # Upsert session
            await conn.execute(
                """INSERT INTO chat_session (session_id, user_id)
                   VALUES ($1, $2)
                   ON CONFLICT (session_id) DO UPDATE SET updated_at = NOW()""",
                session_id, user_id,
            )
            # Insert message
            await conn.execute(
                """INSERT INTO chat_message (message_id, session_id, role, content, enhanced_content, tool_results)
                   VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                   ON CONFLICT (message_id) DO NOTHING""",
                message_id, session_id, role, content, enhanced_content,
                json.dumps(tool_results or []),
            )
        finally:
            await conn.close()
    except Exception as e:
        logger.warning(f"[ChatDB] save failed (non-blocking): {e}")


async def load_chat_sessions(user_id: str = "anonymous", limit: int = 20) -> list:
    """DB에서 사용자 채팅 세션 목록 조회"""
    try:
        await _ensure_chat_tables()
        conn = await _asyncpg.connect(**_CHAT_DB_CONFIG)
        try:
            rows = await conn.fetch(
                """SELECT s.session_id, s.user_id, s.created_at, s.updated_at,
                          (SELECT COUNT(*) FROM chat_message m WHERE m.session_id = s.session_id) AS message_count
                   FROM chat_session s
                   WHERE s.user_id = $1
                   ORDER BY s.updated_at DESC
                   LIMIT $2""",
                user_id, limit,
            )
            return [
                {
                    "id": r["session_id"],
                    "user_id": r["user_id"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                    "message_count": r["message_count"],
                }
                for r in rows
            ]
        finally:
            await conn.close()
    except Exception as e:
        logger.warning(f"[ChatDB] load sessions failed: {e}")
        return []


async def load_chat_messages(session_id: str, limit: int = 100) -> list:
    """DB에서 세션의 채팅 메시지 조회"""
    try:
        await _ensure_chat_tables()
        conn = await _asyncpg.connect(**_CHAT_DB_CONFIG)
        try:
            rows = await conn.fetch(
                """SELECT message_id, role, content, enhanced_content, tool_results, created_at
                   FROM chat_message
                   WHERE session_id = $1
                   ORDER BY created_at ASC
                   LIMIT $2""",
                session_id, limit,
            )
            result = []
            for r in rows:
                tr = r["tool_results"]
                if isinstance(tr, str):
                    try:
                        tr = json.loads(tr)
                    except (json.JSONDecodeError, TypeError):
                        tr = []
                result.append({
                    "id": r["message_id"],
                    "role": r["role"],
                    "content": r["content"],
                    "enhanced_content": r["enhanced_content"],
                    "tool_results": tr or [],
                    "timestamp": r["created_at"].isoformat() if r["created_at"] else None,
                })
            return result
        finally:
            await conn.close()
    except Exception as e:
        logger.warning(f"[ChatDB] load messages failed: {e}")
        return []


# ===== Schema discovery detection =====

SCHEMA_DISCOVERY_KEYWORDS = ["테이블", "스키마", "컬럼", "필드", "구조"]
SCHEMA_ACTION_KEYWORDS = ["찾아", "찾기", "보여", "조회", "알려", "뭐가", "뭐야", "있나", "있어", "어디", "어떤"]

_SCHEMA_KEYWORD_MAP = {
    "진단": "condition_occurrence",
    "질병": "condition_occurrence",
    "질환": "condition_occurrence",
    "환자": "person",
    "방문": "visit_occurrence",
    "입원": "visit_occurrence",
    "외래": "visit_occurrence",
    "응급": "visit_occurrence",
    "약물": "drug_exposure",
    "처방": "drug_exposure",
    "투약": "drug_exposure",
    "검사": "measurement",
    "혈액": "measurement",
    "관찰": "observation",
    "시술": "procedure_occurrence",
    "수술": "procedure_occurrence",
    "영상": "imaging_study",
    "이미지": "imaging_study",
    "x-ray": "imaging_study",
    "xray": "imaging_study",
    "흉부": "imaging_study",
}


def detect_and_handle_schema_query(message: str) -> Optional[str]:
    """Detect schema/table search queries and return table metadata directly"""
    msg_lower = message.lower().replace(" ", "")

    has_schema_kw = any(kw in message for kw in SCHEMA_DISCOVERY_KEYWORDS)
    has_action_kw = any(kw in message for kw in SCHEMA_ACTION_KEYWORDS)

    if not (has_schema_kw and has_action_kw):
        return None

    matched_tables = set()
    for kw, table_name in _SCHEMA_KEYWORD_MAP.items():
        if kw in message.lower():
            matched_tables.add(table_name)

    try:
        from ai_services.xiyan_sql.schema import SAMPLE_TABLES
    except ImportError:
        return None

    if not matched_tables:
        md = ["**OMOP CDM 데이터베이스 테이블 목록**\n"]
        md.append("| 테이블명 | 한글명 | 설명 | 도메인 | 컬럼 수 |")
        md.append("|---------|-------|------|--------|--------|")
        for t in SAMPLE_TABLES:
            md.append(f"| `{t['physical_name']}` | {t['business_name']} | {t['description']} | {t['domain']} | {len(t['columns'])}개 |")
        md.append(f"\n총 **{len(SAMPLE_TABLES)}개** 테이블이 있습니다. 특정 테이블에 대해 자세히 알고 싶으시면 '진단 테이블 구조 보여줘'처럼 질문해 주세요.")
        return "\n".join(md)

    md = []
    for table_name in matched_tables:
        table_meta = next((t for t in SAMPLE_TABLES if t["physical_name"] == table_name), None)
        if not table_meta:
            continue
        md.append(f"### {table_meta['business_name']} (`{table_meta['physical_name']}`)")
        md.append(f"- **설명**: {table_meta['description']}")
        md.append(f"- **도메인**: {table_meta['domain']}")
        md.append(f"- **컬럼 수**: {len(table_meta['columns'])}개\n")
        md.append("| 컬럼명 | 한글명 | 타입 | PK | 설명 |")
        md.append("|--------|-------|------|-----|------|")
        for col in table_meta["columns"]:
            pk = "PK" if col.get("is_pk") else ""
            md.append(f"| `{col['physical_name']}` | {col['business_name']} | {col['data_type']} | {pk} | {col['description']} |")
        md.append("")

    if not md:
        return None

    header = f"**검색 결과: {len(matched_tables)}개 테이블**\n"
    return header + "\n".join(md)


# ===== Imaging query detection + execution =====

IMAGING_KEYWORDS = ["영상", "이미지", "x-ray", "xray", "촬영", "흉부", "chest", "방사선", "엑스레이"]
OMOP_CONTAINER = os.getenv("OMOP_CONTAINER", "infra-omop-db-1")
OMOP_USER = os.getenv("OMOP_USER", "omopuser")
OMOP_DB = os.getenv("OMOP_DB", "omop_cdm")

# SQL forbidden keywords (read-only guarantee)
SQL_FORBIDDEN = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
                 "GRANT", "REVOKE", "EXECUTE", "EXEC", "MERGE", "REPLACE"]


async def detect_and_query_imaging(message: str) -> Optional[Dict[str, Any]]:
    """Detect imaging queries and directly query imaging_study table"""
    msg_lower = message.lower()
    if not any(kw in msg_lower for kw in IMAGING_KEYWORDS):
        return None

    finding_filter = ""
    finding_keywords = {
        "심비대": "Cardiomegaly", "cardiomegaly": "Cardiomegaly",
        "폐기종": "Emphysema", "emphysema": "Emphysema",
        "침윤": "Infiltration", "infiltration": "Infiltration",
        "흉수": "Effusion", "effusion": "Effusion",
        "무기폐": "Atelectasis", "atelectasis": "Atelectasis",
        "기흉": "Pneumothorax", "pneumothorax": "Pneumothorax",
        "종괴": "Mass", "mass": "Mass",
        "결절": "Nodule", "nodule": "Nodule",
        "경화": "Consolidation", "consolidation": "Consolidation",
        "부종": "Edema", "edema": "Edema",
        "섬유화": "Fibrosis", "fibrosis": "Fibrosis",
        "폐렴": "Pneumonia", "pneumonia": "Pneumonia",
    }
    for kor, eng in finding_keywords.items():
        if kor in msg_lower:
            finding_filter = f"WHERE i.finding_labels ILIKE '%{eng}%'"
            break

    count_sql = f"SELECT COUNT(*) FROM imaging_study i {finding_filter};"
    sql = f"""
    SELECT i.imaging_study_id, i.person_id, i.image_filename, i.finding_labels,
           i.view_position, i.patient_age, i.patient_gender, i.image_url
    FROM imaging_study i
    {finding_filter}
    ORDER BY i.imaging_study_id
    LIMIT 50;
    """

    try:
        count_cmd = [
            "docker", "exec", OMOP_CONTAINER,
            "psql", "-U", OMOP_USER, "-d", OMOP_DB,
            "-t", "-A", "-c", count_sql
        ]
        count_proc = await asyncio.create_subprocess_exec(
            *count_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        count_out, _ = await asyncio.wait_for(count_proc.communicate(), timeout=10.0)
        total_count = int(count_out.decode().strip()) if count_proc.returncode == 0 else 0

        cmd = [
            "docker", "exec", OMOP_CONTAINER,
            "psql", "-U", OMOP_USER, "-d", OMOP_DB,
            "-t", "-A", "-F", "\t", "-c", sql
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10.0)

        if process.returncode != 0:
            return None

        output = stdout.decode().strip()
        if not output:
            return None

        columns = ["imaging_study_id", "person_id", "image_filename",
                    "finding_labels", "view_position", "patient_age",
                    "patient_gender", "image_url"]
        rows = []
        for line in output.split("\n"):
            if line.strip():
                rows.append(line.split("\t"))

        fetched = len(rows)
        md_lines = [f"**흉부 X-ray 영상 조회 결과** — 전체 **{total_count:,}건** 중 {fetched}건 표시\n"]

        for row in rows[:4]:
            findings = row[3] if len(row) > 3 else ""
            view = row[4] if len(row) > 4 else ""
            age = row[5] if len(row) > 5 else ""
            gender = row[6] if len(row) > 6 else ""
            url = row[7] if len(row) > 7 else ""
            md_lines.append(f"**환자 {row[1]}** | {age}세 {gender} | {findings} ({view})")
            md_lines.append(f"![{findings}]({url})\n")

        if fetched > 4:
            md_lines.append(f"... 외 {fetched - 4}건이 더 있습니다.")

        if total_count > fetched:
            md_lines.append(f"\n> 성능을 위해 상위 {fetched}건만 표시됩니다. 조건을 추가하면 더 정확한 결과를 얻을 수 있습니다.")
            md_lines.append(f"> 예: \"심비대 흉부 영상\", \"폐렴 X-ray\"")

        # 실행된 SQL 표시
        display_sql = sql.strip()
        md_lines.append(f"\n```sql\n{display_sql}\n```")

        return {
            "message": "\n".join(md_lines),
            "tool_results": [{
                "columns": columns,
                "results": rows,
            }],
        }

    except Exception as e:
        print(f"[Imaging Query Error] {e}")
        return None
