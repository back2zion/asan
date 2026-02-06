"""
SQLite 체크포인터 (개발용)

로컬 개발 환경에서 대화 상태를 SQLite에 저장하는 체크포인터.
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Optional
from uuid import uuid4

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
    QueryContext,
    ReferenceInfo,
)


class SQLiteCheckpointer:
    """SQLite 기반 체크포인터.

    개발 및 테스트 환경에서 사용하는 경량 체크포인터입니다.
    프로덕션 환경에서는 PostgresCheckpointer를 사용하세요.
    """

    def __init__(self, db_path: str = "conversation_memory.db"):
        """SQLiteCheckpointer 초기화.

        Args:
            db_path: SQLite 데이터베이스 파일 경로
        """
        self.db_path = Path(db_path)
        self._init_db()

    def _init_db(self) -> None:
        """데이터베이스 스키마 초기화."""
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS threads (
                    thread_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    turn_count INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS checkpoints (
                    checkpoint_id TEXT PRIMARY KEY,
                    thread_id TEXT NOT NULL,
                    checkpoint_version INTEGER NOT NULL,
                    state_data TEXT NOT NULL,
                    messages_data TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (thread_id) REFERENCES threads(thread_id)
                );

                CREATE TABLE IF NOT EXISTS query_history (
                    history_id TEXT PRIMARY KEY,
                    thread_id TEXT NOT NULL,
                    turn_number INTEGER NOT NULL,
                    original_query TEXT,
                    generated_sql TEXT,
                    executed_sql TEXT,
                    result_count INTEGER,
                    result_summary TEXT,
                    conditions TEXT,
                    tables_used TEXT,
                    execution_time_ms REAL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (thread_id) REFERENCES threads(thread_id)
                );

                CREATE INDEX IF NOT EXISTS idx_checkpoints_thread
                    ON checkpoints(thread_id, checkpoint_version);

                CREATE INDEX IF NOT EXISTS idx_query_history_thread
                    ON query_history(thread_id, turn_number);
            """)

    @contextmanager
    def _get_connection(self) -> Iterator[sqlite3.Connection]:
        """데이터베이스 연결 컨텍스트 매니저."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def create_thread(self, user_id: str, thread_id: Optional[str] = None) -> str:
        """새 대화 스레드를 생성합니다.

        Args:
            user_id: 사용자 ID
            thread_id: 스레드 ID (미지정 시 자동 생성)

        Returns:
            생성된 스레드 ID
        """
        if thread_id is None:
            thread_id = str(uuid4())

        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO threads (thread_id, user_id) VALUES (?, ?)",
                (thread_id, user_id),
            )

        return thread_id

    def save_checkpoint(
        self,
        thread_id: str,
        state: ConversationState,
        checkpoint_version: Optional[int] = None,
    ) -> str:
        """대화 상태를 체크포인트로 저장합니다.

        Args:
            thread_id: 스레드 ID
            state: 저장할 대화 상태
            checkpoint_version: 체크포인트 버전 (미지정 시 자동 증가)

        Returns:
            체크포인트 ID
        """
        checkpoint_id = str(uuid4())

        # 상태를 JSON으로 직렬화
        state_data = self._serialize_state(state)
        messages_data = self._serialize_messages(state.get("messages", []))

        with self._get_connection() as conn:
            # 버전 자동 증가
            if checkpoint_version is None:
                cursor = conn.execute(
                    "SELECT MAX(checkpoint_version) FROM checkpoints WHERE thread_id = ?",
                    (thread_id,),
                )
                row = cursor.fetchone()
                checkpoint_version = (row[0] or 0) + 1

            conn.execute(
                """INSERT INTO checkpoints
                   (checkpoint_id, thread_id, checkpoint_version, state_data, messages_data)
                   VALUES (?, ?, ?, ?, ?)""",
                (checkpoint_id, thread_id, checkpoint_version, state_data, messages_data),
            )

            # 스레드 업데이트
            conn.execute(
                """UPDATE threads
                   SET turn_count = ?, updated_at = ?
                   WHERE thread_id = ?""",
                (state.get("turn_count", 0), datetime.now().isoformat(), thread_id),
            )

        return checkpoint_id

    def load_checkpoint(
        self,
        thread_id: str,
        checkpoint_version: Optional[int] = None,
    ) -> Optional[ConversationState]:
        """체크포인트에서 대화 상태를 로드합니다.

        Args:
            thread_id: 스레드 ID
            checkpoint_version: 로드할 버전 (미지정 시 최신)

        Returns:
            로드된 대화 상태 또는 None
        """
        with self._get_connection() as conn:
            if checkpoint_version is None:
                cursor = conn.execute(
                    """SELECT state_data, messages_data
                       FROM checkpoints
                       WHERE thread_id = ?
                       ORDER BY checkpoint_version DESC
                       LIMIT 1""",
                    (thread_id,),
                )
            else:
                cursor = conn.execute(
                    """SELECT state_data, messages_data
                       FROM checkpoints
                       WHERE thread_id = ? AND checkpoint_version = ?""",
                    (thread_id, checkpoint_version),
                )

            row = cursor.fetchone()

        if row is None:
            return None

        state = self._deserialize_state(row["state_data"])
        state["messages"] = self._deserialize_messages(row["messages_data"])

        return state

    def save_query_context(
        self,
        thread_id: str,
        context: QueryContext,
    ) -> str:
        """쿼리 컨텍스트를 히스토리에 저장합니다.

        Args:
            thread_id: 스레드 ID
            context: 쿼리 컨텍스트

        Returns:
            히스토리 ID
        """
        history_id = str(uuid4())

        with self._get_connection() as conn:
            conn.execute(
                """INSERT INTO query_history
                   (history_id, thread_id, turn_number, original_query, generated_sql,
                    executed_sql, result_count, result_summary, conditions, tables_used,
                    execution_time_ms)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    history_id,
                    thread_id,
                    context.turn_number,
                    context.original_query,
                    context.generated_sql,
                    context.executed_sql,
                    context.result_count,
                    json.dumps(context.result_summary) if context.result_summary else None,
                    json.dumps(context.conditions),
                    json.dumps(context.tables_used),
                    context.execution_time_ms,
                ),
            )

        return history_id

    def get_query_history(
        self,
        thread_id: str,
        limit: int = 50,
    ) -> list[QueryContext]:
        """스레드의 쿼리 히스토리를 조회합니다.

        Args:
            thread_id: 스레드 ID
            limit: 최대 조회 건수

        Returns:
            쿼리 컨텍스트 목록
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """SELECT * FROM query_history
                   WHERE thread_id = ?
                   ORDER BY turn_number DESC
                   LIMIT ?""",
                (thread_id, limit),
            )
            rows = cursor.fetchall()

        return [self._row_to_query_context(row) for row in rows]

    def list_threads(self, user_id: str, limit: int = 20) -> list[dict]:
        """사용자의 스레드 목록을 조회합니다.

        Args:
            user_id: 사용자 ID
            limit: 최대 조회 건수

        Returns:
            스레드 정보 목록
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """SELECT thread_id, user_id, turn_count, created_at, updated_at
                   FROM threads
                   WHERE user_id = ?
                   ORDER BY updated_at DESC
                   LIMIT ?""",
                (user_id, limit),
            )
            rows = cursor.fetchall()

        return [dict(row) for row in rows]

    def delete_thread(self, thread_id: str) -> bool:
        """스레드와 관련 데이터를 삭제합니다.

        Args:
            thread_id: 삭제할 스레드 ID

        Returns:
            삭제 성공 여부
        """
        with self._get_connection() as conn:
            conn.execute("DELETE FROM query_history WHERE thread_id = ?", (thread_id,))
            conn.execute("DELETE FROM checkpoints WHERE thread_id = ?", (thread_id,))
            cursor = conn.execute("DELETE FROM threads WHERE thread_id = ?", (thread_id,))
            return cursor.rowcount > 0

    def _serialize_state(self, state: ConversationState) -> str:
        """상태를 JSON 문자열로 직렬화합니다."""
        # 직렬화할 필드들 (messages 제외)
        serializable = {
            "thread_id": state.get("thread_id"),
            "user_id": state.get("user_id"),
            "current_query": state.get("current_query"),
            "has_references": state.get("has_references"),
            "base_sql": state.get("base_sql"),
            "modified_sql": state.get("modified_sql"),
            "added_conditions": state.get("added_conditions", []),
            "turn_count": state.get("turn_count", 0),
            "session_metadata": state.get("session_metadata", {}),
        }

        # QueryContext 직렬화
        last_ctx = state.get("last_query_context")
        if last_ctx:
            serializable["last_query_context"] = self._query_context_to_dict(last_ctx)
        else:
            serializable["last_query_context"] = None

        serializable["query_history"] = [
            self._query_context_to_dict(ctx)
            for ctx in state.get("query_history", [])
        ]

        # ReferenceInfo 직렬화
        serializable["detected_references"] = [
            {
                "ref_type": ref.ref_type.value,
                "pattern": ref.pattern,
                "position": ref.position,
                "original_text": ref.original_text,
                "resolved_value": ref.resolved_value,
            }
            for ref in state.get("detected_references", [])
        ]

        return json.dumps(serializable, ensure_ascii=False, default=str)

    def _deserialize_state(self, state_json: str) -> ConversationState:
        """JSON 문자열에서 상태를 역직렬화합니다."""
        from ai_services.conversation_memory.state.conversation_state import (
            ReferenceType,
        )

        data = json.loads(state_json)

        # QueryContext 역직렬화
        last_ctx = data.get("last_query_context")
        if last_ctx:
            data["last_query_context"] = self._dict_to_query_context(last_ctx)

        data["query_history"] = [
            self._dict_to_query_context(ctx)
            for ctx in data.get("query_history", [])
        ]

        # ReferenceInfo 역직렬화
        data["detected_references"] = [
            ReferenceInfo(
                ref_type=ReferenceType(ref["ref_type"]),
                pattern=ref["pattern"],
                position=ref["position"],
                original_text=ref["original_text"],
                resolved_value=ref.get("resolved_value"),
            )
            for ref in data.get("detected_references", [])
        ]

        return ConversationState(**data)

    def _serialize_messages(self, messages: list) -> str:
        """메시지 목록을 JSON으로 직렬화합니다."""
        serialized = []
        for msg in messages:
            serialized.append({
                "type": msg.__class__.__name__,
                "content": msg.content,
                "id": getattr(msg, 'id', None),
                "additional_kwargs": getattr(msg, 'additional_kwargs', {}),
            })
        return json.dumps(serialized, ensure_ascii=False)

    def _deserialize_messages(self, messages_json: str) -> list:
        """JSON에서 메시지 목록을 역직렬화합니다."""
        from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

        message_classes = {
            "HumanMessage": HumanMessage,
            "AIMessage": AIMessage,
            "SystemMessage": SystemMessage,
        }

        data = json.loads(messages_json)
        messages = []

        for msg_data in data:
            msg_class = message_classes.get(msg_data["type"], HumanMessage)
            msg = msg_class(
                content=msg_data["content"],
                id=msg_data.get("id"),
                additional_kwargs=msg_data.get("additional_kwargs", {}),
            )
            messages.append(msg)

        return messages

    def _query_context_to_dict(self, ctx: QueryContext) -> dict:
        """QueryContext를 딕셔너리로 변환합니다."""
        return {
            "turn_number": ctx.turn_number,
            "original_query": ctx.original_query,
            "enhanced_query": ctx.enhanced_query,
            "generated_sql": ctx.generated_sql,
            "executed_sql": ctx.executed_sql,
            "result_count": ctx.result_count,
            "result_summary": ctx.result_summary,
            "patient_ids": ctx.patient_ids,
            "conditions": ctx.conditions,
            "tables_used": ctx.tables_used,
            "timestamp": ctx.timestamp.isoformat() if ctx.timestamp else None,
            "execution_time_ms": ctx.execution_time_ms,
        }

    def _dict_to_query_context(self, data: dict) -> QueryContext:
        """딕셔너리를 QueryContext로 변환합니다."""
        timestamp = data.get("timestamp")
        if timestamp and isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        else:
            timestamp = datetime.now()

        return QueryContext(
            turn_number=data.get("turn_number", 0),
            original_query=data.get("original_query", ""),
            enhanced_query=data.get("enhanced_query"),
            generated_sql=data.get("generated_sql"),
            executed_sql=data.get("executed_sql"),
            result_count=data.get("result_count"),
            result_summary=data.get("result_summary"),
            patient_ids=data.get("patient_ids"),
            conditions=data.get("conditions", []),
            tables_used=data.get("tables_used", []),
            timestamp=timestamp,
            execution_time_ms=data.get("execution_time_ms"),
        )

    def _row_to_query_context(self, row: sqlite3.Row) -> QueryContext:
        """데이터베이스 행을 QueryContext로 변환합니다."""
        return QueryContext(
            turn_number=row["turn_number"],
            original_query=row["original_query"],
            generated_sql=row["generated_sql"],
            executed_sql=row["executed_sql"],
            result_count=row["result_count"],
            result_summary=json.loads(row["result_summary"]) if row["result_summary"] else None,
            conditions=json.loads(row["conditions"]) if row["conditions"] else [],
            tables_used=json.loads(row["tables_used"]) if row["tables_used"] else [],
            execution_time_ms=row["execution_time_ms"],
        )
