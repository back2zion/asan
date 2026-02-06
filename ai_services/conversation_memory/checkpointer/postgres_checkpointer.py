"""
PostgreSQL 체크포인터 (프로덕션)

프로덕션 환경에서 대화 상태를 PostgreSQL에 영속화하는 체크포인터.
Redis 캐시와 함께 사용하여 빠른 조회와 영구 저장을 모두 지원합니다.
"""

import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Optional
from uuid import uuid4

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
    QueryContext,
    ReferenceInfo,
    ReferenceType,
)


class PostgresCheckpointer:
    """PostgreSQL + Redis 기반 체크포인터.

    프로덕션 환경에서 사용하는 고성능 체크포인터입니다.
    - PostgreSQL: 영구 저장
    - Redis: 캐시 (빠른 조회, 30분 TTL)

    Attributes:
        pool: asyncpg 연결 풀
        redis: Redis 클라이언트
        session_ttl: 세션 TTL (기본 30분)
        max_turns: 최대 턴 수 (기본 50)
    """

    def __init__(
        self,
        postgres_dsn: str,
        redis_url: Optional[str] = None,
        session_ttl_minutes: int = 30,
        max_turns: int = 50,
    ):
        """PostgresCheckpointer 초기화.

        Args:
            postgres_dsn: PostgreSQL 연결 문자열
            redis_url: Redis 연결 URL (선택)
            session_ttl_minutes: 세션 TTL (분)
            max_turns: 최대 턴 수
        """
        if not ASYNCPG_AVAILABLE:
            raise ImportError("asyncpg is required for PostgresCheckpointer. Install with: pip install asyncpg")

        self.postgres_dsn = postgres_dsn
        self.redis_url = redis_url
        self.session_ttl = timedelta(minutes=session_ttl_minutes)
        self.max_turns = max_turns

        self._pool: Optional[asyncpg.Pool] = None
        self._redis: Optional[Any] = None

    async def initialize(self) -> None:
        """연결 풀 및 스키마 초기화."""
        self._pool = await asyncpg.create_pool(self.postgres_dsn)

        if self.redis_url and REDIS_AVAILABLE:
            self._redis = await aioredis.from_url(self.redis_url)

        await self._init_schema()

    async def close(self) -> None:
        """연결 종료."""
        if self._pool:
            await self._pool.close()
        if self._redis:
            await self._redis.close()

    @asynccontextmanager
    async def _get_connection(self) -> AsyncIterator[asyncpg.Connection]:
        """데이터베이스 연결 컨텍스트 매니저."""
        if not self._pool:
            raise RuntimeError("Checkpointer not initialized. Call initialize() first.")

        async with self._pool.acquire() as conn:
            yield conn

    async def _init_schema(self) -> None:
        """데이터베이스 스키마 초기화."""
        async with self._get_connection() as conn:
            await conn.execute("""
                CREATE SCHEMA IF NOT EXISTS conversation_memory;

                CREATE TABLE IF NOT EXISTS conversation_memory.threads (
                    thread_id UUID PRIMARY KEY,
                    user_id VARCHAR(100) NOT NULL,
                    turn_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    expires_at TIMESTAMP WITH TIME ZONE
                );

                CREATE TABLE IF NOT EXISTS conversation_memory.checkpoints (
                    checkpoint_id UUID PRIMARY KEY,
                    thread_id UUID NOT NULL REFERENCES conversation_memory.threads(thread_id) ON DELETE CASCADE,
                    checkpoint_version INTEGER NOT NULL,
                    state_data JSONB NOT NULL,
                    messages_data JSONB NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    UNIQUE (thread_id, checkpoint_version)
                );

                CREATE TABLE IF NOT EXISTS conversation_memory.query_history (
                    history_id UUID PRIMARY KEY,
                    thread_id UUID NOT NULL REFERENCES conversation_memory.threads(thread_id) ON DELETE CASCADE,
                    turn_number INTEGER NOT NULL,
                    original_query TEXT,
                    enhanced_query TEXT,
                    generated_sql TEXT,
                    executed_sql TEXT,
                    result_count INTEGER,
                    result_summary JSONB,
                    patient_ids JSONB,
                    conditions JSONB,
                    tables_used JSONB,
                    execution_time_ms REAL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_threads_user
                    ON conversation_memory.threads(user_id);
                CREATE INDEX IF NOT EXISTS idx_threads_expires
                    ON conversation_memory.threads(expires_at);
                CREATE INDEX IF NOT EXISTS idx_checkpoints_thread_version
                    ON conversation_memory.checkpoints(thread_id, checkpoint_version DESC);
                CREATE INDEX IF NOT EXISTS idx_query_history_thread_turn
                    ON conversation_memory.query_history(thread_id, turn_number DESC);
            """)

    async def create_thread(self, user_id: str, thread_id: Optional[str] = None) -> str:
        """새 대화 스레드를 생성합니다.

        Args:
            user_id: 사용자 ID
            thread_id: 스레드 ID (미지정 시 자동 생성)

        Returns:
            생성된 스레드 ID
        """
        if thread_id is None:
            thread_id = str(uuid4())

        expires_at = datetime.now() + self.session_ttl

        async with self._get_connection() as conn:
            await conn.execute(
                """INSERT INTO conversation_memory.threads
                   (thread_id, user_id, expires_at)
                   VALUES ($1, $2, $3)""",
                thread_id, user_id, expires_at,
            )

        # Redis 캐시에도 저장
        if self._redis:
            cache_key = f"thread:{thread_id}"
            await self._redis.setex(
                cache_key,
                int(self.session_ttl.total_seconds()),
                json.dumps({"user_id": user_id, "turn_count": 0}),
            )

        return thread_id

    async def save_checkpoint(
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
        state_data = self._serialize_state(state)
        messages_data = self._serialize_messages(state.get("messages", []))
        turn_count = state.get("turn_count", 0)
        expires_at = datetime.now() + self.session_ttl

        async with self._get_connection() as conn:
            async with conn.transaction():
                # 버전 자동 증가
                if checkpoint_version is None:
                    row = await conn.fetchrow(
                        """SELECT COALESCE(MAX(checkpoint_version), 0) + 1 as next_version
                           FROM conversation_memory.checkpoints
                           WHERE thread_id = $1""",
                        thread_id,
                    )
                    checkpoint_version = row["next_version"]

                await conn.execute(
                    """INSERT INTO conversation_memory.checkpoints
                       (checkpoint_id, thread_id, checkpoint_version, state_data, messages_data)
                       VALUES ($1, $2, $3, $4, $5)""",
                    checkpoint_id, thread_id, checkpoint_version, state_data, messages_data,
                )

                # 스레드 업데이트
                await conn.execute(
                    """UPDATE conversation_memory.threads
                       SET turn_count = $1, updated_at = NOW(), expires_at = $2
                       WHERE thread_id = $3""",
                    turn_count, expires_at, thread_id,
                )

        # Redis 캐시 업데이트
        if self._redis:
            cache_key = f"checkpoint:{thread_id}:latest"
            await self._redis.setex(
                cache_key,
                int(self.session_ttl.total_seconds()),
                json.dumps({"state_data": state_data, "messages_data": messages_data}),
            )

        return checkpoint_id

    async def load_checkpoint(
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
        # 최신 버전이면 Redis 캐시 먼저 확인
        if checkpoint_version is None and self._redis:
            cache_key = f"checkpoint:{thread_id}:latest"
            cached = await self._redis.get(cache_key)
            if cached:
                data = json.loads(cached)
                state = self._deserialize_state(data["state_data"])
                state["messages"] = self._deserialize_messages(data["messages_data"])
                return state

        async with self._get_connection() as conn:
            if checkpoint_version is None:
                row = await conn.fetchrow(
                    """SELECT state_data, messages_data
                       FROM conversation_memory.checkpoints
                       WHERE thread_id = $1
                       ORDER BY checkpoint_version DESC
                       LIMIT 1""",
                    thread_id,
                )
            else:
                row = await conn.fetchrow(
                    """SELECT state_data, messages_data
                       FROM conversation_memory.checkpoints
                       WHERE thread_id = $1 AND checkpoint_version = $2""",
                    thread_id, checkpoint_version,
                )

        if row is None:
            return None

        state = self._deserialize_state(row["state_data"])
        state["messages"] = self._deserialize_messages(row["messages_data"])

        # Redis 캐시 업데이트
        if checkpoint_version is None and self._redis:
            cache_key = f"checkpoint:{thread_id}:latest"
            await self._redis.setex(
                cache_key,
                int(self.session_ttl.total_seconds()),
                json.dumps({
                    "state_data": row["state_data"],
                    "messages_data": row["messages_data"],
                }),
            )

        return state

    async def save_query_context(
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

        async with self._get_connection() as conn:
            await conn.execute(
                """INSERT INTO conversation_memory.query_history
                   (history_id, thread_id, turn_number, original_query, enhanced_query,
                    generated_sql, executed_sql, result_count, result_summary,
                    patient_ids, conditions, tables_used, execution_time_ms)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)""",
                history_id,
                thread_id,
                context.turn_number,
                context.original_query,
                context.enhanced_query,
                context.generated_sql,
                context.executed_sql,
                context.result_count,
                json.dumps(context.result_summary) if context.result_summary else None,
                json.dumps(context.patient_ids) if context.patient_ids else None,
                json.dumps(context.conditions),
                json.dumps(context.tables_used),
                context.execution_time_ms,
            )

        return history_id

    async def get_query_history(
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
        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """SELECT * FROM conversation_memory.query_history
                   WHERE thread_id = $1
                   ORDER BY turn_number DESC
                   LIMIT $2""",
                thread_id, limit,
            )

        return [self._row_to_query_context(row) for row in rows]

    async def list_threads(self, user_id: str, limit: int = 20) -> list[dict]:
        """사용자의 스레드 목록을 조회합니다.

        Args:
            user_id: 사용자 ID
            limit: 최대 조회 건수

        Returns:
            스레드 정보 목록
        """
        async with self._get_connection() as conn:
            rows = await conn.fetch(
                """SELECT thread_id, user_id, turn_count, created_at, updated_at, expires_at
                   FROM conversation_memory.threads
                   WHERE user_id = $1 AND (expires_at IS NULL OR expires_at > NOW())
                   ORDER BY updated_at DESC
                   LIMIT $2""",
                user_id, limit,
            )

        return [dict(row) for row in rows]

    async def delete_thread(self, thread_id: str) -> bool:
        """스레드와 관련 데이터를 삭제합니다.

        Args:
            thread_id: 삭제할 스레드 ID

        Returns:
            삭제 성공 여부
        """
        async with self._get_connection() as conn:
            result = await conn.execute(
                "DELETE FROM conversation_memory.threads WHERE thread_id = $1",
                thread_id,
            )

        # Redis 캐시 삭제
        if self._redis:
            await self._redis.delete(
                f"thread:{thread_id}",
                f"checkpoint:{thread_id}:latest",
            )

        return "DELETE 1" in result

    async def cleanup_expired_sessions(self) -> int:
        """만료된 세션을 정리합니다.

        Returns:
            삭제된 세션 수
        """
        async with self._get_connection() as conn:
            result = await conn.execute(
                """DELETE FROM conversation_memory.threads
                   WHERE expires_at < NOW()""",
            )

        # 삭제된 행 수 추출
        count = int(result.split()[-1]) if result else 0
        return count

    async def extend_session(self, thread_id: str) -> bool:
        """세션 만료 시간을 연장합니다.

        Args:
            thread_id: 스레드 ID

        Returns:
            연장 성공 여부
        """
        expires_at = datetime.now() + self.session_ttl

        async with self._get_connection() as conn:
            result = await conn.execute(
                """UPDATE conversation_memory.threads
                   SET expires_at = $1, updated_at = NOW()
                   WHERE thread_id = $2""",
                expires_at, thread_id,
            )

        # Redis TTL 갱신
        if self._redis:
            cache_key = f"checkpoint:{thread_id}:latest"
            await self._redis.expire(cache_key, int(self.session_ttl.total_seconds()))

        return "UPDATE 1" in result

    def _serialize_state(self, state: ConversationState) -> str:
        """상태를 JSON 문자열로 직렬화합니다."""
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

        last_ctx = state.get("last_query_context")
        if last_ctx:
            serializable["last_query_context"] = self._query_context_to_dict(last_ctx)
        else:
            serializable["last_query_context"] = None

        serializable["query_history"] = [
            self._query_context_to_dict(ctx)
            for ctx in state.get("query_history", [])
        ]

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
        data = json.loads(state_json) if isinstance(state_json, str) else state_json

        last_ctx = data.get("last_query_context")
        if last_ctx:
            data["last_query_context"] = self._dict_to_query_context(last_ctx)

        data["query_history"] = [
            self._dict_to_query_context(ctx)
            for ctx in data.get("query_history", [])
        ]

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

        data = json.loads(messages_json) if isinstance(messages_json, str) else messages_json
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

    def _row_to_query_context(self, row) -> QueryContext:
        """데이터베이스 행을 QueryContext로 변환합니다."""
        return QueryContext(
            turn_number=row["turn_number"],
            original_query=row["original_query"],
            enhanced_query=row["enhanced_query"],
            generated_sql=row["generated_sql"],
            executed_sql=row["executed_sql"],
            result_count=row["result_count"],
            result_summary=row["result_summary"],
            patient_ids=row["patient_ids"],
            conditions=row["conditions"] or [],
            tables_used=row["tables_used"] or [],
            execution_time_ms=row["execution_time_ms"],
        )
