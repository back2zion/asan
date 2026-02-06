-- Initialize databases for Asan platform
CREATE DATABASE airflow_db;
CREATE DATABASE mlflow_db;
CREATE DATABASE superset_db;

-- =====================================================
-- ConversationMemory 스키마
-- LangGraph State Management 기반 대화 상태 관리
-- =====================================================

CREATE SCHEMA IF NOT EXISTS conversation_memory;

-- 스레드 테이블: 대화 세션 관리
CREATE TABLE IF NOT EXISTS conversation_memory.threads (
    thread_id UUID PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    turn_count INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE
);

-- 체크포인트 테이블: 대화 상태 스냅샷
CREATE TABLE IF NOT EXISTS conversation_memory.checkpoints (
    checkpoint_id UUID PRIMARY KEY,
    thread_id UUID NOT NULL REFERENCES conversation_memory.threads(thread_id) ON DELETE CASCADE,
    checkpoint_version INTEGER NOT NULL,
    state_data JSONB NOT NULL,
    messages_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (thread_id, checkpoint_version)
);

-- 쿼리 히스토리 테이블: 각 턴의 쿼리 정보 저장
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

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_conversation_threads_user
    ON conversation_memory.threads(user_id);
CREATE INDEX IF NOT EXISTS idx_conversation_threads_expires
    ON conversation_memory.threads(expires_at);
CREATE INDEX IF NOT EXISTS idx_conversation_checkpoints_thread_version
    ON conversation_memory.checkpoints(thread_id, checkpoint_version DESC);
CREATE INDEX IF NOT EXISTS idx_conversation_query_history_thread_turn
    ON conversation_memory.query_history(thread_id, turn_number DESC);

-- 코멘트 추가
COMMENT ON SCHEMA conversation_memory IS 'LangGraph State Management 기반 대화 상태 관리';
COMMENT ON TABLE conversation_memory.threads IS '대화 스레드 - 사용자별 대화 세션 관리';
COMMENT ON TABLE conversation_memory.checkpoints IS '체크포인트 - 대화 상태 스냅샷, 복원 지원';
COMMENT ON TABLE conversation_memory.query_history IS '쿼리 히스토리 - 각 턴의 쿼리 정보 저장';
