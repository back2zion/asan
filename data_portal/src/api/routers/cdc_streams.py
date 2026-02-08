"""
CDC 스트림 관리 - 커넥터 CRUD, 토픽, 오프셋, Iceberg 싱크, 서비스 제어
"""
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
import asyncpg

from .cdc_shared import (
    get_connection, ensure_tables, ensure_seed_data,
    ConnectorCreate, ConnectorUpdate, TopicCreate, IcebergSinkCreate, ServiceAction,
)

router = APIRouter()


# ═══════════════════════════════════════════════════
#  Connectors
# ═══════════════════════════════════════════════════

@router.get("/connectors")
async def list_connectors():
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        await ensure_seed_data(conn)
        rows = await conn.fetch("""
            SELECT c.*,
                   COUNT(t.topic_id) AS topic_count,
                   COUNT(t.topic_id) FILTER (WHERE t.status = 'active') AS active_topics
            FROM cdc_connector c
            LEFT JOIN cdc_topic t ON t.connector_id = c.connector_id
            GROUP BY c.connector_id
            ORDER BY c.connector_id
        """)
        return {
            "connectors": [
                {
                    "connector_id": r["connector_id"],
                    "name": r["name"],
                    "db_type": r["db_type"],
                    "host": r["host"],
                    "port": r["port"],
                    "database_name": r["database_name"],
                    "schema_name": r["schema_name"],
                    "description": r["description"],
                    "status": r["status"],
                    "topic_count": r["topic_count"],
                    "active_topics": r["active_topics"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.get("/connectors/{connector_id}")
async def get_connector(connector_id: int):
    conn = await get_connection()
    try:
        r = await conn.fetchrow("SELECT * FROM cdc_connector WHERE connector_id=$1", connector_id)
        if not r:
            raise HTTPException(status_code=404, detail="커넥터를 찾을 수 없습니다")
        topics = await conn.fetch("SELECT * FROM cdc_topic WHERE connector_id=$1 ORDER BY topic_id", connector_id)
        offsets = await conn.fetch("SELECT * FROM cdc_offset_tracker WHERE connector_id=$1", connector_id)
        sink = await conn.fetchrow("SELECT * FROM cdc_iceberg_sink WHERE connector_id=$1", connector_id)
        return {
            "connector_id": r["connector_id"],
            "name": r["name"],
            "db_type": r["db_type"],
            "host": r["host"],
            "port": r["port"],
            "database_name": r["database_name"],
            "username": r["username"],
            "schema_name": r["schema_name"],
            "extra_config": json.loads(r["extra_config"]) if isinstance(r["extra_config"], str) else (r["extra_config"] or {}),
            "description": r["description"],
            "status": r["status"],
            "topics": [
                {
                    "topic_id": t["topic_id"],
                    "table_name": t["table_name"],
                    "topic_name": t["topic_name"],
                    "capture_mode": t["capture_mode"],
                    "target_table": t["target_table"],
                    "enabled": t["enabled"],
                    "status": t["status"],
                }
                for t in topics
            ],
            "offsets": [
                {
                    "topic_name": o["topic_name"],
                    "lsn": o["lsn"],
                    "binlog_file": o["binlog_file"],
                    "binlog_pos": o["binlog_pos"],
                    "scn": o["scn"],
                    "last_event_time": o["last_event_time"].isoformat() if o["last_event_time"] else None,
                }
                for o in offsets
            ],
            "iceberg_sink": {
                "sink_id": sink["sink_id"],
                "catalog_name": sink["catalog_name"],
                "warehouse_path": sink["warehouse_path"],
                "namespace": sink["namespace"],
                "file_format": sink["file_format"],
                "partition_spec": json.loads(sink["partition_spec"]) if isinstance(sink["partition_spec"], str) else (sink["partition_spec"] or {}),
                "write_mode": sink["write_mode"],
                "enabled": sink["enabled"],
            } if sink else None,
        }
    finally:
        await conn.close()


@router.post("/connectors")
async def create_connector(body: ConnectorCreate):
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        cid = await conn.fetchval("""
            INSERT INTO cdc_connector (name, db_type, host, port, database_name, username, password, schema_name, extra_config, description)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10) RETURNING connector_id
        """, body.name, body.db_type, body.host, body.port, body.database_name,
             body.username, body.password, body.schema_name,
             json.dumps(body.extra_config or {}), body.description)
        return {"success": True, "connector_id": cid}
    finally:
        await conn.close()


@router.put("/connectors/{connector_id}")
async def update_connector(connector_id: int, body: ConnectorUpdate):
    conn = await get_connection()
    try:
        existing = await conn.fetchrow("SELECT * FROM cdc_connector WHERE connector_id=$1", connector_id)
        if not existing:
            raise HTTPException(status_code=404, detail="커넥터를 찾을 수 없습니다")
        await conn.execute("""
            UPDATE cdc_connector SET
                name = COALESCE($1, name), host = COALESCE($2, host), port = COALESCE($3, port),
                database_name = COALESCE($4, database_name), username = COALESCE($5, username),
                password = COALESCE($6, password), schema_name = COALESCE($7, schema_name),
                extra_config = COALESCE($8::jsonb, extra_config), description = COALESCE($9, description),
                updated_at = NOW()
            WHERE connector_id = $10
        """, body.name, body.host, body.port, body.database_name, body.username,
             body.password, body.schema_name,
             json.dumps(body.extra_config) if body.extra_config is not None else None,
             body.description, connector_id)
        return {"success": True}
    finally:
        await conn.close()


@router.delete("/connectors/{connector_id}")
async def delete_connector(connector_id: int):
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM cdc_connector WHERE connector_id=$1", connector_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="커넥터를 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


@router.post("/connectors/{connector_id}/test")
async def test_connector(connector_id: int):
    """커넥터 연결 테스트"""
    conn = await get_connection()
    try:
        r = await conn.fetchrow("SELECT * FROM cdc_connector WHERE connector_id=$1", connector_id)
        if not r:
            raise HTTPException(status_code=404, detail="커넥터를 찾을 수 없습니다")

        # For the OMOP CDM connector, actually test
        if r["db_type"] == "postgresql" and r["host"] == "localhost":
            try:
                test_conn = await asyncpg.connect(
                    host=r["host"], port=r["port"],
                    user=r["username"], password="omop",
                    database=r["database_name"],
                )
                version = await test_conn.fetchval("SELECT version()")
                await test_conn.close()
                return {"success": True, "message": f"연결 성공: {version[:60]}"}
            except Exception as e:
                return {"success": False, "message": f"연결 실패: {str(e)[:200]}"}
        else:
            # Simulated for demo
            return {"success": True, "message": f"{r['db_type'].upper()} 커넥터 연결 테스트 성공 (시뮬레이션)"}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Service Control (start/stop/pause/resume)
# ═══════════════════════════════════════════════════

@router.post("/connectors/{connector_id}/service")
async def control_service(connector_id: int, body: ServiceAction):
    """CDC 서비스 기동/중지/재시작/일시정지/재개"""
    conn = await get_connection()
    try:
        r = await conn.fetchrow("SELECT * FROM cdc_connector WHERE connector_id=$1", connector_id)
        if not r:
            raise HTTPException(status_code=404, detail="커넥터를 찾을 수 없습니다")

        action_map = {
            "start": "running",
            "stop": "stopped",
            "restart": "running",
            "pause": "paused",
            "resume": "running",
        }
        new_status = action_map[body.action]
        await conn.execute(
            "UPDATE cdc_connector SET status=$1, updated_at=NOW() WHERE connector_id=$2",
            new_status, connector_id,
        )

        # Update topic status accordingly
        if body.action in ("start", "restart", "resume"):
            await conn.execute(
                "UPDATE cdc_topic SET status='active' WHERE connector_id=$1 AND enabled=TRUE",
                connector_id,
            )
        elif body.action in ("stop", "pause"):
            await conn.execute(
                "UPDATE cdc_topic SET status='inactive' WHERE connector_id=$1",
                connector_id,
            )

        action_labels = {"start": "기동", "stop": "중지", "restart": "재시작", "pause": "일시정지", "resume": "재개"}
        return {"success": True, "message": f"CDC 서비스 {action_labels[body.action]} 완료", "status": new_status}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Topics
# ═══════════════════════════════════════════════════

@router.get("/topics")
async def list_topics(connector_id: Optional[int] = Query(None)):
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        query = """
            SELECT t.*, c.name AS connector_name, c.db_type
            FROM cdc_topic t
            JOIN cdc_connector c ON c.connector_id = t.connector_id
        """
        params = []
        if connector_id is not None:
            query += " WHERE t.connector_id = $1"
            params.append(connector_id)
        query += " ORDER BY t.connector_id, t.topic_id"
        rows = await conn.fetch(query, *params)
        return {
            "topics": [
                {
                    "topic_id": r["topic_id"],
                    "connector_id": r["connector_id"],
                    "connector_name": r["connector_name"],
                    "db_type": r["db_type"],
                    "table_name": r["table_name"],
                    "topic_name": r["topic_name"],
                    "capture_mode": r["capture_mode"],
                    "target_table": r["target_table"],
                    "enabled": r["enabled"],
                    "status": r["status"],
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.post("/topics")
async def create_topic(body: TopicCreate):
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        topic_name = body.topic_name or f"cdc.{body.table_name}.events"
        tid = await conn.fetchval("""
            INSERT INTO cdc_topic (connector_id, table_name, topic_name, capture_mode, target_table, enabled)
            VALUES ($1, $2, $3, $4, $5, $6) RETURNING topic_id
        """, body.connector_id, body.table_name, topic_name, body.capture_mode, body.target_table, body.enabled)
        return {"success": True, "topic_id": tid}
    except asyncpg.ForeignKeyViolationError:
        raise HTTPException(status_code=400, detail="존재하지 않는 connector_id입니다")
    finally:
        await conn.close()


@router.delete("/topics/{topic_id}")
async def delete_topic(topic_id: int):
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM cdc_topic WHERE topic_id=$1", topic_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="토픽을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Offsets
# ═══════════════════════════════════════════════════

@router.get("/offsets")
async def list_offsets(connector_id: Optional[int] = Query(None)):
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        query = """
            SELECT o.*, c.name AS connector_name, c.db_type
            FROM cdc_offset_tracker o
            JOIN cdc_connector c ON c.connector_id = o.connector_id
        """
        params = []
        if connector_id is not None:
            query += " WHERE o.connector_id = $1"
            params.append(connector_id)
        query += " ORDER BY o.connector_id, o.topic_name"
        rows = await conn.fetch(query, *params)
        return {
            "offsets": [
                {
                    "offset_id": r["offset_id"],
                    "connector_id": r["connector_id"],
                    "connector_name": r["connector_name"],
                    "db_type": r["db_type"],
                    "topic_name": r["topic_name"],
                    "lsn": r["lsn"],
                    "binlog_file": r["binlog_file"],
                    "binlog_pos": r["binlog_pos"],
                    "scn": r["scn"],
                    "resume_token": json.loads(r["resume_token"]) if isinstance(r["resume_token"], str) else r["resume_token"],
                    "last_event_time": r["last_event_time"].isoformat() if r["last_event_time"] else None,
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.post("/offsets/{connector_id}/reset")
async def reset_offset(connector_id: int, topic_name: str = Query(...)):
    """오프셋 초기화 (마지막 지점부터 재시작 시)"""
    conn = await get_connection()
    try:
        result = await conn.execute("""
            UPDATE cdc_offset_tracker SET lsn=NULL, binlog_file=NULL, binlog_pos=NULL, scn=NULL,
                   resume_token=NULL, updated_at=NOW()
            WHERE connector_id=$1 AND topic_name=$2
        """, connector_id, topic_name)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="오프셋을 찾을 수 없습니다")
        return {"success": True, "message": f"{topic_name} 오프셋 초기화 완료 - 처음부터 재캡처됩니다"}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Iceberg Sink
# ═══════════════════════════════════════════════════

@router.get("/iceberg-sinks")
async def list_iceberg_sinks():
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        await ensure_seed_data(conn)
        rows = await conn.fetch("""
            SELECT s.*, c.name AS connector_name
            FROM cdc_iceberg_sink s
            JOIN cdc_connector c ON c.connector_id = s.connector_id
            ORDER BY s.sink_id
        """)
        return {
            "sinks": [
                {
                    "sink_id": r["sink_id"],
                    "connector_id": r["connector_id"],
                    "connector_name": r["connector_name"],
                    "catalog_name": r["catalog_name"],
                    "warehouse_path": r["warehouse_path"],
                    "namespace": r["namespace"],
                    "file_format": r["file_format"],
                    "partition_spec": json.loads(r["partition_spec"]) if isinstance(r["partition_spec"], str) else (r["partition_spec"] or {}),
                    "write_mode": r["write_mode"],
                    "enabled": r["enabled"],
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.post("/iceberg-sinks")
async def create_iceberg_sink(body: IcebergSinkCreate):
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        sid = await conn.fetchval("""
            INSERT INTO cdc_iceberg_sink (connector_id, catalog_name, warehouse_path, namespace, file_format, partition_spec, write_mode)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7) RETURNING sink_id
        """, body.connector_id, body.catalog_name, body.warehouse_path, body.namespace,
             body.file_format, json.dumps(body.partition_spec or {}), body.write_mode)
        return {"success": True, "sink_id": sid}
    finally:
        await conn.close()


@router.put("/iceberg-sinks/{sink_id}")
async def update_iceberg_sink(sink_id: int, body: IcebergSinkCreate):
    conn = await get_connection()
    try:
        result = await conn.execute("""
            UPDATE cdc_iceberg_sink SET catalog_name=$1, warehouse_path=$2, namespace=$3,
                   file_format=$4, partition_spec=$5::jsonb, write_mode=$6
            WHERE sink_id=$7
        """, body.catalog_name, body.warehouse_path, body.namespace,
             body.file_format, json.dumps(body.partition_spec or {}), body.write_mode, sink_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Iceberg 싱크를 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()
