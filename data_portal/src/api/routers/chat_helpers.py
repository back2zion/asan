"""
AI Assistant Chat API — Session management endpoints

- /chat/sessions — list sessions
- /chat/sessions/{id} — get session detail
- /chat/sessions/{id}/timeline — get session timeline
- /chat/sessions/{id}/restore/{msg_id} — restore to a message point
"""
from fastapi import APIRouter, HTTPException

# Import shared session storage from chat_core
from routers.chat_core import sessions

router = APIRouter()


@router.get("/sessions")
async def get_sessions(user_id: str = "anonymous"):
    """사용자의 세션 목록 조회"""
    user_sessions = [
        {"id": s["id"], "created_at": s["created_at"], "message_count": len(s["messages"])}
        for s in sessions.values()
        if s["user_id"] == user_id
    ]
    return {"sessions": user_sessions}


@router.get("/sessions/{session_id}")
async def get_session(session_id: str):
    """세션 상세 조회"""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    return sessions[session_id]


@router.get("/sessions/{session_id}/timeline")
async def get_timeline(session_id: str):
    """세션 타임라인 조회"""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"timeline": sessions[session_id]["messages"]}


@router.post("/sessions/{session_id}/restore/{message_id}")
async def restore_state(session_id: str, message_id: str):
    """특정 메시지 시점으로 상태 복원"""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    messages = sessions[session_id]["messages"]
    restored_messages = []
    for msg in messages:
        restored_messages.append(msg)
        if msg["id"] == message_id:
            break

    sessions[session_id]["messages"] = restored_messages
    return {"status": "restored", "message_count": len(restored_messages)}
