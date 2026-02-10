/**
 * AI Assistant 패널 컴포넌트
 * DPR-001: 우측 AI Assistant (보조 도구)
 * PRD 기반 민트색 테마
 */

import React, { useState, useRef, useEffect, useCallback } from 'react';
import { Input, Button, Spin, Typography, Space, Tooltip, Badge, Drawer } from 'antd';
import {
  SendOutlined,
  RobotOutlined,
  HistoryOutlined,
  ReloadOutlined,
  ExpandOutlined,
  CompressOutlined,
  SearchOutlined,
  CodeOutlined,
  DatabaseOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';
import { chatApi } from '../../services/api';
import ChatHistoryModal from './ChatHistoryModal';
import { buildThinkingSteps } from './buildThinkingSteps';
import MessageBubble from './AIMessageBubble';
import {
  PAGE_LABELS,
  MINT,
  type ChatInputHandle,
  type PageState,
  type Message,
  type AIAssistantPanelProps,
} from './chatConstants';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;

/* ── 입력 영역 분리 컴포넌트 ──
 * inputValue state 를 이 컴포넌트 내부에 격리하여
 * 타이핑 시 부모(메시지 리스트) re-render 를 방지한다.
 */
const ChatInput = React.forwardRef<ChatInputHandle, {
  onSend: (msg: string) => void;
  isLoading: boolean;
}>(({ onSend, isLoading }, ref) => {
  const [value, setValue] = useState('');
  const taRef = useRef<HTMLTextAreaElement>(null);

  React.useImperativeHandle(ref, () => ({
    setValue: (v: string) => setValue(v),
    focus: () => taRef.current?.focus(),
  }));

  const send = () => {
    const t = value.trim();
    if (!t || isLoading) return;
    onSend(t);
    setValue('');
  };

  return (
    <div style={{ padding: 12, borderTop: '1px solid rgba(0,160,176,0.25)', backgroundColor: 'white', flexShrink: 0 }}>
      <Space.Compact style={{ width: '100%' }}>
        <TextArea
          ref={taRef as React.RefObject<any>}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); } }}
          placeholder="질문을 입력하세요..."
          rows={2}
          style={{ resize: 'none' }}
          disabled={isLoading}
        />
        <Button
          type="primary"
          icon={<SendOutlined />}
          onClick={send}
          loading={isLoading}
          disabled={!value.trim()}
          style={{ backgroundColor: '#00A0B0', borderColor: '#00A0B0' }}
        />
      </Space.Compact>
      <Text type="secondary" style={{ fontSize: 11, marginTop: 4, display: 'block' }}>
        Shift+Enter로 줄바꿈, Enter로 전송
      </Text>
    </div>
  );
});

const AIAssistantPanel: React.FC<AIAssistantPanelProps> = ({
  visible,
  onClose,
  currentContext,
}) => {
  const navigate = useNavigate();
  const location = useLocation();

  const capturePageState = useCallback((): PageState => ({
    path: location.pathname,
    search: location.search,
    label: PAGE_LABELS[location.pathname] || location.pathname,
  }), [location.pathname, location.search]);

  const [messages, setMessages] = useState<Message[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [loadingStep, setLoadingStep] = useState(0);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [isExpanded, setIsExpanded] = useState(false);
  const [historyModalVisible, setHistoryModalVisible] = useState(false);
  const [sessions, setSessions] = useState<any[]>([]);
  const [sessionsLoading, setSessionsLoading] = useState(false);
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);
  const [timelineData, setTimelineData] = useState<any[]>([]);
  const [timelineLoading, setTimelineLoading] = useState(false);
  const [restoringMessageId, setRestoringMessageId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const chatInputRef = useRef<ChatInputHandle>(null);

  const lastMessageCountRef = useRef(0);
  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'instant' as ScrollBehavior });
  }, []);

  useEffect(() => {
    if (messages.length !== lastMessageCountRef.current) {
      lastMessageCountRef.current = messages.length;
      scrollToBottom();
    }
  }, [messages.length, scrollToBottom]);

  useEffect(() => {
    if (visible) {
      setTimeout(() => chatInputRef.current?.focus(), 100);
    }
  }, [visible]);

  const handleSendFromInput = useCallback(async (trimmedInput: string) => {
    if (isLoading) return;

    const pageState = capturePageState();
    const userMessage: Message = {
      id: `user_${Date.now()}`,
      role: 'user',
      content: trimmedInput,
      timestamp: new Date(),
      pageState,
    };

    try {
      const stateKey = `ai_page_state_${sessionId || 'new'}`;
      const existing = JSON.parse(localStorage.getItem(stateKey) || '{}');
      existing[userMessage.id] = pageState;
      localStorage.setItem(stateKey, JSON.stringify(existing));
    } catch { /* ignore */ }

    setMessages((prev) => [...prev, userMessage]);
    setIsLoading(true);
    setLoadingStep(1);

    // 단계별 진행 애니메이션 (최소 3초 보장)
    const stepDelay = () => new Promise<void>((resolve) => {
      const t1 = setTimeout(() => { setLoadingStep(2); }, 2000);
      const t2 = setTimeout(() => { setLoadingStep(3); }, 3000);
      const t3 = setTimeout(() => { resolve(); clearTimeout(t1); clearTimeout(t2); }, 4000);
    });

    try {
      // API 호출과 단계 애니메이션 병렬 실행
      const [response] = await Promise.all([
        chatApi.sendMessage({
          message: trimmedInput,
          session_id: sessionId || undefined,
          user_id: 'current_user',
          context: currentContext,
        }),
        stepDelay(),
      ]);

      setSessionId(response.session_id);

      if (response.enhancement_applied && response.enhanced_query) {
        const enhancementMessage: Message = {
          id: `enhance_${Date.now()}`,
          role: 'system',
          content: `(AI가 질의를 강화하고 있다)......\n\n**강화된 질의** → ${response.enhanced_query}`,
          timestamp: new Date(),
          enhancedQuery: response.enhanced_query,
          enhancementApplied: true,
          enhancementConfidence: response.enhancement_confidence,
        };
        setMessages((prev) => [...prev, enhancementMessage]);
      }

      const assistantMessage: Message = {
        id: response.message_id,
        role: 'assistant',
        content: response.assistant_message,
        timestamp: new Date(),
        toolResults: response.tool_results,
        suggestedActions: response.suggested_actions,
        thinkingSteps: buildThinkingSteps(response),
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      const errorMessage: Message = {
        id: `error_${Date.now()}`,
        role: 'assistant',
        content: '죄송합니다. 요청을 처리하는 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.',
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
      setLoadingStep(0);
    }
  }, [isLoading, sessionId, currentContext, capturePageState]);

  const handleReset = () => {
    setMessages([]);
    setSessionId(null);
  };

  const handleOpenHistory = async () => {
    setHistoryModalVisible(true);
    setSessionsLoading(true);
    setSelectedSessionId(null);
    setTimelineData([]);
    try {
      const result = await chatApi.getSessions('current_user');
      setSessions(result?.data?.sessions || result?.sessions || []);
    } catch {
      setSessions([]);
    } finally {
      setSessionsLoading(false);
    }
  };

  const handleSelectSession = async (sid: string) => {
    setSelectedSessionId(sid);
    setTimelineLoading(true);
    try {
      const result = await chatApi.getTimeline(sid);
      setTimelineData(result?.data?.messages || result?.messages || []);
    } catch {
      setTimelineData([]);
    } finally {
      setTimelineLoading(false);
    }
  };

  const handleRestore = async (sid: string, messageId: string) => {
    setRestoringMessageId(messageId);
    try {
      await chatApi.restoreState(sid, messageId);
      // Reload the session messages
      const result = await chatApi.getSession(sid);
      const restoredMessages = (result?.data?.messages || result?.messages || []).map(
        (m: any) => ({
          id: m.message_id || m.id || `msg_${Date.now()}_${Math.random()}`,
          role: m.role,
          content: m.content,
          timestamp: new Date(m.timestamp || m.created_at || Date.now()),
          toolResults: m.tool_results,
          suggestedActions: m.suggested_actions,
        })
      );
      setMessages(restoredMessages);
      setSessionId(sid);
      setHistoryModalVisible(false);

      // 저장된 페이지 상태로 이동
      try {
        const stateKey = `ai_page_state_${sid}`;
        const states = JSON.parse(localStorage.getItem(stateKey) || '{}');
        const savedState: PageState | undefined = states[messageId];
        if (savedState) {
          navigate(savedState.path + savedState.search);
        }
      } catch { /* ignore */ }
    } catch {
      // Silently fail
    } finally {
      setRestoringMessageId(null);
    }
  };

  const handleSuggestedAction = useCallback((action: Record<string, unknown>) => {
    const actionType = (action.type as string) || (action.action as string);
    const actionTarget = (action.target as string) || (action.path as string);

    if (actionType === 'view_table' && actionTarget) {
      navigate(`/catalog?q=${encodeURIComponent(actionTarget)}`);
      onClose();
    } else if (actionType === 'navigate' || actionType === 'open_catalog') {
      const path = actionTarget || '/catalog';
      const query = action.query ? `?q=${encodeURIComponent(action.query as string)}` : '';
      navigate(`${path}${query}`);
      onClose();
    } else if (actionType === 'search' && action.query) {
      chatInputRef.current?.setValue(action.query as string);
    }
  }, [navigate, onClose]);

  return (
    <Drawer
      title={
        <Space>
          <RobotOutlined style={{ color: MINT.PRIMARY }} />
          <span style={{ color: MINT.DARK }}>Assistant</span>
          {sessionId && (
            <Badge
              status="processing"
              color={MINT.PRIMARY}
              text={<Text type="secondary" style={{ fontSize: 12 }}>대화 중</Text>}
            />
          )}
        </Space>
      }
      placement="right"
      width={isExpanded ? 500 : 380}
      onClose={onClose}
      open={visible}
      mask={false}
      extra={
        <Space>
          <Tooltip title="대화 기록">
            <Button icon={<HistoryOutlined />} type="text" size="small" onClick={handleOpenHistory} />
          </Tooltip>
          <Tooltip title={isExpanded ? '축소' : '확장'}>
            <Button
              icon={isExpanded ? <CompressOutlined /> : <ExpandOutlined />}
              type="text"
              size="small"
              onClick={() => setIsExpanded(!isExpanded)}
            />
          </Tooltip>
          <Tooltip title="대화 초기화">
            <Button
              icon={<ReloadOutlined />}
              type="text"
              size="small"
              onClick={handleReset}
              disabled={messages.length === 0}
            />
          </Tooltip>
        </Space>
      }
      styles={{
        header: {
          background: MINT.LIGHT,
          borderBottom: `1px solid ${MINT.PRIMARY}40`,
        },
        body: {
          padding: 0,
          display: 'flex',
          flexDirection: 'column',
          height: '100%',
        },
      }}
    >
      {/* 메시지 영역 - 민트색 배경 */}
      <div
        style={{
          flex: 1,
          overflowY: 'auto',
          padding: 16,
          backgroundColor: MINT.BG,
          minHeight: 0,
        }}
      >
        {messages.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '40px 20px' }}>
            <RobotOutlined style={{ fontSize: 48, color: MINT.PRIMARY }} />
            <Paragraph type="secondary" style={{ marginTop: 16 }}>
              안녕하세요! 데이터 검색, 테이블 분석, SQL 쿼리 생성 등을 도와드립니다.
            </Paragraph>
            <Space direction="vertical" style={{ marginTop: 16 }}>
              <Button
                style={{ borderColor: MINT.PRIMARY, color: MINT.PRIMARY }}
                size="small"
                onClick={() => chatInputRef.current?.setValue('진단 테이블 찾아줘')}
              >
                "진단 테이블 찾아줘"
              </Button>
              <Button
                style={{ borderColor: MINT.PRIMARY, color: MINT.PRIMARY }}
                size="small"
                onClick={() => chatInputRef.current?.setValue('당뇨 환자 몇 명?')}
              >
                "당뇨 환자 몇 명?"
              </Button>
              <Button
                style={{ borderColor: MINT.PRIMARY, color: MINT.PRIMARY }}
                size="small"
                onClick={() => chatInputRef.current?.setValue('폐렴 소견 흉부 X-ray 보여줘')}
              >
                "폐렴 소견 흉부 X-ray 보여줘"
              </Button>
            </Space>
          </div>
        ) : (
          <>
            {messages.map((msg) => (
              <MessageBubble key={msg.id} message={msg} handleSuggestedAction={handleSuggestedAction} />
            ))}
            {isLoading && (
              <div style={{
                padding: '12px 16px',
                margin: '8px 12px',
                background: '#f6ffed',
                borderRadius: 8,
                border: '1px solid #b7eb8f',
              }}>
                {[
                  { step: 1, icon: <SearchOutlined />, label: 'IT메타 기반 비즈메타 생성 중' },
                  { step: 2, icon: <CodeOutlined />, label: 'SQL 생성 중' },
                  { step: 3, icon: <DatabaseOutlined />, label: '데이터 조회 중' },
                ].map(({ step, icon, label }) => (
                  <div key={step} style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 8,
                    padding: '4px 0',
                    opacity: loadingStep >= step ? 1 : 0.25,
                    transition: 'opacity 0.3s',
                  }}>
                    {loadingStep > step
                      ? <CheckCircleOutlined style={{ color: '#52c41a', fontSize: 14 }} />
                      : loadingStep === step
                        ? <Spin size="small" />
                        : <span style={{ width: 14, display: 'inline-block' }}>{icon}</span>
                    }
                    <Text style={{
                      fontSize: 13,
                      color: loadingStep >= step ? '#333' : '#999',
                    }}>
                      {label}{loadingStep === step ? '...' : loadingStep > step ? ' ✓' : ''}
                    </Text>
                  </div>
                ))}
              </div>
            )}
            <div ref={messagesEndRef} />
          </>
        )}
      </div>

      {/* 입력 영역 — ChatInput 별도 컴포넌트 (타이핑 시 메시지 리스트 re-render 방지) */}
      <ChatInput ref={chatInputRef} onSend={handleSendFromInput} isLoading={isLoading} />

      {/* 대화 기록 Modal */}
      <ChatHistoryModal
        visible={historyModalVisible}
        onClose={() => setHistoryModalVisible(false)}
        sessionId={sessionId}
        sessions={sessions}
        sessionsLoading={sessionsLoading}
        selectedSessionId={selectedSessionId}
        timelineData={timelineData}
        timelineLoading={timelineLoading}
        restoringMessageId={restoringMessageId}
        onSelectSession={handleSelectSession}
        onRestore={handleRestore}
        onBackToList={() => { setSelectedSessionId(null); setTimelineData([]); }}
      />
    </Drawer>
  );
};

export default AIAssistantPanel;
