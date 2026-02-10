/**
 * AI Assistant 패널 컴포넌트
 * DPR-001: 우측 AI Assistant (보조 도구)
 * PRD 기반 민트색 테마
 */

import React, { useState, useRef, useEffect, useCallback } from 'react';
import { Input, Button, Spin, Typography, Space, Tooltip, Badge, Drawer, Collapse } from 'antd';
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
import { chatApi, sanitizeHtml } from '../../services/api';
import type { ChatResponse } from '../../services/api';
import ReactMarkdown from 'react-markdown';
import ImageCell from '../common/ImageCell';
import ChatHistoryModal from './ChatHistoryModal';
import { buildThinkingSteps } from './buildThinkingSteps';
import {
  PAGE_LABELS,
  MINT,
  type ChatInputHandle,
  type PageState,
  type ThinkingStep,
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

  const renderMessage = (message: Message) => {
    const isUser = message.role === 'user';
    const isSystem = message.role === 'system';

    if (isSystem) {
      return (
        <div
          key={message.id}
          style={{
            display: 'flex',
            justifyContent: 'center',
            marginBottom: 12,
          }}
        >
          <div
            style={{
              maxWidth: '90%',
              padding: '8px 16px',
              borderRadius: 8,
              backgroundColor: MINT.LIGHT,
              border: `1px solid ${MINT.PRIMARY}`,
              fontSize: 13,
            }}
          >
            <div style={{ color: MINT.DARK, fontStyle: 'italic' }}>
              <ReactMarkdown
                components={{
                  p: ({ children }) => <span>{children}</span>,
                  strong: ({ children }) => (
                    <strong style={{ color: MINT.DARK }}>{children}</strong>
                  ),
                }}
              >
                {message.content}
              </ReactMarkdown>
            </div>
            {message.enhancementConfidence && (
              <Text type="secondary" style={{ fontSize: 10, display: 'block', marginTop: 4 }}>
                신뢰도: {(message.enhancementConfidence * 100).toFixed(0)}%
              </Text>
            )}
          </div>
        </div>
      );
    }

    return (
      <div
        key={message.id}
        style={{
          display: 'flex',
          justifyContent: isUser ? 'flex-end' : 'flex-start',
          marginBottom: 12,
        }}
      >
        <div
          style={{
            maxWidth: '85%',
            padding: '10px 14px',
            borderRadius: isUser ? '16px 16px 4px 16px' : '16px 16px 16px 4px',
            backgroundColor: isUser ? MINT.PRIMARY : 'white',
            color: isUser ? 'white' : 'inherit',
            boxShadow: '0 1px 2px rgba(0,0,0,0.1)',
            overflow: 'hidden',
            wordBreak: 'break-word',
          }}
        >
          {/* 사고 과정 — assistant 메시지 상단에 접힌 상태로 표시 */}
          {!isUser && message.thinkingSteps && message.thinkingSteps.length > 0 && (
            <Collapse
              size="small"
              ghost
              style={{
                marginBottom: 8,
                marginLeft: -8,
                marginRight: -8,
                background: '#e8f4fd',
                borderRadius: 6,
                border: '1px solid #b3d9f2',
              }}
              items={[{
                key: 'thinking',
                label: (
                  <span style={{ fontSize: 12, color: '#1677ff', fontWeight: 500, cursor: 'pointer' }}>
                    💭 사고 과정 보기
                  </span>
                ),
                children: (
                  <div style={{ fontSize: 12, lineHeight: 1.8 }}>
                    {message.thinkingSteps.map((step, si) => {
                      const iconMap = { search: <SearchOutlined />, code: <CodeOutlined />, database: <DatabaseOutlined /> };
                      return (
                        <div key={si} style={{ marginBottom: 10 }}>
                          <div style={{ fontWeight: 600, color: '#333', marginBottom: 2 }}>
                            {iconMap[step.icon]} {step.label}
                          </div>
                          <pre style={{
                            fontSize: 11,
                            background: '#f5f5f5',
                            padding: '6px 8px',
                            borderRadius: 4,
                            margin: 0,
                            whiteSpace: 'pre-wrap',
                            wordBreak: 'break-all',
                            color: '#555',
                          }}>
                            {step.detail}
                          </pre>
                        </div>
                      );
                    })}
                  </div>
                ),
              }]}
            />
          )}
          {isUser ? (
            <Text style={{ color: 'white' }}>{message.content}</Text>
          ) : (
            <div className="ai-message-content" style={{ overflow: 'hidden', wordBreak: 'break-word' }}>
              <ReactMarkdown
                components={{
                  a: ({ href, children }) => (
                    <a
                      href={href}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{ color: MINT.PRIMARY }}
                    >
                      {children}
                    </a>
                  ),
                  img: ({ src, alt }) => (
                    <img
                      src={src}
                      alt={alt || 'image'}
                      style={{ maxWidth: '100%', borderRadius: 8, margin: '8px 0', display: 'block' }}
                      onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }}
                    />
                  ),
                }}
              >
                {sanitizeHtml(message.content)}
              </ReactMarkdown>
            </div>
          )}

          {/* tool_results 렌더링 — SQL 결과 미니 테이블 + 중앙 화면 전송 */}
          {message.toolResults && message.toolResults.length > 0 && (
            <div style={{ marginTop: 8 }}>
              {message.toolResults.map((tr, trIdx) => {
                const columns = (tr as any).columns as string[] | undefined;
                const results = (tr as any).results as any[][] | undefined;
                if (!columns || !results || results.length === 0) return null;
                return (
                  <div key={trIdx} style={{ maxHeight: 200, overflow: 'auto', marginBottom: 8, maxWidth: '100%' }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, tableLayout: 'auto' }}>
                      <thead>
                        <tr style={{ background: '#fafafa', borderBottom: '1px solid #d9d9d9' }}>
                          {columns.map((col, ci) => (
                            <th key={ci} style={{ padding: '4px 6px', textAlign: 'left', border: '1px solid #e8e8e8', whiteSpace: 'nowrap' }}>
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {results.slice(0, 5).map((row, ri) => (
                          <tr key={ri}>
                            {columns.map((_, ci) => (
                              <td key={ci} style={{ padding: '4px 6px', border: '1px solid #e8e8e8' }}>
                                <ImageCell value={row[ci]} />
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {results.length > 5 && (
                      <div style={{ textAlign: 'center', fontSize: 11, color: '#999', marginTop: 4 }}>
                        ... 외 {results.length - 5}건
                      </div>
                    )}
                    {/* 중앙 화면에 표출 버튼 */}
                    <Button
                      size="small"
                      type="link"
                      icon={<ExpandOutlined />}
                      style={{ padding: 0, fontSize: 11, color: MINT.PRIMARY, marginTop: 4 }}
                      onClick={() => {
                        window.dispatchEvent(new CustomEvent('ai:show-results', {
                          detail: { columns, results, query: message.content },
                        }));
                      }}
                    >
                      중앙 화면에 표출
                    </Button>
                  </div>
                );
              })}
            </div>
          )}

          {message.suggestedActions && message.suggestedActions.length > 0 && (
            <div style={{ marginTop: 8 }}>
              <Space wrap>
                {message.suggestedActions.map((action, index) => (
                  <Button
                    key={index}
                    size="small"
                    type="link"
                    onClick={() => handleSuggestedAction(action)}
                    style={{ padding: '0 4px', height: 'auto', color: MINT.PRIMARY }}
                  >
                    {(action.label as string) || (action.action as string)}
                  </Button>
                ))}
              </Space>
            </div>
          )}

          <Text
            type="secondary"
            style={{
              display: 'block',
              fontSize: 11,
              marginTop: 4,
              textAlign: isUser ? 'right' : 'left',
              color: isUser ? 'rgba(255,255,255,0.7)' : undefined,
            }}
          >
            {message.timestamp.toLocaleTimeString('ko-KR', {
              hour: '2-digit',
              minute: '2-digit',
            })}
          </Text>
        </div>
      </div>
    );
  };

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
            {messages.map(renderMessage)}
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
