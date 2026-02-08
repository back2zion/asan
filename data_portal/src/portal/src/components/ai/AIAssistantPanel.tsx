/**
 * AI Assistant 패널 컴포넌트
 * DPR-001: 우측 AI Assistant (보조 도구)
 * PRD 기반 민트색 테마
 */

import React, { useState, useRef, useEffect, useCallback } from 'react';
import { Input, Button, Spin, Typography, Space, Tooltip, Badge, Drawer, Image, Modal, List, Timeline, Avatar, Tag } from 'antd';
import {
  SendOutlined,
  RobotOutlined,
  HistoryOutlined,
  ReloadOutlined,
  ExpandOutlined,
  CompressOutlined,
  UserOutlined,
  MessageOutlined,
  RollbackOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';

const PAGE_LABELS: Record<string, string> = {
  '/dashboard': '홈 대시보드',
  '/etl': 'ETL 파이프라인',
  '/governance': '데이터 거버넌스',
  '/catalog': '데이터 카탈로그',
  '/datamart': '데이터마트',
  '/bi': 'BI 대시보드',
  '/ai-environment': 'AI 분석환경',
  '/cdw': 'CDW 연구지원',
  '/ner': '비정형 구조화',
  '/ai-ops': 'AI 운영관리',
  '/data-design': '데이터 설계',
  '/ontology': '의료 온톨로지',
};
import { chatApi, sanitizeHtml } from '../../services/api';
import type { ChatResponse } from '../../services/api';
import ReactMarkdown from 'react-markdown';
import ImageCell from '../common/ImageCell';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;

// 민트색 테마 컬러
const MINT = {
  PRIMARY: '#00A0B0',
  LIGHT: '#e0f7f7',
  BG: '#f0faf9',
  DARK: '#008080',
  SEND_BTN: '#00A0B0',
};

interface PageState {
  path: string;
  search: string;
  label: string;  // 화면 이름 (예: "데이터 카탈로그")
}

interface Message {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  timestamp: Date;
  toolResults?: Record<string, unknown>[];
  suggestedActions?: Record<string, unknown>[];
  enhancedQuery?: string;
  enhancementApplied?: boolean;
  enhancementConfidence?: number;
  pageState?: PageState;
}

interface AIAssistantPanelProps {
  visible: boolean;
  onClose: () => void;
  currentContext?: {
    currentPage?: string;
    currentTable?: string;
    currentColumns?: string[];
    userRole?: string;
  };
}

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
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
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
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, scrollToBottom]);

  useEffect(() => {
    if (visible) {
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [visible]);

  const handleSendMessage = async () => {
    const trimmedInput = inputValue.trim();
    if (!trimmedInput || isLoading) return;

    const pageState = capturePageState();
    const userMessage: Message = {
      id: `user_${Date.now()}`,
      role: 'user',
      content: trimmedInput,
      timestamp: new Date(),
      pageState,
    };

    // 탐색 상태를 localStorage에 저장 (타임라인 복원용)
    try {
      const stateKey = `ai_page_state_${sessionId || 'new'}`;
      const existing = JSON.parse(localStorage.getItem(stateKey) || '{}');
      existing[userMessage.id] = pageState;
      localStorage.setItem(stateKey, JSON.stringify(existing));
    } catch { /* ignore */ }

    setMessages((prev) => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);

    try {
      const response: ChatResponse = await chatApi.sendMessage({
        message: trimmedInput,
        session_id: sessionId || undefined,
        user_id: 'current_user',
        context: currentContext,
      });

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
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      console.error('메시지 전송 실패:', error);

      const errorMessage: Message = {
        id: `error_${Date.now()}`,
        role: 'assistant',
        content: '죄송합니다. 요청을 처리하는 중 오류가 발생했습니다. 잠시 후 다시 시도해 주세요.',
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

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

  const handleSuggestedAction = (action: Record<string, unknown>) => {
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
      setInputValue(action.query as string);
    }
  };

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
          }}
        >
          {isUser ? (
            <Text style={{ color: 'white' }}>{message.content}</Text>
          ) : (
            <div className="ai-message-content">
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

          {/* tool_results 렌더링 — SQL 결과 미니 테이블 */}
          {message.toolResults && message.toolResults.length > 0 && (
            <div style={{ marginTop: 8 }}>
              {message.toolResults.map((tr, trIdx) => {
                const columns = (tr as any).columns as string[] | undefined;
                const results = (tr as any).results as any[][] | undefined;
                if (!columns || !results || results.length === 0) return null;
                return (
                  <div key={trIdx} style={{ maxHeight: 200, overflow: 'auto', marginBottom: 8 }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11 }}>
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
                onClick={() => setInputValue('진단 테이블 찾아줘')}
              >
                "진단 테이블 찾아줘"
              </Button>
              <Button
                style={{ borderColor: MINT.PRIMARY, color: MINT.PRIMARY }}
                size="small"
                onClick={() => setInputValue('당뇨 환자 몇 명?')}
              >
                "당뇨 환자 몇 명?"
              </Button>
              <Button
                style={{ borderColor: MINT.PRIMARY, color: MINT.PRIMARY }}
                size="small"
                onClick={() => setInputValue('폐렴 소견 흉부 X-ray 보여줘')}
              >
                "폐렴 소견 흉부 X-ray 보여줘"
              </Button>
            </Space>
          </div>
        ) : (
          <>
            {messages.map(renderMessage)}
            {isLoading && (
              <div style={{ textAlign: 'center', padding: 16 }}>
                <Spin size="small" />
                <Text type="secondary" style={{ marginLeft: 8 }}>
                  응답 생성 중...
                </Text>
              </div>
            )}
            <div ref={messagesEndRef} />
          </>
        )}
      </div>

      {/* 입력 영역 */}
      <div
        style={{
          padding: 12,
          borderTop: `1px solid ${MINT.PRIMARY}40`,
          backgroundColor: 'white',
        }}
      >
        <Space.Compact style={{ width: '100%' }}>
          <TextArea
            ref={inputRef as React.RefObject<any>}
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="질문을 입력하세요..."
            autoSize={{ minRows: 1, maxRows: 4 }}
            style={{ resize: 'none' }}
            disabled={isLoading}
          />
          <Button
            type="primary"
            icon={<SendOutlined />}
            onClick={handleSendMessage}
            loading={isLoading}
            disabled={!inputValue.trim()}
            style={{ backgroundColor: MINT.SEND_BTN, borderColor: MINT.SEND_BTN }}
          />
        </Space.Compact>
        <Text type="secondary" style={{ fontSize: 11, marginTop: 4, display: 'block' }}>
          Shift+Enter로 줄바꿈, Enter로 전송
        </Text>
      </div>
      {/* 대화 기록 Modal */}
      <Modal
        title={
          <Space>
            <HistoryOutlined style={{ color: MINT.PRIMARY }} />
            <span>대화 기록</span>
          </Space>
        }
        open={historyModalVisible}
        onCancel={() => setHistoryModalVisible(false)}
        footer={null}
        width={560}
        styles={{ body: { padding: 0, maxHeight: 480, overflow: 'auto' } }}
      >
        {!selectedSessionId ? (
          // 세션 목록
          <Spin spinning={sessionsLoading}>
            {sessions.length > 0 ? (
              <List
                dataSource={sessions}
                renderItem={(s: any) => {
                  const isCurrent = s.session_id === sessionId;
                  return (
                    <List.Item
                      style={{
                        padding: '12px 24px',
                        cursor: 'pointer',
                        background: isCurrent ? MINT.LIGHT : undefined,
                      }}
                      onClick={() => handleSelectSession(s.session_id)}
                    >
                      <List.Item.Meta
                        avatar={<Avatar style={{ background: isCurrent ? MINT.PRIMARY : '#d9d9d9' }} icon={<MessageOutlined />} />}
                        title={
                          <Space>
                            <span>{s.title || s.session_id?.slice(0, 8) || '세션'}</span>
                            {isCurrent && <Tag color="cyan" style={{ fontSize: 10 }}>현재</Tag>}
                          </Space>
                        }
                        description={
                          <Space size={16}>
                            <Text type="secondary" style={{ fontSize: 12 }}>
                              <ClockCircleOutlined /> {s.updated_at ? new Date(s.updated_at).toLocaleString('ko-KR') : s.created_at ? new Date(s.created_at).toLocaleString('ko-KR') : '-'}
                            </Text>
                            <Text type="secondary" style={{ fontSize: 12 }}>
                              메시지 {s.message_count ?? '?'}개
                            </Text>
                          </Space>
                        }
                      />
                    </List.Item>
                  );
                }}
              />
            ) : (
              <div style={{ textAlign: 'center', padding: 40 }}>
                <Text type="secondary">대화 기록이 없습니다.</Text>
              </div>
            )}
          </Spin>
        ) : (
          // 타임라인 뷰
          <div style={{ padding: '16px 24px' }}>
            <Button
              type="link"
              icon={<RollbackOutlined />}
              onClick={() => { setSelectedSessionId(null); setTimelineData([]); }}
              style={{ marginBottom: 12, padding: 0, color: MINT.PRIMARY }}
            >
              세션 목록으로
            </Button>
            <Spin spinning={timelineLoading}>
              {timelineData.length > 0 ? (
                <Timeline
                  items={timelineData.map((msg: any) => {
                    const isUser = msg.role === 'user';
                    const msgId = msg.message_id || msg.id;
                    // 저장된 페이지 상태 조회
                    let savedPageLabel = '';
                    if (isUser) {
                      try {
                        const stateKey = `ai_page_state_${selectedSessionId}`;
                        const states = JSON.parse(localStorage.getItem(stateKey) || '{}');
                        const ps = states[msgId];
                        if (ps?.label) savedPageLabel = ps.label;
                      } catch { /* ignore */ }
                    }
                    return {
                      color: isUser ? MINT.PRIMARY : '#d9d9d9',
                      dot: isUser ? <UserOutlined style={{ fontSize: 14 }} /> : <RobotOutlined style={{ fontSize: 14 }} />,
                      children: (
                        <div style={{ marginBottom: 4 }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                            <div style={{ flex: 1 }}>
                              <Space size={4} style={{ marginBottom: 4 }}>
                                <Tag color={isUser ? 'cyan' : 'default'} style={{ fontSize: 10 }}>
                                  {isUser ? '사용자' : 'AI'}
                                </Tag>
                                {savedPageLabel && (
                                  <Tag color="geekblue" style={{ fontSize: 9 }}>
                                    {savedPageLabel}
                                  </Tag>
                                )}
                              </Space>
                              <Text style={{ fontSize: 12, display: 'block' }}>
                                {(msg.content || '').length > 100 ? `${msg.content.slice(0, 100)}...` : msg.content}
                              </Text>
                              <Text type="secondary" style={{ fontSize: 10 }}>
                                {msg.timestamp ? new Date(msg.timestamp).toLocaleTimeString('ko-KR') : msg.created_at ? new Date(msg.created_at).toLocaleTimeString('ko-KR') : ''}
                              </Text>
                            </div>
                            {isUser && (
                              <Tooltip title={savedPageLabel ? `"${savedPageLabel}" 화면으로 복원` : '이 시점으로 복원'}>
                                <Button
                                  size="small"
                                  type="link"
                                  icon={<RollbackOutlined />}
                                  loading={restoringMessageId === msgId}
                                  onClick={() => handleRestore(selectedSessionId!, msgId)}
                                  style={{ color: MINT.PRIMARY, marginLeft: 8 }}
                                >
                                  복원
                                </Button>
                              </Tooltip>
                            )}
                          </div>
                        </div>
                      ),
                    };
                  })}
                />
              ) : (
                <div style={{ textAlign: 'center', padding: 40 }}>
                  <Text type="secondary">타임라인이 없습니다.</Text>
                </div>
              )}
            </Spin>
          </div>
        )}
      </Modal>
    </Drawer>
  );
};

export default AIAssistantPanel;
