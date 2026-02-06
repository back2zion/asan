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
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { chatApi, sanitizeHtml } from '../../services/api';
import type { ChatResponse } from '../../services/api';
import ReactMarkdown from 'react-markdown';

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
}

interface AIAssistantPanelProps {
  visible: boolean;
  onClose: () => void;
  currentContext?: {
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
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [isExpanded, setIsExpanded] = useState(false);
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

    const userMessage: Message = {
      id: `user_${Date.now()}`,
      role: 'user',
      content: trimmedInput,
      timestamp: new Date(),
    };

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
                }}
              >
                {sanitizeHtml(message.content)}
              </ReactMarkdown>
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
            <Button icon={<HistoryOutlined />} type="text" size="small" />
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
    </Drawer>
  );
};

export default AIAssistantPanel;
