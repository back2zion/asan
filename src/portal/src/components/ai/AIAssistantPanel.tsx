/**
 * AI Assistant 패널 컴포넌트
 * DPR-001: 우측 AI Assistant (보조 도구)
 * 시큐어 코딩 적용
 */

import React, { useState, useRef, useEffect, useCallback } from 'react';
import { Input, Button, Spin, Typography, Space, Tooltip, Badge, Drawer } from 'antd';
import {
  SendOutlined,
  RobotOutlined,
  CloseOutlined,
  HistoryOutlined,
  ReloadOutlined,
  ExpandOutlined,
  CompressOutlined,
} from '@ant-design/icons';
import { chatApi, sanitizeHtml } from '../../services/api';
import type { ChatResponse } from '../../services/api';
import ReactMarkdown from 'react-markdown';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  toolResults?: Record<string, unknown>[];
  suggestedActions?: Record<string, unknown>[];
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
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [isExpanded, setIsExpanded] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // 메시지 목록 스크롤
  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, scrollToBottom]);

  // 패널 열릴 때 입력창 포커스
  useEffect(() => {
    if (visible) {
      setTimeout(() => inputRef.current?.focus(), 100);
    }
  }, [visible]);

  // 메시지 전송
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
        user_id: 'current_user', // TODO: 실제 사용자 ID로 대체
        context: currentContext,
      });

      setSessionId(response.session_id);

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

  // Enter 키 처리
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  // 대화 초기화
  const handleReset = () => {
    setMessages([]);
    setSessionId(null);
  };

  // 추천 액션 클릭
  const handleSuggestedAction = (action: Record<string, unknown>) => {
    if (action.action === 'view_table' && action.target) {
      // 테이블 보기 액션
      window.dispatchEvent(
        new CustomEvent('navigate-to-table', { detail: { table: action.target } })
      );
    } else if (action.action === 'search' && action.query) {
      // 검색 액션
      setInputValue(action.query as string);
    }
  };

  // 메시지 렌더링
  const renderMessage = (message: Message) => {
    const isUser = message.role === 'user';

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
            backgroundColor: isUser ? '#005BAC' : '#f5f5f5',
            color: isUser ? 'white' : 'inherit',
          }}
        >
          {isUser ? (
            <Text style={{ color: 'white' }}>{message.content}</Text>
          ) : (
            <div className="ai-message-content">
              <ReactMarkdown
                components={{
                  // 보안: 링크에 rel 속성 추가
                  a: ({ href, children }) => (
                    <a
                      href={href}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{ color: '#005BAC' }}
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

          {/* 추천 액션 */}
          {message.suggestedActions && message.suggestedActions.length > 0 && (
            <div style={{ marginTop: 8 }}>
              <Space wrap>
                {message.suggestedActions.map((action, index) => (
                  <Button
                    key={index}
                    size="small"
                    type="link"
                    onClick={() => handleSuggestedAction(action)}
                    style={{ padding: '0 4px', height: 'auto' }}
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
          <RobotOutlined style={{ color: '#005BAC' }} />
          <span>AI Assistant</span>
          {sessionId && (
            <Badge
              status="processing"
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
        body: {
          padding: 0,
          display: 'flex',
          flexDirection: 'column',
          height: '100%',
        },
      }}
    >
      {/* 메시지 영역 */}
      <div
        style={{
          flex: 1,
          overflowY: 'auto',
          padding: 16,
          backgroundColor: '#fafafa',
        }}
      >
        {messages.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '40px 20px' }}>
            <RobotOutlined style={{ fontSize: 48, color: '#d9d9d9' }} />
            <Paragraph type="secondary" style={{ marginTop: 16 }}>
              안녕하세요! 데이터 검색, 테이블 분석, SQL 쿼리 생성 등을 도와드립니다.
            </Paragraph>
            <Space direction="vertical" style={{ marginTop: 16 }}>
              <Button
                type="dashed"
                size="small"
                onClick={() => setInputValue('진단 테이블 찾아줘')}
              >
                "진단 테이블 찾아줘"
              </Button>
              <Button
                type="dashed"
                size="small"
                onClick={() => setInputValue('환자 정보와 진료 기록 JOIN 쿼리')}
              >
                "환자 정보와 진료 기록 JOIN 쿼리"
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
          borderTop: '1px solid #f0f0f0',
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
            style={{ backgroundColor: '#005BAC' }}
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
