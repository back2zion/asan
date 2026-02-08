import React, { useState, useRef, useEffect, useCallback } from 'react';
import {
  Card,
  Input,
  Button,
  List,
  Typography,
  Space,
  Badge,
  Progress,
  Alert,
  Tag,
  Divider,
  Spin,
  Avatar,
  Tooltip
} from 'antd';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import {
  SendOutlined,
  UserOutlined,
  RobotOutlined,
  MedicineBoxOutlined,
  HistoryOutlined,
  DeleteOutlined,
  TableOutlined
} from '@ant-design/icons';

const { TextArea } = Input;
const { Text } = Typography;

interface StreamEvent {
  event_type: string;
  data: any;
  timestamp: string;
  session_id: string;
}

interface Message {
  id: string;
  content: string;
  sender: 'user' | 'ai';
  timestamp: string;
  metadata?: any;
  tool_results?: any[];
}

interface StreamingMedicalChatProps {
  sessionId?: string;
  patientId?: string;
  userType?: 'patient' | 'doctor' | 'researcher';
  onSessionUpdate?: (sessionData: any) => void;
}

const STORAGE_KEY = 'asan_chat_messages';
const SESSION_KEY = 'asan_chat_session_id';

// Shared markdown component renderers
const markdownComponents: any = {
  p: ({ children }: any) => <p style={{ marginBottom: '8px', lineHeight: '1.6' }}>{children}</p>,
  code: ({ children, className }: any) => (
    className ? (
      <pre style={{
        background: '#f5f5f5',
        padding: '8px 12px',
        borderRadius: '6px',
        fontSize: '13px',
        overflow: 'auto',
        border: '1px solid #e0e0e0',
        whiteSpace: 'pre-wrap'
      }}>
        <code>{children}</code>
      </pre>
    ) : (
      <code style={{
        background: '#f5f5f5',
        padding: '2px 6px',
        borderRadius: '4px',
        fontSize: '13px',
        border: '1px solid #e0e0e0'
      }}>
        {children}
      </code>
    )
  ),
  ul: ({ children }: any) => <ul style={{ marginBottom: '8px', paddingLeft: '20px' }}>{children}</ul>,
  ol: ({ children }: any) => <ol style={{ marginBottom: '8px', paddingLeft: '20px' }}>{children}</ol>,
  li: ({ children }: any) => <li style={{ marginBottom: '4px' }}>{children}</li>,
  h1: ({ children }: any) => <h3 style={{ color: '#1a5d3a', marginBottom: '8px' }}>{children}</h3>,
  h2: ({ children }: any) => <h4 style={{ color: '#1a5d3a', marginBottom: '6px' }}>{children}</h4>,
  h3: ({ children }: any) => <h5 style={{ color: '#1a5d3a', marginBottom: '6px' }}>{children}</h5>,
  strong: ({ children }: any) => <strong style={{ color: '#1a5d3a' }}>{children}</strong>,
  blockquote: ({ children }: any) => (
    <blockquote style={{
      borderLeft: '4px solid #1a5d3a',
      paddingLeft: '12px',
      margin: '8px 0',
      fontStyle: 'italic',
      background: '#f9f9f9',
      padding: '8px 12px',
      borderRadius: '0 4px 4px 0'
    }}>
      {children}
    </blockquote>
  ),
  // Table components for remark-gfm
  table: ({ children }: any) => (
    <div style={{ overflowX: 'auto', marginBottom: '12px' }}>
      <table style={{
        width: '100%',
        borderCollapse: 'collapse',
        fontSize: '13px',
        border: '1px solid #e0e0e0',
        borderRadius: '6px',
      }}>
        {children}
      </table>
    </div>
  ),
  thead: ({ children }: any) => (
    <thead style={{ background: '#f0f9f4' }}>{children}</thead>
  ),
  tbody: ({ children }: any) => <tbody>{children}</tbody>,
  tr: ({ children }: any) => (
    <tr style={{ borderBottom: '1px solid #e8e8e8' }}>{children}</tr>
  ),
  th: ({ children }: any) => (
    <th style={{
      padding: '8px 12px',
      textAlign: 'left',
      fontWeight: 600,
      color: '#1a5d3a',
      borderBottom: '2px solid #1a5d3a',
      whiteSpace: 'nowrap'
    }}>
      {children}
    </th>
  ),
  td: ({ children }: any) => (
    <td style={{
      padding: '6px 12px',
      borderBottom: '1px solid #f0f0f0'
    }}>
      {children}
    </td>
  ),
};

const StreamingMedicalChat: React.FC<StreamingMedicalChatProps> = ({
  sessionId: propSessionId,
  patientId,
  userType = 'researcher',
  onSessionUpdate
}) => {
  // Restore session ID from localStorage or use prop
  const [sessionId] = useState<string>(() => {
    if (propSessionId) return propSessionId;
    const saved = localStorage.getItem(SESSION_KEY);
    if (saved) return saved;
    const newId = `session_${Date.now()}`;
    localStorage.setItem(SESSION_KEY, newId);
    return newId;
  });

  // Restore messages from localStorage
  const [messages, setMessages] = useState<Message[]>(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      return saved ? JSON.parse(saved) : [];
    } catch {
      return [];
    }
  });

  const [currentInput, setCurrentInput] = useState('');
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamingMessage, setStreamingMessage] = useState('');
  const [progress, setProgress] = useState(0);
  const [currentStep, setCurrentStep] = useState('');
  const [memoryContext, setMemoryContext] = useState<any>({});
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);

  // Persist messages to localStorage
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(messages));
    } catch { /* storage full - ignore */ }
  }, [messages]);

  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, streamingMessage, scrollToBottom]);

  const connectEventSource = useCallback((query: string) => {
    const controller = new AbortController();
    abortRef.current = controller;

    fetch('/api/v1/chat/stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message: query,
        session_id: sessionId,
        user_id: userType,
      }),
      signal: controller.signal,
    })
    .then(response => {
      if (!response.ok) throw new Error('스트리밍 요청 실패');

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();

      const readStream = async () => {
        if (!reader) return;
        let buffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            setIsStreaming(false);
            setIsConnected(false);
            break;
          }

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          // Keep incomplete last line in buffer
          buffer = lines.pop() || '';

          for (const line of lines) {
            if (line.startsWith('data: ')) {
              try {
                const eventData: StreamEvent = JSON.parse(line.substring(6));
                handleStreamEvent(eventData);
              } catch (e) {
                // skip malformed lines
              }
            }
          }
        }
      };

      setIsConnected(true);
      setIsStreaming(true);
      readStream().catch(err => {
        if (err.name !== 'AbortError') {
          setError(err.message);
        }
        setIsStreaming(false);
        setIsConnected(false);
      });
    })
    .catch(err => {
      if (err.name !== 'AbortError') {
        setError(err.message);
        setIsStreaming(false);
        setIsConnected(false);
      }
    });
  }, [sessionId, userType]);

  const handleStreamEvent = useCallback((event: StreamEvent) => {
    switch (event.event_type) {
      case 'session_start':
        setProgress(10);
        setCurrentStep('세션 시작');
        break;

      case 'memory_context':
        setMemoryContext(event.data);
        setProgress(20);
        setCurrentStep('기존 대화 이력 조회');
        if (onSessionUpdate) onSessionUpdate(event.data);
        break;

      case 'token':
        setStreamingMessage(prev => prev + (event.data?.content || ''));
        setProgress(prev => Math.min(prev + 2, 90));
        break;

      case 'step_update':
        setCurrentStep(event.data?.step || '');
        setProgress(prev => Math.min(prev + 15, 85));
        if (event.data?.step === 'model' && event.data?.content) {
          setStreamingMessage(prev => prev + event.data.content);
        }
        break;

      case 'completion':
        setProgress(100);
        setCurrentStep('완료');
        setStreamingMessage(currentContent => {
          const finalContent = currentContent.trim();
          if (finalContent) {
            const newMessage: Message = {
              id: `msg_${Date.now()}`,
              content: finalContent,
              sender: 'ai',
              timestamp: new Date().toISOString(),
              metadata: event.data?.final_memory,
              tool_results: event.data?.tool_results,
            };
            setMessages(prev => [...prev, newMessage]);
          }
          return '';
        });
        setIsStreaming(false);
        setIsConnected(false);
        if (event.data?.final_memory) {
          setMemoryContext(event.data.final_memory);
        }
        break;

      case 'error':
        setError(event.data?.error_message || '알 수 없는 오류');
        setIsStreaming(false);
        setIsConnected(false);
        break;
    }
  }, [onSessionUpdate]);

  const handleSendMessage = useCallback(() => {
    if (!currentInput.trim() || isStreaming) return;

    const userMessage: Message = {
      id: `msg_${Date.now()}`,
      content: currentInput.trim(),
      sender: 'user',
      timestamp: new Date().toISOString(),
    };

    setMessages(prev => [...prev, userMessage]);
    setStreamingMessage('');
    setProgress(0);
    setCurrentStep('');
    setError(null);

    connectEventSource(currentInput.trim());
    setCurrentInput('');
  }, [currentInput, isStreaming, connectEventSource]);

  const handleKeyPress = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  }, [handleSendMessage]);

  const handleClearHistory = useCallback(() => {
    if (isStreaming) return;
    setMessages([]);
    localStorage.removeItem(STORAGE_KEY);
    const newSessionId = `session_${Date.now()}`;
    localStorage.setItem(SESSION_KEY, newSessionId);
  }, [isStreaming]);

  const renderToolResults = (toolResults: any[]) => {
    if (!toolResults || toolResults.length === 0) return null;
    return toolResults.map((result: any, idx: number) => {
      if (!result.columns || !result.results) return null;
      return (
        <div key={idx} style={{ overflowX: 'auto', marginTop: '8px' }}>
          <div style={{ marginBottom: '4px' }}>
            <Tag icon={<TableOutlined />} color="green">
              {result.results.length}건 조회
            </Tag>
          </div>
          <table style={{
            width: '100%',
            borderCollapse: 'collapse',
            fontSize: '12px',
            border: '1px solid #e0e0e0'
          }}>
            <thead style={{ background: '#f0f9f4' }}>
              <tr>
                {result.columns.map((col: string, i: number) => (
                  <th key={i} style={{
                    padding: '6px 8px',
                    textAlign: 'left',
                    fontWeight: 600,
                    color: '#1a5d3a',
                    borderBottom: '2px solid #1a5d3a',
                    whiteSpace: 'nowrap'
                  }}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.results.slice(0, 20).map((row: any[], ri: number) => (
                <tr key={ri}>
                  {row.map((cell: any, ci: number) => (
                    <td key={ci} style={{
                      padding: '4px 8px',
                      borderBottom: '1px solid #f0f0f0',
                      maxWidth: '200px',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis'
                    }}>{cell ?? '-'}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {result.results.length > 20 && (
            <Text type="secondary" style={{ fontSize: '11px' }}>
              ... {result.results.length - 20}건 더 있음
            </Text>
          )}
        </div>
      );
    });
  };

  const renderMessage = (message: Message) => (
    <List.Item key={message.id} style={{ padding: '12px 0' }}>
      <List.Item.Meta
        avatar={
          <Avatar
            icon={message.sender === 'user' ? <UserOutlined /> : <RobotOutlined />}
            style={{
              backgroundColor: message.sender === 'user' ? '#1a5d3a' : '#52c41a'
            }}
          />
        }
        title={
          <Space>
            <Text strong>
              {message.sender === 'user' ? '사용자' : '의료 AI'}
            </Text>
            <Text type="secondary" style={{ fontSize: '12px' }}>
              {new Date(message.timestamp).toLocaleTimeString()}
            </Text>
          </Space>
        }
        description={
          message.sender === 'ai' ? (
            <div style={{ marginBottom: 0 }}>
              <ReactMarkdown
                remarkPlugins={[remarkGfm]}
                components={markdownComponents}
              >
                {message.content}
              </ReactMarkdown>
              {renderToolResults(message.tool_results || [])}
            </div>
          ) : (
            <div style={{
              marginBottom: 0,
              whiteSpace: 'pre-wrap',
              lineHeight: '1.6',
              fontSize: '14px'
            }}>
              {message.content}
            </div>
          )
        }
      />
    </List.Item>
  );

  return (
    <div style={{
      height: '100vh',
      display: 'flex',
      flexDirection: 'column',
      maxHeight: '100vh',
      overflow: 'hidden'
    }}>
      {/* Header */}
      <Card style={{
        marginBottom: 16,
        flexShrink: 0,
        background: 'linear-gradient(135deg, #ffffff 0%, #f0f9f4 100%)',
        border: '1px solid #e6f4ea',
        borderRadius: '12px',
        boxShadow: '0 4px 12px rgba(26, 93, 58, 0.08)'
      }}>
        <Space split={<Divider type="vertical" />} wrap>
          <Space>
            <MedicineBoxOutlined style={{ color: '#1a5d3a', fontSize: '22px' }} />
            <Text strong style={{ color: '#1a5d3a', fontSize: '16px' }}>서울아산병원 의료 AI</Text>
          </Space>

          <Space>
            <Badge
              status={isConnected ? 'processing' : 'default'}
              text={isConnected ? '스트리밍 중' : '대기중'}
            />
          </Space>

          <Space>
            <Text type="secondary">대화:</Text>
            <Tag color="green">{messages.length}건</Tag>
          </Space>

          <Tooltip title="대화 기록 삭제">
            <Button
              icon={<DeleteOutlined />}
              size="small"
              danger
              onClick={handleClearHistory}
              disabled={isStreaming || messages.length === 0}
            >
              초기화
            </Button>
          </Tooltip>
        </Space>

        {/* Memory context */}
        {memoryContext && Object.keys(memoryContext).length > 0 && (
          <div style={{
            marginTop: 12,
            padding: '12px 16px',
            background: 'linear-gradient(135deg, #e8f5e8 0%, #f1f8e9 100%)',
            borderRadius: '8px',
            border: '1px solid #a5d6a7'
          }}>
            <Space wrap>
              {memoryContext.previous_symptoms?.length > 0 && (
                <Space>
                  <Text type="secondary">기록된 증상:</Text>
                  {memoryContext.previous_symptoms.map((symptom: string) => (
                    <Tag key={symptom} color="red">{symptom}</Tag>
                  ))}
                </Space>
              )}
              {memoryContext.medication_history?.length > 0 && (
                <Space>
                  <Text type="secondary">약물 이력:</Text>
                  {memoryContext.medication_history.map((med: string) => (
                    <Tag key={med} color="blue">{med}</Tag>
                  ))}
                </Space>
              )}
              <Text type="secondary">
                총 {memoryContext.message_count || 0}개 대화
              </Text>
            </Space>
          </div>
        )}
      </Card>

      {/* Streaming progress */}
      {isStreaming && (
        <Card style={{
          marginBottom: 16,
          flexShrink: 0,
          background: 'linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%)',
          border: '1px solid #ffb74d',
          borderRadius: '8px'
        }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            <Progress
              percent={progress}
              status={error ? 'exception' : 'active'}
              showInfo={false}
            />
            <Space>
              <Spin size="small" />
              <Text>{currentStep || '처리 중...'}</Text>
            </Space>
          </Space>
        </Card>
      )}

      {/* Error */}
      {error && (
        <Alert
          message="오류"
          description={error}
          type="error"
          closable
          onClose={() => setError(null)}
          style={{
            marginBottom: 16,
            flexShrink: 0,
            borderRadius: '8px',
            border: '1px solid #f44336'
          }}
        />
      )}

      {/* Chat messages */}
      <Card
        title={
          <Space>
            <HistoryOutlined style={{ color: '#1a5d3a' }} />
            <Text style={{ color: '#1a5d3a', fontWeight: 600 }}>대화 이력</Text>
            <Badge count={messages.length} style={{ backgroundColor: '#1a5d3a' }} />
          </Space>
        }
        style={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          background: '#ffffff',
          border: '1px solid #e6f4ea',
          borderRadius: '12px',
          boxShadow: '0 4px 12px rgba(26, 93, 58, 0.08)',
          minHeight: 0,
        }}
        styles={{
          body: {
            flex: 1,
            overflow: 'auto',
            padding: '16px',
            maxHeight: 'calc(100vh - 300px)',
            display: 'flex',
            flexDirection: 'column'
          }
        }}
      >
        <div style={{ flex: 1, overflow: 'auto' }}>
          <List
            dataSource={messages}
            renderItem={renderMessage}
            locale={{ emptyText: '대화를 시작해보세요! 예: "진단 테이블 보여줘", "당뇨 환자 몇 명?"' }}
            style={{ height: '100%' }}
          />
        </div>

        {/* Streaming message preview */}
        {isStreaming && streamingMessage && (
          <div style={{
            padding: '12px 0',
            opacity: 0.9,
            borderTop: '1px solid #f0f0f0',
            marginTop: '8px',
            background: 'rgba(25, 118, 210, 0.02)',
            borderRadius: '8px',
            margin: '8px 0'
          }}>
            <List.Item style={{ padding: '12px 16px' }}>
              <List.Item.Meta
                avatar={<Avatar icon={<RobotOutlined />} style={{ backgroundColor: '#52c41a' }} />}
                title={
                  <Space>
                    <Text strong>의료 AI</Text>
                    <Spin size="small" />
                    <Text type="secondary" style={{ fontSize: '12px' }}>실시간 응답 중...</Text>
                  </Space>
                }
                description={
                  <div style={{ marginBottom: 0 }}>
                    <ReactMarkdown
                      remarkPlugins={[remarkGfm]}
                      components={markdownComponents}
                    >
                      {streamingMessage}
                    </ReactMarkdown>
                    <span className="streaming-cursor" style={{ color: '#1a5d3a', fontWeight: 'bold' }}>|</span>
                  </div>
                }
              />
            </List.Item>
          </div>
        )}

        <div ref={messagesEndRef} />
      </Card>

      {/* Input */}
      <Card style={{
        marginTop: 16,
        flexShrink: 0,
        background: 'linear-gradient(135deg, #ffffff 0%, #f0f9f4 100%)',
        border: '1px solid #e6f4ea',
        borderRadius: '12px',
        boxShadow: '0 4px 12px rgba(26, 93, 58, 0.08)'
      }}>
        <Space.Compact style={{ width: '100%' }}>
          <TextArea
            value={currentInput}
            onChange={(e) => setCurrentInput(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="질문을 입력하세요... 예: 진단 테이블 찾아줘, 당뇨 환자 몇명이야?"
            rows={2}
            disabled={isStreaming}
            style={{
              flex: 1,
              borderColor: '#e3f2fd',
              borderRadius: '8px',
              fontSize: '14px'
            }}
          />
          <Button
            type="primary"
            icon={<SendOutlined />}
            onClick={handleSendMessage}
            disabled={!currentInput.trim() || isStreaming}
            className="send-button"
            style={{
              height: '40px',
              background: 'linear-gradient(135deg, #1a5d3a 0%, #165030 100%)',
              border: 'none',
              borderRadius: '8px',
              fontWeight: 600
            }}
          >
            전송
          </Button>
        </Space.Compact>

        <div style={{ marginTop: 12, textAlign: 'center' }}>
          <Text style={{ fontSize: '12px', color: '#607d8b' }}>
            Enter: 전송 | Shift+Enter: 줄바꿈 | 실시간 스트리밍 지원
          </Text>
        </div>
      </Card>

      <style>
        {`
          .streaming-cursor {
            animation: blink 1s infinite;
            font-weight: bold;
          }

          @keyframes blink {
            0%, 50% { opacity: 1; }
            51%, 100% { opacity: 0; }
          }

          .send-button, .send-button:hover, .send-button:focus, .send-button:active {
            color: #ffffff !important;
          }

          .send-button.ant-btn[disabled], .send-button.ant-btn[disabled]:hover {
            color: rgba(255, 255, 255, 0.6) !important;
          }
        `}
      </style>
    </div>
  );
};

export default StreamingMedicalChat;
