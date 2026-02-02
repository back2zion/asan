import React, { useState, useRef, useEffect } from 'react';
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
  Avatar
} from 'antd';
import ReactMarkdown from 'react-markdown';
import {
  SendOutlined,
  UserOutlined,
  RobotOutlined,
  MedicineBoxOutlined,
  ToolOutlined,
  HistoryOutlined
} from '@ant-design/icons';

const { TextArea } = Input;
const { Text, Paragraph } = Typography;

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
}

interface StreamingMedicalChatProps {
  sessionId?: string;
  patientId?: string;
  userType?: 'patient' | 'doctor' | 'researcher';
  onSessionUpdate?: (sessionData: any) => void;
}

const StreamingMedicalChat: React.FC<StreamingMedicalChatProps> = ({
  sessionId = `session_${Date.now()}`,
  patientId,
  userType = 'patient',
  onSessionUpdate
}) => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [currentInput, setCurrentInput] = useState('');
  const [isStreaming, setIsStreaming] = useState(false);
  const [streamingMessage, setStreamingMessage] = useState('');
  const [progress, setProgress] = useState(0);
  const [currentStep, setCurrentStep] = useState('');
  const [memoryContext, setMemoryContext] = useState<any>({});
  const [streamMode] = useState<'updates' | 'messages' | 'custom' | 'multi'>('updates');
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  
  // const eventSourceRef = useRef<EventSource | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, streamingMessage]);

  const connectEventSource = (query: string) => {
    const url = 'http://localhost:8001/api/v1/streaming/medical-query';
    
    // POST 요청으로 스트리밍 시작
    fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query,
        session_id: sessionId,
        user_type: userType,
        urgency_level: 'medium',
        patient_id: patientId,
        stream_mode: streamMode
      }),
    })
    .then(response => {
      if (!response.ok) {
        throw new Error('스트리밍 요청 실패');
      }
      
      const reader = response.body?.getReader();
      const decoder = new TextDecoder();
      
      const readStream = async () => {
        if (!reader) return;
        
        while (true) {
          const { done, value } = await reader.read();
          
          if (done) {
            setIsStreaming(false);
            setIsConnected(false);
            break;
          }
          
          const chunk = decoder.decode(value);
          const lines = chunk.split('\\n');
          
          for (const line of lines) {
            if (line.startsWith('data: ')) {
              try {
                const eventData = JSON.parse(line.substring(6));
                handleStreamEvent(eventData);
              } catch (e) {
                console.warn('이벤트 파싱 오류:', e);
              }
            }
          }
        }
      };
      
      setIsConnected(true);
      setIsStreaming(true);
      readStream().catch(error => {
        console.error('스트림 읽기 오류:', error);
        setError(error.message);
        setIsStreaming(false);
        setIsConnected(false);
      });
    })
    .catch(error => {
      console.error('스트리밍 연결 오류:', error);
      setError(error.message);
      setIsStreaming(false);
      setIsConnected(false);
    });
  };

  const handleStreamEvent = (event: StreamEvent) => {
    console.log('Stream Event:', event);

    switch (event.event_type) {
      case 'session_start':
        setProgress(10);
        setCurrentStep('세션 시작');
        break;

      case 'memory_context':
        setMemoryContext(event.data);
        setProgress(20);
        setCurrentStep('기존 대화 이력 조회');
        if (onSessionUpdate) {
          onSessionUpdate(event.data);
        }
        break;

      case 'token':
        // 토큰 단위 스트리밍
        const tokenContent = event.data?.content || '';
        console.log('Token received:', tokenContent);
        setStreamingMessage(prev => {
          const newContent = prev + tokenContent;
          console.log('Updated streaming message:', newContent);
          return newContent;
        });
        setProgress(prev => Math.min(prev + 2, 90));
        break;

      case 'step_update':
        // 단계별 업데이트
        setCurrentStep(event.data?.step || '');
        setProgress(prev => Math.min(prev + 15, 85));
        
        if (event.data?.step === 'model' && event.data?.content) {
          setStreamingMessage(prev => prev + event.data.content);
        }
        break;

      case 'custom_update':
        // 커스텀 업데이트 (도구 실행 등)
        setCurrentStep(`🔧 ${event.data.message}`);
        break;

      case 'multi_updates':
      case 'multi_custom':
        // 다중 모드 업데이트
        setCurrentStep(`[${event.data.stream_type}] 처리 중...`);
        if (event.data.content) {
          setStreamingMessage(prev => prev + event.data.content);
        }
        break;

      case 'completion':
        setProgress(100);
        setCurrentStep('완료');
        
        // 현재 streamingMessage 상태를 얻기 위해 콜백 사용
        setStreamingMessage(currentStreamingMessage => {
          const finalContent = currentStreamingMessage.trim();
          console.log('Final streaming content:', finalContent);
          
          if (finalContent) {
            const newMessage: Message = {
              id: `msg_${Date.now()}`,
              content: finalContent,
              sender: 'ai',
              timestamp: new Date().toISOString(),
              metadata: event.data?.final_memory
            };
            
            setMessages(prev => {
              console.log('Adding final message:', newMessage);
              return [...prev, newMessage];
            });
          } else {
            console.warn('No streaming message to save');
          }
          
          return ''; // 스트리밍 메시지 초기화
        });
        
        // 상태 업데이트
        setIsStreaming(false);
        setIsConnected(false);
        
        // 메모리 컨텍스트 업데이트
        if (event.data?.final_memory) {
          setMemoryContext(event.data.final_memory);
        }
        break;

      case 'error':
        setError(event.data.error_message);
        setIsStreaming(false);
        setIsConnected(false);
        setCurrentStep('오류 발생');
        break;

      default:
        console.log('알 수 없는 이벤트:', event);
    }
  };

  const handleSendMessage = () => {
    if (!currentInput.trim() || isStreaming) return;

    // 사용자 메시지 추가
    const userMessage: Message = {
      id: `msg_${Date.now()}`,
      content: currentInput.trim(),
      sender: 'user',
      timestamp: new Date().toISOString()
    };

    console.log('Adding user message:', userMessage);
    setMessages(prev => {
      const newMessages = [...prev, userMessage];
      console.log('Updated messages:', newMessages);
      return newMessages;
    });
    
    // 스트리밍 시작
    setStreamingMessage('');
    setProgress(0);
    setCurrentStep('');
    setError(null);
    
    connectEventSource(currentInput.trim());
    setCurrentInput('');
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSendMessage();
    }
  };

  const testStream = () => {
    fetch('http://localhost:8001/api/v1/streaming/test-stream')
      .then(response => response.body?.getReader())
      .then(reader => {
        if (!reader) return;
        
        const decoder = new TextDecoder();
        const readStream = () => {
          reader.read().then(({ done, value }) => {
            if (done) return;
            
            const chunk = decoder.decode(value);
            console.log('테스트 스트림:', chunk);
            
            readStream();
          });
        };
        
        readStream();
      })
      .catch(error => console.error('테스트 스트림 오류:', error));
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
              {message.sender === 'user' ? '환자' : '의료 AI'}
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
                components={{
                  p: ({ children }) => <p style={{ marginBottom: '8px', lineHeight: '1.6' }}>{children}</p>,
                  code: ({ children, className }) => (
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
                  ul: ({ children }) => <ul style={{ marginBottom: '8px', paddingLeft: '20px' }}>{children}</ul>,
                  ol: ({ children }) => <ol style={{ marginBottom: '8px', paddingLeft: '20px' }}>{children}</ol>,
                  li: ({ children }) => <li style={{ marginBottom: '4px' }}>{children}</li>,
                  h1: ({ children }) => <h3 style={{ color: '#1a5d3a', marginBottom: '8px' }}>{children}</h3>,
                  h2: ({ children }) => <h4 style={{ color: '#1a5d3a', marginBottom: '6px' }}>{children}</h4>,
                  h3: ({ children }) => <h5 style={{ color: '#1a5d3a', marginBottom: '6px' }}>{children}</h5>,
                  strong: ({ children }) => <strong style={{ color: '#1a5d3a' }}>{children}</strong>,
                  blockquote: ({ children }) => (
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
                  )
                }}
              >
                {message.content}
              </ReactMarkdown>
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
      {/* 헤더 */}
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
            <Text type="secondary">세션:</Text>
            <Text code>{sessionId}</Text>
          </Space>
          
          <Space>
            <Text type="secondary">사용자:</Text>
            <Tag color={userType === 'doctor' ? 'green' : userType === 'researcher' ? 'blue' : 'orange'}>
              {userType === 'doctor' ? '의료진' : userType === 'researcher' ? '연구자' : '환자'}
            </Tag>
          </Space>
          
          <Space>
            <Badge 
              status={isConnected ? 'processing' : 'default'} 
              text={isConnected ? '연결됨' : '대기중'}
            />
          </Space>
          
          <Space>
            <Text type="secondary">스트림 모드:</Text>
            <Tag color="purple">{streamMode}</Tag>
          </Space>
        </Space>

        {/* 메모리 컨텍스트 표시 */}
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

      {/* 스트리밍 진행 상태 */}
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

      {/* 에러 표시 */}
      {error && (
        <Alert
          message="스트리밍 오류"
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

      {/* 대화 목록 */}
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
          height: 'auto'
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
            locale={{ emptyText: '대화를 시작해보세요!' }}
            style={{ height: '100%' }}
          />
        </div>
        
        {/* 실시간 스트리밍 메시지 */}
        {isStreaming && streamingMessage && (
          <div style={{ 
            padding: '12px 0', 
            opacity: 0.8,
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
                    components={{
                      p: ({ children }) => <p style={{ marginBottom: '8px', lineHeight: '1.6' }}>{children}</p>,
                      code: ({ children, className }) => (
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
                      ul: ({ children }) => <ul style={{ marginBottom: '8px', paddingLeft: '20px' }}>{children}</ul>,
                      ol: ({ children }) => <ol style={{ marginBottom: '8px', paddingLeft: '20px' }}>{children}</ol>,
                      li: ({ children }) => <li style={{ marginBottom: '4px' }}>{children}</li>,
                      h1: ({ children }) => <h3 style={{ color: '#1a5d3a', marginBottom: '8px' }}>{children}</h3>,
                      h2: ({ children }) => <h4 style={{ color: '#1a5d3a', marginBottom: '6px' }}>{children}</h4>,
                      h3: ({ children }) => <h5 style={{ color: '#1a5d3a', marginBottom: '6px' }}>{children}</h5>,
                      strong: ({ children }) => <strong style={{ color: '#1a5d3a' }}>{children}</strong>,
                      blockquote: ({ children }) => (
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
                      )
                    }}
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

      {/* 입력 영역 */}
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
            placeholder={`의료 질의를 입력하세요... (${userType === 'doctor' ? '의료진' : userType === 'researcher' ? '연구자' : '환자'} 모드)`}
            rows={2}
            disabled={isStreaming}
            style={{ 
              flex: 1,
              borderColor: '#e3f2fd',
              borderRadius: '8px',
              fontSize: '14px'
            }}
          />
          <div style={{ display: 'flex', gap: '8px' }}>
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
            <Button
              icon={<ToolOutlined />}
              onClick={testStream}
              disabled={isStreaming}
              style={{ 
                height: '40px',
                borderColor: '#1a5d3a',
                color: '#1a5d3a',
                borderRadius: '8px'
              }}
              title="스트리밍 테스트"
            >
              테스트
            </Button>
          </div>
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