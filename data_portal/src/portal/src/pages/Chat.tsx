import React, { useState, useEffect } from 'react';
import { Card, Select, Space, Typography, Tag, Alert } from 'antd';
import { UserOutlined, MedicineBoxOutlined, ExperimentOutlined } from '@ant-design/icons';
import StreamingMedicalChat from '../components/StreamingMedicalChat';

const { Title, Text } = Typography;
const { Option } = Select;

const Chat: React.FC = () => {
  const [userType, setUserType] = useState<'patient' | 'doctor' | 'researcher'>('patient');
  const [sessionId, setSessionId] = useState(`session_${Date.now()}`);
  const [connectionStatus, setConnectionStatus] = useState<'connecting' | 'connected' | 'disconnected'>('connecting');
  const [modelStatus, setModelStatus] = useState<{loaded: boolean, name: string}>({loaded: false, name: 'Loading...'});

  // 사용자 타입별 설정
  const userTypeConfig = {
    patient: {
      title: '환자 상담',
      description: '의료 정보 문의 및 건강 상담',
      icon: <UserOutlined />,
      color: 'orange'
    },
    doctor: {
      title: '임상 진단 지원',
      description: '진단 보조 및 임상 의사결정 지원',
      icon: <MedicineBoxOutlined />,
      color: 'green'
    },
    researcher: {
      title: '의료 연구 분석',
      description: '연구 데이터 분석 및 통계 자문',
      icon: <ExperimentOutlined />,
      color: 'blue'
    }
  };

  // 세션 상태 모니터링 — /api/v1 프록시 사용 (Vite proxy → FastAPI)
  useEffect(() => {
    const controller = new AbortController();

    const checkConnection = async () => {
      try {
        const response = await fetch(`/api/v1/health`, {
          signal: controller.signal,
        });
        if (response.ok) {
          setConnectionStatus('connected');
          setModelStatus({ loaded: true, name: 'Qwen3-LLM' });
        } else {
          setConnectionStatus('disconnected');
        }
      } catch {
        if (!controller.signal.aborted) {
          setConnectionStatus('disconnected');
        }
      }
    };

    // 초기 연결 확인을 2초 후에 시작 (서버 로딩 시간 고려)
    const initTimer = setTimeout(checkConnection, 2000);
    // 30초마다 연결 상태 확인
    const interval = setInterval(checkConnection, 30000);
    return () => {
      controller.abort();
      clearTimeout(initTimer);
      clearInterval(interval);
    };
  }, [sessionId]);

  const handleUserTypeChange = (newUserType: 'patient' | 'doctor' | 'researcher') => {
    setUserType(newUserType);
    setSessionId(`session_${newUserType}_${Date.now()}`); // 새 세션 ID 생성
  };

  const handleSessionUpdate = (sessionData: any) => {
    if (sessionData.status === 'connected') {
      setConnectionStatus('connected');
    }
  };

  return (
    <div style={{
      height: '100%',
      minHeight: 0,
      display: 'flex',
      flexDirection: 'column',
      background: 'transparent'
    }}>
      {/* 헤더 - 사용자 타입 선택 */}
      <Card style={{ 
        marginBottom: '20px', 
        flexShrink: 0,
        background: 'linear-gradient(135deg, #ffffff 0%, #f0f9f4 100%)',
        border: '1px solid #e6f4ea',
        borderRadius: '12px',
        boxShadow: '0 4px 16px rgba(26, 93, 58, 0.08)'
      }}>
        <Space align="center" style={{ width: '100%', justifyContent: 'space-between' }}>
          <Space align="center">
            <Title level={4} style={{ margin: 0, color: '#1a5d3a', fontWeight: 600 }}>
              서울아산병원 AI 의료 플랫폼
            </Title>
            <Tag color={userTypeConfig[userType].color}>
              {userTypeConfig[userType].icon}
              {userTypeConfig[userType].title}
            </Tag>
          </Space>
          
          <Space align="center">
            <Text type="secondary">사용자 유형:</Text>
            <Select
              value={userType}
              onChange={handleUserTypeChange}
              style={{ minWidth: 140 }}
            >
              <Option value="patient">
                <Space>
                  <UserOutlined />
                  환자
                </Space>
              </Option>
              <Option value="doctor">
                <Space>
                  <MedicineBoxOutlined />
                  의료진
                </Space>
              </Option>
              <Option value="researcher">
                <Space>
                  <ExperimentOutlined />
                  연구자
                </Space>
              </Option>
            </Select>
          </Space>
        </Space>
        
        <div style={{ marginTop: '8px' }}>
          <Text style={{ color: '#5b8f72', fontSize: '14px' }}>
            {userTypeConfig[userType].description}
          </Text>
        </div>

        {/* 연결 상태 알림 */}
        {connectionStatus === 'connecting' && (
          <Alert
            message="🔄 서버 연결 중..."
            type="warning"
            showIcon
            style={{ 
              marginTop: '12px',
              borderRadius: '8px',
              border: '1px solid #ff9800',
              background: '#fff8e1'
            }}
          />
        )}
        {connectionStatus === 'connected' && !modelStatus.loaded && (
          <Alert
            message={`🤖 AI 서버 연결됨 - ${modelStatus.name} 모델 로딩 중...`}
            type="info"
            showIcon
            style={{ 
              marginTop: '12px',
              borderRadius: '8px',
              border: '1px solid #1a5d3a',
              background: '#f0f9f4'
            }}
          />
        )}
        {connectionStatus === 'connected' && modelStatus.loaded && (
          <Alert
            message={`✅ ${modelStatus.name} 모델 준비 완료`}
            type="success"
            showIcon
            style={{ 
              marginTop: '12px',
              borderRadius: '8px',
              border: '1px solid #4caf50',
              background: '#e8f5e8'
            }}
          />
        )}
        {connectionStatus === 'disconnected' && (
          <Alert
            message="⚠️ 서버 연결 실패"
            type="error"
            showIcon
            style={{ 
              marginTop: '12px',
              borderRadius: '8px',
              border: '1px solid #f44336',
              background: '#ffebee'
            }}
          />
        )}
      </Card>

      {/* 스트리밍 채팅 컴포넌트 */}
      <div style={{ flex: 1, overflow: 'hidden' }}>
        <StreamingMedicalChat 
          key={sessionId} // userType 변경시 컴포넌트 리렌더링
          sessionId={sessionId}
          userType={userType}
          patientId={userType === 'patient' ? 'current_patient' : undefined}
          onSessionUpdate={handleSessionUpdate}
        />
      </div>

      {/* 푸터 정보 */}
      <Card size="small" style={{ 
        marginTop: '16px', 
        flexShrink: 0,
        background: 'rgba(255, 255, 255, 0.9)',
        border: '1px solid #e6f4ea',
        borderRadius: '8px'
      }}>
        <Space split={<span style={{ color: '#d9d9d9' }}>|</span>} wrap>
          <Text style={{ fontSize: '12px', color: '#607d8b' }}>
            세션: {sessionId.substring(0, 20)}...
          </Text>
          <Text style={{ fontSize: '12px', color: '#1a5d3a', fontWeight: 500 }}>
            데이터스트림즈
          </Text>
          <Text style={{ fontSize: '12px', color: '#607d8b' }}>
            실시간 스트리밍 지원
          </Text>
          <Text style={{ fontSize: '12px', color: '#607d8b' }}>
            GraphRAG & 멀티모달 분석
          </Text>
        </Space>
      </Card>
    </div>
  );
};

export default Chat;