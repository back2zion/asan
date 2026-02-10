import React, { useState } from 'react';
import { Form, Input, Button, Card, Typography, Space, Divider, Alert } from 'antd';
import { UserOutlined, LockOutlined, MedicineBoxOutlined, ExperimentOutlined, HeartOutlined, SettingOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext.tsx';
import asanLogo from '../assets/asan_logo.png';

const { Title, Text } = Typography;

interface LoginForm {
  email: string;
  password: string;
}

const Login: React.FC = () => {
  const navigate = useNavigate();
  const { login, isLoading } = useAuth();
  const [error, setError] = useState<string>('');

  const onFinish = async (values: LoginForm) => {
    try {
      setError('');
      await login(values.email, values.password);
      navigate('/dashboard');
    } catch (err) {
      setError('로그인에 실패했습니다. 이메일과 비밀번호를 확인해주세요.');
    }
  };

  const demoAccounts = [
    { role: '의사', email: 'doctor@amc.seoul.kr', password: 'doctor123', icon: <MedicineBoxOutlined /> },
    { role: '연구자', email: 'researcher@amc.seoul.kr', password: 'research123', icon: <ExperimentOutlined /> },
    { role: '환자', email: 'patient@amc.seoul.kr', password: 'patient123', icon: <HeartOutlined /> },
    { role: '관리자', email: 'admin@amc.seoul.kr', password: 'admin123', icon: <SettingOutlined /> },
  ];

  const handleDemoLogin = (email: string, password: string) => {
    onFinish({ email, password });
  };

  return (
    <div style={{
      minHeight: '100vh',
      background: 'linear-gradient(135deg, #1a5d3a 0%, #165030 100%)',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      padding: '20px',
      position: 'relative'
    }}>
      <div style={{
        position: 'absolute',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        background: 'radial-gradient(circle at 30% 20%, rgba(26,93,58,0.1) 0%, transparent 50%), radial-gradient(circle at 70% 80%, rgba(26,93,58,0.1) 0%, transparent 50%)',
        pointerEvents: 'none'
      }} />
      <Card style={{
        width: '100%',
        maxWidth: '480px',
        boxShadow: '0 12px 40px rgba(0, 0, 0, 0.15)',
        borderRadius: '16px',
        border: 'none',
        background: 'rgba(255, 255, 255, 0.98)',
        backdropFilter: 'blur(10px)',
        position: 'relative',
        zIndex: 1
      }}>
        <div style={{ textAlign: 'center', marginBottom: '36px' }}>
          <div style={{
            width: '140px',
            height: '90px',
            background: 'white',
            borderRadius: '12px',
            margin: '0 auto 20px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: '0 8px 24px rgba(26, 93, 58, 0.15)',
            padding: '10px'
          }}>
            <img 
              src={asanLogo}
              alt="서울아산병원" 
              style={{ 
                width: '100%', 
                height: '100%', 
                objectFit: 'contain' 
              }} 
              onError={(e) => {
                // 이미지 로드 실패시 대체 텍스트 표시
                e.currentTarget.style.display = 'none';
                e.currentTarget.parentElement!.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;font-size:24px;color:#1a5d3a;font-weight:bold;">🏥 아산병원</div>';
              }}
            />
          </div>
          <Title level={2} style={{ color: '#1a5d3a', marginBottom: '8px', fontWeight: 700, fontSize: '28px' }}>
            서울아산병원
          </Title>
          <Text style={{ color: '#546e7a', fontSize: '17px', fontWeight: 500 }}>AI 데이터 분석 플랫폼</Text>
        </div>

        {error && (
          <Alert
            message={error}
            type="error"
            closable
            style={{ marginBottom: '24px' }}
            onClose={() => setError('')}
          />
        )}

        <Form
          name="login"
          onFinish={onFinish}
          layout="vertical"
          size="large"
          autoComplete="off"
        >
          <Form.Item
            name="email"
            rules={[
              { required: true, message: '이메일을 입력해주세요!' },
              { type: 'email', message: '올바른 이메일 형식이 아닙니다!' }
            ]}
          >
            <Input
              prefix={<UserOutlined style={{ fontSize: '18px' }} />}
              placeholder="이메일"
              style={{ height: '48px', fontSize: '16px' }}
              autoComplete="one-time-code"
              readOnly
              onFocus={(e) => e.currentTarget.removeAttribute('readOnly')}
            />
          </Form.Item>

          <Form.Item
            name="password"
            rules={[{ required: true, message: '비밀번호를 입력해주세요!' }]}
          >
            <Input.Password
              prefix={<LockOutlined style={{ fontSize: '18px' }} />}
              placeholder="비밀번호"
              autoComplete="current-password"
              style={{ height: '48px', fontSize: '16px' }}
            />
          </Form.Item>

          <Form.Item>
            <Button 
              type="primary" 
              htmlType="submit" 
              block
              loading={isLoading}
              style={{
                height: '52px',
                fontSize: '18px',
                fontWeight: 600,
                background: 'linear-gradient(135deg, #1a5d3a 0%, #165030 100%)',
                border: 'none',
                borderRadius: '8px',
                boxShadow: '0 4px 12px rgba(26, 93, 58, 0.3)'
              }}
            >
              로그인
            </Button>
          </Form.Item>
        </Form>

        <Divider style={{ borderColor: '#e6f4ea', color: '#607d8b', fontSize: '15px' }}>데모 계정</Divider>

        <Space direction="vertical" style={{ width: '100%' }}>
          <Text style={{ fontSize: '14px', color: '#607d8b' }}>
            아래 데모 계정으로 빠르게 체험해보세요:
          </Text>
          
          {demoAccounts.map((account, index) => (
            <Button
              key={index}
              type="dashed"
              block
              size="small"
              onClick={() => handleDemoLogin(account.email, account.password)}
              style={{ 
                height: 'auto', 
                padding: '12px 16px',
                textAlign: 'left',
                borderColor: '#e6f4ea',
                borderRadius: '8px',
                background: 'rgba(26, 93, 58, 0.02)',
                transition: 'all 0.3s'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.borderColor = '#1a5d3a';
                e.currentTarget.style.background = 'rgba(26, 93, 58, 0.05)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.borderColor = '#e6f4ea';
                e.currentTarget.style.background = 'rgba(26, 93, 58, 0.02)';
              }}
            >
              <div>
                <div style={{ fontWeight: 600, fontSize: '15px', color: '#1a5d3a', display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <span style={{ fontSize: '17px' }}>{account.icon}</span>
                  {account.role}
                </div>
                <div style={{ fontSize: '13px', color: '#607d8b', marginTop: '4px' }}>
                  {account.email}
                </div>
              </div>
            </Button>
          ))}
        </Space>

        <div style={{ 
          marginTop: '28px', 
          textAlign: 'center',
          borderTop: '1px solid #e6f4ea',
          paddingTop: '20px'
        }}>
          <Text style={{ fontSize: '13px', color: '#607d8b' }}>
            © 2026 데이터스트림즈
          </Text>
        </div>
      </Card>
    </div>
  );
};

export default Login;