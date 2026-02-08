import React, { useState } from 'react';
import { Card, Input, Button, Space, Alert, Typography, App } from 'antd';
import { SendOutlined, LoadingOutlined } from '@ant-design/icons';
import axios from 'axios';

const { TextArea } = Input;
const { Title } = Typography;

const TestCDW: React.FC = () => {
  const { message } = App.useApp();
  const [question, setQuestion] = useState('');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<any>(null);

  const handleSubmit = async () => {
    if (!question.trim()) {
      message.warning('질문을 입력해주세요.');
      return;
    }

    setLoading(true);
    try {
      const response = await axios.post('/api/v1/text2sql/generate', {
        question,
        include_explanation: true
      });
      setResult(response.data);
      message.success('SQL이 생성되었습니다!');
    } catch (error) {
      message.error('오류가 발생했습니다.');
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>🏥 Text2SQL 질의창 테스트</Title>
      
      <Alert
        message="자연어를 SQL로 변환"
        description="질문을 입력하면 Claude AI가 SQL 쿼리를 생성합니다."
        type="info"
        showIcon
        style={{ marginBottom: '24px' }}
      />

      <Card title="질문 입력창" style={{ marginBottom: '24px' }}>
        <Space direction="vertical" style={{ width: '100%' }}>
          <TextArea
            rows={4}
            placeholder="예: 당뇨병 환자는 몇 명인가요?"
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
          />
          <Button
            type="primary"
            icon={loading ? <LoadingOutlined /> : <SendOutlined />}
            onClick={handleSubmit}
            loading={loading}
            size="large"
          >
            SQL 생성
          </Button>
        </Space>
      </Card>

      {result && (
        <Card title="생성 결과" style={{ marginBottom: '24px' }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            <div>
              <strong>생성된 SQL:</strong>
              <pre style={{ 
                background: '#f5f5f5', 
                padding: '16px', 
                borderRadius: '8px',
                overflow: 'auto'
              }}>
                {result.sql}
              </pre>
            </div>
            
            <div>
              <strong>설명:</strong>
              <p>{result.explanation}</p>
            </div>
            
            <div>
              <strong>신뢰도:</strong> {(result.confidence * 100).toFixed(1)}%
            </div>
          </Space>
        </Card>
      )}

      <Card title="예시 질문">
        <Space wrap>
          <Button 
            size="small" 
            onClick={() => setQuestion('당뇨병 환자는 몇 명인가요?')}
          >
            당뇨병 환자 수
          </Button>
          <Button 
            size="small" 
            onClick={() => setQuestion('50대 여성 고혈압 환자는 몇 명인가요?')}
          >
            50대 여성 고혈압 환자
          </Button>
          <Button 
            size="small" 
            onClick={() => setQuestion('2023년에 암 진단받은 환자는 몇 명인가요?')}
          >
            2023년 암 환자
          </Button>
        </Space>
      </Card>
    </div>
  );
};

export default TestCDW;