import React, { useState } from 'react';
import { 
  Card, 
  Row, 
  Col, 
  Select, 
  DatePicker, 
  Button, 
  Table, 
  Typography,
  Statistic,
  Progress,
  Space
} from 'antd';
import {
  BarChartOutlined,
  LineChartOutlined,
  PieChartOutlined,
  DownloadOutlined,
  ReloadOutlined
} from '@ant-design/icons';
import type { RangePickerProps } from 'antd/es/date-picker';

const { Title } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;

const Analysis: React.FC = () => {
  const [analysisType, setAnalysisType] = useState('medical_diagnosis');
  const [dateRange, setDateRange] = useState<RangePickerProps['value']>();
  const [isLoading, setIsLoading] = useState(false);

  const analysisData = [
    {
      key: '1',
      category: '진단 정확도',
      value: '94.2%',
      change: '+2.1%',
      trend: 'up'
    },
    {
      key: '2',
      category: '응답 시간',
      value: '3.8초',
      change: '-0.5초',
      trend: 'down'
    },
    {
      key: '3',
      category: '처리된 케이스',
      value: '1,247건',
      change: '+156건',
      trend: 'up'
    },
    {
      key: '4',
      category: '사용자 만족도',
      value: '4.7/5.0',
      change: '+0.2',
      trend: 'up'
    }
  ];

  const detailColumns = [
    {
      title: '분석 항목',
      dataIndex: 'category',
      key: 'category',
    },
    {
      title: '현재 값',
      dataIndex: 'value',
      key: 'value',
      render: (text: string) => <strong>{text}</strong>
    },
    {
      title: '변화량',
      dataIndex: 'change',
      key: 'change',
      render: (text: string, record: any) => (
        <span style={{ 
          color: record.trend === 'up' ? '#52c41a' : record.trend === 'down' ? '#ff4d4f' : '#666'
        }}>
          {text}
        </span>
      )
    }
  ];

  const handleRunAnalysis = () => {
    setIsLoading(true);
    setTimeout(() => {
      setIsLoading(false);
    }, 3000);
  };

  const handleExport = () => {
    // 실제로는 데이터 내보내기 로직 구현
    console.log('데이터 내보내기');
  };

  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>데이터 분석</Title>
      
      {/* 분석 설정 */}
      <Card style={{ marginBottom: '24px' }}>
        <Title level={4}>분석 설정</Title>
        <Row gutter={[16, 16]} align="middle">
          <Col xs={24} sm={8}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <label>분석 유형:</label>
              <Select 
                value={analysisType} 
                onChange={setAnalysisType}
                style={{ width: '100%' }}
              >
                <Option value="medical_diagnosis">의료 진단 분석</Option>
                <Option value="image_analysis">의료 영상 분석</Option>
                <Option value="research_data">연구 데이터 분석</Option>
                <Option value="user_behavior">사용자 행동 분석</Option>
              </Select>
            </Space>
          </Col>
          <Col xs={24} sm={8}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <label>기간 선택:</label>
              <RangePicker 
                value={dateRange}
                onChange={setDateRange}
                style={{ width: '100%' }}
              />
            </Space>
          </Col>
          <Col xs={24} sm={8}>
            <Space direction="vertical" style={{ width: '100%' }}>
              <label>&nbsp;</label>
              <Space>
                <Button 
                  type="primary" 
                  icon={<BarChartOutlined />}
                  loading={isLoading}
                  onClick={handleRunAnalysis}
                >
                  분석 실행
                </Button>
                <Button 
                  icon={<ReloadOutlined />}
                  onClick={() => window.location.reload()}
                >
                  새로고침
                </Button>
              </Space>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 분석 결과 개요 */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="AI 모델 정확도"
              value={94.2}
              precision={1}
              suffix="%"
              valueStyle={{ color: '#3f8600' }}
            />
            <Progress percent={94.2} size="small" showInfo={false} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="평균 처리 시간"
              value={3.8}
              precision={1}
              suffix="초"
              valueStyle={{ color: '#1890ff' }}
            />
            <Progress percent={76} size="small" showInfo={false} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="월간 케이스"
              value={1247}
              valueStyle={{ color: '#722ed1' }}
            />
            <Progress percent={85} size="small" showInfo={false} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="사용자 만족도"
              value={4.7}
              precision={1}
              suffix="/5.0"
              valueStyle={{ color: '#eb2f96' }}
            />
            <Progress percent={94} size="small" showInfo={false} />
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        {/* 상세 분석 결과 */}
        <Col xs={24} lg={16}>
          <Card>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
              <Title level={4} style={{ margin: 0 }}>상세 분석 결과</Title>
              <Button icon={<DownloadOutlined />} onClick={handleExport}>
                내보내기
              </Button>
            </div>
            <Table 
              columns={detailColumns} 
              dataSource={analysisData}
              pagination={false}
              size="small"
            />
          </Card>
        </Col>

        {/* 차트 영역 */}
        <Col xs={24} lg={8}>
          <Card>
            <Title level={4}>분석 차트</Title>
            <Space direction="vertical" style={{ width: '100%' }}>
              <Button 
                block 
                icon={<BarChartOutlined />}
                style={{ textAlign: 'left', height: '60px' }}
              >
                <div>
                  <div>막대 차트</div>
                  <small style={{ color: '#666' }}>카테고리별 비교 분석</small>
                </div>
              </Button>
              <Button 
                block 
                icon={<LineChartOutlined />}
                style={{ textAlign: 'left', height: '60px' }}
              >
                <div>
                  <div>선형 차트</div>
                  <small style={{ color: '#666' }}>시계열 트렌드 분석</small>
                </div>
              </Button>
              <Button 
                block 
                icon={<PieChartOutlined />}
                style={{ textAlign: 'left', height: '60px' }}
              >
                <div>
                  <div>원형 차트</div>
                  <small style={{ color: '#666' }}>구성 비율 분석</small>
                </div>
              </Button>
            </Space>
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default Analysis;