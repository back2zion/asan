/**
 * 데이터 분석 — /api/v1/portal-ops/analysis-stats에서 실제 데이터 로딩
 */
import React, { useState, useEffect } from 'react';
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
  Space,
  Spin,
  Empty,
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

interface AnalysisItem {
  key: string;
  category: string;
  value: string;
  raw_value: number;
  change: string;
  trend: string;
}

const Analysis: React.FC = () => {
  const [analysisType, setAnalysisType] = useState('medical_diagnosis');
  const [dateRange, setDateRange] = useState<RangePickerProps['value']>();
  const [isLoading, setIsLoading] = useState(false);
  const [dataLoading, setDataLoading] = useState(true);
  const [analysisData, setAnalysisData] = useState<AnalysisItem[]>([]);

  useEffect(() => {
    fetch('/api/v1/portal-ops/analysis-stats')
      .then(res => res.json())
      .then(d => setAnalysisData(d.items || []))
      .catch(() => {})
      .finally(() => setDataLoading(false));
  }, []);

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
      render: (text: string, record: AnalysisItem) => (
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
    setDataLoading(true);
    fetch('/api/v1/portal-ops/analysis-stats')
      .then(res => res.json())
      .then(d => setAnalysisData(d.items || []))
      .catch(() => {})
      .finally(() => { setIsLoading(false); setDataLoading(false); });
  };

  const handleExport = () => {
    console.log('데이터 내보내기');
  };

  // 항목별 값 추출 헬퍼
  const getItem = (key: string): AnalysisItem | undefined => analysisData.find(i => i.key === key);
  const accuracy = getItem('1');
  const avgTime = getItem('2');
  const totalCases = getItem('3');
  const satisfaction = getItem('4');

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
                  onClick={handleRunAnalysis}
                >
                  새로고침
                </Button>
              </Space>
            </Space>
          </Col>
        </Row>
      </Card>

      {dataLoading ? (
        <div style={{ textAlign: 'center', padding: 80 }}>
          <Spin size="large" tip="분석 데이터 로딩 중..." />
        </div>
      ) : analysisData.length === 0 ? (
        <Empty description="분석 데이터를 불러올 수 없습니다." />
      ) : (
        <>
          {/* 분석 결과 개요 */}
          <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
            <Col xs={12} sm={6}>
              <Card>
                <Statistic
                  title="AI 모델 정확도"
                  value={accuracy?.raw_value ?? 0}
                  precision={1}
                  suffix="%"
                  valueStyle={{ color: '#3f8600' }}
                />
                <Progress percent={accuracy?.raw_value ?? 0} size="small" showInfo={false} />
              </Card>
            </Col>
            <Col xs={12} sm={6}>
              <Card>
                <Statistic
                  title="평균 처리 시간"
                  value={avgTime?.raw_value ?? 0}
                  precision={1}
                  suffix="초"
                  valueStyle={{ color: '#1890ff' }}
                />
                <Progress percent={Math.min((avgTime?.raw_value ?? 0) / 5 * 100, 100)} size="small" showInfo={false} />
              </Card>
            </Col>
            <Col xs={12} sm={6}>
              <Card>
                <Statistic
                  title="월간 케이스"
                  value={totalCases?.raw_value ?? 0}
                  valueStyle={{ color: '#722ed1' }}
                />
                <Progress percent={Math.min((totalCases?.raw_value ?? 0) / 2000 * 100, 100)} size="small" showInfo={false} />
              </Card>
            </Col>
            <Col xs={12} sm={6}>
              <Card>
                <Statistic
                  title="사용자 만족도"
                  value={satisfaction?.raw_value ?? 0}
                  precision={1}
                  suffix="/5.0"
                  valueStyle={{ color: '#eb2f96' }}
                />
                <Progress percent={(satisfaction?.raw_value ?? 0) / 5 * 100} size="small" showInfo={false} />
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
        </>
      )}
    </div>
  );
};

export default Analysis;
