import React, { useState } from 'react';
import { Card, Tabs, Select, Button, Space, Row, Col, Table, message } from 'antd';
import { BarChart, Bar, LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { useQuery, useMutation } from 'react-query';
import axios from 'axios';

const { TabPane } = Tabs;

interface OLAPQuery {
  dimensions: string[];
  metrics: string[];
  filters?: any;
  drill_level?: string;
}

const OLAP: React.FC = () => {
  const [selectedDimensions, setSelectedDimensions] = useState<string[]>(['age_group']);
  const [selectedMetrics, setSelectedMetrics] = useState<string[]>(['patient_count']);
  const [activeTab, setActiveTab] = useState('slice-dice');

  // Fetch available dimensions
  const { data: dimensions } = useQuery('olap-dimensions', async () => {
    const response = await axios.get('/api/v1/olap/dimensions');
    return response.data.dimensions;
  });

  // Fetch available metrics
  const { data: metrics } = useQuery('olap-metrics', async () => {
    const response = await axios.get('/api/v1/olap/metrics');
    return response.data.metrics;
  });

  // Execute OLAP query
  const executeQuery = useMutation(
    async (query: OLAPQuery) => {
      const response = await axios.post('/api/v1/olap/query', query);
      return response.data;
    },
    {
      onSuccess: () => {
        message.success('OLAP 분석이 완료되었습니다.');
      },
      onError: () => {
        message.error('OLAP 분석 중 오류가 발생했습니다.');
      }
    }
  );

  const handleExecuteQuery = () => {
    executeQuery.mutate({
      dimensions: selectedDimensions,
      metrics: selectedMetrics
    });
  };

  // Sample data for visualization
  const sampleData = [
    { name: '20대', patient_count: 150, avg_duration: 3.2 },
    { name: '30대', patient_count: 230, avg_duration: 4.5 },
    { name: '40대', patient_count: 280, avg_duration: 5.1 },
    { name: '50대', patient_count: 320, avg_duration: 6.8 },
    { name: '60대', patient_count: 250, avg_duration: 7.2 },
  ];

  const COLORS = ['#1a5d3a', '#52c41a', '#ff6600', '#1890ff', '#722ed1'];

  return (
    <Card title="SFR-004: OLAP 다차원 분석">
      <Tabs activeKey={activeTab} onChange={setActiveTab}>
        <TabPane tab="슬라이스 & 다이스" key="slice-dice">
          <Row gutter={[16, 16]}>
            <Col span={24}>
              <Card title="분석 설정">
                <Row gutter={[16, 16]}>
                  <Col span={12}>
                    <Space direction="vertical" style={{ width: '100%' }}>
                      <label>차원 선택:</label>
                      <Select
                        mode="multiple"
                        style={{ width: '100%' }}
                        placeholder="차원을 선택하세요"
                        value={selectedDimensions}
                        onChange={setSelectedDimensions}
                        options={dimensions?.map((d: any) => ({
                          label: d.label,
                          value: d.id
                        }))}
                      />
                    </Space>
                  </Col>
                  <Col span={12}>
                    <Space direction="vertical" style={{ width: '100%' }}>
                      <label>지표 선택:</label>
                      <Select
                        mode="multiple"
                        style={{ width: '100%' }}
                        placeholder="지표를 선택하세요"
                        value={selectedMetrics}
                        onChange={setSelectedMetrics}
                        options={metrics?.map((m: any) => ({
                          label: m.label,
                          value: m.id
                        }))}
                      />
                    </Space>
                  </Col>
                </Row>
                <Row style={{ marginTop: 16 }}>
                  <Col>
                    <Button 
                      type="primary" 
                      onClick={handleExecuteQuery}
                      loading={executeQuery.isLoading}
                    >
                      분석 실행
                    </Button>
                  </Col>
                </Row>
              </Card>
            </Col>
            <Col span={24}>
              <Card title="분석 결과">
                <ResponsiveContainer width="99%" height={400}>
                  <BarChart data={executeQuery.data?.data || sampleData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="patient_count" fill="#1a5d3a" name="환자 수" />
                    {selectedMetrics.includes('avg_duration') && (
                      <Bar dataKey="avg_duration" fill="#ff6600" name="평균 입원일수" />
                    )}
                  </BarChart>
                </ResponsiveContainer>
              </Card>
            </Col>
          </Row>
        </TabPane>
        
        <TabPane tab="피벗 테이블" key="pivot">
          <Card>
            <Table
              dataSource={executeQuery.data?.data || sampleData}
              columns={[
                {
                  title: '연령대',
                  dataIndex: 'name',
                  key: 'name',
                },
                {
                  title: '환자 수',
                  dataIndex: 'patient_count',
                  key: 'patient_count',
                  sorter: (a: any, b: any) => a.patient_count - b.patient_count,
                },
                {
                  title: '평균 입원일수',
                  dataIndex: 'avg_duration',
                  key: 'avg_duration',
                  render: (value: number) => value.toFixed(1),
                },
              ]}
              pagination={false}
            />
          </Card>
        </TabPane>
        
        <TabPane tab="드릴다운/롤업" key="drilldown">
          <Card>
            <Space direction="vertical" style={{ width: '100%' }}>
              <Button onClick={() => message.info('연도 → 분기 → 월로 드릴다운')}>
                시간 차원 드릴다운
              </Button>
              <Button onClick={() => message.info('시/도 → 구/군 → 병원으로 드릴다운')}>
                지역 차원 드릴다운
              </Button>
              <ResponsiveContainer width="99%" height={300}>
                <PieChart>
                  <Pie
                    data={sampleData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="patient_count"
                  >
                    {sampleData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip />
                  <Legend />
                </PieChart>
              </ResponsiveContainer>
            </Space>
          </Card>
        </TabPane>
        
        <TabPane tab="다차원 분석" key="multi-dimensional">
          <Card>
            <ResponsiveContainer width="99%" height={400}>
              <LineChart data={sampleData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis yAxisId="left" orientation="left" stroke="#1a5d3a" />
                <YAxis yAxisId="right" orientation="right" stroke="#ff6600" />
                <Tooltip />
                <Legend />
                <Line yAxisId="left" type="monotone" dataKey="patient_count" stroke="#1a5d3a" name="환자 수" />
                <Line yAxisId="right" type="monotone" dataKey="avg_duration" stroke="#ff6600" name="평균 입원일수" />
              </LineChart>
            </ResponsiveContainer>
          </Card>
        </TabPane>
      </Tabs>
    </Card>
  );
};

export default OLAP;