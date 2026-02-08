import React, { useState } from 'react';
import { Card, InputNumber, Button, Typography, Space, Descriptions, Timeline, Spin, Alert, Tag } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import { cohortApi, type PatientTimeline, type TimelineEvent } from '../../services/cohortApi';

const { Text, Title } = Typography;

const DOMAIN_COLORS: Record<string, string> = {
  condition: '#ff4d4f',
  drug: '#1890ff',
  visit: '#52c41a',
  measurement: '#fa8c16',
  procedure: '#722ed1',
  observation: '#13c2c2',
};

const DOMAIN_LABELS: Record<string, string> = {
  condition: '진단',
  drug: '약물',
  visit: '방문',
  measurement: '검사',
  procedure: '시술',
  observation: '관찰',
};

const VISIT_LABELS: Record<string, string> = {
  '9201': '입원',
  '9202': '외래',
  '9203': '응급',
};

const ChartReview: React.FC = () => {
  const [personId, setPersonId] = useState<number | null>(null);
  const [data, setData] = useState<PatientTimeline | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async () => {
    if (!personId) return;
    setLoading(true);
    setError(null);
    setData(null);
    try {
      const result = await cohortApi.patientTimeline(personId);
      setData(result);
    } catch (err: any) {
      const msg = err?.response?.data?.detail || '환자 타임라인 조회 중 오류가 발생했습니다.';
      setError(msg);
    } finally {
      setLoading(false);
    }
  };

  const formatEvent = (e: TimelineEvent) => {
    const label = DOMAIN_LABELS[e.domain] || e.domain;
    let detail = e.code;
    if (e.domain === 'visit') {
      detail = VISIT_LABELS[e.code] || `visit_concept_id=${e.code}`;
    }
    if (e.value) {
      detail += ` (${e.value})`;
    }
    return { label, detail };
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card title="환자 차트 리뷰">
        <Space>
          <Text strong>환자 ID:</Text>
          <InputNumber
            style={{ width: 200 }}
            placeholder="person_id 입력"
            value={personId}
            onChange={(v) => setPersonId(v)}
            min={1}
          />
          <Button
            type="primary"
            icon={<SearchOutlined />}
            onClick={handleSearch}
            loading={loading}
            disabled={!personId}
          >
            조회
          </Button>
        </Space>
      </Card>

      {error && (
        <Alert message="오류" description={error} type="error" showIcon closable onClose={() => setError(null)} />
      )}

      {loading && (
        <Card>
          <div style={{ textAlign: 'center', padding: '40px 0' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16 }}><Text>타임라인 조회 중...</Text></div>
          </div>
        </Card>
      )}

      {data && !loading && (
        <>
          <Card title="환자 기본정보">
            <Descriptions bordered column={2} size="small">
              <Descriptions.Item label="환자 ID">{data.patient.person_id}</Descriptions.Item>
              <Descriptions.Item label="성별">
                <Tag color={data.patient.gender === 'M' ? 'blue' : 'pink'}>
                  {data.patient.gender === 'M' ? '남성' : '여성'}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="출생연도">{data.patient.birth_year}</Descriptions.Item>
              <Descriptions.Item label="나이">{data.patient.age}세</Descriptions.Item>
            </Descriptions>
          </Card>

          <Card title={`타임라인 (${data.events.length}건)`}>
            {data.events.length === 0 ? (
              <Text type="secondary">이벤트가 없습니다.</Text>
            ) : (
              <div style={{ maxHeight: 600, overflow: 'auto' }}>
                <Timeline>
                  {data.events.map((event, idx) => {
                    const { label, detail } = formatEvent(event);
                    const color = DOMAIN_COLORS[event.domain] || '#999';
                    return (
                      <Timeline.Item key={idx} color={color}>
                        <Space>
                          <Tag color={color} style={{ minWidth: 50, textAlign: 'center' }}>{label}</Tag>
                          <Text type="secondary">{event.event_date}</Text>
                          <Text>{detail}</Text>
                        </Space>
                      </Timeline.Item>
                    );
                  })}
                </Timeline>
              </div>
            )}
          </Card>
        </>
      )}
    </Space>
  );
};

export default ChartReview;
