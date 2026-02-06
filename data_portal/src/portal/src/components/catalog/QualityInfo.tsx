/**
 * 품질 정보 컴포넌트 (API 연동)
 */

import React from 'react';
import { Tag, Space, Typography, Spin, Empty, Descriptions, Divider } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { semanticApi } from '../../services/api';

const { Title, Text } = Typography;

const QualityInfo: React.FC<{ tableName: string }> = ({ tableName }) => {
  const { data, isLoading } = useQuery({
    queryKey: ['quality', tableName],
    queryFn: () => semanticApi.getQuality(tableName),
    enabled: !!tableName,
  });

  if (isLoading) return <div style={{ textAlign: 'center', padding: 20 }}><Spin /><div style={{ marginTop: 8, color: '#888' }}>품질 정보 로딩 중...</div></div>;
  if (!data?.quality) return <Empty description="품질 정보 없음" />;

  const { metrics, rules_applied, last_checked } = data.quality;

  return (
    <div style={{ padding: '16px 0' }}>
      <Title level={5}>데이터 품질 지표 (DGR-004)</Title>
      <Descriptions bordered column={2} size="small">
        <Descriptions.Item label="완전성">
          <Tag color={metrics.completeness >= 95 ? 'green' : 'orange'}>{metrics.completeness}%</Tag>
        </Descriptions.Item>
        <Descriptions.Item label="정확성">
          <Tag color={metrics.accuracy >= 95 ? 'green' : 'orange'}>{metrics.accuracy}%</Tag>
        </Descriptions.Item>
        <Descriptions.Item label="일관성">
          <Tag color={metrics.consistency >= 95 ? 'green' : 'orange'}>{metrics.consistency}%</Tag>
        </Descriptions.Item>
        <Descriptions.Item label="최신성">
          <Tag color="blue">{metrics.timeliness}</Tag>
        </Descriptions.Item>
        <Descriptions.Item label="마지막 검증" span={2}>
          <Space>
            <ClockCircleOutlined />
            <Text>{new Date(last_checked).toLocaleString('ko-KR')}</Text>
          </Space>
        </Descriptions.Item>
      </Descriptions>
      <Divider />
      <Title level={5}>적용된 품질 규칙</Title>
      <Space direction="vertical" style={{ width: '100%' }}>
        {rules_applied?.map((rule: any) => (
          <Tag key={rule.name} color={rule.violations === 0 ? 'blue' : 'orange'}>
            {rule.name} {rule.violations > 0 && `(위반: ${rule.violations})`}
          </Tag>
        ))}
      </Space>
    </div>
  );
};

export default QualityInfo;
