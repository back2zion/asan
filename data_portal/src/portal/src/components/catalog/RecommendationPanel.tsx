/**
 * 추천 패널 (DPR-002: 지능형 추천)
 * FilterPanel 아래에 배치, 개인 맞춤 추천 데이터셋 카드
 */

import React from 'react';
import { Card, Space, Typography, Tag, Badge, Spin, Empty, Tooltip } from 'antd';
import { BulbOutlined, RightOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { catalogRecommendApi } from '../../services/catalogRecommendApi';
import type { Recommendation } from '../../services/catalogRecommendApi';

const { Text } = Typography;

const DOMAIN_COLORS: Record<string, string> = {
  Clinical: 'blue',
  Demographics: 'cyan',
  'Lab/Vital': 'green',
  Derived: 'purple',
  Financial: 'gold',
  'Health System': 'orange',
};

interface RecommendationPanelProps {
  onSearch: (term: string) => void;
}

const RecommendationPanel: React.FC<RecommendationPanelProps> = ({ onSearch }) => {
  const { data, isLoading } = useQuery({
    queryKey: ['catalog-recommendations'],
    queryFn: () => catalogRecommendApi.getForUser('anonymous', 6),
    staleTime: 60_000,
  });

  const recommendations: Recommendation[] = data?.recommendations || [];

  return (
    <Card
      title={<><BulbOutlined style={{ color: '#FAAD14' }} /> 추천 데이터셋</>}
      size="small"
      style={{ marginTop: 16 }}
    >
      <Spin spinning={isLoading}>
        {recommendations.length > 0 ? (
          <Space direction="vertical" style={{ width: '100%' }} size={8}>
            {recommendations.slice(0, 5).map((rec) => (
              <div
                key={rec.table_name}
                onClick={() => onSearch(rec.table_name)}
                style={{
                  padding: '8px 10px',
                  borderRadius: 6,
                  border: '1px solid #f0f0f0',
                  cursor: 'pointer',
                  transition: 'background 0.2s',
                }}
                onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.background = '#f6f8ff'; }}
                onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.background = 'transparent'; }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Space size={4}>
                    <Text strong style={{ fontSize: 13 }}>{rec.table_name}</Text>
                    <Tag color={DOMAIN_COLORS[rec.domain] || 'default'} style={{ fontSize: 10, lineHeight: '16px' }}>
                      {rec.domain}
                    </Tag>
                  </Space>
                  <Space size={4}>
                    <Badge
                      count={rec.relevance_score}
                      showZero
                      color="#005BAC"
                      style={{ fontSize: 10 }}
                      title="관련도 점수"
                    />
                    <RightOutlined style={{ fontSize: 10, color: '#999' }} />
                  </Space>
                </div>
                <Tooltip title={rec.reasons.join(' / ')}>
                  <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 2 }} ellipsis>
                    {rec.reasons[0] || rec.label}
                  </Text>
                </Tooltip>
              </div>
            ))}
          </Space>
        ) : (
          <Empty description="추천 데이터 없음" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        )}
      </Spin>
    </Card>
  );
};

export default RecommendationPanel;
