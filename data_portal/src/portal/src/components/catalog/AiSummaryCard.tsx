/**
 * AiSummaryCard - AI 요약 답변 및 의미 확장 검색 결과 표시
 */

import React from 'react';
import { Card, Space, Typography, Tag, Spin, Avatar } from 'antd';
import { SearchOutlined, RobotOutlined } from '@ant-design/icons';
import ReactMarkdown from 'react-markdown';

const { Text } = Typography;

interface Expansion {
  original: string;
  expanded_terms: string[];
  expansion_source?: string[];
}

interface AiSummaryCardProps {
  searchQuery: string;
  aiSummary: string | null;
  aiSummaryLoading: boolean;
  expansion: Expansion | null;
  onSearch: (term: string) => void;
}

const AiSummaryCard: React.FC<AiSummaryCardProps> = ({
  searchQuery,
  aiSummary,
  aiSummaryLoading,
  expansion,
  onSearch,
}) => {
  return (
    <>
      {/* AI 요약 답변 */}
      {(aiSummaryLoading || aiSummary) && searchQuery && (
        <Card
          size="small"
          style={{ marginBottom: 16, borderLeft: '3px solid #00A0B0' }}
        >
          <Space align="start">
            <Avatar size={28} style={{ background: '#00A0B0', flexShrink: 0, marginTop: 2 }}>
              <RobotOutlined />
            </Avatar>
            <div style={{ flex: 1 }}>
              {aiSummaryLoading ? (
                <Space><Spin size="small" /><Text type="secondary">AI 요약 생성 중...</Text></Space>
              ) : (
                <div style={{ fontSize: 13, lineHeight: 1.6 }}>
                  <ReactMarkdown
                    components={{
                      p: ({ children }) => <p style={{ margin: '0 0 4px 0' }}>{children}</p>,
                    }}
                  >
                    {aiSummary || ''}
                  </ReactMarkdown>
                </div>
              )}
            </div>
          </Space>
        </Card>
      )}

      {/* 의미 확장 검색 결과 (AAR-001) */}
      {expansion && expansion.expanded_terms?.length > 0 && searchQuery && (
        <Card
          size="small"
          style={{ marginBottom: 16, borderLeft: '3px solid #52c41a' }}
        >
          <Space direction="vertical" size={4} style={{ width: '100%' }}>
            <Space>
              <SearchOutlined style={{ color: '#52c41a' }} />
              <Text strong style={{ color: '#52c41a' }}>의미 확장 검색</Text>
              <Text type="secondary" style={{ fontSize: 12 }}>
                "{expansion.original}" 검색어가 관련 용어로 확장되었습니다
              </Text>
            </Space>
            <Space wrap size={[4, 4]}>
              {expansion.expanded_terms.slice(0, 12).map((term: string) => (
                <Tag
                  key={term}
                  color="green"
                  style={{ cursor: 'pointer' }}
                  onClick={() => onSearch(term)}
                >
                  {term}
                </Tag>
              ))}
              {expansion.expanded_terms.length > 12 && (
                <Tag>+{expansion.expanded_terms.length - 12}개</Tag>
              )}
            </Space>
            {expansion.expansion_source && expansion.expansion_source.length > 0 && (
              <Text type="secondary" style={{ fontSize: 11 }}>
                출처: {expansion.expansion_source.join(' / ')}
              </Text>
            )}
          </Space>
        </Card>
      )}
    </>
  );
};

export default AiSummaryCard;
