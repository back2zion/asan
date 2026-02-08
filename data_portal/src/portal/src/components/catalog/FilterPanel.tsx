/**
 * FilterPanel - 좌측 사이드바: 도메인/태그/민감도 필터, 인기 데이터, 저장된 스냅샷
 */

import React from 'react';
import {
  Card, Checkbox, Select, Collapse, Tag, Badge, Button,
  Space, Typography, Empty,
} from 'antd';
import type { CollapseProps } from 'antd';
import {
  DatabaseOutlined, TagOutlined, FilterOutlined,
  StarOutlined, FolderOpenOutlined, DeleteOutlined, FireOutlined,
} from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { catalogAnalyticsApi } from '../../services/catalogAnalyticsApi';

const { Text } = Typography;

export interface Snapshot {
  id: string;
  name: string;
  query: string;
  domains: string[];
  tags: string[];
  sensitivity: string[];
  createdAt: string;
}

interface FilterPanelProps {
  // Filter state
  domains: string[];
  tags: string[];
  selectedDomains: string[];
  selectedTags: string[];
  selectedSensitivity: string[];
  onDomainsChange: (values: string[]) => void;
  onTagsChange: (values: string[]) => void;
  onSensitivityChange: (values: string[]) => void;
  onClearFilters: () => void;
  // Popular data
  popular: Array<{ name: string; count: number }>;
  onSearch: (term: string) => void;
  // Snapshots
  snapshots: Snapshot[];
  onRestoreSnapshot: (snap: Snapshot) => void;
  onDeleteSnapshot: (id: string) => void;
}

const sensitivityLabels: Record<string, string> = {
  Normal: '일반',
  PHI: '개인건강정보(PHI)',
  Restricted: '제한',
};

const sensitivityColors: Record<string, string> = {
  Normal: 'green',
  PHI: 'orange',
  Restricted: 'red',
};

const FilterPanel: React.FC<FilterPanelProps> = ({
  domains,
  tags,
  selectedDomains,
  selectedTags,
  selectedSensitivity,
  onDomainsChange,
  onTagsChange,
  onSensitivityChange,
  onClearFilters,
  popular,
  onSearch,
  snapshots,
  onRestoreSnapshot,
  onDeleteSnapshot,
}) => {
  const hasActiveFilters =
    selectedDomains.length > 0 || selectedTags.length > 0 || selectedSensitivity.length > 0;

  const { data: trendingData } = useQuery({
    queryKey: ['catalog-trending'],
    queryFn: () => catalogAnalyticsApi.getTrending(),
    staleTime: 60_000,
  });
  const trendingSearches: Array<{ term: string; count: number }> = trendingData?.trending_searches || [];

  const filterCollapseItems: CollapseProps['items'] = [
    {
      key: 'domain',
      label: <><DatabaseOutlined /> 도메인</>,
      children: (
        <Checkbox.Group
          value={selectedDomains}
          onChange={(values) => onDomainsChange(values as string[])}
          style={{ display: 'flex', flexDirection: 'column', gap: 8 }}
        >
          {domains.map((domain: string) => (
            <Checkbox key={domain} value={domain}>{domain}</Checkbox>
          ))}
        </Checkbox.Group>
      ),
    },
    {
      key: 'tags',
      label: <><TagOutlined /> 태그</>,
      children: (
        <Select
          mode="multiple"
          placeholder="태그 선택"
          value={selectedTags}
          onChange={onTagsChange}
          style={{ width: '100%' }}
          options={tags.map((tag: string) => ({ label: tag, value: tag }))}
          maxTagCount={2}
        />
      ),
    },
    {
      key: 'sensitivity',
      label: '민감도',
      children: (
        <Checkbox.Group
          value={selectedSensitivity}
          onChange={(values) => onSensitivityChange(values as string[])}
          style={{ display: 'flex', flexDirection: 'column', gap: 8 }}
        >
          {Object.entries(sensitivityLabels).map(([key, label]) => (
            <Checkbox key={key} value={key}>
              <Tag color={sensitivityColors[key]}>{label}</Tag>
            </Checkbox>
          ))}
        </Checkbox.Group>
      ),
    },
  ];

  return (
    <>
      {/* 필터 카드 */}
      <Card title={<><FilterOutlined /> 필터</>} size="small">
        <Collapse
          defaultActiveKey={['domain', 'tags', 'sensitivity']}
          ghost
          expandIconPosition="end"
          items={filterCollapseItems}
        />
        {hasActiveFilters && (
          <Button
            type="link"
            onClick={onClearFilters}
            style={{ marginTop: 16 }}
          >
            필터 초기화
          </Button>
        )}
      </Card>

      {/* 인기 데이터 */}
      <Card title={<><StarOutlined /> 인기 데이터</>} size="small" style={{ marginTop: 16 }}>
        {popular.length > 0 ? (
          <Space direction="vertical" style={{ width: '100%' }}>
            {popular.slice(0, 5).map((item, index: number) => (
              <div
                key={item.name}
                style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 0' }}
              >
                <Space>
                  <Text type="secondary">{index + 1}.</Text>
                  <Text ellipsis style={{ maxWidth: 140, cursor: 'pointer' }} onClick={() => onSearch(item.name)}>
                    {item.name}
                  </Text>
                </Space>
                <Badge count={item.count} size="small" />
              </div>
            ))}
          </Space>
        ) : (
          <Empty description="데이터 없음" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        )}
      </Card>

      {/* 스냅샷/레시피 */}
      {snapshots.length > 0 && (
        <Card title={<><FolderOpenOutlined /> 저장된 스냅샷</>} size="small" style={{ marginTop: 16 }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            {snapshots.slice(0, 5).map((snap) => (
              <div
                key={snap.id}
                style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '4px 0' }}
              >
                <Text
                  ellipsis
                  style={{ maxWidth: 120, cursor: 'pointer', flex: 1 }}
                  onClick={() => onRestoreSnapshot(snap)}
                >
                  {snap.name}
                </Text>
                <Space size={4}>
                  <Text type="secondary" style={{ fontSize: 10 }}>
                    {new Date(snap.createdAt).toLocaleDateString('ko-KR')}
                  </Text>
                  <Button
                    type="text"
                    size="small"
                    icon={<DeleteOutlined />}
                    onClick={() => onDeleteSnapshot(snap.id)}
                    style={{ color: '#999' }}
                  />
                </Space>
              </div>
            ))}
          </Space>
        </Card>
      )}

      {/* 트렌딩 검색어 */}
      {trendingSearches.length > 0 && (
        <Card title={<><FireOutlined style={{ color: '#FF4D4F' }} /> 트렌딩 검색어</>} size="small" style={{ marginTop: 16 }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            {trendingSearches.slice(0, 6).map((item, index) => (
              <div
                key={item.term}
                style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '3px 0' }}
              >
                <Space>
                  <Text type="secondary" style={{ fontSize: 11, width: 16, textAlign: 'right' }}>{index + 1}.</Text>
                  <Text
                    ellipsis
                    style={{ maxWidth: 130, cursor: 'pointer', fontSize: 12 }}
                    onClick={() => onSearch(item.term)}
                  >
                    {item.term}
                  </Text>
                </Space>
                <Badge count={item.count} size="small" style={{ backgroundColor: index < 3 ? '#FF4D4F' : '#999' }} />
              </div>
            ))}
          </Space>
        </Card>
      )}
    </>
  );
};

export default FilterPanel;
