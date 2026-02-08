/**
 * CatalogSearchBar - 검색 입력, 뷰 모드 전환, 스냅샷 저장 버튼
 */

import React from 'react';
import { Card, Input, Row, Col, Space, Segmented, Tooltip, Button } from 'antd';
import {
  SearchOutlined, UnorderedListOutlined, ApartmentOutlined, SaveOutlined, ShareAltOutlined,
} from '@ant-design/icons';

const { Search } = Input;

interface CatalogSearchBarProps {
  searchQuery: string;
  viewMode: string;
  hasActiveFilters: boolean;
  onSearchChange: (value: string) => void;
  onSearch: (value: string) => void;
  onViewModeChange: (mode: string) => void;
  onSaveSnapshotClick: () => void;
}

const CatalogSearchBar: React.FC<CatalogSearchBarProps> = ({
  searchQuery,
  viewMode,
  hasActiveFilters,
  onSearchChange,
  onSearch,
  onViewModeChange,
  onSaveSnapshotClick,
}) => {
  return (
    <Card style={{ marginBottom: 24 }}>
      <Row gutter={[16, 16]} align="middle">
        <Col flex="auto">
          <Search
            placeholder="테이블명, 컬럼명, 설명으로 검색..."
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
            onSearch={onSearch}
            enterButton={<><SearchOutlined /> 검색</>}
            size="large"
            allowClear
          />
        </Col>
        <Col>
          <Space>
            <Segmented
              value={viewMode}
              onChange={(v) => onViewModeChange(v as string)}
              options={[
                { label: <Tooltip title="테이블 뷰"><UnorderedListOutlined /></Tooltip>, value: 'table' },
                { label: <Tooltip title="트리 뷰"><ApartmentOutlined /></Tooltip>, value: 'tree' },
                { label: <Tooltip title="관계 그래프"><ShareAltOutlined /></Tooltip>, value: 'graph' },
              ]}
              size="large"
            />
            <Tooltip title="현재 검색 조건 저장">
              <Button
                icon={<SaveOutlined />}
                size="large"
                onClick={onSaveSnapshotClick}
                disabled={!hasActiveFilters}
              />
            </Tooltip>
          </Space>
        </Col>
      </Row>
    </Card>
  );
};

export default CatalogSearchBar;
