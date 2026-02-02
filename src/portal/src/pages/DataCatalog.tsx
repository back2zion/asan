/**
 * 데이터 카탈로그 페이지
 * DPR-002: 메타데이터 기반 통합 검색 환경
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Input,
  Table,
  Tag,
  Space,
  Typography,
  Row,
  Col,
  Select,
  Checkbox,
  Collapse,
  Badge,
  Button,
  Tooltip,
  Empty,
  Spin,
  Descriptions,
  Modal,
  message,
} from 'antd';
import {
  SearchOutlined,
  TableOutlined,
  DatabaseOutlined,
  TagOutlined,
  FilterOutlined,
  StarOutlined,
  StarFilled,
  EyeOutlined,
  CopyOutlined,
  NodeIndexOutlined,
} from '@ant-design/icons';
import { useSearchParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { semanticApi, sanitizeText } from '../services/api';
import type { TableInfo, ColumnInfo } from '../services/api';
import type { ColumnsType } from 'antd/es/table';

const { Title, Text, Paragraph } = Typography;
const { Search } = Input;
const { Panel } = Collapse;

// 민감도 레벨 색상
const sensitivityColors: Record<string, string> = {
  PUBLIC: 'green',
  INTERNAL: 'blue',
  CONFIDENTIAL: 'orange',
  RESTRICTED: 'red',
};

// 민감도 레벨 라벨
const sensitivityLabels: Record<string, string> = {
  PUBLIC: '공개',
  INTERNAL: '내부',
  CONFIDENTIAL: '기밀',
  RESTRICTED: '제한',
};

const DataCatalog: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchQuery, setSearchQuery] = useState(searchParams.get('q') || '');
  const [selectedDomains, setSelectedDomains] = useState<string[]>([]);
  const [selectedTags, setSelectedTags] = useState<string[]>([]);
  const [selectedSensitivity, setSelectedSensitivity] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [detailModalVisible, setDetailModalVisible] = useState(false);

  // 검색 결과 조회
  const {
    data: searchResult,
    isLoading: isSearching,
    refetch: refetchSearch,
  } = useQuery({
    queryKey: ['catalog-search', searchQuery, selectedDomains, selectedTags, selectedSensitivity],
    queryFn: () =>
      semanticApi.facetedSearch({
        q: searchQuery || undefined,
        domains: selectedDomains.length > 0 ? selectedDomains : undefined,
        tags: selectedTags.length > 0 ? selectedTags : undefined,
        sensitivity: selectedSensitivity.length > 0 ? selectedSensitivity : undefined,
        limit: 50,
      }),
    enabled: true,
  });

  // 도메인 목록 조회
  const { data: domainsData } = useQuery({
    queryKey: ['domains'],
    queryFn: () => semanticApi.getDomains(),
  });

  // 태그 목록 조회
  const { data: tagsData } = useQuery({
    queryKey: ['tags'],
    queryFn: () => semanticApi.getTags(),
  });

  // 인기 데이터 조회
  const { data: popularData } = useQuery({
    queryKey: ['popular-data'],
    queryFn: () => semanticApi.getPopularData(10),
  });

  // URL 쿼리 파라미터 동기화
  useEffect(() => {
    const q = searchParams.get('q');
    if (q) {
      setSearchQuery(q);
    }
  }, [searchParams]);

  // 검색 실행
  const handleSearch = useCallback(
    (value: string) => {
      const sanitized = sanitizeText(value);
      setSearchQuery(sanitized);
      setSearchParams(sanitized ? { q: sanitized } : {});
    },
    [setSearchParams]
  );

  // 테이블 상세 보기
  const handleViewDetail = async (table: TableInfo) => {
    setSelectedTable(table);
    setDetailModalVisible(true);

    // 활용 기록
    try {
      await semanticApi.recordUsage('current_user', 'view', 'table', table.physical_name);
    } catch (error) {
      console.error('활용 기록 실패:', error);
    }
  };

  // 테이블명 복사
  const handleCopyTableName = (name: string) => {
    navigator.clipboard.writeText(name);
    message.success('테이블명이 복사되었습니다.');
  };

  // 테이블 목록 컬럼 정의
  const tableColumns: ColumnsType<TableInfo> = [
    {
      title: '테이블명',
      key: 'name',
      width: 280,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Space>
            <TableOutlined style={{ color: '#005BAC' }} />
            <Text strong>{record.business_name}</Text>
          </Space>
          <Text type="secondary" style={{ fontSize: 12 }}>
            {record.physical_name}
          </Text>
        </Space>
      ),
    },
    {
      title: '도메인',
      dataIndex: 'domain',
      key: 'domain',
      width: 120,
      render: (domain: string) => (
        <Tag icon={<DatabaseOutlined />} color="blue">
          {domain}
        </Tag>
      ),
    },
    {
      title: '설명',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: '태그',
      key: 'tags',
      width: 200,
      render: (_, record) => (
        <Space wrap size={[4, 4]}>
          {record.tags?.slice(0, 3).map((tag) => (
            <Tag key={tag} color="default">
              {tag}
            </Tag>
          ))}
          {record.tags?.length > 3 && (
            <Tooltip title={record.tags.slice(3).join(', ')}>
              <Tag>+{record.tags.length - 3}</Tag>
            </Tooltip>
          )}
        </Space>
      ),
    },
    {
      title: '활용',
      dataIndex: 'usage_count',
      key: 'usage_count',
      width: 80,
      align: 'center',
      render: (count: number) => (
        <Badge count={count} showZero color="#005BAC" overflowCount={999} />
      ),
    },
    {
      title: '',
      key: 'actions',
      width: 120,
      render: (_, record) => (
        <Space>
          <Tooltip title="상세 보기">
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => handleViewDetail(record)}
            />
          </Tooltip>
          <Tooltip title="테이블명 복사">
            <Button
              type="text"
              icon={<CopyOutlined />}
              onClick={() => handleCopyTableName(record.physical_name)}
            />
          </Tooltip>
          <Tooltip title="데이터 계보">
            <Button type="text" icon={<NodeIndexOutlined />} />
          </Tooltip>
        </Space>
      ),
    },
  ];

  // 컬럼 목록 컬럼 정의
  const columnColumns: ColumnsType<ColumnInfo> = [
    {
      title: '컬럼명',
      key: 'name',
      width: 200,
      render: (_, record) => (
        <Space direction="vertical" size={0}>
          <Text strong>{record.business_name}</Text>
          <Text type="secondary" style={{ fontSize: 11 }}>
            {record.physical_name}
          </Text>
        </Space>
      ),
    },
    {
      title: '타입',
      dataIndex: 'data_type',
      key: 'data_type',
      width: 100,
      render: (type: string) => <Tag color="geekblue">{type}</Tag>,
    },
    {
      title: '설명',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: 'PK',
      dataIndex: 'is_pk',
      key: 'is_pk',
      width: 60,
      align: 'center',
      render: (isPk: boolean) => (isPk ? <Tag color="gold">PK</Tag> : '-'),
    },
    {
      title: '민감도',
      dataIndex: 'sensitivity',
      key: 'sensitivity',
      width: 80,
      render: (sensitivity: string) => (
        <Tag color={sensitivityColors[sensitivity] || 'default'}>
          {sensitivityLabels[sensitivity] || sensitivity}
        </Tag>
      ),
    },
  ];

  const domains = domainsData?.data?.domains || [];
  const tags = tagsData?.data?.tags || [];
  const tables = searchResult?.data?.tables || [];
  const popular = popularData?.data?.items || [];

  return (
    <div>
      {/* 검색 영역 */}
      <Card style={{ marginBottom: 24 }}>
        <Row gutter={[16, 16]} align="middle">
          <Col flex="auto">
            <Search
              placeholder="테이블명, 컬럼명, 설명으로 검색..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onSearch={handleSearch}
              enterButton={<><SearchOutlined /> 검색</>}
              size="large"
              allowClear
            />
          </Col>
        </Row>
      </Card>

      <Row gutter={24}>
        {/* 필터 패널 */}
        <Col span={6}>
          <Card title={<><FilterOutlined /> 필터</>} size="small">
            <Collapse
              defaultActiveKey={['domain', 'tags', 'sensitivity']}
              ghost
              expandIconPosition="end"
            >
              <Panel header={<><DatabaseOutlined /> 도메인</>} key="domain">
                <Checkbox.Group
                  value={selectedDomains}
                  onChange={(values) => setSelectedDomains(values as string[])}
                  style={{ display: 'flex', flexDirection: 'column', gap: 8 }}
                >
                  {domains.map((domain: string) => (
                    <Checkbox key={domain} value={domain}>
                      {domain}
                    </Checkbox>
                  ))}
                </Checkbox.Group>
              </Panel>

              <Panel header={<><TagOutlined /> 태그</>} key="tags">
                <Select
                  mode="multiple"
                  placeholder="태그 선택"
                  value={selectedTags}
                  onChange={setSelectedTags}
                  style={{ width: '100%' }}
                  options={tags.map((tag: string) => ({ label: tag, value: tag }))}
                  maxTagCount={2}
                />
              </Panel>

              <Panel header="민감도" key="sensitivity">
                <Checkbox.Group
                  value={selectedSensitivity}
                  onChange={(values) => setSelectedSensitivity(values as string[])}
                  style={{ display: 'flex', flexDirection: 'column', gap: 8 }}
                >
                  {Object.entries(sensitivityLabels).map(([key, label]) => (
                    <Checkbox key={key} value={key}>
                      <Tag color={sensitivityColors[key]}>{label}</Tag>
                    </Checkbox>
                  ))}
                </Checkbox.Group>
              </Panel>
            </Collapse>

            {(selectedDomains.length > 0 ||
              selectedTags.length > 0 ||
              selectedSensitivity.length > 0) && (
              <Button
                type="link"
                onClick={() => {
                  setSelectedDomains([]);
                  setSelectedTags([]);
                  setSelectedSensitivity([]);
                }}
                style={{ marginTop: 16 }}
              >
                필터 초기화
              </Button>
            )}
          </Card>

          {/* 인기 데이터 */}
          <Card
            title={<><StarOutlined /> 인기 데이터</>}
            size="small"
            style={{ marginTop: 16 }}
          >
            {popular.length > 0 ? (
              <Space direction="vertical" style={{ width: '100%' }}>
                {popular.slice(0, 5).map((item: any, index: number) => (
                  <div
                    key={item.name}
                    style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      padding: '4px 0',
                    }}
                  >
                    <Space>
                      <Text type="secondary">{index + 1}.</Text>
                      <Text
                        ellipsis
                        style={{ maxWidth: 140, cursor: 'pointer' }}
                        onClick={() => handleSearch(item.name)}
                      >
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
        </Col>

        {/* 검색 결과 */}
        <Col span={18}>
          <Card
            title={
              <Space>
                <TableOutlined />
                검색 결과
                <Text type="secondary">({tables.length}개)</Text>
              </Space>
            }
          >
            <Spin spinning={isSearching}>
              {tables.length > 0 ? (
                <Table
                  dataSource={tables}
                  columns={tableColumns}
                  rowKey="physical_name"
                  pagination={{
                    pageSize: 10,
                    showSizeChanger: true,
                    showTotal: (total) => `총 ${total}개`,
                  }}
                  size="middle"
                />
              ) : (
                <Empty
                  description={
                    searchQuery
                      ? '검색 결과가 없습니다.'
                      : '검색어를 입력하거나 필터를 선택하세요.'
                  }
                />
              )}
            </Spin>
          </Card>
        </Col>
      </Row>

      {/* 테이블 상세 모달 */}
      <Modal
        title={
          <Space>
            <TableOutlined style={{ color: '#005BAC' }} />
            {selectedTable?.business_name}
          </Space>
        }
        open={detailModalVisible}
        onCancel={() => setDetailModalVisible(false)}
        width={900}
        footer={null}
      >
        {selectedTable && (
          <>
            <Descriptions bordered column={2} size="small" style={{ marginBottom: 24 }}>
              <Descriptions.Item label="물리명">
                <Space>
                  <Text code>{selectedTable.physical_name}</Text>
                  <Button
                    type="text"
                    size="small"
                    icon={<CopyOutlined />}
                    onClick={() => handleCopyTableName(selectedTable.physical_name)}
                  />
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label="도메인">
                <Tag color="blue">{selectedTable.domain}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="설명" span={2}>
                {selectedTable.description}
              </Descriptions.Item>
              <Descriptions.Item label="태그" span={2}>
                <Space wrap>
                  {selectedTable.tags?.map((tag) => (
                    <Tag key={tag}>{tag}</Tag>
                  ))}
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label="활용 횟수">
                {selectedTable.usage_count}회
              </Descriptions.Item>
            </Descriptions>

            <Title level={5}>컬럼 정보</Title>
            <Table
              dataSource={selectedTable.columns || []}
              columns={columnColumns}
              rowKey="physical_name"
              pagination={false}
              size="small"
              scroll={{ y: 300 }}
            />
          </>
        )}
      </Modal>
    </div>
  );
};

export default DataCatalog;
