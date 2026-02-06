/**
 * 데이터 카탈로그 페이지
 * DGR-003: 데이터 카탈로그 - 표준 메타 + Biz 메타 집약, 오너쉽 관리
 * DPR-002: 지능형 데이터 카탈로그 및 탐색 - Lineage, 연관 추천, Semantic Layer
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
  App,
  Tabs,
  Divider,
  Avatar,
} from 'antd';
import type { CollapseProps } from 'antd';
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
  CodeOutlined,
  ApiOutlined,
  UserOutlined,
  TeamOutlined,
  ClockCircleOutlined,
  LinkOutlined,
  BranchesOutlined,
  FileTextOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { useSearchParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { semanticApi, sanitizeText } from '../services/api';
import type { TableInfo, ColumnInfo } from '../services/api';
import type { ColumnsType } from 'antd/es/table';
import LineageGraph from '../components/lineage/LineageGraph';

const { Title, Text, Paragraph } = Typography;
const { Search } = Input;

// 품질 정보 컴포넌트 (API 연동)
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

// 연관 테이블 컴포넌트 (API 연동)
const RelatedTables: React.FC<{ tableName: string }> = ({ tableName }) => {
  const { data, isLoading } = useQuery({
    queryKey: ['lineage', tableName],
    queryFn: () => semanticApi.getLineage(tableName),
    enabled: !!tableName,
  });

  if (isLoading) return <Spin size="small" />;

  const relatedTables = data?.lineage?.related_tables || [];

  return (
    <Space wrap>
      {relatedTables.length > 0 ? (
        relatedTables.map((t: any) => (
          <Tag key={t.name} icon={<LinkOutlined />} color="cyan">
            {t.name} ({t.business_name})
          </Tag>
        ))
      ) : (
        <Text type="secondary">연관 테이블 없음</Text>
      )}
    </Space>
  );
};

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
  const { message } = App.useApp();
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchQuery, setSearchQuery] = useState(searchParams.get('q') || '');
  const [selectedDomains, setSelectedDomains] = useState<string[]>([]);
  const [selectedTags, setSelectedTags] = useState<string[]>([]);
  const [selectedSensitivity, setSelectedSensitivity] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [activeDetailTab, setActiveDetailTab] = useState('columns');
  const [lineageModalVisible, setLineageModalVisible] = useState(false);
  const [lineageTable, setLineageTable] = useState<TableInfo | null>(null);

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
            <Button
              type="text"
              icon={<NodeIndexOutlined />}
              onClick={() => {
                setLineageTable(record);
                setLineageModalVisible(true);
              }}
            />
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

  // Collapse items for filter panel (DPR-002: Faceted Search)
  const filterCollapseItems: CollapseProps['items'] = [
    {
      key: 'domain',
      label: <><DatabaseOutlined /> 도메인</>,
      children: (
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
          onChange={setSelectedTags}
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
          onChange={(values) => setSelectedSensitivity(values as string[])}
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

  // Generate SQL sample code (DGR-003: SQL 카탈로그 제공)
  const generateSqlCode = (table: TableInfo) => {
    const columns = table.columns?.map(c => c.physical_name).join(',\n       ') || '*';
    return `SELECT ${columns}
FROM ${table.physical_name}
LIMIT 100;`;
  };

  // Generate Python code (DPR-001: Python/R 즉시 로딩 코드 제공)
  const generatePythonCode = (table: TableInfo) => {
    return `import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:pass@host:5432/db')
df = pd.read_sql('SELECT * FROM ${table.physical_name} LIMIT 100', engine)
print(df.head())`;
  };

  // Generate API endpoint (DGR-003: API 카탈로그 제공)
  const generateApiEndpoint = (table: TableInfo) => {
    return `GET /api/v1/data/${table.physical_name.toLowerCase()}?limit=100

# curl 예시
curl -X GET "https://idp.amc.seoul.kr/api/v1/data/${table.physical_name.toLowerCase()}?limit=100" \\
  -H "Authorization: Bearer <TOKEN>"`;
  };

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
              items={filterCollapseItems}
            />

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

      {/* 테이블 상세 모달 (DPR-002: 지능형 카탈로그) */}
      <Modal
        title={
          <Space>
            <TableOutlined style={{ color: '#005BAC' }} />
            {selectedTable?.business_name}
            <Tag color="blue">{selectedTable?.domain}</Tag>
          </Space>
        }
        open={detailModalVisible}
        onCancel={() => setDetailModalVisible(false)}
        width={1000}
        footer={null}
      >
        {selectedTable && (
          <>
            {/* 기본 정보 (DGR-003: 오너쉽 정보 포함) */}
            <Descriptions bordered column={3} size="small" style={{ marginBottom: 16 }}>
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
              <Descriptions.Item label="오너">
                <Space>
                  <Avatar size="small" icon={<UserOutlined />} />
                  <Text>데이터관리팀</Text>
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label="활용 횟수">
                <Badge count={selectedTable.usage_count} showZero color="#005BAC" />
              </Descriptions.Item>
              <Descriptions.Item label="설명" span={3}>
                {selectedTable.description}
              </Descriptions.Item>
              <Descriptions.Item label="태그" span={2}>
                <Space wrap>
                  {selectedTable.tags?.map((tag) => (
                    <Tag key={tag}>{tag}</Tag>
                  ))}
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label="공유 범위">
                <Tag icon={<TeamOutlined />} color="green">전체 공개</Tag>
              </Descriptions.Item>
            </Descriptions>

            {/* 탭 기반 상세 정보 */}
            <Tabs
              activeKey={activeDetailTab}
              onChange={setActiveDetailTab}
              items={[
                {
                  key: 'columns',
                  label: <><TableOutlined /> 컬럼 정보</>,
                  children: (
                    <Table
                      dataSource={selectedTable.columns || []}
                      columns={columnColumns}
                      rowKey="physical_name"
                      pagination={false}
                      size="small"
                      scroll={{ y: 280 }}
                    />
                  ),
                },
                {
                  key: 'lineage',
                  label: <><BranchesOutlined /> 데이터 계보</>,
                  forceRender: true,
                  children: (
                    <div style={{ padding: '8px 0' }}>
                      <Title level={5} style={{ marginBottom: 16 }}>Data Lineage (DPR-002)</Title>
                      {activeDetailTab === 'lineage' && detailModalVisible && (
                        <LineageGraph
                          key={`lineage-${selectedTable.physical_name}`}
                          tableName={selectedTable.business_name}
                          physicalName={selectedTable.physical_name}
                        />
                      )}
                      <Divider />
                      <Title level={5}>연관 테이블 (DPR-002: 연관 데이터셋 추천)</Title>
                      <RelatedTables tableName={selectedTable.physical_name} />
                    </div>
                  ),
                },
                {
                  key: 'code',
                  label: <><CodeOutlined /> 코드 샘플</>,
                  children: (
                    <div style={{ padding: '16px 0' }}>
                      {/* SQL (DGR-003: SQL 카탈로그 제공) */}
                      <div style={{ marginBottom: 16 }}>
                        <Space style={{ marginBottom: 8 }}>
                          <DatabaseOutlined />
                          <Text strong>SQL Query</Text>
                          <Button
                            size="small"
                            icon={<CopyOutlined />}
                            onClick={() => {
                              navigator.clipboard.writeText(generateSqlCode(selectedTable));
                              message.success('SQL이 복사되었습니다.');
                            }}
                          >
                            복사
                          </Button>
                        </Space>
                        <pre style={{
                          background: '#f5f5f5',
                          padding: 12,
                          borderRadius: 4,
                          fontSize: 12,
                          overflow: 'auto',
                        }}>
                          {generateSqlCode(selectedTable)}
                        </pre>
                      </div>

                      {/* Python (DPR-001: Python/R 즉시 로딩 코드 제공) */}
                      <div style={{ marginBottom: 16 }}>
                        <Space style={{ marginBottom: 8 }}>
                          <ThunderboltOutlined />
                          <Text strong>Python (pandas)</Text>
                          <Button
                            size="small"
                            icon={<CopyOutlined />}
                            onClick={() => {
                              navigator.clipboard.writeText(generatePythonCode(selectedTable));
                              message.success('Python 코드가 복사되었습니다.');
                            }}
                          >
                            복사
                          </Button>
                        </Space>
                        <pre style={{
                          background: '#f5f5f5',
                          padding: 12,
                          borderRadius: 4,
                          fontSize: 12,
                          overflow: 'auto',
                        }}>
                          {generatePythonCode(selectedTable)}
                        </pre>
                      </div>

                      {/* API (DGR-003: API 카탈로그 제공) */}
                      <div>
                        <Space style={{ marginBottom: 8 }}>
                          <ApiOutlined />
                          <Text strong>REST API</Text>
                          <Button
                            size="small"
                            icon={<CopyOutlined />}
                            onClick={() => {
                              navigator.clipboard.writeText(generateApiEndpoint(selectedTable));
                              message.success('API 정보가 복사되었습니다.');
                            }}
                          >
                            복사
                          </Button>
                        </Space>
                        <pre style={{
                          background: '#f5f5f5',
                          padding: 12,
                          borderRadius: 4,
                          fontSize: 12,
                          overflow: 'auto',
                        }}>
                          {generateApiEndpoint(selectedTable)}
                        </pre>
                      </div>
                    </div>
                  ),
                },
                {
                  key: 'quality',
                  label: <><FileTextOutlined /> 품질 정보</>,
                  children: (
                    <QualityInfo tableName={selectedTable.physical_name} />
                  ),
                },
              ]}
            />
          </>
        )}
      </Modal>

      {/* 데이터 계보 모달 (검색 결과에서 직접 열기) */}
      <Modal
        title={
          <Space>
            <BranchesOutlined style={{ color: '#005BAC' }} />
            데이터 계보
            {lineageTable && <Tag color="blue">{lineageTable.business_name}</Tag>}
          </Space>
        }
        open={lineageModalVisible}
        onCancel={() => {
          setLineageModalVisible(false);
          setLineageTable(null);
        }}
        width="90%"
        style={{ maxWidth: 1600, top: 20 }}
        footer={null}
        destroyOnHidden
      >
        {lineageTable && lineageModalVisible && (
          <div>
            <Descriptions bordered column={2} size="small" style={{ marginBottom: 16 }}>
              <Descriptions.Item label="테이블명">
                <Text code>{lineageTable.physical_name}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="도메인">
                <Tag color="blue">{lineageTable.domain}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="설명" span={2}>
                {lineageTable.description}
              </Descriptions.Item>
            </Descriptions>

            <Title level={5} style={{ marginBottom: 16 }}>
              <BranchesOutlined /> 데이터 흐름 (Source → Bronze → Silver → Gold)
            </Title>
            <LineageGraph
              key={`lineage-modal-${lineageTable.physical_name}`}
              tableName={lineageTable.business_name}
              physicalName={lineageTable.physical_name}
            />

            <Divider />
            <Title level={5}>연관 테이블</Title>
            <RelatedTables tableName={lineageTable.physical_name} />
          </div>
        )}
      </Modal>
    </div>
  );
};

export default DataCatalog;
