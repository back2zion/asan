/**
 * 전문 의학지식 검색 페이지
 * PRD AAR-001 — RAG 의료 지식 기반 (medical_knowledge Milvus 컬렉션)
 * 탭: 의학 지식 검색 | ETL 상태 모니터링
 */

import React, { useState, useCallback, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import {
  Card, Typography, Space, Row, Col, Button, Input, Tag, Table, Statistic, Alert,
  Tabs, Select, Spin, App, Progress, Descriptions, Empty, Tooltip, Badge,
} from 'antd';
import {
  SearchOutlined, DatabaseOutlined, ReloadOutlined, CloudUploadOutlined,
  BookOutlined, FileTextOutlined, CheckCircleOutlined, ExclamationCircleOutlined,
  DeleteOutlined, PlayCircleOutlined, MedicineBoxOutlined,
} from '@ant-design/icons';
import { fetchPost } from '../services/apiUtils';

const { Title, Text, Paragraph } = Typography;
const { Search } = Input;

const API_BASE = '/api/v1/medical-knowledge';

// 문서 유형 태그 색상
const DOC_TYPE_COLORS: Record<string, { color: string; label: string }> = {
  textbook: { color: 'blue', label: '교과서' },
  guideline: { color: 'green', label: '가이드라인' },
  journal: { color: 'purple', label: '학술논문' },
  online: { color: 'cyan', label: '온라인정보' },
  qa_case: { color: 'red', label: '증례형 Q&A' },
  qa_short: { color: 'orange', label: '단답형 Q&A' },
  qa_essay: { color: 'gold', label: '서술형 Q&A' },
};

interface SearchResult {
  score: number;
  content: string;
  doc_type: string;
  source: string;
  department: string;
  metadata: Record<string, any>;
}

interface CollectionStats {
  exists: boolean;
  collection_name: string;
  count: number;
}

interface ETLStatus {
  running: boolean;
  total_zips: number;
  processed_zips: number;
  total_docs: number;
  total_chunks: number;
  current_zip: string;
  errors: string[];
  started_at: string | null;
  finished_at: string | null;
}

// ── 검색 탭 ──
const SearchTab: React.FC<{ initialQuery?: string }> = ({ initialQuery }) => {
  const { message } = App.useApp();
  const [query, setQuery] = useState(initialQuery || '');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [resultCount, setResultCount] = useState(0);
  const [topK, setTopK] = useState(10);
  const [docTypeFilter, setDocTypeFilter] = useState<string | undefined>(undefined);
  const prevQuery = React.useRef<string>('');

  const handleSearch = useCallback(async (searchQuery: string) => {
    if (!searchQuery.trim()) return;
    setLoading(true);
    try {
      const res = await fetchPost(`${API_BASE}/search`, {
        query: searchQuery.trim(),
        top_k: topK,
        doc_type: docTypeFilter || null,
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setResults(data.results || []);
      setResultCount(data.count || 0);
    } catch (e: any) {
      message.error(`검색 실패: ${e.message || '서버 오류'}`);
      setResults([]);
    } finally {
      setLoading(false);
    }
  }, [topK, docTypeFilter, message]);

  // URL ?q= 파라미터 변경 시 자동 검색
  useEffect(() => {
    if (initialQuery && initialQuery !== prevQuery.current) {
      prevQuery.current = initialQuery;
      setQuery(initialQuery);
      handleSearch(initialQuery);
    }
  }, [initialQuery, handleSearch]);

  const columns = [
    {
      title: '유사도',
      dataIndex: 'score',
      key: 'score',
      width: 90,
      render: (score: number) => (
        <Tag color={score > 0.8 ? 'green' : score > 0.6 ? 'blue' : 'default'}>
          {(score * 100).toFixed(1)}%
        </Tag>
      ),
      sorter: (a: SearchResult, b: SearchResult) => a.score - b.score,
    },
    {
      title: '유형',
      dataIndex: 'doc_type',
      key: 'doc_type',
      width: 120,
      render: (type: string) => {
        const info = DOC_TYPE_COLORS[type] || { color: 'default', label: type };
        return <Tag color={info.color}>{info.label}</Tag>;
      },
      filters: Object.entries(DOC_TYPE_COLORS).map(([key, val]) => ({
        text: val.label,
        value: key,
      })),
      onFilter: (value: any, record: SearchResult) => record.doc_type === value,
    },
    {
      title: '출처',
      dataIndex: 'source',
      key: 'source',
      width: 140,
      render: (source: string) => source || '-',
    },
    {
      title: '진료과',
      dataIndex: 'department',
      key: 'department',
      width: 120,
      render: (dept: string) => dept ? <Tag>{dept}</Tag> : '-',
    },
    {
      title: '내용',
      dataIndex: 'content',
      key: 'content',
      render: (content: string) => (
        <Paragraph
          ellipsis={{ rows: 3, expandable: true, symbol: '더보기' }}
          style={{ marginBottom: 0, fontSize: 13 }}
        >
          {content}
        </Paragraph>
      ),
    },
  ];

  const sampleQueries = [
    '수근관 증후군 진단',
    '백내장 위험인자',
    '급성 심근경색 초기 치료',
    '제2형 당뇨병 약물 치료',
    'COPD 진단 기준',
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      <Card>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Row gutter={16} align="middle">
            <Col flex="auto">
              <Search
                placeholder="의학 지식 검색 (예: 수근관 증후군 진단 방법)"
                enterButton={<><SearchOutlined /> 검색</>}
                size="large"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onSearch={handleSearch}
                loading={loading}
              />
            </Col>
            <Col>
              <Select
                value={topK}
                onChange={setTopK}
                style={{ width: 100 }}
                options={[
                  { value: 5, label: '5건' },
                  { value: 10, label: '10건' },
                  { value: 20, label: '20건' },
                  { value: 50, label: '50건' },
                ]}
              />
            </Col>
          </Row>
          <Space wrap>
            <Text type="secondary">빠른 검색:</Text>
            {sampleQueries.map((sq) => (
              <Tag
                key={sq}
                style={{ cursor: 'pointer' }}
                onClick={() => { setQuery(sq); handleSearch(sq); }}
              >
                {sq}
              </Tag>
            ))}
          </Space>
        </Space>
      </Card>

      {results.length > 0 && (
        <Card
          title={<><FileTextOutlined /> 검색 결과 ({resultCount}건)</>}
          size="small"
        >
          <Table
            columns={columns}
            dataSource={results.map((r, i) => ({ ...r, key: i }))}
            pagination={{ pageSize: 10, showSizeChanger: true }}
            size="small"
          />
        </Card>
      )}

      {!loading && results.length === 0 && query && (
        <Empty description="검색 결과가 없습니다. 다른 키워드로 시도해보세요." />
      )}
    </Space>
  );
};

// ── ETL 모니터링 탭 ──
const ETLTab: React.FC = () => {
  const { message, modal } = App.useApp();
  const [stats, setStats] = useState<CollectionStats | null>(null);
  const [etlStatus, setEtlStatus] = useState<ETLStatus | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchStats = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/stats`);
      const data = await res.json();
      setStats(data);
    } catch {
      setStats(null);
    }
  }, []);

  const fetchETLStatus = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/etl/status`);
      const data = await res.json();
      setEtlStatus(data);
    } catch {
      setEtlStatus(null);
    }
  }, []);

  useEffect(() => {
    fetchStats();
    fetchETLStatus();
  }, [fetchStats, fetchETLStatus]);

  // ETL 실행 중이면 5초마다 상태 갱신
  useEffect(() => {
    if (!etlStatus?.running) return;
    const interval = setInterval(() => {
      fetchETLStatus();
      fetchStats();
    }, 5000);
    return () => clearInterval(interval);
  }, [etlStatus?.running, fetchETLStatus, fetchStats]);

  const handleStartETL = useCallback(async (reset: boolean) => {
    setLoading(true);
    try {
      const res = await fetchPost(`${API_BASE}/etl/start`, { reset, batch_size: 64 });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      message.success('ETL이 시작되었습니다');
      setTimeout(fetchETLStatus, 1000);
    } catch (e: any) {
      message.error(`ETL 시작 실패: ${e.message || '서버 오류'}`);
    } finally {
      setLoading(false);
    }
  }, [message, fetchETLStatus]);

  const handleReset = useCallback(() => {
    modal.confirm({
      title: '컬렉션 초기화',
      content: '모든 의학 지식 데이터가 삭제됩니다. 계속하시겠습니까?',
      okText: '초기화',
      okType: 'danger',
      cancelText: '취소',
      onOk: async () => {
        try {
          const res = await fetch(`${API_BASE}/reset`, { method: 'DELETE' });
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          message.success('컬렉션이 초기화되었습니다');
          fetchStats();
        } catch (e: any) {
          message.error(`초기화 실패: ${e.message}`);
        }
      },
    });
  }, [modal, message, fetchStats]);

  const progressPct = etlStatus && etlStatus.total_zips > 0
    ? Math.round((etlStatus.processed_zips / etlStatus.total_zips) * 100)
    : 0;

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* 컬렉션 상태 */}
      <Card title={<><DatabaseOutlined /> 컬렉션 상태</>}>
        <Row gutter={[24, 16]}>
          <Col span={6}>
            <Statistic
              title="컬렉션 상태"
              value={stats?.exists ? '활성' : '미생성'}
              prefix={stats?.exists
                ? <CheckCircleOutlined style={{ color: '#52c41a' }} />
                : <ExclamationCircleOutlined style={{ color: '#faad14' }} />
              }
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="적재된 문서 수"
              value={stats?.count || 0}
              suffix="건"
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="ETL 상태"
              value={etlStatus?.running ? '실행 중' : '대기'}
              prefix={etlStatus?.running
                ? <Badge status="processing" />
                : <Badge status="default" />
              }
            />
          </Col>
          <Col span={6}>
            <Space>
              <Button
                icon={<ReloadOutlined />}
                onClick={() => { fetchStats(); fetchETLStatus(); }}
              >
                새로고침
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* ETL 실행 */}
      <Card title={<><CloudUploadOutlined /> ETL 관리</>}>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <Row gutter={16}>
            <Col>
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={() => handleStartETL(false)}
                loading={loading}
                disabled={etlStatus?.running}
              >
                ETL 시작 (이어서)
              </Button>
            </Col>
            <Col>
              <Button
                icon={<ReloadOutlined />}
                onClick={() => handleStartETL(true)}
                loading={loading}
                disabled={etlStatus?.running}
              >
                전체 재적재
              </Button>
            </Col>
            <Col>
              <Button
                danger
                icon={<DeleteOutlined />}
                onClick={handleReset}
                disabled={etlStatus?.running}
              >
                컬렉션 초기화
              </Button>
            </Col>
          </Row>

          {etlStatus?.running && (
            <>
              <Progress
                percent={progressPct}
                status="active"
                format={() => `${etlStatus.processed_zips}/${etlStatus.total_zips} ZIP`}
              />
              <Descriptions size="small" column={2}>
                <Descriptions.Item label="현재 파일">{etlStatus.current_zip || '-'}</Descriptions.Item>
                <Descriptions.Item label="적재 청크">{etlStatus.total_chunks}건</Descriptions.Item>
                <Descriptions.Item label="시작 시각">{etlStatus.started_at || '-'}</Descriptions.Item>
                <Descriptions.Item label="오류 수">{etlStatus.errors.length}건</Descriptions.Item>
              </Descriptions>
            </>
          )}

          {etlStatus?.finished_at && !etlStatus.running && (
            <Alert
              type="success"
              message={`ETL 완료 (${etlStatus.finished_at})`}
              description={`${etlStatus.total_chunks}건 적재, ${etlStatus.errors.length}건 오류`}
              showIcon
            />
          )}

          {etlStatus && etlStatus.errors.length > 0 && (
            <Alert
              type="warning"
              message={`오류 ${etlStatus.errors.length}건`}
              description={
                <ul style={{ margin: 0, paddingLeft: 16 }}>
                  {etlStatus.errors.slice(0, 5).map((err, i) => (
                    <li key={i}><Text type="secondary">{err}</Text></li>
                  ))}
                </ul>
              }
              showIcon
            />
          )}
        </Space>
      </Card>

      {/* 데이터 소스 설명 */}
      <Card title={<><BookOutlined /> 데이터 소스</>} size="small">
        <Descriptions column={2} size="small">
          <Descriptions.Item label="원천데이터 (TS_)">
            의학 교과서, 학회 가이드라인, 학술 논문, 온라인 의료정보 (국문/영문)
          </Descriptions.Item>
          <Descriptions.Item label="라벨링데이터 (TL_/VL_)">
            13개 진료과별 Q&A (증례형/단답형/서술형)
          </Descriptions.Item>
          <Descriptions.Item label="ZIP 파일 수">37개</Descriptions.Item>
          <Descriptions.Item label="예상 문서 수">~126,000개</Descriptions.Item>
        </Descriptions>
      </Card>
    </Space>
  );
};

// ── 메인 페이지 ──
const MedicalKnowledge: React.FC = () => {
  const [searchParams] = useSearchParams();
  const urlQuery = searchParams.get('q') || '';

  return (
    <div style={{ padding: 24 }}>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <div>
          <Title level={3} style={{ marginBottom: 4 }}>
            <MedicineBoxOutlined /> 전문 의학지식
          </Title>
          <Text type="secondary">
            AI Hub 전문 의학지식 데이터 기반 RAG 검색 — 교과서, 가이드라인, 학술논문, Q&A
          </Text>
        </div>

        <Tabs
          defaultActiveKey="search"
          items={[
            {
              key: 'search',
              label: <><SearchOutlined /> 의학 지식 검색</>,
              children: <SearchTab initialQuery={urlQuery} />,
            },
            {
              key: 'etl',
              label: <><DatabaseOutlined /> ETL 관리</>,
              children: <ETLTab />,
            },
          ]}
        />
      </Space>
    </div>
  );
};

export default MedicalKnowledge;
