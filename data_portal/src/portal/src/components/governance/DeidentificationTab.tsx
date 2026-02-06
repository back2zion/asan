/**
 * 비식별화 탭 컴포넌트 (DGR-006)
 */

import React from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col, Statistic, Progress,
} from 'antd';
import {
  CheckCircleOutlined, DatabaseOutlined, LockOutlined, EyeInvisibleOutlined,
} from '@ant-design/icons';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip as RechartsTooltip, ResponsiveContainer, Legend } from 'recharts';

const { Title, Text } = Typography;

// ── Mock 데이터 ──
const PII_TYPES = [
  { key: 'name', type: '환자명', count: 1247, method: '가명처리', status: 'done', example: '홍길동 → 홍*동' },
  { key: 'ssn', type: '주민등록번호', count: 892, method: '마스킹', status: 'done', example: '880101-1****** → ******-*******' },
  { key: 'mrn', type: '진료기록번호', count: 3456, method: '일방향 해시', status: 'done', example: 'MRN-12345 → a3f8c2...' },
  { key: 'phone', type: '연락처', count: 678, method: '마스킹', status: 'done', example: '010-1234-5678 → 010-****-5678' },
  { key: 'addr', type: '주소', count: 421, method: '범주화', status: 'partial', example: '서울시 송파구 올림픽로 43 → 서울시 송파구' },
  { key: 'birth', type: '생년월일', count: 2103, method: '라운딩', status: 'done', example: '1988-01-15 → 1988-01' },
  { key: 'email', type: '이메일', count: 156, method: '마스킹', status: 'partial', example: 'hong@amc.seoul.kr → h***@***.***' },
];

const DEIDENT_TREND = [
  { date: '01/30', detected: 823, masked: 810 },
  { date: '01/31', detected: 912, masked: 900 },
  { date: '02/01', detected: 1045, masked: 1038 },
  { date: '02/02', detected: 756, masked: 750 },
  { date: '02/03', detected: 1123, masked: 1115 },
  { date: '02/04', detected: 987, masked: 982 },
  { date: '02/05', detected: 1201, masked: 1195 },
];

const SAMPLE_BEFORE = `[진료기록] 환자 홍길동(880101-1234567), 만 38세 남성.
연락처: 010-1234-5678, 주소: 서울시 송파구 올림픽로 43길 12.
진료기록번호: MRN-20240315, 주치의: 김의사.
진단: 제2형 당뇨병(E11.9), HbA1c 7.8%.
처방: Metformin 500mg bid, Glimepiride 2mg qd.`;

const SAMPLE_AFTER = `[진료기록] 환자 홍*동(******-*******), 만 38세 남성.
연락처: 010-****-5678, 주소: 서울시 송파구.
진료기록번호: a3f8c2e1..., 주치의: 김**.
진단: 제2형 당뇨병(E11.9), HbA1c 7.8%.
처방: Metformin 500mg bid, Glimepiride 2mg qd.`;

// ── 하이라이트 헬퍼 ──
function highlightText(
  text: string,
  patterns: { regex: RegExp; color: string }[],
  style: React.CSSProperties,
): React.ReactNode[] {
  const allMatches: { start: number; end: number; color: string }[] = [];

  patterns.forEach(({ regex, color }) => {
    let match;
    while ((match = regex.exec(text)) !== null) {
      allMatches.push({ start: match.index, end: match.index + match[0].length, color });
    }
  });

  allMatches.sort((a, b) => a.start - b.start);

  const result: React.ReactNode[] = [];
  let lastIndex = 0;
  allMatches.forEach((m, i) => {
    if (m.start > lastIndex) {
      result.push(<span key={`t-${i}`}>{text.slice(lastIndex, m.start)}</span>);
    }
    result.push(
      <span key={`h-${i}`} style={style}>
        {text.slice(m.start, m.end)}
      </span>
    );
    lastIndex = m.end;
  });
  if (lastIndex < text.length) {
    result.push(<span key="end">{text.slice(lastIndex)}</span>);
  }
  return result;
}

const PII_HIGHLIGHT_PATTERNS = [
  { regex: /홍길동/g, color: '#ff4d4f' },
  { regex: /880101-1234567/g, color: '#ff4d4f' },
  { regex: /010-1234-5678/g, color: '#ff4d4f' },
  { regex: /서울시 송파구 올림픽로 43길 12/g, color: '#ff4d4f' },
  { regex: /MRN-20240315/g, color: '#ff4d4f' },
  { regex: /김의사/g, color: '#ff4d4f' },
];

const MASKED_HIGHLIGHT_PATTERNS = [
  { regex: /홍\*동/g, color: '#52c41a' },
  { regex: /\*{6}-\*{7}/g, color: '#52c41a' },
  { regex: /010-\*{4}-5678/g, color: '#52c41a' },
  { regex: /서울시 송파구\./g, color: '#52c41a' },
  { regex: /a3f8c2e1\.\.\./g, color: '#52c41a' },
  { regex: /김\*\*/g, color: '#52c41a' },
];

const DeidentificationTab: React.FC = () => {
  const totalScanned = 128453;
  const piiDetected = PII_TYPES.reduce((s, p) => s + p.count, 0);
  const maskingRate = 97.2;
  const kAnonymity = 5;

  const piiColumns = [
    { title: 'PII 유형', dataIndex: 'type', key: 'type', render: (v: string) => <Text strong>{v}</Text> },
    {
      title: '탐지 건수',
      dataIndex: 'count',
      key: 'count',
      sorter: (a: typeof PII_TYPES[0], b: typeof PII_TYPES[0]) => a.count - b.count,
      render: (v: number) => <Text>{v.toLocaleString()}건</Text>,
    },
    { title: '비식별화 기법', dataIndex: 'method', key: 'method', render: (v: string) => <Tag color="blue">{v}</Tag> },
    {
      title: '처리 상태',
      dataIndex: 'status',
      key: 'status',
      render: (v: string) => v === 'done'
        ? <Tag color="green">완료</Tag>
        : <Tag color="orange">진행중</Tag>,
    },
    { title: '변환 예시', dataIndex: 'example', key: 'example', render: (v: string) => <Text code style={{ fontSize: 12 }}>{v}</Text> },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* 요약 카드 */}
      <Row gutter={16}>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="총 스캔 레코드" value={totalScanned} prefix={<DatabaseOutlined />} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="PII 탐지 건수" value={piiDetected} prefix={<EyeInvisibleOutlined />} valueStyle={{ color: '#cf1322' }} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="마스킹 완료율" value={maskingRate} suffix="%" prefix={<CheckCircleOutlined />} valueStyle={{ color: '#3f8600' }} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card size="small">
            <Statistic title="K-익명성 수준" value={kAnonymity} prefix={<LockOutlined />} suffix="(k)" valueStyle={{ color: '#005BAC' }} />
          </Card>
        </Col>
      </Row>

      {/* PII 유형별 탐지 현황 */}
      <Card title="PII 유형별 탐지 현황" size="small">
        <Table
          columns={piiColumns}
          dataSource={PII_TYPES}
          rowKey="key"
          size="small"
          pagination={false}
        />
      </Card>

      {/* Before/After 비교 */}
      <Row gutter={16}>
        <Col xs={24} md={12}>
          <Card
            title={<><Tag color="red">Before</Tag> 원본 텍스트 (PII 포함)</>}
            size="small"
          >
            <div style={{ fontFamily: 'monospace', fontSize: 13, lineHeight: 1.8, whiteSpace: 'pre-wrap' }}>
              {highlightText(
                SAMPLE_BEFORE,
                PII_HIGHLIGHT_PATTERNS,
                { background: '#fff1f0', color: '#cf1322', fontWeight: 600, padding: '0 2px', borderRadius: 2 },
              )}
            </div>
          </Card>
        </Col>
        <Col xs={24} md={12}>
          <Card
            title={<><Tag color="green">After</Tag> 비식별화 텍스트</>}
            size="small"
          >
            <div style={{ fontFamily: 'monospace', fontSize: 13, lineHeight: 1.8, whiteSpace: 'pre-wrap' }}>
              {highlightText(
                SAMPLE_AFTER,
                MASKED_HIGHLIGHT_PATTERNS,
                { background: '#f6ffed', color: '#389e0d', fontWeight: 600, padding: '0 2px', borderRadius: 2 },
              )}
            </div>
          </Card>
        </Col>
      </Row>

      {/* 비식별화 추이 차트 */}
      <Card title="비식별화 처리 추이 (최근 7일)" size="small">
        <ResponsiveContainer width="100%" height={280}>
          <AreaChart data={DEIDENT_TREND}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="date" />
            <YAxis />
            <RechartsTooltip />
            <Legend />
            <Area type="monotone" dataKey="detected" name="PII 탐지" stroke="#cf1322" fill="#fff1f0" strokeWidth={2} />
            <Area type="monotone" dataKey="masked" name="마스킹 완료" stroke="#389e0d" fill="#f6ffed" strokeWidth={2} />
          </AreaChart>
        </ResponsiveContainer>
      </Card>

      {/* 프라이버시 보호 기법 상태 */}
      <Card title={<><LockOutlined /> 프라이버시 보호 기법 상태</>} size="small">
        <Row gutter={16}>
          <Col xs={24} md={8}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic title="K-익명성" value={5} suffix="(k)" valueStyle={{ color: '#005BAC', fontSize: 28 }} />
              <Progress percent={100} status="success" size="small" />
              <Text type="secondary" style={{ fontSize: 12 }}>모든 레코드가 최소 5개의 동일 그룹 보장</Text>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Statistic title="차등 프라이버시" value="ε=1.0" valueStyle={{ color: '#005BAC', fontSize: 28 }} />
              <Progress percent={100} status="success" size="small" />
              <Text type="secondary" style={{ fontSize: 12 }}>통계 쿼리에 Laplace 노이즈 적용</Text>
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small" style={{ textAlign: 'center' }}>
              <Title level={4} style={{ color: '#3f8600', margin: '8px 0' }}>HIPAA Compliant</Title>
              <Progress percent={100} status="success" size="small" />
              <Text type="secondary" style={{ fontSize: 12 }}>18개 식별자 Safe Harbor 기준 충족</Text>
            </Card>
          </Col>
        </Row>
      </Card>
    </Space>
  );
};

export default DeidentificationTab;
