import React, { useState, useCallback } from 'react';
import {
  App, Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Alert, Segmented, Spin, Empty,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, ClockCircleOutlined,
  PlayCircleOutlined, BarChartOutlined, DatabaseOutlined,
  SafetyCertificateOutlined, AuditOutlined, DashboardOutlined,
  ExperimentOutlined, FieldTimeOutlined,
} from '@ant-design/icons';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RTooltip, Legend, ResponsiveContainer } from 'recharts';
import { fetchPost } from '../../services/apiUtils';

const { Text } = Typography;

async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function postJSON(url: string, body: any) {
  const res = await fetchPost(url, body);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const MigrationVerificationTab: React.FC = () => {
  const { message, modal } = App.useApp();
  const [section, setSection] = useState<string>('verify');
  const [verifyData, setVerifyData] = useState<any>(null);
  const [benchData, setBenchData] = useState<any>(null);
  const [deidentData, setDeidentData] = useState<any>(null);
  const [verifyLoading, setVerifyLoading] = useState(false);
  const [benchLoading, setBenchLoading] = useState(false);
  const [deidentLoading, setDeidentLoading] = useState(false);

  const runVerify = useCallback(() => {
    modal.confirm({
      title: '이관 검증 실행',
      content: '전체 테이블 이관 검증을 실행합니다. 데이터 양에 따라 10초 이상 소요될 수 있습니다.',
      okText: '실행',
      cancelText: '취소',
      onOk: async () => {
        setVerifyLoading(true);
        const hide = message.loading('이관 검증 중입니다...', 0);
        try {
          const data = await fetchJSON('/api/v1/migration/verify');
          setVerifyData(data);
        } catch { message.error('이관 검증 실패'); }
        finally { hide(); setVerifyLoading(false); }
      },
    });
  }, [modal, message]);

  const runBenchmark = useCallback(() => {
    modal.confirm({
      title: '성능 벤치마크 실행',
      content: '대표 쿼리 벤치마크를 실행합니다. 대량 데이터 조회로 인해 10초 이상 소요될 수 있습니다.',
      okText: '실행',
      cancelText: '취소',
      onOk: async () => {
        setBenchLoading(true);
        const hide = message.loading('벤치마크 실행 중...', 0);
        try {
          const data = await postJSON('/api/v1/migration/benchmark', {});
          setBenchData(data);
        } catch { message.error('벤치마크 실행 실패'); }
        finally { hide(); setBenchLoading(false); }
      },
    });
  }, [modal, message]);

  const runDeidentBenchmark = useCallback(() => {
    modal.confirm({
      title: '비식별화 벤치마크 실행',
      content: '비식별화 처리 벤치마크를 실행합니다. 데이터 양에 따라 10초 이상 소요될 수 있습니다.',
      okText: '실행',
      cancelText: '취소',
      onOk: async () => {
        setDeidentLoading(true);
        const hide = message.loading('비식별화 벤치마크 실행 중...', 0);
        try {
          const data = await postJSON('/api/v1/migration/deident-benchmark', {});
          setDeidentData(data);
        } catch { message.error('비식별화 벤치마크 실패'); }
        finally { hide(); setDeidentLoading(false); }
      },
    });
  }, [modal, message]);

  const statusTag = (status: string) => {
    switch (status) {
      case 'pass': return <Tag color="success">Pass</Tag>;
      case 'warning': return <Tag color="warning">Warning</Tag>;
      case 'fail': return <Tag color="error">Fail</Tag>;
      case 'skip': return <Tag color="default">Skip</Tag>;
      case 'error': return <Tag color="error">Error</Tag>;
      default: return <Tag>{status}</Tag>;
    }
  };

  const renderVerifySection = () => {
    const summary = verifyData?.summary;
    const results = verifyData?.results || [];

    const verifyCols = [
      { title: '테이블', dataIndex: 'table', key: 'table', width: 180, render: (v: string) => <Text strong code>{v}</Text> },
      { title: '기대 건수', dataIndex: 'expected', key: 'expected', width: 120, render: (v: number) => v.toLocaleString() },
      { title: '실측 건수', dataIndex: 'actual', key: 'actual', width: 120, render: (v: number) => <Text strong>{v.toLocaleString()}</Text> },
      { title: '건수', dataIndex: 'count_status', key: 'cs', width: 80, render: statusTag },
      { title: 'NULL', dataIndex: 'null_status', key: 'ns', width: 80, render: statusTag },
      { title: 'FK', dataIndex: 'fk_status', key: 'fk', width: 80, render: statusTag },
      { title: '미래일자', dataIndex: 'future_status', key: 'fs', width: 90, render: statusTag },
      { title: '종합', dataIndex: 'overall', key: 'overall', width: 80, render: statusTag },
    ];

    return (
      <div>
        <div style={{ textAlign: 'right', marginBottom: 16 }}>
          <Button type="primary" icon={<PlayCircleOutlined />} onClick={runVerify} loading={verifyLoading}>검증 실행</Button>
        </div>
        {summary && (
          <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
            <Col xs={12} md={6}><Card size="small"><Statistic title="총 테이블" value={summary.total_tables} prefix={<DatabaseOutlined />} /></Card></Col>
            <Col xs={12} md={6}><Card size="small"><Statistic title="Pass" value={summary.pass} valueStyle={{ color: '#3f8600' }} prefix={<CheckCircleOutlined />} /></Card></Col>
            <Col xs={12} md={6}><Card size="small"><Statistic title="Warning" value={summary.warning} valueStyle={{ color: '#d48806' }} prefix={<ClockCircleOutlined />} /></Card></Col>
            <Col xs={12} md={6}><Card size="small"><Statistic title="Fail" value={summary.fail} valueStyle={{ color: '#cf1322' }} prefix={<CloseCircleOutlined />} /></Card></Col>
          </Row>
        )}
        <Spin spinning={verifyLoading}>
          {results.length > 0 ? (
            <Table
              dataSource={results.map((r: any) => ({ ...r, key: r.table }))}
              columns={verifyCols}
              size="small"
              pagination={false}
              scroll={{ x: 900 }}
              expandable={{
                expandedRowRender: (record: any) => (
                  <div style={{ padding: '8px 16px' }}>
                    {record.null_details?.length > 0 && (
                      <div style={{ marginBottom: 8 }}>
                        <Text strong>필수 컬럼 NULL 상세:</Text>
                        <Table
                          dataSource={record.null_details.map((d: any, i: number) => ({ ...d, key: i }))}
                          columns={[
                            { title: '컬럼', dataIndex: 'column', key: 'col', render: (v: string) => <Text code>{v}</Text> },
                            { title: 'NULL 건수', dataIndex: 'null_count', key: 'nc', render: (v: number) => v >= 0 ? v.toLocaleString() : '-' },
                            { title: 'NULL %', dataIndex: 'null_pct', key: 'np', render: (v: number) => `${v}%` },
                            { title: '상태', dataIndex: 'status', key: 'st', render: statusTag },
                          ]}
                          size="small"
                          pagination={false}
                        />
                      </div>
                    )}
                    {record.orphan_count > 0 && (
                      <Alert type="warning" message={`FK 고아 레코드: ${record.orphan_count.toLocaleString()}건 (person_id 불일치)`} style={{ marginBottom: 8 }} />
                    )}
                    {record.future_count > 0 && (
                      <Alert type="warning" message={`미래 날짜 레코드: ${record.future_count.toLocaleString()}건`} />
                    )}
                  </div>
                ),
              }}
            />
          ) : !verifyLoading ? <Empty description="'검증 실행' 버튼을 클릭하세요" /> : null}
        </Spin>
        {summary && (
          <Alert
            style={{ marginTop: 16 }}
            type={summary.fail > 0 ? 'error' : summary.warning > 0 ? 'warning' : 'success'}
            showIcon
            message={`검증 완료: ${summary.pass} Pass / ${summary.warning} Warning / ${summary.fail} Fail (총 ${summary.total_rows?.toLocaleString()} rows, ${summary.duration_ms}ms)`}
          />
        )}
      </div>
    );
  };

  const renderBenchmarkSection = () => {
    const summary = benchData?.summary;
    const results = benchData?.results || [];

    const chartData = results.map((r: any) => ({ name: r.id, elapsed: r.elapsed_ms, threshold: r.threshold_ms }));

    const benchCols = [
      { title: 'ID', dataIndex: 'id', key: 'id', width: 60 },
      { title: '쿼리명', dataIndex: 'name', key: 'name', width: 140 },
      { title: '카테고리', dataIndex: 'category', key: 'cat', width: 90, render: (v: string) => <Tag>{v}</Tag> },
      { title: '설명', dataIndex: 'description', key: 'desc', width: 220 },
      { title: '임계값(ms)', dataIndex: 'threshold_ms', key: 'thr', width: 100, render: (v: number) => v.toLocaleString() },
      { title: '응답(ms)', dataIndex: 'elapsed_ms', key: 'ela', width: 100, render: (v: number, r: any) => <Text strong style={{ color: r.status === 'pass' ? '#3f8600' : '#cf1322' }}>{v.toLocaleString()}</Text> },
      { title: '상태', dataIndex: 'status', key: 'st', width: 80, render: statusTag },
      { title: '권고', dataIndex: 'recommendation', key: 'rec', ellipsis: true, render: (v: string | null) => v || '-' },
    ];

    return (
      <div>
        <div style={{ textAlign: 'right', marginBottom: 16 }}>
          <Button type="primary" icon={<PlayCircleOutlined />} onClick={runBenchmark} loading={benchLoading}>벤치마크 실행</Button>
        </div>
        {summary && (
          <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
            <Col xs={12} md={6}><Card size="small"><Statistic title="총 쿼리" value={summary.total_queries} prefix={<BarChartOutlined />} /></Card></Col>
            <Col xs={12} md={6}><Card size="small"><Statistic title="Pass" value={summary.pass} valueStyle={{ color: '#3f8600' }} prefix={<CheckCircleOutlined />} /></Card></Col>
            <Col xs={12} md={6}><Card size="small"><Statistic title="Fail" value={summary.fail} valueStyle={{ color: '#cf1322' }} prefix={<CloseCircleOutlined />} /></Card></Col>
            <Col xs={12} md={6}><Card size="small"><Statistic title="총 소요" value={summary.duration_ms} suffix="ms" prefix={<FieldTimeOutlined />} /></Card></Col>
          </Row>
        )}
        <Spin spinning={benchLoading}>
          {results.length > 0 ? (
            <>
              <Card size="small" title="응답시간 vs 임계값" style={{ marginBottom: 16 }}>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="name" />
                    <YAxis label={{ value: 'ms', angle: -90, position: 'insideLeft' }} />
                    <RTooltip />
                    <Legend />
                    <Bar dataKey="elapsed" name="실측값(ms)" fill="#1890ff" />
                    <Bar dataKey="threshold" name="임계값(ms)" fill="#d9d9d9" />
                  </BarChart>
                </ResponsiveContainer>
              </Card>
              <Table dataSource={results.map((r: any) => ({ ...r, key: r.id }))} columns={benchCols} size="small" pagination={false} scroll={{ x: 1000 }} />
              {results.some((r: any) => r.recommendation) && (
                <Alert style={{ marginTop: 16 }} type="warning" showIcon message="개선 권고사항"
                  description={<ul style={{ margin: 0, paddingLeft: 20 }}>{results.filter((r: any) => r.recommendation).map((r: any) => (
                    <li key={r.id}><Text strong>{r.id} ({r.name})</Text>: {r.recommendation}</li>
                  ))}</ul>} />
              )}
            </>
          ) : !benchLoading ? <Empty description="'벤치마크 실행' 버튼을 클릭하세요" /> : null}
        </Spin>
      </div>
    );
  };

  const renderDeidentSection = () => {
    const summary = deidentData?.summary;
    const results = deidentData?.results || [];

    const deidentCols = [
      { title: 'ID', dataIndex: 'id', key: 'id', width: 60 },
      { title: '작업명', dataIndex: 'name', key: 'name', width: 160 },
      { title: '설명', dataIndex: 'description', key: 'desc', width: 280 },
      { title: '처리 건수', dataIndex: 'records_processed', key: 'rp', width: 120, render: (v: number) => v.toLocaleString() },
      { title: '소요(ms)', dataIndex: 'elapsed_ms', key: 'ela', width: 100, render: (v: number) => v.toLocaleString() },
      { title: '처리량', key: 'tp', width: 140, render: (_: any, r: any) => <Text strong>{r.throughput?.toLocaleString()} {r.unit}</Text> },
    ];

    return (
      <div>
        <div style={{ textAlign: 'right', marginBottom: 16 }}>
          <Button type="primary" icon={<PlayCircleOutlined />} onClick={runDeidentBenchmark} loading={deidentLoading}>비식별화 벤치마크 실행</Button>
        </div>
        {summary && (
          <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
            <Col xs={12} md={8}><Card size="small"><Statistic title="측정 항목" value={summary.total_tests} prefix={<ExperimentOutlined />} /></Card></Col>
            <Col xs={12} md={8}><Card size="small"><Statistic title="총 처리 건수" value={summary.total_processed} prefix={<DatabaseOutlined />} /></Card></Col>
            <Col xs={12} md={8}><Card size="small"><Statistic title="총 소요" value={summary.duration_ms} suffix="ms" prefix={<FieldTimeOutlined />} /></Card></Col>
          </Row>
        )}
        <Spin spinning={deidentLoading}>
          {results.length > 0 ? (
            <Table dataSource={results.map((r: any) => ({ ...r, key: r.id }))} columns={deidentCols} size="small" pagination={false} scroll={{ x: 800 }} />
          ) : !deidentLoading ? <Empty description="'비식별화 벤치마크 실행' 버튼을 클릭하세요" /> : null}
        </Spin>
      </div>
    );
  };

  return (
    <div>
      <Segmented
        block
        options={[
          { label: '이관 데이터 검증', value: 'verify', icon: <AuditOutlined /> },
          { label: '성능 벤치마크', value: 'benchmark', icon: <DashboardOutlined /> },
          { label: '비식별화 성능', value: 'deident', icon: <SafetyCertificateOutlined /> },
        ]}
        value={section}
        onChange={(v) => setSection(v as string)}
        style={{ marginBottom: 16 }}
      />
      {section === 'verify' && renderVerifySection()}
      {section === 'benchmark' && renderBenchmarkSection()}
      {section === 'deident' && renderDeidentSection()}
    </div>
  );
};

export default MigrationVerificationTab;
