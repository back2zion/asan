/**
 * Tab 1: AI 모델 관리 — 설정 편집 + 연결 테스트 + 테스트 쿼리
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Typography, Row, Col, Table, Tag, Badge, Statistic, Progress,
  Button, Space, Input, Tooltip, Spin, Drawer, Form,
  Descriptions, Modal, Alert, App,
} from 'antd';
import {
  SettingOutlined, DashboardOutlined, ReloadOutlined,
  ThunderboltOutlined, SendOutlined,
} from '@ant-design/icons';
import {
  XAxis, YAxis, CartesianGrid,
  Tooltip as RTooltip, ResponsiveContainer, BarChart, Bar,
} from 'recharts';

import { API_BASE, STATUS_COLOR, fetchJSON, postJSON, putJSON } from './helpers';

const { Text } = Typography;
const { TextArea } = Input;

const ModelManagementTab: React.FC = () => {
  const { message } = App.useApp();
  const [models, setModels] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [testing, setTesting] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<Record<string, any>>({});

  // Config drawer
  const [configDrawer, setConfigDrawer] = useState<{ open: boolean; model: any }>({ open: false, model: null });
  const [configForm] = Form.useForm();

  // Test query drawer
  const [queryDrawer, setQueryDrawer] = useState<{ open: boolean; model: any }>({ open: false, model: null });
  const [queryText, setQueryText] = useState('');
  const [queryResult, setQueryResult] = useState<any>(null);
  const [querying, setQuerying] = useState(false);

  // Metrics modal
  const [metricsModal, setMetricsModal] = useState<any>(null);
  const [metrics, setMetrics] = useState<any>(null);

  const loadModels = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchJSON(`${API_BASE}/models`);
      setModels(data.models || []);
    } catch {
      message.error('모델 목록 로드 실패');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadModels(); }, [loadModels]);

  const handleTestConnection = async (model: any) => {
    setTesting(model.id);
    try {
      const result = await postJSON(`${API_BASE}/models/${model.id}/test-connection`, {});
      setTestResults(prev => ({ ...prev, [model.id]: result }));
      if (result.success) message.success(`${model.name}: 연결 정상 (${result.total_latency_ms}ms)`);
      else message.warning(`${model.name}: 연결 실패`);
    } catch { message.error('연결 테스트 실패'); }
    setTesting(null);
  };

  const openConfig = (model: any) => {
    setConfigDrawer({ open: true, model });
    configForm.setFieldsValue({
      health_url: model.health_url,
      test_url: model.test_url,
      description: model.description,
      temperature: model.config?.temperature,
      max_tokens: model.config?.max_tokens,
      system_prompt: model.config?.system_prompt,
    });
  };

  const saveConfig = async () => {
    const model = configDrawer.model;
    if (!model) return;
    try {
      const values = configForm.getFieldsValue();
      await putJSON(`${API_BASE}/models/${model.id}`, {
        health_url: values.health_url,
        test_url: values.test_url,
        description: values.description,
        config: {
          temperature: values.temperature,
          max_tokens: values.max_tokens,
          system_prompt: values.system_prompt,
        },
      });
      message.success('모델 설정 저장됨');
      setConfigDrawer({ open: false, model: null });
      loadModels();
    } catch { message.error('설정 저장 실패'); }
  };

  const openTestQuery = (model: any) => {
    setQueryDrawer({ open: true, model });
    setQueryText('');
    setQueryResult(null);
  };

  const runTestQuery = async () => {
    const model = queryDrawer.model;
    if (!model || !queryText.trim()) return;
    setQuerying(true);
    try {
      const result = await postJSON(`${API_BASE}/models/${model.id}/test-query`, {
        prompt: queryText,
        max_tokens: model.config?.max_tokens || 256,
      });
      setQueryResult(result);
    } catch { message.error('쿼리 실행 실패'); }
    setQuerying(false);
  };

  const openMetrics = async (modelId: string) => {
    setMetricsModal(modelId);
    try {
      const data = await fetchJSON(`${API_BASE}/models/${modelId}/metrics`);
      setMetrics(data);
    } catch { message.error('지표 로드 실패'); }
  };

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Text type="secondary">등록된 AI 모델의 상태 확인, 설정 편집, 테스트 쿼리를 수행합니다.</Text>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={loadModels} loading={loading}>새로고침</Button>
          <Button onClick={() => models.forEach(m => handleTestConnection(m))}>전체 연결 테스트</Button>
        </Space>
      </div>

      <Row gutter={[16, 16]}>
        {models.map((m) => {
          const connResult = testResults[m.id];
          return (
            <Col xs={24} md={8} key={m.id}>
              <Card
                title={
                  <Space>
                    <Badge status={m.status === 'healthy' ? 'success' : m.status === 'offline' ? 'error' : 'warning'} />
                    <span>{m.name}</span>
                  </Space>
                }
                extra={<Tag color={STATUS_COLOR[m.status]}>{m.status}</Tag>}
              >
                <Descriptions column={1} size="small">
                  <Descriptions.Item label="유형">{m.type}</Descriptions.Item>
                  <Descriptions.Item label="버전">{m.version}</Descriptions.Item>
                  <Descriptions.Item label="파라미터">{m.parameters}</Descriptions.Item>
                  <Descriptions.Item label="GPU 메모리">{(m.gpu_memory_mb / 1024).toFixed(1)} GB</Descriptions.Item>
                  <Descriptions.Item label="Temperature">{m.config?.temperature ?? '-'}</Descriptions.Item>
                </Descriptions>

                {connResult && (
                  <div style={{ marginTop: 8, padding: 8, background: connResult.success ? '#f6ffed' : '#fff2e8', borderRadius: 6, fontSize: 12 }}>
                    {Object.entries(connResult.results || {}).map(([k, v]: [string, any]) => (
                      <div key={k}>
                        <Badge status={v.status === 'ok' || v.status === 'reachable' ? 'success' : 'error'} />
                        <Text style={{ fontSize: 12 }}>{k}: {v.status} ({v.latency_ms}ms)</Text>
                      </div>
                    ))}
                  </div>
                )}

                <div style={{ marginTop: 12, display: 'flex', gap: 8 }}>
                  <Button size="small" icon={<ThunderboltOutlined />}
                    loading={testing === m.id}
                    onClick={() => handleTestConnection(m)} block>
                    연결 테스트
                  </Button>
                  <Button size="small" icon={<SendOutlined />}
                    onClick={() => openTestQuery(m)} block type="primary" ghost>
                    테스트 쿼리
                  </Button>
                </div>
                <div style={{ marginTop: 8, display: 'flex', gap: 8 }}>
                  <Button size="small" icon={<SettingOutlined />}
                    onClick={() => openConfig(m)} block>
                    설정 편집
                  </Button>
                  <Button size="small" icon={<DashboardOutlined />}
                    onClick={() => openMetrics(m.id)} block>
                    성능 지표
                  </Button>
                </div>
              </Card>
            </Col>
          );
        })}
      </Row>

      {loading && <div style={{ textAlign: 'center', padding: 40 }}><Spin /></div>}

      {/* Config Drawer */}
      <Drawer
        title={`모델 설정 편집: ${configDrawer.model?.name || ''}`}
        open={configDrawer.open}
        onClose={() => setConfigDrawer({ open: false, model: null })}
        width={520}
        extra={<Button type="primary" onClick={saveConfig}>저장</Button>}
      >
        <Form form={configForm} layout="vertical">
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item name="health_url" label="Health Check URL">
            <Input placeholder="http://localhost:28888/v1/models" />
          </Form.Item>
          <Form.Item name="test_url" label="Inference URL">
            <Input placeholder="http://localhost:28888/v1/chat/completions" />
          </Form.Item>
          <Row gutter={12}>
            <Col span={12}>
              <Form.Item name="temperature" label="Temperature">
                <Input type="number" step={0.1} min={0} max={2} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="max_tokens" label="Max Tokens">
                <Input type="number" step={256} min={64} max={32768} />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item name="system_prompt" label="System Prompt">
            <Input.TextArea rows={4} placeholder="시스템 프롬프트" />
          </Form.Item>
        </Form>
      </Drawer>

      {/* Test Query Drawer */}
      <Drawer
        title={`테스트 쿼리: ${queryDrawer.model?.name || ''}`}
        open={queryDrawer.open}
        onClose={() => setQueryDrawer({ open: false, model: null })}
        width={640}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <TextArea
            rows={4}
            placeholder={queryDrawer.model?.type === 'Medical NER'
              ? '분석할 의료 텍스트를 입력하세요...\n예: The patient has diabetes mellitus type 2 and hypertension.'
              : '테스트 질문을 입력하세요...\n예: OMOP CDM에서 당뇨 환자 수를 조회하는 SQL을 생성해줘'}
            value={queryText}
            onChange={(e) => setQueryText(e.target.value)}
          />
          <Button type="primary" icon={<SendOutlined />} onClick={runTestQuery}
            loading={querying} disabled={!queryText.trim()}>
            쿼리 전송
          </Button>

          {queryResult && (
            <Card size="small" title={
              <Space>
                <Badge status={queryResult.success ? 'success' : 'error'} />
                <Text>{queryResult.success ? '성공' : '실패'}</Text>
                <Tag color="blue">{queryResult.latency_ms}ms</Tag>
                {queryResult.tokens_used && <Tag>{queryResult.tokens_used} tokens</Tag>}
              </Space>
            }>
              {queryResult.success ? (
                <pre style={{
                  fontSize: 13, background: '#1e1e1e', color: '#d4d4d4',
                  padding: 16, borderRadius: 8, maxHeight: 400, overflow: 'auto',
                  whiteSpace: 'pre-wrap', wordBreak: 'break-word',
                }}>
                  {queryResult.response}
                </pre>
              ) : (
                <Alert type="error" message={queryResult.error} />
              )}
            </Card>
          )}
        </Space>
      </Drawer>

      {/* Metrics Modal */}
      <Modal
        title={`모델 성능 지표 — ${metricsModal}`}
        open={!!metricsModal}
        onCancel={() => { setMetricsModal(null); setMetrics(null); }}
        footer={null}
        width={600}
      >
        {metrics ? (
          <div>
            {metrics.note && <Alert message={metrics.note} type="info" showIcon style={{ marginBottom: 16 }} />}
            <Row gutter={16}>
              <Col span={8}><Statistic title="총 요청" value={metrics.total_requests} /></Col>
              <Col span={8}><Statistic title="평균 지연" value={metrics.avg_latency_ms} suffix="ms" /></Col>
              <Col span={8}><Statistic title="오류율" value={metrics.error_rate} suffix="%" valueStyle={{ color: metrics.error_rate > 5 ? '#ff4d4f' : '#52c41a' }} /></Col>
            </Row>
            {metrics.daily_trend?.length > 0 && (
              <div style={{ marginTop: 24 }}>
                <Text strong>일별 요청 추이</Text>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={metrics.daily_trend}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                    <YAxis />
                    <RTooltip />
                    <Bar dataKey="count" fill="#005BAC" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
        ) : (
          <div style={{ textAlign: 'center', padding: 40 }}><Spin /></div>
        )}
      </Modal>
    </div>
  );
};

export default ModelManagementTab;
