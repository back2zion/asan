import React, { useState, useEffect } from 'react';
import { Card, Input, Button, Typography, Space, Row, Col, Alert, Divider, Tag, Spin, Tabs, Badge, Select, Switch, Empty } from 'antd';
import { SendOutlined, ClearOutlined, CopyOutlined, ExperimentOutlined, DatabaseOutlined, SafetyOutlined, UserOutlined, FileSearchOutlined, CheckCircleOutlined, ReloadOutlined } from '@ant-design/icons';
import axios from 'axios';

const { Title, Paragraph, Text } = Typography;
const { TextArea } = Input;

interface EnhancementResult {
  original_question: string;
  enhanced_question: string;
  enhancements_applied: string[];
  enhancement_confidence: number;
  sql: string;
  sql_explanation: string;
  sql_confidence: number;
  execution_result?: {
    results: any[];
    row_count: number;
    columns: string[];
    execution_time_ms: number;
    natural_language_explanation: string;
    error?: string;
  };
}

interface TableInfo {
  name: string;
  description: string;
  category: string;
  row_count: number;
  column_count: number;
}

interface SnomedTerm {
  term: string;
  code: string;
  name: string;
  codeSystem: string;
}

const API_BASE = '/api/v1';

const PromptEnhancement: React.FC = () => {
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState<EnhancementResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState('query');
  const [userRole, setUserRole] = useState('researcher');
  const [dataQualityCheck, setDataQualityCheck] = useState(true);
  const [approvalRequired, setApprovalRequired] = useState(true);

  // API에서 로드하는 데이터
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [snomedTerms, setSnomedTerms] = useState<SnomedTerm[]>([]);
  const [metaLoading, setMetaLoading] = useState(false);

  // 예시 질의 (UX 가이드용)
  const exampleQuestions = [
    "고혈압 환자",
    "당뇨 입원",
    "50대 남성 고혈압",
    "심방세동 약물",
    "당뇨 검사결과",
    "뇌졸중 환자 수"
  ];

  // 메타데이터 로드
  const loadMetadata = async () => {
    setMetaLoading(true);
    try {
      const [tablesRes, termsRes] = await Promise.all([
        fetch(`${API_BASE}/datamart/tables`).then(r => r.ok ? r.json() : null),
        fetch(`${API_BASE}/text2sql/metadata/terms`).then(r => r.ok ? r.json() : null),
      ]);
      if (tablesRes?.tables) {
        setTables(tablesRes.tables);
      }
      if (termsRes?.snomed_codes) {
        setSnomedTerms(termsRes.snomed_codes);
      }
    } catch {
      // 조용히 실패 (메타데이터 탭에서 Empty 표시)
    } finally {
      setMetaLoading(false);
    }
  };

  useEffect(() => { loadMetadata(); }, []);

  const handleEnhance = async () => {
    if (!question.trim()) return;

    setLoading(true);
    setError(null);

    try {
      const response = await axios.post(`${API_BASE}/text2sql/enhanced-generate`, {
        question: question.trim(),
        enhancement_type: 'medical',
        include_explanation: true,
        auto_execute: true
      });

      setResult(response.data);
    } catch (err) {
      console.error('Enhancement error:', err);
      setError('프롬프트 강화 중 오류가 발생했습니다.');
    } finally {
      setLoading(false);
    }
  };

  const handleClear = () => {
    setQuestion('');
    setResult(null);
    setError(null);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.8) return 'success';
    if (confidence >= 0.6) return 'warning';
    return 'error';
  };

  return (
    <div style={{ padding: '0' }}>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* CDW Header */}
        <Card style={{
          borderRadius: '12px',
          border: '1px solid #e9ecef',
          boxShadow: '0 4px 12px rgba(0, 0, 0, 0.08)',
          background: '#ffffff'
        }}>
          <Row align="middle" justify="space-between">
            <Col>
              <Title level={3} style={{
                margin: 0,
                color: '#333',
                fontWeight: '600'
              }}>
                <DatabaseOutlined style={{
                  color: '#006241',
                  marginRight: '12px',
                  fontSize: '28px'
                }} />
                CDW 데이터 추출 및 연구 지원
              </Title>
              <Paragraph type="secondary" style={{
                margin: '8px 0 0 40px',
                fontSize: '15px',
                color: '#6c757d'
              }}>
                Clinical Data Warehouse 통합 분석 플랫폼
              </Paragraph>
            </Col>
            <Col>
              <Space direction="vertical" size="small" style={{ textAlign: 'right' }}>
                <Badge
                  status="processing"
                  text={<span style={{ color: '#006241', fontWeight: '500' }}>시스템 정상</span>}
                />
                <Badge
                  status="success"
                  text={<span style={{ color: '#52A67D', fontWeight: '500' }}>OMOP CDM 연결됨</span>}
                />
              </Space>
            </Col>
          </Row>
        </Card>

        {/* User Context & Settings */}
        <Card
          title={
            <span style={{ color: '#333', fontWeight: '600' }}>
              <UserOutlined style={{ color: '#006241', marginRight: '8px' }} />
              사용자 컨텍스트 및 설정
            </span>
          }
          style={{
            borderRadius: '8px',
            border: '1px solid #e9ecef',
            boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)'
          }}
        >
          <Row gutter={[24, 16]}>
            <Col xs={24} sm={8}>
              <Space direction="vertical" size="small" style={{ width: '100%' }}>
                <Text strong style={{ color: '#333', fontSize: '14px' }}>사용자 역할:</Text>
                <Select
                  value={userRole}
                  onChange={setUserRole}
                  style={{ width: '100%' }}
                  size="large"
                  options={[
                    { value: 'researcher', label: '연구자' },
                    { value: 'clinician', label: '임상의' },
                    { value: 'analyst', label: '데이터 분석가' },
                    { value: 'admin', label: '시스템 관리자' }
                  ]}
                />
              </Space>
            </Col>
            <Col xs={24} sm={8}>
              <Space direction="vertical" size="small">
                <Text strong style={{ color: '#333', fontSize: '14px' }}>데이터 품질 검증:</Text>
                <Switch
                  checked={dataQualityCheck}
                  onChange={setDataQualityCheck}
                  checkedChildren="활성화"
                  unCheckedChildren="비활성화"
                  size="default"
                />
              </Space>
            </Col>
            <Col xs={24} sm={8}>
              <Space direction="vertical" size="small">
                <Text strong style={{ color: '#333', fontSize: '14px' }}>승인 프로세스:</Text>
                <Switch
                  checked={approvalRequired}
                  onChange={setApprovalRequired}
                  checkedChildren="필수"
                  unCheckedChildren="생략"
                  size="default"
                />
              </Space>
            </Col>
          </Row>
        </Card>

        {/* Main Tabs */}
        <Card
          styles={{ body: { padding: 0 } }}
          style={{
            borderRadius: '8px',
            border: '1px solid #e9ecef',
            boxShadow: '0 2px 8px rgba(0, 0, 0, 0.06)'
          }}
        >
          <Tabs
            activeKey={activeTab}
            onChange={setActiveTab}
            size="large"
            style={{
              padding: '0 24px',
              background: '#ffffff'
            }}
            items={[
              {
                key: 'query',
                label: (
                  <span style={{ fontWeight: '500', fontSize: '15px' }}>
                    <ExperimentOutlined style={{ color: '#006241', marginRight: '8px' }} />
                    자연어 질의
                  </span>
                )
              },
              {
                key: 'metadata',
                label: (
                  <span style={{ fontWeight: '500', fontSize: '15px' }}>
                    <FileSearchOutlined style={{ color: '#006241', marginRight: '8px' }} />
                    메타데이터 관리
                  </span>
                )
              },
              {
                key: 'quality',
                label: (
                  <span style={{ fontWeight: '500', fontSize: '15px' }}>
                    <CheckCircleOutlined style={{ color: '#006241', marginRight: '8px' }} />
                    데이터 품질
                  </span>
                )
              },
              {
                key: 'approval',
                label: (
                  <span style={{ fontWeight: '500', fontSize: '15px' }}>
                    <SafetyOutlined style={{ color: '#006241', marginRight: '8px' }} />
                    승인 관리
                  </span>
                )
              }
            ]}
          />

          <div style={{ padding: '24px' }}>
            {activeTab === 'query' && (
              <div>
                {/* Query Interface */}
                <Card title="자연어 → SQL 변환 (Text2SQL)" extra={
                  <Space>
                    <Button icon={<ClearOutlined />} onClick={handleClear}>
                      초기화
                    </Button>
                    <Button
                      type="primary"
                      icon={<SendOutlined />}
                      onClick={handleEnhance}
                      loading={loading}
                      disabled={!question.trim()}
                    >
                      질의 실행
                    </Button>
                  </Space>
                }>
                  <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                    <TextArea
                      placeholder="의료 질의를 자연어로 입력하세요... (예: 홍길동 환자의 당뇨 경과기록과 TG, 콜레스테롤 검사결과 보여주세요)"
                      value={question}
                      onChange={(e) => setQuestion(e.target.value)}
                      rows={5}
                      showCount
                      maxLength={1000}
                      size="large"
                      style={{
                        borderRadius: '6px',
                        border: '1px solid #d1ecf1',
                        fontSize: '15px',
                        backgroundColor: '#ffffff'
                      }}
                    />

                    <div>
                      <Text strong style={{ color: '#333', fontSize: '15px' }}>예시 질의:</Text>
                      <div style={{ marginTop: 12 }}>
                        {exampleQuestions.map((example, index) => (
                          <Tag
                            key={index}
                            style={{
                              margin: '4px 6px 4px 0',
                              cursor: 'pointer',
                              borderRadius: '6px',
                              padding: '4px 12px',
                              border: '1px solid #006241',
                              color: '#006241',
                              backgroundColor: '#f0f8f3',
                              fontWeight: '500',
                              transition: 'all 0.2s ease'
                            }}
                            onClick={() => setQuestion(example)}
                          >
                            예시 {index + 1}
                          </Tag>
                        ))}
                      </div>
                    </div>
                  </Space>
                </Card>
              </div>
            )}

            {activeTab === 'metadata' && (
              <div>
                <Card title="메타데이터 관리 및 표준 용어 매핑" extra={
                  <Button icon={<ReloadOutlined />} onClick={loadMetadata} loading={metaLoading} size="small">
                    새로고침
                  </Button>
                }>
                  <Spin spinning={metaLoading}>
                    <Row gutter={[16, 16]}>
                      <Col xs={24} lg={12}>
                        <Card type="inner" title="OMOP CDM 데이터 모델 구조" size="small">
                          {tables.length > 0 ? (
                            <Space direction="vertical" style={{ width: '100%' }}>
                              {tables.map((table, index) => (
                                <div key={index} style={{
                                  padding: '8px',
                                  background: '#e6f7ff',
                                  borderRadius: '4px'
                                }}>
                                  <div>
                                    <Text strong>{table.name}</Text>{' '}
                                    <Tag color="blue">{table.category}</Tag>
                                  </div>
                                  <div>
                                    <Text type="secondary" style={{ fontSize: '12px' }}>
                                      {table.description} ({table.row_count.toLocaleString()}건, {table.column_count}컬럼)
                                    </Text>
                                  </div>
                                </div>
                              ))}
                            </Space>
                          ) : (
                            <Empty description="테이블 정보를 불러올 수 없습니다" />
                          )}
                        </Card>
                      </Col>
                      <Col xs={24} lg={12}>
                        <Card type="inner" title="표준 용어 매핑 (SNOMED CT)" size="small">
                          {snomedTerms.length > 0 ? (
                            <Space direction="vertical" style={{ width: '100%' }}>
                              {snomedTerms.map((term, index) => (
                                <div key={index} style={{ padding: '8px', background: '#f6ffed', borderRadius: '4px' }}>
                                  <div>
                                    <Text strong>{term.term}</Text> →{' '}
                                    <Text code>{term.code} ({term.codeSystem})</Text>
                                  </div>
                                  <div>
                                    <Text type="secondary" style={{ fontSize: '12px' }}>표준명: {term.name}</Text>
                                  </div>
                                </div>
                              ))}
                            </Space>
                          ) : (
                            <Empty description="표준 용어를 불러올 수 없습니다" />
                          )}
                        </Card>
                      </Col>
                    </Row>
                  </Spin>
                </Card>
              </div>
            )}

            {activeTab === 'quality' && (
              <div>
                <Card title="데이터 품질관리 자동화">
                  <Alert
                    message="준비 중"
                    description="데이터 품질 자동 검증 기능이 준비 중입니다. 데이터 누락, 이상치, 중복 등 품질 이슈를 자동으로 탐지하여 리포트를 생성할 예정입니다."
                    type="info"
                    showIcon
                  />
                </Card>
              </div>
            )}

            {activeTab === 'approval' && (
              <div>
                <Card title="연구용 데이터셋 추출 승인 프로세스">
                  <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                    <Alert
                      message="준비 중"
                      description="데이터 활용 승인 및 연구자 인증 기반의 데이터 추출 요청, 승인, 이력 관리 기능이 준비 중입니다."
                      type="info"
                      showIcon
                    />
                  </Space>
                </Card>
              </div>
            )}
          </div>
        </Card>


        {error && (
          <Alert
            message="오류 발생"
            description={error}
            type="error"
            showIcon
            closable
            onClose={() => setError(null)}
          />
        )}

        {loading && (
          <Card>
            <div style={{ textAlign: 'center', padding: '40px 0' }}>
              <Spin size="large" />
              <div style={{ marginTop: 16 }}>
                <Text>프롬프트를 강화하고 있습니다...</Text>
              </div>
            </div>
          </Card>
        )}

        {result && !loading && (
          <Card title="강화 결과" extra={
            <Space>
              <Tag color={getConfidenceColor(result.enhancement_confidence)}>
                강화 신뢰도: {(result.enhancement_confidence * 100).toFixed(1)}%
              </Tag>
              <Tag color={getConfidenceColor(result.sql_confidence)}>
                SQL 신뢰도: {(result.sql_confidence * 100).toFixed(1)}%
              </Tag>
            </Space>
          }>
            <Space direction="vertical" size="large" style={{ width: '100%' }}>
              <div>
                <Title level={5}>원본 질의:</Title>
                <Card type="inner" style={{ background: '#fafafa' }}>
                  <Text>{result.original_question}</Text>
                </Card>
              </div>

              <div>
                <Title level={5}>
                  강화된 질의:
                  <Button
                    type="text"
                    icon={<CopyOutlined />}
                    onClick={() => copyToClipboard(result.enhanced_question)}
                    style={{ marginLeft: 8 }}
                  >
                    복사
                  </Button>
                </Title>
                <Card type="inner" style={{ background: '#f6ffed', border: '1px solid #b7eb8f' }}>
                  <Text strong style={{ color: '#52c41a' }}>{result.enhanced_question}</Text>
                </Card>
              </div>

              {result.enhancements_applied && result.enhancements_applied.length > 0 && (
                <div>
                  <Title level={5}>적용된 강화 사항:</Title>
                  <Space wrap>
                    {result.enhancements_applied.map((enhancement, index) => (
                      <Tag key={index} color="blue">
                        {enhancement}
                      </Tag>
                    ))}
                  </Space>
                </div>
              )}

              <Divider />

              <div>
                <Title level={5}>
                  생성된 SQL:
                  <Button
                    type="text"
                    icon={<CopyOutlined />}
                    onClick={() => copyToClipboard(result.sql)}
                    style={{ marginLeft: 8 }}
                  >
                    복사
                  </Button>
                </Title>
                <Card type="inner" style={{ background: '#f0f8ff', border: '1px solid #91d5ff' }}>
                  <Text code style={{ whiteSpace: 'pre-wrap', fontSize: '12px' }}>{result.sql}</Text>
                </Card>
                {result.sql_explanation && (
                  <div style={{ marginTop: 8 }}>
                    <Text type="secondary">{result.sql_explanation}</Text>
                  </div>
                )}
              </div>

              {result.execution_result && (
                <div>
                  <Title level={5}>실행 결과:</Title>
                  {result.execution_result.error ? (
                    <Alert
                      message="SQL 실행 오류"
                      description={result.execution_result.error}
                      type="error"
                      showIcon
                    />
                  ) : (
                    <Card type="inner">
                      <Space direction="vertical" style={{ width: '100%' }}>
                        <div>
                          <Text strong>조회 결과: </Text>
                          <Text>{result.execution_result.row_count}건</Text>
                          <Text type="secondary" style={{ marginLeft: 16 }}>
                            실행시간: {result.execution_result.execution_time_ms?.toFixed(1)}ms
                          </Text>
                        </div>
                        {result.execution_result.natural_language_explanation && (
                          <Alert
                            message={result.execution_result.natural_language_explanation}
                            type="success"
                            showIcon
                          />
                        )}
                        {result.execution_result.results && result.execution_result.results.length > 0 && (
                          <div style={{ maxHeight: '300px', overflow: 'auto' }}>
                            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                              <thead>
                                <tr style={{ background: '#fafafa', borderBottom: '1px solid #d9d9d9' }}>
                                  {result.execution_result.columns?.map((col, idx) => (
                                    <th key={idx} style={{ padding: '8px', textAlign: 'left', border: '1px solid #d9d9d9' }}>
                                      {col}
                                    </th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {result.execution_result.results.slice(0, 10).map((row, idx) => (
                                  <tr key={idx} style={{ borderBottom: '1px solid #f0f0f0' }}>
                                    {result.execution_result!.columns?.map((col, colIdx) => (
                                      <td key={colIdx} style={{ padding: '8px', border: '1px solid #d9d9d9' }}>
                                        {String(row[col] || '')}
                                      </td>
                                    ))}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                            {result.execution_result.results.length > 10 && (
                              <div style={{ textAlign: 'center', marginTop: 8 }}>
                                <Text type="secondary">... 더 많은 결과가 있습니다 (총 {result.execution_result.row_count}건)</Text>
                              </div>
                            )}
                          </div>
                        )}
                      </Space>
                    </Card>
                  )}
                </div>
              )}

              <Alert
                message="질의 완료"
                description="SQL 생성 및 실행이 완료되었습니다."
                type="success"
                showIcon
              />
            </Space>
          </Card>
        )}

      </Space>
    </div>
  );
};

export default PromptEnhancement;
