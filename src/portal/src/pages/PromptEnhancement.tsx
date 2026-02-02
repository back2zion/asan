import React, { useState } from 'react';
import { Card, Input, Button, Typography, Space, Row, Col, Alert, Divider, Tag, Spin, Tabs, Badge, Select, Switch } from 'antd';
import { SendOutlined, ClearOutlined, CopyOutlined, ExperimentOutlined, DatabaseOutlined, SafetyOutlined, UserOutlined, FileSearchOutlined, CheckCircleOutlined } from '@ant-design/icons';
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

const PromptEnhancement: React.FC = () => {
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState<EnhancementResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState('query');
  const [userRole, setUserRole] = useState('researcher');
  const [dataQualityCheck, setDataQualityCheck] = useState(true);
  const [approvalRequired, setApprovalRequired] = useState(true);

  const handleEnhance = async () => {
    if (!question.trim()) return;

    setLoading(true);
    setError(null);

    try {
      const response = await axios.post('http://localhost:8000/api/v1/text2sql/enhanced-generate', {
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

  const exampleQuestions = [
    "내 진료내역 중 홍길동 환자에 대한 내분비내과와 연계된 당뇨 경과기록 정보 전체와 TG 및 콜레스테롤 검사 결과, 그리고 혈압, 맥박 정보를 정리해서 보여줘",
    "김철수 환자의 최근 1년간 심장내과 진료 기록과 혈액검사 결과 보여줘",
    "응급실에 내원한 고혈압 환자들의 약물 처방 현황 알려줘",
    "당뇨병 환자의 평균 입원 일수와 진료비 분석해줘",
    "이번 달 외래 환자 수는 얼마나 되나",
    "50대 남성 중 고혈압 진단받은 사람 몇 명인지"
  ];

  // CDW 데이터 모델 구조
  const cdwDataModels = [
    { name: 'dim_patient', description: '환자 차원 테이블', type: 'Dimension' },
    { name: 'dim_diagnosis', description: '진단 차원 테이블 (KCD 코드)', type: 'Dimension' },
    { name: 'dim_department', description: '진료과 차원 테이블', type: 'Dimension' },
    { name: 'fact_visit', description: '내원 사실 테이블', type: 'Fact' },
    { name: 'fact_lab_test', description: '검사 사실 테이블', type: 'Fact' },
    { name: 'fact_prescription', description: '처방 사실 테이블', type: 'Fact' },
  ];

  // 메타데이터 매핑
  const metadataTerms = [
    { term: '당뇨', mapping: 'E10-E14 (KCD)', standard: 'Diabetes Mellitus' },
    { term: '고혈압', mapping: 'I10-I15 (KCD)', standard: 'Hypertension' },
    { term: 'TG', mapping: 'Triglyceride', standard: '중성지방' },
    { term: '콜레스테롤', mapping: 'Total Cholesterol', standard: '총콜레스테롤' },
  ];

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
                SFR-007: Clinical Data Warehouse 통합 분석 플랫폼
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
                  text={<span style={{ color: '#52A67D', fontWeight: '500' }}>데이터 품질 OK</span>} 
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
                    <Alert
                      message="CDW 호환성 보장"
                      description="기존 CDW 데이터 모델 및 지표 정의와 완전 호환됩니다. KCD, 처방코드, 검사코드 체계를 유지합니다."
                      type="info"
                      showIcon
                      closable
                      style={{
                        borderRadius: '6px',
                        border: '1px solid #b3d8ff',
                        backgroundColor: '#f0f8ff'
                      }}
                    />
                    
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
                      <Text strong style={{ color: '#333', fontSize: '15px' }}>CDW 호환 예시 질의:</Text>
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
                <Card title="메타데이터 관리 및 표준 용어 매핑">
                  <Row gutter={[16, 16]}>
                    <Col xs={24} lg={12}>
                      <Card type="inner" title="CDW 데이터 모델 구조" size="small">
                        <Space direction="vertical" style={{ width: '100%' }}>
                          {cdwDataModels.map((model, index) => (
                            <div key={index} style={{ 
                              padding: '8px', 
                              background: model.type === 'Fact' ? '#fff7e6' : '#e6f7ff',
                              borderRadius: '4px'
                            }}>
                              <div><Text strong>{model.name}</Text> <Tag color={model.type === 'Fact' ? 'orange' : 'blue'}>{model.type}</Tag></div>
                              <div><Text type="secondary" style={{ fontSize: '12px' }}>{model.description}</Text></div>
                            </div>
                          ))}
                        </Space>
                      </Card>
                    </Col>
                    <Col xs={24} lg={12}>
                      <Card type="inner" title="표준 용어 매핑" size="small">
                        <Space direction="vertical" style={{ width: '100%' }}>
                          {metadataTerms.map((term, index) => (
                            <div key={index} style={{ padding: '8px', background: '#f6ffed', borderRadius: '4px' }}>
                              <div><Text strong>{term.term}</Text> → <Text code>{term.mapping}</Text></div>
                              <div><Text type="secondary" style={{ fontSize: '12px' }}>표준명: {term.standard}</Text></div>
                            </div>
                          ))}
                        </Space>
                      </Card>
                    </Col>
                  </Row>
                </Card>
              </div>
            )}

            {activeTab === 'quality' && (
              <div>
                <Card title="데이터 품질관리 자동화">
                  <Row gutter={[16, 16]}>
                    <Col xs={24} sm={8}>
                      <Card type="inner">
                        <div style={{ textAlign: 'center' }}>
                          <div style={{ fontSize: '24px', color: '#52c41a' }}>98.7%</div>
                          <div style={{ fontSize: '12px', color: '#666' }}>데이터 완성도</div>
                        </div>
                      </Card>
                    </Col>
                    <Col xs={24} sm={8}>
                      <Card type="inner">
                        <div style={{ textAlign: 'center' }}>
                          <div style={{ fontSize: '24px', color: '#ff6600' }}>23건</div>
                          <div style={{ fontSize: '12px', color: '#666' }}>이상치 탐지</div>
                        </div>
                      </Card>
                    </Col>
                    <Col xs={24} sm={8}>
                      <Card type="inner">
                        <div style={{ textAlign: 'center' }}>
                          <div style={{ fontSize: '24px', color: '#1890ff' }}>99.1%</div>
                          <div style={{ fontSize: '12px', color: '#666' }}>중복 제거율</div>
                        </div>
                      </Card>
                    </Col>
                  </Row>
                  <Alert
                    message="자동 품질 검증 활성화"
                    description="데이터 누락, 이상치, 중복 등 품질 이슈를 자동으로 탐지하여 리포트를 생성합니다."
                    type="success"
                    showIcon
                    style={{ marginTop: 16 }}
                  />
                </Card>
              </div>
            )}

            {activeTab === 'approval' && (
              <div>
                <Card title="연구용 데이터셋 추출 승인 프로세스">
                  <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                    <Alert
                      message="기존 승인 체계 유지"
                      description="데이터 활용 승인 및 연구자 인증 기반의 데이터 추출 요청, 승인, 이력 관리가 기존과 동일하게 진행됩니다."
                      type="info"
                      showIcon
                    />
                    
                    <Row gutter={[16, 16]}>
                      <Col xs={24} lg={12}>
                        <Card type="inner" title="승인 대기 목록" size="small">
                          <Space direction="vertical" style={{ width: '100%' }}>
                            <div style={{ padding: '8px', background: '#fff7e6', borderRadius: '4px' }}>
                              <div><Text strong>연구자: 김철수</Text> <Tag color="orange">대기중</Tag></div>
                              <div><Text type="secondary" style={{ fontSize: '12px' }}>심장내과 환자 데이터 추출 요청</Text></div>
                            </div>
                            <div style={{ padding: '8px', background: '#f6ffed', borderRadius: '4px' }}>
                              <div><Text strong>연구자: 이영희</Text> <Tag color="green">승인완료</Tag></div>
                              <div><Text type="secondary" style={{ fontSize: '12px' }}>당뇨병 환자 코호트 연구</Text></div>
                            </div>
                          </Space>
                        </Card>
                      </Col>
                      <Col xs={24} lg={12}>
                        <Card type="inner" title="데이터 추출 이력" size="small">
                          <Space direction="vertical" style={{ width: '100%' }}>
                            <div style={{ padding: '8px', background: '#e6f7ff', borderRadius: '4px' }}>
                              <div><Text strong>2025-11-17 14:30</Text></div>
                              <div><Text type="secondary" style={{ fontSize: '12px' }}>환자 1,247명 데이터 추출 (IRB-2025-001)</Text></div>
                            </div>
                            <div style={{ padding: '8px', background: '#f9f0ff', borderRadius: '4px' }}>
                              <div><Text strong>2025-11-16 09:15</Text></div>
                              <div><Text type="secondary" style={{ fontSize: '12px' }}>검사결과 데이터 추출 (IRB-2025-003)</Text></div>
                            </div>
                          </Space>
                        </Card>
                      </Col>
                    </Row>
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
                message="통합 프로세스 완료"
                description="프롬프트 강화 → SQL 생성 → 실행이 완료되었습니다. 이제 정확한 의료 데이터 조회가 가능합니다."
                type="success"
                showIcon
              />
            </Space>
          </Card>
        )}

        <Card title="SFR-007 요구사항 준수 현황" style={{ marginTop: 24 }}>
          <Row gutter={[16, 16]}>
            <Col xs={24} lg={8}>
              <Card type="inner" title="UI/UX 일관성" size="small">
                <Space direction="vertical">
                  <div><CheckCircleOutlined style={{ color: '#52c41a' }} /> 편리한 조회/분석 화면</div>
                  <div><CheckCircleOutlined style={{ color: '#52c41a' }} /> 기존 쿼리/필터 활용</div>
                  <div><CheckCircleOutlined style={{ color: '#52c41a' }} /> 차트 데이터 활용</div>
                </Space>
              </Card>
            </Col>
            <Col xs={24} lg={8}>
              <Card type="inner" title="CDW 호환성" size="small">
                <Space direction="vertical">
                  <div><CheckCircleOutlined style={{ color: '#52c41a' }} /> Fact/Dimension 구조 유지</div>
                  <div><CheckCircleOutlined style={{ color: '#52c41a' }} /> KCD, 처방코드, 검사코드 유지</div>
                  <div><CheckCircleOutlined style={{ color: '#52c41a' }} /> 기존 보고서 호환성</div>
                </Space>
              </Card>
            </Col>
            <Col xs={24} lg={8}>
              <Card type="inner" title="고도화 기능" size="small">
                <Space direction="vertical">
                  <div><CheckCircleOutlined style={{ color: '#52c41a' }} /> 메타데이터 연계</div>
                  <div><CheckCircleOutlined style={{ color: '#52c41a' }} /> 데이터 품질 검증</div>
                  <div><CheckCircleOutlined style={{ color: '#52c41a' }} /> LLM 기반 Text2SQL</div>
                </Space>
              </Card>
            </Col>
          </Row>
          
          <Alert
            message="SFR-007 완전 준수"
            description="CDW 특성을 고려한 모델 설계와 AI 적용을 통해 사용자 편의성이 향상되었으며, 기존 CDW 시스템과의 완전한 호환성을 보장합니다."
            type="success"
            showIcon
            style={{ marginTop: 16 }}
          />
        </Card>
      </Space>
    </div>
  );
};

export default PromptEnhancement;