import React, { useState } from 'react';
import { Card, Input, Button, Typography, Space, Row, Col, Alert, Divider, Tag, Spin, Badge, Select } from 'antd';
import { SendOutlined, ClearOutlined, CopyOutlined, DatabaseOutlined, UserOutlined } from '@ant-design/icons';
import axios from 'axios';
import ImageCell from '../components/common/ImageCell';
import ResultChart from '../components/common/ResultChart';

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

const CDWResearch: React.FC = () => {
  const [question, setQuestion] = useState('');
  const [result, setResult] = useState<EnhancementResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [userRole, setUserRole] = useState('researcher');
  const [showAllRows, setShowAllRows] = useState(false);

  const handleEnhance = async () => {
    if (!question.trim()) return;

    setLoading(true);
    setError(null);
    setShowAllRows(false);

    try {
      const response = await axios.post('/api/v1/text2sql/enhanced-generate', {
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
    setShowAllRows(false);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.8) return 'success';
    if (confidence >= 0.6) return 'warning';
    return 'error';
  };

  const exampleQuestionsByRole: Record<string, string[]> = {
    researcher: [
      "폐렴 소견 흉부 X-ray 보여줘",
      "고혈압 진단 환자 중 동시에 다른 진단을 보유한 환자의 동반질환 분포",
      "50세 이상 남성 환자 중 흉부 X-ray 촬영 이력이 있는 환자 목록과 소견",
      "2010년 이후 고혈압 환자의 연도별 외래 방문 건수 추이",
    ],
    clinician: [
      "고혈압 환자이면서 약물 처방 이력이 있는 환자 목록과 처방 약물 정보",
      "입원 환자 중 재원일수가 7일 이상인 환자의 진단 기록",
      "흉부 X-ray에서 Cardiomegaly 소견이 있는 환자의 영상과 진단 이력",
      "응급실 방문 후 입원으로 전환된 환자 수와 주요 진단명",
    ],
    analyst: [
      "연도별 외래, 입원, 응급 방문 건수 추이 비교",
      "성별, 연령대별 환자 수 분포 현황",
      "진단 코드별 환자 수 상위 10개 질환과 환자 비율",
      "환자당 평균 방문 횟수와 평균 진단 건수 통계",
    ],
    admin: [
      "OMOP CDM 주요 테이블별 전체 레코드 수 현황",
      "imaging_study 테이블의 전체 레코드 수와 소견 유형별 분포",
      "person 테이블의 성별 분포와 출생연도 범위",
      "방문 기록의 최초 및 최종 일자와 연간 데이터 건수 추이",
    ],
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* CDW Header */}
        <Card>
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
                  text={<span style={{ color: '#52A67D', fontWeight: '500' }}>데이터 품질 OK</span>} 
                />
              </Space>
            </Col>
          </Row>
        </Card>

        {/* User Role Select */}
        <Card>
          <Space align="center">
            <UserOutlined style={{ color: '#006241', fontSize: '16px' }} />
            <Text strong style={{ color: '#333', fontSize: '14px' }}>사용자 역할:</Text>
            <Select
              value={userRole}
              onChange={setUserRole}
              style={{ width: 160 }}
              options={[
                { value: 'researcher', label: '연구자' },
                { value: 'clinician', label: '임상의' },
                { value: 'analyst', label: '데이터 분석가' },
                { value: 'admin', label: '시스템 관리자' }
              ]}
            />
          </Space>
        </Card>

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
                {(exampleQuestionsByRole[userRole] || []).map((text, index) => (
                  <Tag
                    key={`${userRole}-${index}`}
                    style={{
                      margin: '4px 6px 4px 0',
                      cursor: 'pointer',
                      borderRadius: '6px',
                      padding: '6px 12px',
                      border: '1px solid #006241',
                      color: '#006241',
                      backgroundColor: '#f0f8f3',
                      fontWeight: '400',
                      fontSize: '13px',
                      lineHeight: '1.4',
                      maxWidth: '100%',
                      whiteSpace: 'normal' as const,
                      height: 'auto',
                      transition: 'all 0.2s ease'
                    }}
                    onClick={() => setQuestion(text)}
                  >
                    {text}
                  </Tag>
                ))}
              </div>
            </div>
          </Space>
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
                          <div style={{ maxHeight: showAllRows ? '600px' : '300px', overflow: 'auto' }}>
                            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                              <thead>
                                <tr style={{ background: '#fafafa', borderBottom: '1px solid #d9d9d9', position: 'sticky', top: 0, zIndex: 1 }}>
                                  {result.execution_result.columns?.map((col, idx) => (
                                    <th key={idx} style={{ padding: '8px', textAlign: 'left', border: '1px solid #d9d9d9', background: '#fafafa' }}>
                                      {col}
                                    </th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {(showAllRows ? result.execution_result.results : result.execution_result.results.slice(0, 10)).map((row, idx) => (
                                  <tr key={idx} style={{ borderBottom: '1px solid #f0f0f0' }}>
                                    {result.execution_result!.columns?.map((_, colIdx) => (
                                      <td key={colIdx} style={{ padding: '8px', border: '1px solid #d9d9d9' }}>
                                        <ImageCell value={row[colIdx]} />
                                      </td>
                                    ))}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                            {result.execution_result.results.length > 10 && (
                              <div style={{ textAlign: 'center', marginTop: 8 }}>
                                <Button
                                  type="link"
                                  onClick={() => setShowAllRows(!showAllRows)}
                                  style={{ color: '#006241' }}
                                >
                                  {showAllRows
                                    ? '접기'
                                    : `더보기 (총 ${result.execution_result.row_count}건)`
                                  }
                                </Button>
                              </div>
                            )}
                          </div>
                        )}
                        {result.execution_result.columns && result.execution_result.results && (
                          <ResultChart
                            columns={result.execution_result.columns}
                            results={result.execution_result.results}
                          />
                        )}
                      </Space>
                    </Card>
                  )}
                </div>
              )}

            </Space>
          </Card>
        )}

      </Space>
  );
};

export default CDWResearch;
