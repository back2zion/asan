/**
 * DataDesign - Tab 4: 용어 표준 & 명명 규칙
 * Naming rules table and naming check tool
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Button, Typography, Row, Col,
  Input, Select, App,
} from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, SearchOutlined,
} from '@ant-design/icons';
import { dataDesignApi } from '../../services/api';
import { NamingRule, NamingCheckResult } from './types';

const { Text } = Typography;

const NamingTab: React.FC = () => {
  const { message } = App.useApp();
  const [loading, setLoading] = useState(false);
  const [namingRules, setNamingRules] = useState<NamingRule[]>([]);

  // Naming check state
  const [namingCheckInput, setNamingCheckInput] = useState('');
  const [namingCheckTarget, setNamingCheckTarget] = useState('table');
  const [namingCheckResults, setNamingCheckResults] = useState<NamingCheckResult[]>([]);
  const [namingCheckLoading, setNamingCheckLoading] = useState(false);

  const fetchNamingRules = useCallback(async () => {
    setLoading(true);
    try {
      const data = await dataDesignApi.getNamingRules();
      setNamingRules(data.rules || []);
    } catch {
      message.error('명명 규칙 로딩 실패');
    } finally {
      setLoading(false);
    }
  }, [message]);

  useEffect(() => {
    fetchNamingRules();
  }, [fetchNamingRules]);

  const handleNamingCheck = async () => {
    const names = namingCheckInput.split(',').map(s => s.trim()).filter(Boolean);
    if (names.length === 0) { message.warning('이름을 입력하세요 (쉼표 구분)'); return; }
    setNamingCheckLoading(true);
    try {
      const data = await dataDesignApi.checkNaming(names, namingCheckTarget);
      setNamingCheckResults(data.results || []);
    } catch {
      message.error('명명 검증 실패');
    } finally {
      setNamingCheckLoading(false);
    }
  };

  // ─── Column definitions ───

  const namingRuleColumns = [
    { title: '규칙 이름', dataIndex: 'rule_name', key: 'rule_name', render: (v: string) => <Text strong>{v}</Text> },
    {
      title: '대상',
      dataIndex: 'target',
      key: 'target',
      render: (v: string) => {
        const color = { table: 'blue', column: 'green', schema: 'purple', index: 'orange', constraint: 'red' }[v] || 'default';
        return <Tag color={color}>{v}</Tag>;
      },
    },
    {
      title: '패턴',
      dataIndex: 'pattern',
      key: 'pattern',
      render: (v: string) => <Text code style={{ fontSize: 12 }}>{v}</Text>,
    },
    { title: '예시', dataIndex: 'example', key: 'example', render: (v: string) => <Text type="secondary">{v}</Text> },
    { title: '설명', dataIndex: 'description', key: 'description', ellipsis: true },
  ];

  const namingCheckColumns = [
    {
      title: '이름',
      dataIndex: 'name',
      key: 'name',
      render: (v: string) => <Text code>{v}</Text>,
    },
    {
      title: '결과',
      dataIndex: 'valid',
      key: 'valid',
      render: (v: boolean) => v
        ? <Tag icon={<CheckCircleOutlined />} color="success">통과</Tag>
        : <Tag icon={<CloseCircleOutlined />} color="error">위반</Tag>,
    },
    {
      title: '위반 사항',
      dataIndex: 'violations',
      key: 'violations',
      render: (violations: NamingCheckResult['violations']) =>
        violations.length === 0 ? '-' : (
          <Space direction="vertical" size={2}>
            {violations.map((v, i) => (
              <Text key={i} type="secondary" style={{ fontSize: 12 }}>
                <CloseCircleOutlined style={{ color: '#ff4d4f', marginRight: 4 }} />
                {v.rule}: <Text code style={{ fontSize: 11 }}>{v.pattern}</Text>
              </Text>
            ))}
          </Space>
        ),
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {/* Rules table */}
      <Card title={`명명 규칙 (${namingRules.length})`} size="small">
        <Table
          dataSource={namingRules}
          columns={namingRuleColumns}
          rowKey="rule_id"
          size="small"
          pagination={false}
        />
      </Card>

      {/* Naming check tool */}
      <Card title="명명 검증 도구" size="small">
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <Row gutter={8} align="middle">
            <Col flex="auto">
              <Input
                placeholder="이름 입력 (쉼표로 구분, e.g. person, Visit_Occurrence, 123_test)"
                value={namingCheckInput}
                onChange={(e) => setNamingCheckInput(e.target.value)}
                onPressEnter={handleNamingCheck}
                prefix={<SearchOutlined />}
              />
            </Col>
            <Col>
              <Select
                value={namingCheckTarget}
                onChange={setNamingCheckTarget}
                style={{ width: 120 }}
                options={[
                  { value: 'table', label: '테이블' },
                  { value: 'column', label: '컬럼' },
                  { value: 'schema', label: '스키마' },
                ]}
              />
            </Col>
            <Col>
              <Button type="primary" onClick={handleNamingCheck} loading={namingCheckLoading}>검증</Button>
            </Col>
          </Row>
          {namingCheckResults.length > 0 && (
            <Table
              dataSource={namingCheckResults}
              columns={namingCheckColumns}
              rowKey="name"
              size="small"
              pagination={false}
            />
          )}
        </Space>
      </Card>
    </Space>
  );
};

export default NamingTab;
