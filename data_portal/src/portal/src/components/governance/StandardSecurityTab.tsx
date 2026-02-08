/**
 * 표준/보안 탭 컴포넌트 - OMOP CDM DB 기반
 * 민감도 등급 수정 + RBAC 역할 관리 기능 포함
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card, Table, Tag, Space, Typography, Row, Col, Statistic,
  Spin, Progress, Badge, Button, Alert, Drawer, Select, Input,
  Modal, Form, message, Popconfirm,
} from 'antd';
import {
  CheckCircleOutlined, LockOutlined, SafetyCertificateOutlined, ReloadOutlined,
  EditOutlined, PlusOutlined, DeleteOutlined,
} from '@ant-design/icons';
import { executeSQL } from './helpers';
import { governanceApi } from '../../services/api';
import { OMOP_TABLES, type SecurityClassification } from './types';

const { Text } = Typography;

interface RoleInfo {
  role_id: number;
  role: string;
  description: string;
  access_scope: string;
  table_count: number;
  tables: string[];
  security_level: string;
}

const ACCESS_SCOPES = ['전체 접근', '비식별 데이터', '담당 환자', '집계 데이터'];
const SECURITY_LEVELS = ['Row/Column/Cell', 'Row/Column', 'Row', 'Table'];

/** Drawer 테이블 행별 등급 변경 셀 (Hooks 규칙 준수를 위해 별도 컴포넌트) */
const SensitivityChangeCell: React.FC<{
  fullColumn: string;
  currentLevel: string;
  saving: boolean;
  onSave: (fullColumn: string, level: string, reason?: string) => void;
}> = ({ fullColumn, currentLevel, saving, onSave }) => {
  const [selectedLevel, setSelectedLevel] = useState(currentLevel);
  const [reason, setReason] = useState('');
  return (
    <Space direction="vertical" size={4} style={{ width: '100%' }}>
      <Space size={4}>
        <Select
          size="small"
          value={selectedLevel}
          onChange={setSelectedLevel}
          style={{ width: 80 }}
          options={[
            { value: '극비', label: '극비' },
            { value: '민감', label: '민감' },
            { value: '일반', label: '일반' },
          ]}
        />
        <Button
          size="small"
          type="primary"
          loading={saving}
          disabled={selectedLevel === currentLevel && !reason}
          onClick={() => onSave(fullColumn, selectedLevel, reason || undefined)}
        >
          저장
        </Button>
      </Space>
      <Input
        size="small"
        placeholder="사유 (선택)"
        value={reason}
        onChange={(e) => setReason(e.target.value)}
        style={{ fontSize: 12 }}
      />
    </Space>
  );
};

const StandardSecurityTab: React.FC = () => {
  const [cdmCheck, setCdmCheck] = useState<{ table: string; exists: boolean }[]>([]);
  const [loading, setLoading] = useState(false);

  // API 데이터 상태
  const [sensitivity, setSensitivity] = useState<SecurityClassification[]>([]);
  const [roles, setRoles] = useState<RoleInfo[]>([]);
  const [apiLoading, setApiLoading] = useState(true);
  const [apiError, setApiError] = useState<string | null>(null);

  // 민감도 Drawer 상태
  const [sensitivityDrawerOpen, setSensitivityDrawerOpen] = useState(false);
  const [drawerLevel, setDrawerLevel] = useState<string>('');
  const [drawerColumns, setDrawerColumns] = useState<string[]>([]);
  const [sensitivitySaving, setSensitivitySaving] = useState<string | null>(null);

  // RBAC Modal 상태
  const [roleModalOpen, setRoleModalOpen] = useState(false);
  const [editingRole, setEditingRole] = useState<RoleInfo | null>(null);
  const [roleForm] = Form.useForm();

  const checkCDMTables = useCallback(async () => {
    setLoading(true);
    try {
      const sql = `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`;
      const result = await executeSQL(sql);
      const existing = new Set(result.results.map((r) => String(r[0]).toLowerCase()));
      setCdmCheck(OMOP_TABLES.map((t) => ({ table: t, exists: existing.has(t) })));
    } catch {
      setCdmCheck(OMOP_TABLES.map((t) => ({ table: t, exists: false })));
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchGovernanceData = useCallback(async () => {
    setApiLoading(true);
    setApiError(null);
    try {
      const [sensitivityData, rolesData] = await Promise.all([
        governanceApi.getSensitivity(),
        governanceApi.getRoles(),
      ]);
      setSensitivity(sensitivityData);
      setRoles(rolesData);
    } catch (err: any) {
      setApiError(err?.message || 'API 호출 실패');
    } finally {
      setApiLoading(false);
    }
  }, []);

  useEffect(() => {
    checkCDMTables();
    fetchGovernanceData();
  }, [checkCDMTables, fetchGovernanceData]);

  // ── 민감도 등급 변경 핸들러 ──
  const openSensitivityDrawer = (level: string, columns: string[]) => {
    setDrawerLevel(level);
    setDrawerColumns(columns);
    setSensitivityDrawerOpen(true);
  };

  const handleSensitivityChange = async (fullColumn: string, newLevel: string, reason?: string) => {
    const [tableName, columnName] = fullColumn.split('.');
    if (!tableName || !columnName) return;
    setSensitivitySaving(fullColumn);
    try {
      await governanceApi.updateSensitivity({ table_name: tableName, column_name: columnName, level: newLevel, reason });
      message.success(`${fullColumn} → ${newLevel} 변경 완료`);
      await fetchGovernanceData();
    } catch (err: any) {
      message.error(err?.response?.data?.detail || '변경 실패');
    } finally {
      setSensitivitySaving(null);
    }
  };

  // ── RBAC 핸들러 ──
  const openRoleModal = (role?: RoleInfo) => {
    setEditingRole(role || null);
    if (role) {
      roleForm.setFieldsValue({
        role_name: role.role,
        description: role.description,
        access_scope: role.access_scope,
        allowed_tables: role.tables,
        security_level: role.security_level,
      });
    } else {
      roleForm.resetFields();
    }
    setRoleModalOpen(true);
  };

  const handleRoleSave = async () => {
    try {
      const values = await roleForm.validateFields();
      if (editingRole) {
        await governanceApi.updateRole(editingRole.role_id, values);
        message.success('역할 수정 완료');
      } else {
        await governanceApi.createRole(values);
        message.success('역할 추가 완료');
      }
      setRoleModalOpen(false);
      await fetchGovernanceData();
    } catch (err: any) {
      if (err?.response?.data?.detail) message.error(err.response.data.detail);
    }
  };

  const handleRoleDelete = async (roleId: number) => {
    try {
      await governanceApi.deleteRole(roleId);
      message.success('역할 삭제 완료');
      await fetchGovernanceData();
    } catch (err: any) {
      message.error(err?.response?.data?.detail || '삭제 실패');
    }
  };

  const existsCount = cdmCheck.filter((c) => c.exists).length;

  return (
    <Space direction="vertical" size="middle" style={{ width: '100%' }}>
      {apiError && (
        <Alert
          type="warning"
          message="거버넌스 데이터 로드 실패"
          description={apiError}
          showIcon
          closable
          action={<Button size="small" onClick={fetchGovernanceData}>재시도</Button>}
        />
      )}

      <Row gutter={16}>
        {/* 민감도 분류 현황 */}
        <Col xs={24} lg={12}>
          <Card title={<><LockOutlined /> 민감도 분류 현황</>} size="small">
            <Spin spinning={apiLoading}>
              <Row gutter={12} style={{ marginBottom: 16 }}>
                {sensitivity.map((s) => (
                  <Col span={8} key={s.level}>
                    <Card size="small">
                      <Statistic
                        title={<Tag color={s.color}>{s.level}</Tag>}
                        value={s.count}
                        suffix="컬럼"
                      />
                    </Card>
                  </Col>
                ))}
              </Row>
              <Table
                columns={[
                  { title: '등급', dataIndex: 'level', key: 'level', render: (v: string, r: SecurityClassification) => <Tag color={r.color}>{v}</Tag> },
                  { title: '컬럼 수', dataIndex: 'count', key: 'count' },
                  { title: '주요 컬럼', dataIndex: 'columns', key: 'columns', render: (v: string[]) => v.slice(0, 5).map((c) => <Tag key={c} style={{ marginBottom: 2 }}>{c}</Tag>) },
                  {
                    title: '관리',
                    key: 'action',
                    width: 100,
                    render: (_: any, r: SecurityClassification) => (
                      <Button size="small" icon={<EditOutlined />} onClick={() => openSensitivityDrawer(r.level, r.columns)}>등급 변경</Button>
                    ),
                  },
                ]}
                dataSource={sensitivity}
                rowKey="level"
                size="small"
                pagination={false}
              />
            </Spin>
          </Card>
        </Col>

        {/* RBAC 현황 */}
        <Col xs={24} lg={12}>
          <Card
            title={<><SafetyCertificateOutlined /> 접근 권한 현황 (RBAC)</>}
            size="small"
            extra={<Button size="small" icon={<PlusOutlined />} type="primary" onClick={() => openRoleModal()}>역할 추가</Button>}
          >
            <Spin spinning={apiLoading}>
              <Table
                columns={[
                  { title: '역할', dataIndex: 'role', key: 'role', render: (v: string) => <Text strong>{v}</Text> },
                  { title: '접근 범위', dataIndex: 'access_scope', key: 'access_scope' },
                  { title: '대상 테이블', dataIndex: 'table_count', key: 'table_count', render: (v: number) => `${v}개 테이블` },
                  { title: '보안 레벨', dataIndex: 'security_level', key: 'security_level', render: (v: string) => <Tag color="blue">{v}</Tag> },
                  {
                    title: '관리',
                    key: 'action',
                    width: 120,
                    render: (_: any, r: RoleInfo) => (
                      <Space size="small">
                        <Button size="small" icon={<EditOutlined />} onClick={() => openRoleModal(r)} />
                        <Popconfirm title="이 역할을 삭제하시겠습니까?" onConfirm={() => handleRoleDelete(r.role_id)} okText="삭제" cancelText="취소">
                          <Button size="small" icon={<DeleteOutlined />} danger />
                        </Popconfirm>
                      </Space>
                    ),
                  },
                ]}
                dataSource={roles}
                rowKey="role_id"
                size="small"
                pagination={false}
              />
            </Spin>
          </Card>
        </Col>
      </Row>

      {/* OMOP CDM 표준 준수 */}
      <Card
        title={<><CheckCircleOutlined /> OMOP CDM v5.4 표준 테이블 체크리스트</>}
        extra={
          <Space>
            <Badge status={existsCount === OMOP_TABLES.length ? 'success' : 'warning'} />
            <Text>{existsCount} / {OMOP_TABLES.length} 존재</Text>
            <Button icon={<ReloadOutlined />} size="small" onClick={checkCDMTables} loading={loading}>확인</Button>
          </Space>
        }
      >
        <Spin spinning={loading}>
          <Progress
            percent={Math.round((existsCount / OMOP_TABLES.length) * 100)}
            status={existsCount === OMOP_TABLES.length ? 'success' : 'active'}
            style={{ marginBottom: 16 }}
          />
          <Row gutter={[8, 8]}>
            {cdmCheck.map((c) => (
              <Col key={c.table} xs={12} sm={8} md={6}>
                <Badge
                  status={c.exists ? 'success' : 'error'}
                  text={
                    <Text
                      style={{ fontFamily: 'monospace', fontSize: 12 }}
                      type={c.exists ? undefined : 'danger'}
                    >
                      {c.table}
                    </Text>
                  }
                />
              </Col>
            ))}
          </Row>
        </Spin>
      </Card>

      {/* 민감도 등급 변경 Drawer */}
      <Drawer
        title={<>민감도 등급 변경 — <Tag color={drawerLevel === '극비' ? 'red' : drawerLevel === '민감' ? 'orange' : 'green'}>{drawerLevel}</Tag> ({drawerColumns.length}개 컬럼)</>}
        open={sensitivityDrawerOpen}
        onClose={() => setSensitivityDrawerOpen(false)}
        width={520}
      >
        <Table
          dataSource={drawerColumns.map((c) => ({ fullColumn: c }))}
          rowKey="fullColumn"
          size="small"
          pagination={{ pageSize: 15, size: 'small' }}
          columns={[
            {
              title: '컬럼명',
              dataIndex: 'fullColumn',
              key: 'fullColumn',
              render: (v: string) => <Text code style={{ fontSize: 12 }}>{v}</Text>,
            },
            {
              title: '등급 변경',
              key: 'change',
              width: 200,
              render: (_: any, rec: { fullColumn: string }) => (
                <SensitivityChangeCell
                  fullColumn={rec.fullColumn}
                  currentLevel={drawerLevel}
                  saving={sensitivitySaving === rec.fullColumn}
                  onSave={handleSensitivityChange}
                />
              ),
            },
          ]}
        />
      </Drawer>

      {/* RBAC 역할 추가/수정 Modal */}
      <Modal
        title={editingRole ? '역할 수정' : '역할 추가'}
        open={roleModalOpen}
        onOk={handleRoleSave}
        onCancel={() => setRoleModalOpen(false)}
        okText={editingRole ? '수정' : '추가'}
        cancelText="취소"
        width={560}
      >
        <Form form={roleForm} layout="vertical" style={{ marginTop: 16 }}>
          <Form.Item name="role_name" label="역할명" rules={[{ required: true, message: '역할명을 입력하세요' }]}>
            <Input placeholder="예: 관리자" />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} placeholder="역할에 대한 설명" />
          </Form.Item>
          <Form.Item name="access_scope" label="접근 범위" rules={[{ required: true, message: '접근 범위를 선택하세요' }]}>
            <Select options={ACCESS_SCOPES.map((s) => ({ value: s, label: s }))} placeholder="접근 범위 선택" />
          </Form.Item>
          <Form.Item name="allowed_tables" label="대상 테이블">
            <Select
              mode="multiple"
              options={OMOP_TABLES.map((t) => ({ value: t, label: t }))}
              placeholder="테이블 선택"
              maxTagCount={6}
            />
          </Form.Item>
          <Form.Item name="security_level" label="보안 레벨" rules={[{ required: true, message: '보안 레벨을 선택하세요' }]}>
            <Select options={SECURITY_LEVELS.map((s) => ({ value: s, label: s }))} placeholder="보안 레벨 선택" />
          </Form.Item>
        </Form>
      </Modal>
    </Space>
  );
};

export default StandardSecurityTab;
