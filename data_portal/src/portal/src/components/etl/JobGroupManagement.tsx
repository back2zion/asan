/**
 * DIR-001: Job/Group 관리 탭
 * Job Group 트리 + Job 카드 리스트 + CRUD + Type 전환
 */
import React, { useState, useEffect, useCallback } from 'react';
import {
  App, Card, Table, Tag, Space, Button, Typography, Row, Col, Statistic,
  Modal, Spin, Empty, Select, Input, Form, Switch, Collapse, Badge,
  Tooltip, Popconfirm,
} from 'antd';
import {
  PlusOutlined, EditOutlined, DeleteOutlined, PlayCircleOutlined,
  ReloadOutlined, AppstoreOutlined, ThunderboltOutlined,
  SyncOutlined, CheckCircleOutlined, CloseCircleOutlined,
  FolderOutlined, SettingOutlined,
} from '@ant-design/icons';

const { Text } = Typography;

const API_BASE = '/api/v1/etl';

interface JobGroup {
  group_id: number;
  name: string;
  description: string | null;
  priority: number;
  enabled: boolean;
  job_count: number;
}

interface Job {
  job_id: number;
  group_id: number;
  group_name: string;
  name: string;
  job_type: string;
  source_id: string | null;
  target_table: string | null;
  schedule: string | null;
  dag_id: string | null;
  config: Record<string, any>;
  enabled: boolean;
  sort_order: number;
  last_status: string | null;
  last_run: string | null;
}

const JOB_TYPE_CONFIG: Record<string, { color: string; label: string }> = {
  batch: { color: 'blue', label: 'Batch' },
  stream: { color: 'green', label: 'Stream' },
  cdc: { color: 'orange', label: 'CDC' },
};

const STATUS_TAG: Record<string, React.ReactNode> = {
  success: <Tag icon={<CheckCircleOutlined />} color="success">성공</Tag>,
  failed: <Tag icon={<CloseCircleOutlined />} color="error">실패</Tag>,
  running: <Tag icon={<SyncOutlined spin />} color="processing">실행중</Tag>,
};

async function fetchJSON(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function postJSON(url: string, body: any) {
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function putJSON(url: string, body: any) {
  const res = await fetch(url, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}
async function deleteJSON(url: string) {
  const res = await fetch(url, { method: 'DELETE' });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

const JobGroupManagement: React.FC = () => {
  const { message } = App.useApp();
  const [groups, setGroups] = useState<JobGroup[]>([]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [groupModalOpen, setGroupModalOpen] = useState(false);
  const [jobModalOpen, setJobModalOpen] = useState(false);
  const [editingGroup, setEditingGroup] = useState<JobGroup | null>(null);
  const [editingJob, setEditingJob] = useState<Job | null>(null);
  const [groupForm] = Form.useForm();
  const [jobForm] = Form.useForm();

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const [gData, jData] = await Promise.all([
        fetchJSON(`${API_BASE}/job-groups`),
        fetchJSON(`${API_BASE}/jobs`),
      ]);
      setGroups(gData.groups || []);
      setJobs(jData.jobs || []);
    } catch { message.error('데이터 로드 실패'); }
    finally { setLoading(false); }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  // Group CRUD
  const openGroupModal = (group?: JobGroup) => {
    setEditingGroup(group || null);
    groupForm.setFieldsValue(group || { name: '', description: '', priority: 0, enabled: true });
    setGroupModalOpen(true);
  };

  const handleGroupSave = async () => {
    const values = await groupForm.validateFields();
    try {
      if (editingGroup) {
        await putJSON(`${API_BASE}/job-groups/${editingGroup.group_id}`, values);
        message.success('그룹 수정 완료');
      } else {
        await postJSON(`${API_BASE}/job-groups`, values);
        message.success('그룹 생성 완료');
      }
      setGroupModalOpen(false);
      loadData();
    } catch { message.error('저장 실패'); }
  };

  const handleGroupDelete = async (groupId: number) => {
    try {
      await deleteJSON(`${API_BASE}/job-groups/${groupId}`);
      message.success('그룹 삭제 완료');
      loadData();
    } catch { message.error('삭제 실패'); }
  };

  // Job CRUD
  const openJobModal = (job?: Job, defaultGroupId?: number) => {
    setEditingJob(job || null);
    jobForm.setFieldsValue(job || {
      name: '', job_type: 'batch', group_id: defaultGroupId || groups[0]?.group_id,
      source_id: '', target_table: '', schedule: '', dag_id: '', enabled: true,
    });
    setJobModalOpen(true);
  };

  const handleJobSave = async () => {
    const values = await jobForm.validateFields();
    try {
      if (editingJob) {
        await putJSON(`${API_BASE}/jobs/${editingJob.job_id}`, values);
        message.success('Job 수정 완료');
      } else {
        await postJSON(`${API_BASE}/jobs`, values);
        message.success('Job 생성 완료');
      }
      setJobModalOpen(false);
      loadData();
    } catch { message.error('저장 실패'); }
  };

  const handleJobDelete = async (jobId: number) => {
    try {
      await deleteJSON(`${API_BASE}/jobs/${jobId}`);
      message.success('Job 삭제 완료');
      loadData();
    } catch { message.error('삭제 실패'); }
  };

  const handleTrigger = async (jobId: number) => {
    try {
      const data = await postJSON(`${API_BASE}/jobs/${jobId}/trigger`, {});
      message.success(data.message || '실행 트리거됨');
      setTimeout(loadData, 1000);
    } catch { message.error('실행 실패'); }
  };

  const handleTypeChange = async (jobId: number, newType: string) => {
    try {
      await putJSON(`${API_BASE}/jobs/${jobId}`, { job_type: newType });
      message.success(`취득 방식 → ${newType} 변경`);
      loadData();
    } catch { message.error('변경 실패'); }
  };

  // Stats
  const stats = {
    total: jobs.length,
    running: jobs.filter(j => j.last_status === 'running').length,
    batch: jobs.filter(j => j.job_type === 'batch').length,
    stream: jobs.filter(j => j.job_type === 'stream').length,
    cdc: jobs.filter(j => j.job_type === 'cdc').length,
  };

  // Group collapse items
  const collapseItems = groups.map(g => {
    const groupJobs = jobs.filter(j => j.group_id === g.group_id);
    return {
      key: String(g.group_id),
      label: (
        <Space>
          <FolderOutlined />
          <Text strong>{g.name}</Text>
          <Badge count={groupJobs.length} style={{ backgroundColor: '#005BAC' }} />
          {!g.enabled && <Tag color="default">비활성</Tag>}
        </Space>
      ),
      extra: (
        <Space size="small" onClick={e => e.stopPropagation()}>
          <Button size="small" icon={<PlusOutlined />} onClick={() => openJobModal(undefined, g.group_id)}>Job</Button>
          <Button size="small" icon={<EditOutlined />} onClick={() => openGroupModal(g)} />
          <Popconfirm title="그룹을 삭제하시겠습니까?" onConfirm={() => handleGroupDelete(g.group_id)} okText="삭제" cancelText="취소">
            <Button size="small" icon={<DeleteOutlined />} danger />
          </Popconfirm>
        </Space>
      ),
      children: groupJobs.length > 0 ? (
        <Table
          dataSource={groupJobs.map(j => ({ ...j, key: j.job_id }))}
          size="small"
          pagination={false}
          columns={[
            {
              title: 'Job', key: 'name', width: 220,
              render: (_: any, r: Job) => (
                <Space>
                  <Text strong>{r.name}</Text>
                </Space>
              ),
            },
            {
              title: '취득 방식', key: 'type', width: 130,
              render: (_: any, r: Job) => (
                <Select
                  size="small"
                  value={r.job_type}
                  onChange={(v) => handleTypeChange(r.job_id, v)}
                  style={{ width: 100 }}
                  options={[
                    { value: 'batch', label: <Tag color="blue">Batch</Tag> },
                    { value: 'stream', label: <Tag color="green">Stream</Tag> },
                    { value: 'cdc', label: <Tag color="orange">CDC</Tag> },
                  ]}
                />
              ),
            },
            {
              title: '소스 → 타겟', key: 'flow', width: 200,
              render: (_: any, r: Job) => (
                <Text type="secondary" style={{ fontSize: 12 }}>{r.source_id || '-'} → {r.target_table || '-'}</Text>
              ),
            },
            { title: '스케줄', dataIndex: 'schedule', key: 'schedule', width: 100, render: (v: string | null) => <Text code>{v || '-'}</Text> },
            {
              title: '최근 상태', key: 'status', width: 80,
              render: (_: any, r: Job) => STATUS_TAG[r.last_status || ''] || <Tag color="default">-</Tag>,
            },
            {
              title: '', key: 'actions', width: 140,
              render: (_: any, r: Job) => (
                <Space size="small">
                  <Tooltip title="수동 실행"><Button size="small" icon={<PlayCircleOutlined />} onClick={() => handleTrigger(r.job_id)} /></Tooltip>
                  <Tooltip title="수정"><Button size="small" icon={<EditOutlined />} onClick={() => openJobModal(r)} /></Tooltip>
                  <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleJobDelete(r.job_id)} okText="삭제" cancelText="취소">
                    <Button size="small" icon={<DeleteOutlined />} danger />
                  </Popconfirm>
                </Space>
              ),
            },
          ]}
        />
      ) : <Empty description="Job이 없습니다" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
    };
  });

  return (
    <Spin spinning={loading}>
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={12} md={4}><Card size="small"><Statistic title="총 Job" value={stats.total} prefix={<AppstoreOutlined />} /></Card></Col>
        <Col xs={12} md={4}><Card size="small"><Statistic title="실행중" value={stats.running} valueStyle={{ color: '#1890ff' }} prefix={<SyncOutlined spin={stats.running > 0} />} /></Card></Col>
        <Col xs={12} md={4}><Card size="small"><Statistic title="Batch" value={stats.batch} prefix={<Tag color="blue">B</Tag>} /></Card></Col>
        <Col xs={12} md={4}><Card size="small"><Statistic title="Stream" value={stats.stream} prefix={<Tag color="green">S</Tag>} /></Card></Col>
        <Col xs={12} md={4}><Card size="small"><Statistic title="CDC" value={stats.cdc} prefix={<Tag color="orange">C</Tag>} /></Card></Col>
        <Col xs={12} md={4}>
          <Card size="small" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%' }}>
            <Space>
              <Button icon={<PlusOutlined />} type="primary" onClick={() => openGroupModal()}>그룹 추가</Button>
              <Button icon={<ReloadOutlined />} onClick={loadData}>새로고침</Button>
            </Space>
          </Card>
        </Col>
      </Row>

      {groups.length > 0 ? (
        <Collapse items={collapseItems} defaultActiveKey={groups.map(g => String(g.group_id))} />
      ) : !loading ? (
        <Card><Empty description="등록된 Job Group이 없습니다" /></Card>
      ) : null}

      {/* Group Modal */}
      <Modal
        title={editingGroup ? '그룹 수정' : '그룹 추가'}
        open={groupModalOpen}
        onOk={handleGroupSave}
        onCancel={() => setGroupModalOpen(false)}
        okText="저장"
        cancelText="취소"
      >
        <Form form={groupForm} layout="vertical">
          <Form.Item name="name" label="그룹명" rules={[{ required: true, message: '그룹명을 입력하세요' }]}>
            <Input />
          </Form.Item>
          <Form.Item name="description" label="설명">
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item name="priority" label="우선순위">
            <Select options={[{ value: 1, label: '높음' }, { value: 2, label: '보통' }, { value: 3, label: '낮음' }]} />
          </Form.Item>
          <Form.Item name="enabled" label="활성화" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>

      {/* Job Modal */}
      <Modal
        title={editingJob ? 'Job 수정' : 'Job 추가'}
        open={jobModalOpen}
        onOk={handleJobSave}
        onCancel={() => setJobModalOpen(false)}
        okText="저장"
        cancelText="취소"
        width={600}
      >
        <Form form={jobForm} layout="vertical">
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="name" label="Job명" rules={[{ required: true }]}>
                <Input />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="group_id" label="소속 그룹" rules={[{ required: true }]}>
                <Select options={groups.map(g => ({ value: g.group_id, label: g.name }))} />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={8}>
              <Form.Item name="job_type" label="취득 방식" rules={[{ required: true }]}>
                <Select options={[
                  { value: 'batch', label: 'Batch' },
                  { value: 'stream', label: 'Stream' },
                  { value: 'cdc', label: 'CDC' },
                ]} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="source_id" label="소스 ID">
                <Input />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="target_table" label="타겟 테이블">
                <Input />
              </Form.Item>
            </Col>
          </Row>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="schedule" label="스케줄">
                <Input placeholder="0 2 * * * 또는 실시간" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="dag_id" label="DAG ID">
                <Input />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item name="enabled" label="활성화" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>
    </Spin>
  );
};

export default JobGroupManagement;
