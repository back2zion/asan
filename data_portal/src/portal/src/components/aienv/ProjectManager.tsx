import React, { useState, useCallback, useEffect } from 'react';
import {
  Card, Row, Col, Tag, Button, Space, Progress, Spin, Empty, Modal,
  Form, Input, InputNumber, Tooltip, Divider, App, Typography,
} from 'antd';
import {
  PlusOutlined,
  ReloadOutlined,
  EditOutlined,
  DeleteOutlined,
  ProjectOutlined,
  CodeOutlined,
} from '@ant-design/icons';
import { fetchPost, fetchPut, fetchDelete } from '../../services/apiUtils';

const { Text } = Typography;
const { TextArea } = Input;

const API_BASE = '/api/v1/ai-environment';

interface ProjectInfo {
  id: string;
  name: string;
  description: string;
  owner: string;
  status: string;
  cpu_quota: number;
  memory_quota_gb: number;
  storage_quota_gb: number;
  cpu_used: number;
  memory_used_gb: number;
  storage_used_gb: number;
  container_count: number;
  created: string;
  updated: string;
}

interface LanguageInfo {
  id: string;
  name: string;
  version: string;
  description: string;
  available: boolean;
}

const ProjectManager: React.FC = () => {
  const { message, modal } = App.useApp();
  const [projects, setProjects] = useState<ProjectInfo[]>([]);
  const [languages, setLanguages] = useState<LanguageInfo[]>([]);
  const [projectLoading, setProjectLoading] = useState(true);
  const [projectModalOpen, setProjectModalOpen] = useState(false);
  const [projectForm] = Form.useForm();
  const [editingProject, setEditingProject] = useState<ProjectInfo | null>(null);

  const fetchProjects = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/projects`);
      if (!res.ok) return;
      const data = await res.json();
      setProjects(data.projects || []);
    } catch {
      /* 프로젝트 로드 실패 — 빈 목록 유지 */
    } finally {
      setProjectLoading(false);
    }
  }, []);

  const fetchLanguages = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/languages`);
      if (!res.ok) return;
      const data = await res.json();
      setLanguages(data.languages || []);
    } catch {
      /* 언어 목록 로드 실패 — 빈 목록 유지 */
    }
  }, []);

  useEffect(() => {
    fetchProjects();
    fetchLanguages();
  }, [fetchProjects, fetchLanguages]);

  const handleCreateProject = async (values: any) => {
    try {
      const res = await fetchPost(`${API_BASE}/projects`, values);
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || '생성 실패');
      }
      message.success('프로젝트가 생성되었습니다');
      setProjectModalOpen(false);
      projectForm.resetFields();
      setEditingProject(null);
      fetchProjects();
    } catch (e: any) {
      message.error(e.message);
    }
  };

  const handleUpdateProject = async (values: any) => {
    if (!editingProject) return;
    try {
      const res = await fetchPut(`${API_BASE}/projects/${editingProject.id}`, values);
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || '수정 실패');
      }
      message.success('프로젝트가 수정되었습니다');
      setProjectModalOpen(false);
      projectForm.resetFields();
      setEditingProject(null);
      fetchProjects();
    } catch (e: any) {
      message.error(e.message);
    }
  };

  const handleDeleteProject = (projectId: string, projectName: string) => {
    modal.confirm({
      title: '프로젝트 삭제',
      content: `'${projectName}' 프로젝트를 삭제하시겠습니까?`,
      okText: '삭제',
      okType: 'danger',
      cancelText: '취소',
      onOk: async () => {
        try {
          const res = await fetchDelete(`${API_BASE}/projects/${projectId}`);
          if (!res.ok) throw new Error('삭제 실패');
          message.success('프로젝트가 삭제되었습니다');
          fetchProjects();
        } catch {
          message.error('프로젝트 삭제에 실패했습니다');
        }
      },
    });
  };

  const openEditProject = (proj: ProjectInfo) => {
    setEditingProject(proj);
    projectForm.setFieldsValue({
      description: proj.description,
      cpu_quota: proj.cpu_quota,
      memory_quota_gb: proj.memory_quota_gb,
      storage_quota_gb: proj.storage_quota_gb,
    });
    setProjectModalOpen(true);
  };

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { setEditingProject(null); projectForm.resetFields(); setProjectModalOpen(true); }}>새 프로젝트</Button>
        <Button icon={<ReloadOutlined />} onClick={fetchProjects}>새로고침</Button>
      </Space>

      {projectLoading ? <Spin tip="프로젝트 로딩 중..."><div style={{ padding: 60, textAlign: 'center' }} /></Spin> : projects.length === 0 ? (
        <Empty description="등록된 분석 프로젝트가 없습니다" />
      ) : (
        <Row gutter={[16, 16]}>
          {projects.map((proj) => {
            const cpuPct = proj.cpu_quota > 0 ? Math.round((proj.cpu_used / proj.cpu_quota) * 100) : 0;
            const memPct = proj.memory_quota_gb > 0 ? Math.round((proj.memory_used_gb / proj.memory_quota_gb) * 100) : 0;
            const storagePct = proj.storage_quota_gb > 0 ? Math.round((proj.storage_used_gb / proj.storage_quota_gb) * 100) : 0;
            return (
              <Col span={8} key={proj.id}>
                <Card
                  size="small"
                  title={
                    <Space>
                      <ProjectOutlined style={{ color: '#006241' }} />
                      <strong>{proj.name}</strong>
                      <Tag color={proj.status === 'active' ? 'green' : 'default'}>{proj.status === 'active' ? '활성' : '비활성'}</Tag>
                    </Space>
                  }
                  extra={
                    <Space size="small">
                      <Tooltip title="수정"><Button size="small" icon={<EditOutlined />} onClick={() => openEditProject(proj)} /></Tooltip>
                      <Tooltip title="삭제"><Button size="small" danger icon={<DeleteOutlined />} onClick={() => handleDeleteProject(proj.id, proj.name)} /></Tooltip>
                    </Space>
                  }
                >
                  <p style={{ fontSize: 12, color: '#666', marginBottom: 12 }}>{proj.description || '설명 없음'}</p>
                  <div style={{ marginBottom: 8 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12 }}>
                      <span>CPU</span>
                      <span>{proj.cpu_used} / {proj.cpu_quota} cores</span>
                    </div>
                    <Progress percent={cpuPct} size="small" strokeColor={cpuPct > 80 ? '#ff4d4f' : '#006241'} />
                  </div>
                  <div style={{ marginBottom: 8 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12 }}>
                      <span>메모리</span>
                      <span>{proj.memory_used_gb} / {proj.memory_quota_gb} GB</span>
                    </div>
                    <Progress percent={memPct} size="small" strokeColor={memPct > 80 ? '#ff4d4f' : '#1890ff'} />
                  </div>
                  <div style={{ marginBottom: 8 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12 }}>
                      <span>스토리지</span>
                      <span>{proj.storage_used_gb} / {proj.storage_quota_gb} GB</span>
                    </div>
                    <Progress percent={storagePct} size="small" strokeColor={storagePct > 80 ? '#ff4d4f' : '#722ed1'} />
                  </div>
                  <Divider style={{ margin: '8px 0' }} />
                  <Row>
                    <Col span={8}><Text type="secondary" style={{ fontSize: 11 }}>컨테이너</Text><br /><Text strong>{proj.container_count}</Text></Col>
                    <Col span={8}><Text type="secondary" style={{ fontSize: 11 }}>소유자</Text><br /><Text style={{ fontSize: 12 }}>{proj.owner}</Text></Col>
                    <Col span={8}><Text type="secondary" style={{ fontSize: 11 }}>생성일</Text><br /><Text style={{ fontSize: 11 }}>{proj.created?.split(' ')[0]}</Text></Col>
                  </Row>
                </Card>
              </Col>
            );
          })}
        </Row>
      )}

      {/* Supported languages */}
      <Card size="small" title={<span><CodeOutlined style={{ color: '#1890ff', marginRight: 8 }} />지원 분석 언어</span>} style={{ marginTop: 16 }}>
        <Row gutter={[16, 16]}>
          {languages.map((lang) => (
            <Col span={8} key={lang.id}>
              <Card size="small" style={{ textAlign: 'center', border: lang.available ? '1px solid #b7eb8f' : '1px solid #f0f0f0' }}>
                <div style={{ fontSize: 24, marginBottom: 4 }}>
                  {lang.id === 'python' ? '🐍' : lang.id === 'r' ? '📊' : '⚡'}
                </div>
                <Text strong>{lang.name} {lang.version}</Text>
                <br />
                <Text type="secondary" style={{ fontSize: 12 }}>{lang.description}</Text>
                <br />
                <Tag color={lang.available ? 'green' : 'default'} style={{ marginTop: 4 }}>
                  {lang.available ? '사용 가능' : '준비 중'}
                </Tag>
              </Card>
            </Col>
          ))}
        </Row>
      </Card>

      {/* Project create/edit modal */}
      <Modal
        title={editingProject ? `프로젝트 수정: ${editingProject.name}` : '새 분석 프로젝트 생성'}
        open={projectModalOpen}
        onCancel={() => { setProjectModalOpen(false); projectForm.resetFields(); setEditingProject(null); }}
        onOk={() => projectForm.submit()}
        okText={editingProject ? '수정' : '생성'}
        cancelText="취소"
      >
        <Form
          form={projectForm}
          layout="vertical"
          onFinish={editingProject ? handleUpdateProject : handleCreateProject}
          initialValues={{ cpu_quota: 4, memory_quota_gb: 8, storage_quota_gb: 50, owner: '데이터분석팀' }}
        >
          {!editingProject && (
            <>
              <Form.Item name="name" label="프로젝트 이름" rules={[{ required: true, message: '이름을 입력하세요' }]}>
                <Input placeholder="예: 당뇨 코호트 분석" />
              </Form.Item>
              <Form.Item name="owner" label="소유자">
                <Input placeholder="팀 또는 담당자" />
              </Form.Item>
            </>
          )}
          <Form.Item name="description" label="설명">
            <TextArea rows={2} placeholder="프로젝트 목적 및 설명" />
          </Form.Item>
          <Row gutter={16}>
            <Col span={8}>
              <Form.Item name="cpu_quota" label="CPU 할당 (cores)">
                <InputNumber min={1} max={32} step={1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="memory_quota_gb" label="메모리 (GB)">
                <InputNumber min={1} max={128} step={1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item name="storage_quota_gb" label="스토리지 (GB)">
                <InputNumber min={1} max={500} step={10} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>
        </Form>
      </Modal>
    </div>
  );
};

export default ProjectManager;
