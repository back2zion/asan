import React, { useState, useCallback, useEffect } from 'react';
import {
  Card, Row, Col, Tag, Button, Space, Statistic, Spin, Empty, Upload,
  Modal, Form, Select, Radio, Dropdown, Divider, Tooltip,
  App, Typography,
} from 'antd';
import type { MenuProps } from 'antd';
import {
  ShareAltOutlined,
  CodeOutlined,
  MessageOutlined,
  ReloadOutlined,
  LinkOutlined,
  UploadOutlined,
  PlayCircleOutlined,
  EyeOutlined,
  DownloadOutlined,
  DeleteOutlined,
  FileTextOutlined,
  FileExcelOutlined,
  LockOutlined,
  TeamOutlined,
  GlobalOutlined,
} from '@ant-design/icons';

import AnalysisRequestManager from './AnalysisRequestManager';
import { fetchPost, fetchPut, fetchDelete, getCsrfToken } from '../../services/apiUtils';

const { Text } = Typography;

const API_BASE = '/api/v1/ai-environment';
const JUPYTER_DIRECT_URL = 'http://localhost:18888';

interface NotebookInfo {
  filename: string;
  name: string;
  size_kb: number;
  modified: string;
  cell_count: number;
  jupyter_url?: string;
  last_modified_by?: string;
  permission?: string;
  permission_group?: string;
}

const PERMISSION_LABELS: Record<string, { label: string; color: string; icon: React.ReactNode }> = {
  private: { label: '개인', color: 'red', icon: <LockOutlined /> },
  group: { label: '그룹', color: 'blue', icon: <TeamOutlined /> },
  public: { label: '전체', color: 'green', icon: <GlobalOutlined /> },
};

const NotebookManager: React.FC = () => {
  const { message, modal } = App.useApp();
  const [notebooks, setNotebooks] = useState<NotebookInfo[]>([]);
  const [workspaceNotebooks, setWorkspaceNotebooks] = useState<NotebookInfo[]>([]);
  const [loading, setLoading] = useState(true);

  // Preview modal
  const [previewModalOpen, setPreviewModalOpen] = useState(false);
  const [previewData, setPreviewData] = useState<{
    filename: string;
    cells: { cell_type: string; source: string }[];
    kernel: string;
    language: string;
  } | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);

  // Permission modal
  const [permModalOpen, setPermModalOpen] = useState(false);
  const [permTarget, setPermTarget] = useState<string>('');
  const [permLevel, setPermLevel] = useState<string>('public');
  const [permGroup, setPermGroup] = useState<string>('전체');
  const [permLoading, setPermLoading] = useState(false);

  // --- Fetching ---

  const fetchNotebooks = useCallback(async () => {
    try {
      const [sharedR, workspaceR] = await Promise.all([
        fetch(`${API_BASE}/shared`),
        fetch(`${API_BASE}/jupyter/workspace`),
      ]);
      if (sharedR.ok) {
        const sharedRes = await sharedR.json();
        setNotebooks(sharedRes.notebooks || []);
      }
      if (workspaceR.ok) {
        const workspaceRes = await workspaceR.json();
        setWorkspaceNotebooks(workspaceRes.notebooks || []);
      }
    } catch {
      /* 노트북 로드 실패 — 빈 목록 유지 */
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchNotebooks();
  }, [fetchNotebooks]);

  // --- Notebook Actions ---

  const handlePreviewNotebook = async (filename: string) => {
    setPreviewLoading(true);
    setPreviewModalOpen(true);
    try {
      const res = await fetch(`${API_BASE}/shared/${encodeURIComponent(filename)}/preview`);
      if (!res.ok) throw new Error('미리보기 실패');
      setPreviewData(await res.json());
    } catch {
      message.error('노트북 미리보기에 실패했습니다');
      setPreviewModalOpen(false);
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleOpenInJupyter = async (filename: string) => {
    try {
      const res = await fetchPost(`${API_BASE}/shared/${encodeURIComponent(filename)}/open-in-jupyter`);
      if (!res.ok) throw new Error('JupyterLab 열기 실패');
      const data = await res.json();
      const directUrl = data.jupyter_url.replace(/^\/jupyter/, JUPYTER_DIRECT_URL);
      window.open(directUrl, '_blank');
      message.success('JupyterLab에서 노트북을 열었습니다');
      fetchNotebooks();
    } catch {
      message.error('JupyterLab에서 노트북 열기에 실패했습니다');
    }
  };

  const handleOpenWorkspaceNotebook = (jupyterUrl: string) => {
    const directUrl = jupyterUrl.replace(/^\/jupyter/, JUPYTER_DIRECT_URL);
    window.open(directUrl, '_blank');
  };

  const handleDeleteNotebook = (filename: string) => {
    modal.confirm({
      title: '노트북 삭제',
      content: `'${filename}' 파일을 삭제하시겠습니까?`,
      okText: '삭제',
      okType: 'danger',
      cancelText: '취소',
      onOk: async () => {
        try {
          const res = await fetchDelete(`${API_BASE}/shared/${encodeURIComponent(filename)}`);
          if (!res.ok) throw new Error('삭제 실패');
          message.success('노트북이 삭제되었습니다');
          fetchNotebooks();
        } catch {
          message.error('노트북 삭제에 실패했습니다');
        }
      },
    });
  };

  const handleUploadNotebook = async (file: File) => {
    const formData = new FormData();
    formData.append('file', file);
    try {
      const res = await fetch(`${API_BASE}/shared/upload`, { method: 'POST', headers: { 'X-CSRF-Token': getCsrfToken() }, body: formData });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || '업로드 실패');
      }
      message.success('노트북이 업로드되었습니다');
      fetchNotebooks();
    } catch (e: any) {
      message.error(e.message || '업로드에 실패했습니다');
    }
    return false;
  };

  // --- Permission ---

  const handleOpenPermission = async (filename: string) => {
    setPermTarget(filename);
    setPermLoading(true);
    setPermModalOpen(true);
    try {
      const res = await fetch(`${API_BASE}/shared/${encodeURIComponent(filename)}/permissions`);
      const data = await res.json();
      setPermLevel(data.level || 'public');
      setPermGroup(data.group || '전체');
    } catch {
      setPermLevel('public');
      setPermGroup('전체');
    } finally {
      setPermLoading(false);
    }
  };

  const handleSavePermission = async () => {
    setPermLoading(true);
    try {
      const res = await fetchPut(`${API_BASE}/shared/${encodeURIComponent(permTarget)}/permissions`, { level: permLevel, group: permGroup });
      if (!res.ok) throw new Error('권한 변경 실패');
      message.success('공유 설정이 변경되었습니다');
      setPermModalOpen(false);
      fetchNotebooks();
    } catch {
      message.error('공유 설정 변경에 실패했습니다');
    } finally {
      setPermLoading(false);
    }
  };

  // --- Export ---

  const getExportMenuItems = (_filename: string): MenuProps['items'] => [
    { key: 'ipynb', label: 'Jupyter Notebook (.ipynb)', icon: <FileTextOutlined /> },
    { key: 'html', label: 'HTML 보고서 (.html)', icon: <FileTextOutlined /> },
    { key: 'csv', label: 'CSV 데이터 (.csv)', icon: <FileExcelOutlined /> },
  ];

  const handleExport = (filename: string, format: string) => {
    window.open(`${API_BASE}/shared/${encodeURIComponent(filename)}/export/${format}`, '_blank');
  };

  // --- Render ---

  const notebookOptions = notebooks.map(nb => ({ value: nb.filename, label: nb.name }));

  return (
    <div>
      {/* Stats + Action buttons */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col span={5}>
          <Card size="small">
            <Statistic title="공유 노트북" value={notebooks.length} prefix={<ShareAltOutlined />} valueStyle={{ color: '#006241' }} loading={loading} />
          </Card>
        </Col>
        <Col span={5}>
          <Card size="small">
            <Statistic title="워크스페이스" value={workspaceNotebooks.length} prefix={<CodeOutlined />} valueStyle={{ color: '#1890ff' }} loading={loading} />
          </Card>
        </Col>
        <Col span={5}>
          <Card size="small">
            <Statistic title="분석 요청" value={'-'} prefix={<MessageOutlined />} valueStyle={{ color: '#fa8c16' }} />
          </Card>
        </Col>
        <Col span={9}>
          <Card size="small" style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Space>
              <Button type="primary" size="large" icon={<LinkOutlined />} style={{ background: '#006241', borderColor: '#006241' }} onClick={() => window.open(`${JUPYTER_DIRECT_URL}/lab`, '_blank')}>
                JupyterLab 열기
              </Button>
              <Upload accept=".ipynb" showUploadList={false} beforeUpload={(file) => { handleUploadNotebook(file); return false; }}>
                <Button icon={<UploadOutlined />}>업로드</Button>
              </Upload>
              <Button icon={<ReloadOutlined />} onClick={() => { fetchNotebooks(); }}>새로고침</Button>
            </Space>
          </Card>
        </Col>
      </Row>

      {/* Shared notebooks + Workspace */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col span={14}>
          <Card size="small" title={<span><ShareAltOutlined style={{ color: '#006241', marginRight: 8 }} />공유 노트북 라이브러리</span>}>
            {loading ? <Spin /> : notebooks.length === 0 ? (
              <Empty description="공유된 노트북이 없습니다" />
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {notebooks.map((nb) => {
                  const perm = PERMISSION_LABELS[nb.permission || 'public'];
                  return (
                    <Card key={nb.filename} size="small" hoverable style={{ border: '1px solid #f0f0f0' }}>
                      <Row align="middle" justify="space-between">
                        <Col flex="auto">
                          <Space direction="vertical" size={2}>
                            <Space>
                              <FileTextOutlined style={{ color: '#fa8c16', fontSize: 16 }} />
                              <strong>{nb.name}</strong>
                              <Tag>{nb.cell_count}셀</Tag>
                              <Tag color={perm.color} icon={perm.icon}>{perm.label}</Tag>
                              <span style={{ fontSize: 11, color: '#999' }}>{nb.size_kb} KB</span>
                            </Space>
                            <span style={{ fontSize: 11, color: '#999', marginLeft: 22 }}>
                              {nb.filename} | {nb.modified}
                              {nb.last_modified_by && <> | 수정: <span style={{ color: '#006241' }}>{nb.last_modified_by}</span></>}
                            </span>
                          </Space>
                        </Col>
                        <Col>
                          <Space size="small">
                            <Tooltip title="JupyterLab에서 열기">
                              <Button type="primary" size="small" icon={<PlayCircleOutlined />} style={{ background: '#006241', borderColor: '#006241' }} onClick={() => handleOpenInJupyter(nb.filename)}>열기</Button>
                            </Tooltip>
                            <Tooltip title="미리보기">
                              <Button size="small" icon={<EyeOutlined />} onClick={() => handlePreviewNotebook(nb.filename)} />
                            </Tooltip>
                            <Dropdown
                              menu={{
                                items: getExportMenuItems(nb.filename),
                                onClick: ({ key }) => handleExport(nb.filename, key),
                              }}
                              trigger={['click']}
                            >
                              <Tooltip title="내보내기">
                                <Button size="small" icon={<DownloadOutlined />} />
                              </Tooltip>
                            </Dropdown>
                            <Tooltip title="공유 설정">
                              <Button size="small" icon={<TeamOutlined />} onClick={() => handleOpenPermission(nb.filename)} />
                            </Tooltip>
                            <Tooltip title="삭제">
                              <Button size="small" danger icon={<DeleteOutlined />} onClick={() => handleDeleteNotebook(nb.filename)} />
                            </Tooltip>
                          </Space>
                        </Col>
                      </Row>
                    </Card>
                  );
                })}
              </div>
            )}
          </Card>
        </Col>

        <Col span={10}>
          <Card size="small" title={<span><CodeOutlined style={{ color: '#1890ff', marginRight: 8 }} />JupyterLab 워크스페이스</span>}>
            {workspaceNotebooks.length === 0 ? (
              <Empty description="워크스페이스에 노트북이 없습니다" />
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                {workspaceNotebooks.map((nb) => (
                  <div
                    key={nb.filename}
                    style={{ padding: '8px 12px', borderRadius: 6, border: '1px solid #f0f0f0', cursor: 'pointer', transition: 'all 0.2s' }}
                    onClick={() => nb.jupyter_url && handleOpenWorkspaceNotebook(nb.jupyter_url)}
                    onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.background = '#f0f8f3'; (e.currentTarget as HTMLElement).style.borderColor = '#006241'; }}
                    onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.background = ''; (e.currentTarget as HTMLElement).style.borderColor = '#f0f0f0'; }}
                  >
                    <Space>
                      <FileTextOutlined style={{ color: '#1890ff' }} />
                      <span style={{ fontWeight: 500 }}>{nb.name}</span>
                      <Tag style={{ fontSize: 10 }}>{nb.cell_count}셀</Tag>
                    </Space>
                    <div style={{ fontSize: 11, color: '#999', marginLeft: 22 }}>{nb.filename} | {nb.modified}</div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        </Col>
      </Row>

      {/* Analysis requests section */}
      <AnalysisRequestManager notebookOptions={notebookOptions} />

      {/* Preview modal */}
      <Modal
        title={previewData ? `미리보기: ${previewData.filename}` : '미리보기'}
        open={previewModalOpen}
        onCancel={() => { setPreviewModalOpen(false); setPreviewData(null); }}
        footer={previewData ? [
          <Button key="jupyter" type="primary" icon={<PlayCircleOutlined />} style={{ background: '#006241', borderColor: '#006241' }}
            onClick={() => { handleOpenInJupyter(previewData.filename); setPreviewModalOpen(false); }}>JupyterLab에서 열기</Button>,
          <Button key="close" onClick={() => { setPreviewModalOpen(false); setPreviewData(null); }}>닫기</Button>,
        ] : null}
        width={900}
      >
        {previewLoading ? (
          <div style={{ textAlign: 'center', padding: 40 }}><Spin size="large" /></div>
        ) : previewData ? (
          <div style={{ maxHeight: 500, overflow: 'auto' }}>
            {previewData.cells.map((cell, idx) => (
              <div key={idx} style={{ marginBottom: 12 }}>
                <Tag color={cell.cell_type === 'code' ? 'blue' : 'green'} style={{ marginBottom: 4 }}>
                  {cell.cell_type === 'code' ? 'Code' : 'Markdown'} [{idx + 1}]
                </Tag>
                <pre style={{ background: cell.cell_type === 'code' ? '#f5f5f5' : '#fafff5', padding: '10px 14px', borderRadius: 6, fontSize: 12, overflow: 'auto', maxHeight: 200, border: '1px solid #e8e8e8', whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                  {cell.source}
                </pre>
              </div>
            ))}
          </div>
        ) : null}
      </Modal>

      {/* Permission modal */}
      <Modal
        title={<Space><TeamOutlined />공유 설정 - {permTarget}</Space>}
        open={permModalOpen}
        onCancel={() => setPermModalOpen(false)}
        onOk={handleSavePermission}
        okText="저장"
        cancelText="취소"
        confirmLoading={permLoading}
      >
        {permLoading && !permLevel ? <Spin /> : (
          <div>
            <div style={{ marginBottom: 16 }}>
              <Text strong>접근 권한 수준</Text>
              <div style={{ marginTop: 8 }}>
                <Radio.Group value={permLevel} onChange={(e) => setPermLevel(e.target.value)}>
                  <Space direction="vertical">
                    <Radio value="private">
                      <Space><LockOutlined style={{ color: '#f5222d' }} /><span><strong>개인</strong> - 본인만 접근 가능</span></Space>
                    </Radio>
                    <Radio value="group">
                      <Space><TeamOutlined style={{ color: '#1890ff' }} /><span><strong>그룹</strong> - 지정 그룹만 접근 가능</span></Space>
                    </Radio>
                    <Radio value="public">
                      <Space><GlobalOutlined style={{ color: '#52c41a' }} /><span><strong>전체</strong> - 모든 사용자 접근 가능</span></Space>
                    </Radio>
                  </Space>
                </Radio.Group>
              </div>
            </div>
            {permLevel === 'group' && (
              <div style={{ marginBottom: 16 }}>
                <Text strong>공유 그룹</Text>
                <Select
                  style={{ width: '100%', marginTop: 8 }}
                  value={permGroup}
                  onChange={setPermGroup}
                  options={[
                    { value: '전체', label: '전체' },
                    { value: '경영분석팀', label: '경영분석팀' },
                    { value: '의료정보팀', label: '의료정보팀' },
                    { value: '연구지원팀', label: '연구지원팀' },
                    { value: 'AI센터', label: 'AI센터' },
                    { value: '임상연구팀', label: '임상연구팀' },
                  ]}
                />
              </div>
            )}
            <Divider />
            <div>
              <Text type="secondary" style={{ fontSize: 12 }}>
                공유 설정 변경 시 접근 로그가 기록되며, 감사 이력에서 확인할 수 있습니다.
              </Text>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default NotebookManager;
