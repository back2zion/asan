import React, { useState, useEffect, useCallback } from 'react';
import { Card, Segmented, Row, Col, Spin, Empty, Button, Modal, Input, Select, Space, Typography, Popconfirm, Tag, Alert, App } from 'antd';
import {
  DashboardOutlined, PlusOutlined, DeleteOutlined, EditOutlined,
  EyeOutlined, ExpandOutlined, ReloadOutlined,
} from '@ant-design/icons';
import { biApi } from '../../services/biApi';
import EChartsRenderer from './EChartsRenderer';
import RawDataModal from './RawDataModal';

const { Text, Title } = Typography;
const SUPERSET_URL = 'http://localhost:18088';

interface DashboardChart {
  chart_id: number;
  name: string;
  chart_type: string;
  sql_query: string;
  config: any;
  data: { columns: string[]; rows: any[][] };
}

interface Dashboard {
  dashboard_id: number;
  name: string;
  description: string;
  chart_ids: number[];
  charts: DashboardChart[];
  creator: string;
  shared: boolean;
}

const DashboardBuilder: React.FC = () => {
  const { message } = App.useApp();
  const [mode, setMode] = useState<string>('커스텀');
  const [dashboards, setDashboards] = useState<any[]>([]);
  const [selectedDashboard, setSelectedDashboard] = useState<Dashboard | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);

  // Create/Edit modal
  const [modalOpen, setModalOpen] = useState(false);
  const [editId, setEditId] = useState<number | null>(null);
  const [formName, setFormName] = useState('');
  const [formDesc, setFormDesc] = useState('');
  const [formChartIds, setFormChartIds] = useState<number[]>([]);
  const [allCharts, setAllCharts] = useState<any[]>([]);

  // Raw data modal
  const [rawModal, setRawModal] = useState<{ open: boolean; name: string; sql: string; columns: string[]; rows: any[][] }>({
    open: false, name: '', sql: '', columns: [], rows: [],
  });

  // Superset iframe
  const [iframeLoading, setIframeLoading] = useState(true);
  const [iframeError, setIframeError] = useState(false);
  const [supersetPage, setSupersetPage] = useState('dashboard/list');

  useEffect(() => {
    loadDashboards();
  }, []);

  const loadDashboards = async () => {
    setLoading(true);
    try {
      const list = await biApi.getDashboards();
      setDashboards(list);
      if (list.length > 0 && !selectedDashboard) {
        loadDashboardDetail(list[0].dashboard_id);
      }
    } catch { /* ignore */ }
    setLoading(false);
  };

  const loadDashboardDetail = async (id: number) => {
    setDetailLoading(true);
    try {
      const detail = await biApi.getDashboard(id);
      setSelectedDashboard(detail);
    } catch { message.error('대시보드 로드 실패'); }
    setDetailLoading(false);
  };

  const openCreateModal = async () => {
    setEditId(null);
    setFormName('');
    setFormDesc('');
    setFormChartIds([]);
    try {
      const charts = await biApi.getCharts();
      setAllCharts(charts);
    } catch { /* ignore */ }
    setModalOpen(true);
  };

  const openEditModal = async (db: any) => {
    setEditId(db.dashboard_id);
    setFormName(db.name);
    setFormDesc(db.description || '');
    setFormChartIds(db.chart_ids || []);
    try {
      const charts = await biApi.getCharts();
      setAllCharts(charts);
    } catch { /* ignore */ }
    setModalOpen(true);
  };

  const handleSave = async () => {
    if (!formName.trim()) { message.warning('이름을 입력하세요'); return; }
    try {
      if (editId) {
        await biApi.updateDashboard(editId, { name: formName, description: formDesc, chart_ids: formChartIds });
        message.success('대시보드가 수정되었습니다');
        loadDashboardDetail(editId);
      } else {
        const result = await biApi.createDashboard({ name: formName, description: formDesc, chart_ids: formChartIds });
        message.success('대시보드가 생성되었습니다');
        loadDashboardDetail(result.dashboard_id);
      }
      setModalOpen(false);
      loadDashboards();
    } catch { message.error('저장 실패'); }
  };

  const handleDelete = async (id: number) => {
    try {
      await biApi.deleteDashboard(id);
      setDashboards(prev => prev.filter(d => d.dashboard_id !== id));
      if (selectedDashboard?.dashboard_id === id) setSelectedDashboard(null);
      message.success('대시보드가 삭제되었습니다');
    } catch { message.error('삭제 실패'); }
  };

  const handleViewRaw = (chart: DashboardChart) => {
    setRawModal({
      open: true,
      name: chart.name,
      sql: chart.sql_query,
      columns: chart.data.columns,
      rows: chart.data.rows,
    });
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, height: '100%' }}>
      <Card size="small">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Segmented
            options={['커스텀', 'Superset']}
            value={mode}
            onChange={v => setMode(v as string)}
          />
          {mode === '커스텀' && (
            <Space>
              <Button icon={<ReloadOutlined />} size="small" onClick={() => selectedDashboard && loadDashboardDetail(selectedDashboard.dashboard_id)}>새로고침</Button>
              <Button type="primary" icon={<PlusOutlined />} size="small" onClick={openCreateModal}>대시보드 생성</Button>
            </Space>
          )}
        </div>
      </Card>

      {mode === '커스텀' ? (
        <>
          {/* Dashboard selector */}
          {dashboards.length > 0 && (
            <Card size="small" styles={{ body: { padding: '8px 12px' } }}>
              <Space wrap>
                {dashboards.map(db => (
                  <Tag
                    key={db.dashboard_id}
                    color={selectedDashboard?.dashboard_id === db.dashboard_id ? '#006241' : undefined}
                    style={{ cursor: 'pointer', padding: '4px 12px' }}
                    onClick={() => loadDashboardDetail(db.dashboard_id)}
                  >
                    <DashboardOutlined /> {db.name}
                  </Tag>
                ))}
              </Space>
            </Card>
          )}

          {/* Dashboard content */}
          {loading || detailLoading ? (
            <div style={{ textAlign: 'center', padding: 40 }}><Spin size="large" /></div>
          ) : !selectedDashboard ? (
            <Card><Empty description="대시보드를 선택하거나 생성하세요" /></Card>
          ) : (
            <>
              <Card size="small" styles={{ body: { padding: '8px 16px' } }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <div>
                    <Text strong style={{ fontSize: 16 }}>{selectedDashboard.name}</Text>
                    {selectedDashboard.description && (
                      <Text type="secondary" style={{ marginLeft: 12, fontSize: 13 }}>{selectedDashboard.description}</Text>
                    )}
                  </div>
                  <Space>
                    <Button size="small" icon={<EditOutlined />} onClick={() => openEditModal(selectedDashboard)}>편집</Button>
                    <Popconfirm title="삭제하시겠습니까?" onConfirm={() => handleDelete(selectedDashboard.dashboard_id)} okText="삭제" cancelText="취소">
                      <Button size="small" icon={<DeleteOutlined />} danger>삭제</Button>
                    </Popconfirm>
                  </Space>
                </div>
              </Card>
              <Row gutter={[12, 12]}>
                {selectedDashboard.charts?.map(chart => (
                  <Col xs={24} md={12} key={chart.chart_id}>
                    <Card
                      size="small"
                      title={
                        <Space>
                          <Text strong>{chart.name}</Text>
                          <Tag>{chart.chart_type}</Tag>
                        </Space>
                      }
                      extra={
                        <Button size="small" icon={<EyeOutlined />} onClick={() => handleViewRaw(chart)}>원본</Button>
                      }
                    >
                      {chart.data.columns.length > 0 ? (
                        <EChartsRenderer
                          columns={chart.data.columns}
                          results={chart.data.rows}
                          chartType={chart.chart_type !== 'table' ? chart.chart_type : undefined}
                          xField={chart.config?.xField}
                          yField={chart.config?.yField}
                          groupField={chart.config?.groupField}
                          height={320}
                        />
                      ) : (
                        <Empty description="데이터 없음" />
                      )}
                    </Card>
                  </Col>
                ))}
                {(!selectedDashboard.charts || selectedDashboard.charts.length === 0) && (
                  <Col span={24}>
                    <Empty description="차트가 없습니다. 편집에서 차트를 추가하세요." />
                  </Col>
                )}
              </Row>
            </>
          )}
        </>
      ) : (
        /* Superset iframe mode */
        <Card style={{ flex: 1, overflow: 'hidden' }} styles={{ body: { padding: 0, height: '100%', position: 'relative' } }}>
          <div style={{ padding: '8px 12px', borderBottom: '1px solid #f0f0f0' }}>
            <Space>
              {[
                { key: 'dashboard/list', label: '대시보드' },
                { key: 'chart/list', label: '차트' },
                { key: 'tablemodelview/list', label: '데이터셋' },
                { key: 'sqllab', label: 'SQL Lab' },
              ].map(link => (
                <Button key={link.key} size="small"
                  type={supersetPage === link.key ? 'primary' : 'default'}
                  onClick={() => { setSupersetPage(link.key); setIframeLoading(true); setIframeError(false); }}
                >{link.label}</Button>
              ))}
              <Button size="small" icon={<ExpandOutlined />}
                href={`${SUPERSET_URL}/${supersetPage}/`} target="_blank"
              >새 창</Button>
            </Space>
          </div>
          {iframeLoading && !iframeError && (
            <div style={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%, -50%)', zIndex: 10, textAlign: 'center' }}>
              <Spin size="large" />
              <div style={{ marginTop: 8, color: '#666' }}>Superset 로딩 중...</div>
            </div>
          )}
          {iframeError ? (
            <div style={{ padding: 24, textAlign: 'center' }}>
              <Alert type="warning" message="Superset 임베딩이 차단되었습니다"
                description={
                  <Button type="primary" icon={<ExpandOutlined />} href={`${SUPERSET_URL}/${supersetPage}/`} target="_blank" style={{ marginTop: 8 }}>
                    새 창에서 열기
                  </Button>
                }
              />
            </div>
          ) : (
            <iframe
              src={`${SUPERSET_URL}/${supersetPage}/?standalone=3`}
              style={{ width: '100%', height: 'calc(100% - 40px)', border: 'none', minHeight: 500 }}
              title="Apache Superset"
              onLoad={() => setIframeLoading(false)}
              onError={() => setIframeError(true)}
            />
          )}
        </Card>
      )}

      {/* Create/Edit modal */}
      <Modal title={editId ? '대시보드 편집' : '대시보드 생성'} open={modalOpen} onOk={handleSave} onCancel={() => setModalOpen(false)} okText="저장" width={600}>
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          <Input placeholder="대시보드 이름 *" value={formName} onChange={e => setFormName(e.target.value)} />
          <Input.TextArea placeholder="설명 (선택)" value={formDesc} onChange={e => setFormDesc(e.target.value)} rows={2} />
          <Select
            mode="multiple"
            placeholder="차트 선택"
            style={{ width: '100%' }}
            value={formChartIds}
            onChange={setFormChartIds}
            options={allCharts.map(c => ({ label: `${c.name} (${c.chart_type})`, value: c.chart_id }))}
          />
        </Space>
      </Modal>

      <RawDataModal
        open={rawModal.open}
        onClose={() => setRawModal(prev => ({ ...prev, open: false }))}
        chartName={rawModal.name}
        sqlQuery={rawModal.sql}
        columns={rawModal.columns}
        rows={rawModal.rows}
      />
    </div>
  );
};

export default DashboardBuilder;
