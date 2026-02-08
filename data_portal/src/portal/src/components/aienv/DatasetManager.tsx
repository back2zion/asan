import React, { useState, useCallback, useEffect } from 'react';
import {
  Card, Row, Col, Tag, Button, Space, Table, Spin, Empty, Modal,
  Form, InputNumber, Tooltip, App, Typography,
} from 'antd';
import {
  ReloadOutlined,
  SwapOutlined,
  DatabaseOutlined,
  FileTextOutlined,
  LinkOutlined,
  DeleteOutlined,
  TableOutlined,
} from '@ant-design/icons';

const { Text } = Typography;

const API_BASE = '/api/v1/ai-environment';
const JUPYTER_DIRECT_URL = 'http://localhost:18888';

interface OmopTableInfo {
  table_name: string;
  row_count: number;
  description: string;
}

interface ExportedDataset {
  filename: string;
  table_name: string;
  size_kb: number;
  modified: string;
  jupyter_url: string;
}

const DatasetManager: React.FC = () => {
  const { message, modal } = App.useApp();
  const [omopTables, setOmopTables] = useState<OmopTableInfo[]>([]);
  const [exportedDatasets, setExportedDatasets] = useState<ExportedDataset[]>([]);
  const [datasetLoading, setDatasetLoading] = useState({ tables: true, exported: true });
  const [exportModalOpen, setExportModalOpen] = useState(false);
  const [exportTarget, setExportTarget] = useState<OmopTableInfo | null>(null);
  const [exportForm] = Form.useForm();
  const [exporting, setExporting] = useState(false);

  const fetchOmopTables = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/datasets`);
      const data = await res.json();
      setOmopTables(data.tables || []);
    } catch {
      console.error('OMOP 테이블 목록 로드 실패');
    } finally {
      setDatasetLoading(prev => ({ ...prev, tables: false }));
    }
  }, []);

  const fetchExportedDatasets = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/datasets/exported`);
      const data = await res.json();
      setExportedDatasets(data.datasets || []);
    } catch {
      console.error('이관 데이터셋 목록 로드 실패');
    } finally {
      setDatasetLoading(prev => ({ ...prev, exported: false }));
    }
  }, []);

  useEffect(() => {
    fetchOmopTables();
    fetchExportedDatasets();
  }, [fetchOmopTables, fetchExportedDatasets]);

  const handleExportDataset = async (values: any) => {
    if (!exportTarget) return;
    setExporting(true);
    try {
      const res = await fetch(`${API_BASE}/datasets/export`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          table_name: exportTarget.table_name,
          limit: values.limit || 10000,
        }),
      });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || '이관 실패');
      }
      const data = await res.json();
      message.success(data.message);
      setExportModalOpen(false);
      exportForm.resetFields();
      fetchExportedDatasets();
    } catch (e: any) {
      message.error(e.message || '데이터 이관에 실패했습니다');
    } finally {
      setExporting(false);
    }
  };

  const handleDeleteDataset = (filename: string) => {
    modal.confirm({
      title: '데이터셋 삭제',
      content: `'${filename}' 파일을 삭제하시겠습니까?`,
      okText: '삭제',
      okType: 'danger',
      cancelText: '취소',
      onOk: async () => {
        try {
          const res = await fetch(`${API_BASE}/datasets/${encodeURIComponent(filename)}`, { method: 'DELETE' });
          if (!res.ok) throw new Error('삭제 실패');
          message.success('데이터셋이 삭제되었습니다');
          fetchExportedDatasets();
        } catch {
          message.error('데이터셋 삭제에 실패했습니다');
        }
      },
    });
  };

  return (
    <div>
      <Space style={{ marginBottom: 16 }}>
        <Button icon={<ReloadOutlined />} onClick={() => { fetchOmopTables(); fetchExportedDatasets(); }}>새로고침</Button>
        <Tag color="blue">OMOP CDM -&gt; JupyterLab 워크스페이스</Tag>
      </Space>
      <Row gutter={[16, 16]}>
        <Col span={14}>
          <Card
            size="small"
            title={<span><DatabaseOutlined style={{ color: '#006241', marginRight: 8 }} />OMOP CDM 테이블</span>}
          >
            {datasetLoading.tables ? <Spin /> : (
              <Table
                size="small"
                dataSource={omopTables}
                rowKey="table_name"
                pagination={{ pageSize: 10, size: 'small' }}
                columns={[
                  {
                    title: '테이블명', dataIndex: 'table_name', key: 'table_name',
                    render: (text: string) => <Text code style={{ fontSize: 12 }}>{text}</Text>,
                  },
                  {
                    title: '설명', dataIndex: 'description', key: 'description',
                    render: (text: string) => <span style={{ fontSize: 12 }}>{text || '-'}</span>,
                  },
                  {
                    title: '행 수', dataIndex: 'row_count', key: 'row_count',
                    render: (count: number) => <Text strong>{count?.toLocaleString()}</Text>,
                    sorter: (a: OmopTableInfo, b: OmopTableInfo) => a.row_count - b.row_count,
                    defaultSortOrder: 'descend' as const,
                  },
                  {
                    title: '이관', key: 'export',
                    render: (_: any, record: OmopTableInfo) => (
                      <Button
                        type="primary"
                        size="small"
                        icon={<SwapOutlined />}
                        onClick={() => {
                          setExportTarget(record);
                          exportForm.setFieldsValue({ limit: Math.min(record.row_count, 10000) });
                          setExportModalOpen(true);
                        }}
                      >
                        이관
                      </Button>
                    ),
                  },
                ]}
              />
            )}
          </Card>
        </Col>
        <Col span={10}>
          <Card
            size="small"
            title={<span><FileTextOutlined style={{ color: '#1890ff', marginRight: 8 }} />이관된 데이터셋</span>}
          >
            {datasetLoading.exported ? <Spin /> : exportedDatasets.length === 0 ? (
              <Empty description="이관된 데이터셋이 없습니다" />
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {exportedDatasets.map((ds) => (
                  <Card key={ds.filename} size="small" hoverable style={{ border: '1px solid #f0f0f0' }}>
                    <Row align="middle" justify="space-between">
                      <Col flex="auto">
                        <Space direction="vertical" size={2}>
                          <Space>
                            <TableOutlined style={{ color: '#006241' }} />
                            <Text strong style={{ fontSize: 13 }}>{ds.table_name}</Text>
                            <Tag>{ds.size_kb} KB</Tag>
                          </Space>
                          <Text type="secondary" style={{ fontSize: 11, marginLeft: 22 }}>{ds.filename} | {ds.modified}</Text>
                        </Space>
                      </Col>
                      <Col>
                        <Space size="small">
                          <Tooltip title="JupyterLab에서 열기">
                            <Button
                              type="primary"
                              size="small"
                              icon={<LinkOutlined />}
                              style={{ background: '#006241', borderColor: '#006241' }}
                              onClick={() => {
                                const url = ds.jupyter_url.replace(/^\/jupyter/, JUPYTER_DIRECT_URL);
                                window.open(url, '_blank');
                              }}
                            />
                          </Tooltip>
                          <Tooltip title="삭제">
                            <Button size="small" danger icon={<DeleteOutlined />} onClick={() => handleDeleteDataset(ds.filename)} />
                          </Tooltip>
                        </Space>
                      </Col>
                    </Row>
                  </Card>
                ))}
              </div>
            )}
          </Card>
        </Col>
      </Row>

      {/* Export modal */}
      <Modal
        title={<Space><SwapOutlined />데이터셋 이관 - {exportTarget?.table_name}</Space>}
        open={exportModalOpen}
        onCancel={() => { setExportModalOpen(false); setExportTarget(null); exportForm.resetFields(); }}
        onOk={() => exportForm.submit()}
        okText="이관 시작"
        cancelText="취소"
        confirmLoading={exporting}
      >
        {exportTarget && (
          <div>
            <div style={{ background: '#fafafa', padding: 12, borderRadius: 6, marginBottom: 16 }}>
              <div><Text type="secondary">테이블:</Text> <Text code>{exportTarget.table_name}</Text></div>
              <div><Text type="secondary">설명:</Text> <Text>{exportTarget.description || '-'}</Text></div>
              <div><Text type="secondary">전체 행 수:</Text> <Text strong>{exportTarget.row_count.toLocaleString()}행</Text></div>
            </div>
            <Form form={exportForm} layout="vertical" onFinish={handleExportDataset} initialValues={{ limit: 10000 }}>
              <Form.Item name="limit" label="이관 행 수 (최대 500,000)" rules={[{ required: true, message: '행 수를 입력하세요' }]}>
                <InputNumber min={100} max={500000} step={1000} style={{ width: '100%' }} />
              </Form.Item>
            </Form>
            <div style={{ background: '#fffbe6', padding: 12, borderRadius: 6, border: '1px solid #ffe58f' }}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                데이터는 CSV 형식으로 JupyterLab 워크스페이스의 datasets/ 폴더에 저장됩니다.
                대용량 테이블의 경우 행 수를 제한하여 이관하는 것을 권장합니다.
              </Text>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default DatasetManager;
