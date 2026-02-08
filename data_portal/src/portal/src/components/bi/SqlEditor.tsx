import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Card, Button, Space, Table, Spin, Tag, Tooltip, Typography, Drawer, List, Input, Popconfirm, Modal, Select, App } from 'antd';
import {
  PlayCircleOutlined, SaveOutlined, HistoryOutlined, TableOutlined,
  EyeOutlined, DeleteOutlined, BarChartOutlined, CodeOutlined,
} from '@ant-design/icons';
import CodeMirror from '@uiw/react-codemirror';
import { sql, PostgreSQL } from '@codemirror/lang-sql';
import { biApi } from '../../services/biApi';
import ResultChart from '../common/ResultChart';

const { Text } = Typography;

interface TableMeta {
  table_name: string;
  row_count: number;
}

const SqlEditor: React.FC = () => {
  const { message } = App.useApp();
  const [sqlText, setSqlText] = useState('SELECT gender_source_value AS 성별, COUNT(*) AS 환자수\nFROM person\nGROUP BY gender_source_value');
  const [executing, setExecuting] = useState(false);
  const [columns, setColumns] = useState<string[]>([]);
  const [rows, setRows] = useState<any[][]>([]);
  const [rowCount, setRowCount] = useState(0);
  const [execTime, setExecTime] = useState(0);
  const [errorMsg, setErrorMsg] = useState('');
  const [showChart, setShowChart] = useState(false);

  // Schema sidebar
  const [tables, setTables] = useState<TableMeta[]>([]);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [tableColumns, setTableColumns] = useState<any[]>([]);
  const [loadingCols, setLoadingCols] = useState(false);

  // History & saved queries
  const [historyOpen, setHistoryOpen] = useState(false);
  const [history, setHistory] = useState<any[]>([]);
  const [savedQueries, setSavedQueries] = useState<any[]>([]);
  const [saveModalOpen, setSaveModalOpen] = useState(false);
  const [saveName, setSaveName] = useState('');
  const [saveDesc, setSaveDesc] = useState('');

  // Load tables
  useEffect(() => {
    biApi.getTables().then(setTables).catch(() => {});
    biApi.getSavedQueries().then(setSavedQueries).catch(() => {});
  }, []);

  const loadColumns = useCallback(async (table: string) => {
    setSelectedTable(table);
    setLoadingCols(true);
    try {
      const cols = await biApi.getColumns(table);
      setTableColumns(cols);
    } catch { setTableColumns([]); }
    setLoadingCols(false);
  }, []);

  const doExecute = useCallback(async () => {
    setExecuting(true);
    setErrorMsg('');
    setColumns([]);
    setRows([]);
    const hide = message.loading('쿼리 실행 중입니다. 대량 데이터 조회 시 10초 이상 소요될 수 있습니다...', 0);
    try {
      const result = await biApi.executeQuery(sqlText);
      setColumns(result.columns || []);
      setRows(result.rows || []);
      setRowCount(result.row_count || 0);
      setExecTime(result.execution_time_ms || 0);
    } catch (err: any) {
      const detail = err?.response?.data?.detail || err.message || '실행 오류';
      setErrorMsg(detail);
      message.error(detail);
    }
    hide();
    setExecuting(false);
  }, [sqlText]);

  const executeQuery = useCallback(() => {
    if (!sqlText.trim()) return;
    // 대량 테이블 풀스캔 가능성 체크
    const upper = sqlText.toUpperCase();
    const hasNoWhere = !upper.includes('WHERE') && !upper.includes('LIMIT') && !upper.includes('GROUP BY');
    if (hasNoWhere) {
      Modal.confirm({
        title: '대량 데이터 조회 경고',
        content: 'WHERE, LIMIT, GROUP BY 절이 없는 쿼리입니다. 대량 데이터 조회 시 10초 이상 소요될 수 있으며, 최대 1,000행까지 반환됩니다. 실행하시겠습니까?',
        okText: '실행',
        cancelText: '취소',
        onOk: doExecute,
      });
    } else {
      doExecute();
    }
  }, [sqlText, doExecute]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      executeQuery();
    }
  }, [executeQuery]);

  const handleSaveQuery = async () => {
    if (!saveName.trim()) { message.warning('쿼리 이름을 입력하세요'); return; }
    try {
      await biApi.saveQuery({ name: saveName, description: saveDesc, sql_text: sqlText });
      message.success('쿼리가 저장되었습니다');
      setSaveModalOpen(false);
      setSaveName('');
      setSaveDesc('');
      const updated = await biApi.getSavedQueries();
      setSavedQueries(updated);
    } catch { message.error('저장 실패'); }
  };

  const loadHistory = async () => {
    try {
      const h = await biApi.getQueryHistory(50);
      setHistory(h);
    } catch { /* ignore */ }
    setHistoryOpen(true);
  };

  const handleDeleteSaved = async (qid: number) => {
    try {
      await biApi.deleteSavedQuery(qid);
      setSavedQueries(prev => prev.filter(q => q.query_id !== qid));
      message.success('삭제되었습니다');
    } catch { message.error('삭제 실패'); }
  };

  // Result table columns
  const resultTableCols = columns.map((col, idx) => ({
    title: col,
    dataIndex: idx.toString(),
    key: col,
    ellipsis: true,
    sorter: (a: any, b: any) => {
      const av = a[idx.toString()], bv = b[idx.toString()];
      if (typeof av === 'number' && typeof bv === 'number') return av - bv;
      return String(av ?? '').localeCompare(String(bv ?? ''));
    },
  }));

  const resultTableData = rows.map((row, i) => {
    const obj: any = { key: i };
    row.forEach((val, j) => { obj[j.toString()] = val; });
    return obj;
  });

  return (
    <div style={{ display: 'flex', gap: 12, height: '100%' }}>
      {/* Schema sidebar */}
      <Card size="small" style={{ width: 220, flexShrink: 0, overflow: 'auto' }}
        title={<><TableOutlined /> 테이블</>}
        styles={{ body: { padding: '4px 8px', maxHeight: 'calc(100vh - 300px)', overflow: 'auto' } }}
      >
        {tables.map(t => (
          <div key={t.table_name}
            onClick={() => loadColumns(t.table_name)}
            style={{
              padding: '4px 6px', cursor: 'pointer', borderRadius: 4, fontSize: 12,
              background: selectedTable === t.table_name ? '#e6f7ff' : undefined,
            }}
          >
            <Text strong style={{ fontSize: 12 }}>{t.table_name}</Text>
            <Text type="secondary" style={{ fontSize: 10, marginLeft: 4 }}>
              {t.row_count > 1000000 ? `${(t.row_count / 1000000).toFixed(1)}M` : t.row_count > 1000 ? `${(t.row_count / 1000).toFixed(0)}K` : t.row_count}
            </Text>
            {selectedTable === t.table_name && (
              <div style={{ paddingLeft: 8, marginTop: 2 }}>
                {loadingCols ? <Spin size="small" /> : tableColumns.map(c => (
                  <div key={c.column_name} style={{ fontSize: 11, color: '#666', padding: '1px 0', cursor: 'pointer' }}
                    onClick={(e) => { e.stopPropagation(); setSqlText(prev => prev + c.column_name); }}
                  >
                    <Text style={{ fontSize: 11 }}>{c.column_name}</Text>
                    <Text type="secondary" style={{ fontSize: 10, marginLeft: 4 }}>{c.data_type}</Text>
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </Card>

      {/* Main editor area */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 8, minWidth: 0 }}>
        {/* Editor */}
        <Card size="small" styles={{ body: { padding: 0 } }}>
          <div onKeyDown={handleKeyDown}>
            <CodeMirror
              value={sqlText}
              height="160px"
              extensions={[sql({ dialect: PostgreSQL })]}
              onChange={setSqlText}
              basicSetup={{ lineNumbers: true, foldGutter: false }}
            />
          </div>
          <div style={{ padding: '6px 12px', borderTop: '1px solid #f0f0f0', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Space>
              <Button type="primary" icon={<PlayCircleOutlined />} onClick={executeQuery} loading={executing} size="small">
                실행 <Text type="secondary" style={{ fontSize: 11, marginLeft: 4 }}>(Ctrl+Enter)</Text>
              </Button>
              <Button icon={<SaveOutlined />} onClick={() => setSaveModalOpen(true)} size="small">저장</Button>
              <Button icon={<HistoryOutlined />} onClick={loadHistory} size="small">이력</Button>
            </Space>
            {/* Saved queries dropdown */}
            {savedQueries.length > 0 && (
              <Select
                placeholder="저장된 쿼리 불러오기"
                size="small"
                style={{ width: 200 }}
                onChange={(qid: number) => {
                  const q = savedQueries.find(s => s.query_id === qid);
                  if (q) setSqlText(q.sql_text);
                }}
                options={savedQueries.map(q => ({ label: q.name, value: q.query_id }))}
                allowClear
              />
            )}
          </div>
        </Card>

        {/* Error */}
        {errorMsg && (
          <Card size="small" style={{ background: '#fff2f0', border: '1px solid #ffccc7' }}>
            <Text type="danger" style={{ fontSize: 13 }}>{errorMsg}</Text>
          </Card>
        )}

        {/* Results */}
        {columns.length > 0 && (
          <Card size="small" style={{ flex: 1, overflow: 'hidden' }}
            title={
              <Space>
                <Text strong>{rowCount}건</Text>
                <Text type="secondary" style={{ fontSize: 12 }}>{execTime}ms</Text>
                <Button size="small" type={showChart ? 'primary' : 'default'} icon={<BarChartOutlined />}
                  onClick={() => setShowChart(!showChart)}
                >
                  {showChart ? '테이블' : '차트'}
                </Button>
              </Space>
            }
            styles={{ body: { padding: 0, overflow: 'auto', maxHeight: 'calc(100vh - 480px)' } }}
          >
            {showChart ? (
              <div style={{ padding: 16 }}>
                <ResultChart columns={columns} results={rows} />
              </div>
            ) : (
              <Table
                columns={resultTableCols}
                dataSource={resultTableData}
                size="small"
                scroll={{ x: columns.length * 140 }}
                pagination={{ pageSize: 50, showSizeChanger: true, pageSizeOptions: ['20', '50', '100'] }}
              />
            )}
          </Card>
        )}
      </div>

      {/* History Drawer */}
      <Drawer title="쿼리 실행 이력" open={historyOpen} onClose={() => setHistoryOpen(false)} width={500}>
        <List
          size="small"
          dataSource={history}
          renderItem={(item: any) => (
            <List.Item
              actions={[
                <Button size="small" type="link" onClick={() => { setSqlText(item.sql_text); setHistoryOpen(false); }}>불러오기</Button>
              ]}
            >
              <List.Item.Meta
                title={<Text code style={{ fontSize: 11 }}>{item.sql_text?.substring(0, 80)}...</Text>}
                description={
                  <Space>
                    <Tag color={item.status === 'success' ? 'green' : 'red'}>{item.status}</Tag>
                    <Text type="secondary" style={{ fontSize: 11 }}>{item.row_count}건 / {item.execution_time_ms}ms</Text>
                  </Space>
                }
              />
            </List.Item>
          )}
        />
      </Drawer>

      {/* Save Modal */}
      <Modal title="쿼리 저장" open={saveModalOpen} onOk={handleSaveQuery} onCancel={() => setSaveModalOpen(false)} okText="저장">
        <Space direction="vertical" style={{ width: '100%' }}>
          <Input placeholder="쿼리 이름" value={saveName} onChange={e => setSaveName(e.target.value)} />
          <Input.TextArea placeholder="설명 (선택)" value={saveDesc} onChange={e => setSaveDesc(e.target.value)} rows={2} />
          <div style={{ padding: 8, background: '#f5f5f5', borderRadius: 4 }}>
            <Text code style={{ fontSize: 11 }}>{sqlText.substring(0, 200)}{sqlText.length > 200 ? '...' : ''}</Text>
          </div>
        </Space>
      </Modal>
    </div>
  );
};

export default SqlEditor;
