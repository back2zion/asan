import React from 'react';
import { Modal, Table, Typography, Button, Space, App } from 'antd';
import { CopyOutlined, DownloadOutlined } from '@ant-design/icons';

const { Text } = Typography;

interface RawDataModalProps {
  open: boolean;
  onClose: () => void;
  chartName: string;
  sqlQuery: string;
  columns: string[];
  rows: any[][];
}

const RawDataModal: React.FC<RawDataModalProps> = ({ open, onClose, chartName, sqlQuery, columns, rows }) => {
  const { message } = App.useApp();
  const tableColumns = columns.map((col, idx) => ({
    title: col,
    dataIndex: idx.toString(),
    key: col,
    ellipsis: true,
    width: 150,
  }));

  const tableData = rows.map((row, i) => {
    const obj: any = { key: i };
    row.forEach((val, j) => { obj[j.toString()] = val; });
    return obj;
  });

  const handleCopySQL = () => {
    navigator.clipboard.writeText(sqlQuery);
    message.success('SQL이 클립보드에 복사되었습니다');
  };

  const handleDownloadCSV = () => {
    const bom = '\uFEFF';
    const header = columns.join(',');
    const body = rows.map(r => r.map(v => `"${String(v ?? '').replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob([bom + header + '\n' + body], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${chartName}_data.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <Modal
      title={`원본 데이터: ${chartName}`}
      open={open}
      onCancel={onClose}
      width={900}
      footer={null}
    >
      <div style={{ marginBottom: 12, padding: '8px 12px', background: '#f5f5f5', borderRadius: 6 }}>
        <Text code style={{ fontSize: 12, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>{sqlQuery}</Text>
      </div>
      <Space style={{ marginBottom: 12 }}>
        <Button size="small" icon={<CopyOutlined />} onClick={handleCopySQL}>SQL 복사</Button>
        <Button size="small" icon={<DownloadOutlined />} onClick={handleDownloadCSV}>CSV 다운로드</Button>
        <Text type="secondary">{rows.length}건</Text>
      </Space>
      <Table
        columns={tableColumns}
        dataSource={tableData}
        size="small"
        scroll={{ x: columns.length * 150, y: 400 }}
        pagination={{ pageSize: 50, showSizeChanger: false }}
      />
    </Modal>
  );
};

export default RawDataModal;
