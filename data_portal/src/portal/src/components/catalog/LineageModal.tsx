/**
 * 데이터 계보 모달 (검색 결과에서 직접 열기)
 */

import React from 'react';
import { Tag, Space, Typography, Descriptions, Modal, Divider } from 'antd';
import { BranchesOutlined } from '@ant-design/icons';
import type { TableInfo } from '../../services/api';
import LineageGraph from '../lineage/LineageGraph';
import RelatedTables from './RelatedTables';

const { Title, Text } = Typography;

interface LineageModalProps {
  table: TableInfo | null;
  visible: boolean;
  onClose: () => void;
}

const LineageModal: React.FC<LineageModalProps> = ({ table, visible, onClose }) => {
  return (
    <Modal
      title={
        <Space>
          <BranchesOutlined style={{ color: '#005BAC' }} />
          데이터 계보
          {table && <Tag color="blue">{table.business_name}</Tag>}
        </Space>
      }
      open={visible}
      onCancel={onClose}
      width="90%"
      style={{ maxWidth: 1600, top: 20 }}
      footer={null}
      destroyOnHidden
    >
      {table && visible && (
        <div>
          <Descriptions bordered column={2} size="small" style={{ marginBottom: 16 }}>
            <Descriptions.Item label="테이블명">
              <Text code>{table.physical_name}</Text>
            </Descriptions.Item>
            <Descriptions.Item label="도메인">
              <Tag color="blue">{table.domain}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="설명" span={2}>
              {table.description}
            </Descriptions.Item>
          </Descriptions>

          <Title level={5} style={{ marginBottom: 16 }}>
            <BranchesOutlined /> 데이터 흐름 (Source → Bronze → Silver → Gold)
          </Title>
          <LineageGraph
            key={`lineage-modal-${table.physical_name}`}
            tableName={table.business_name}
            physicalName={table.physical_name}
            height={400}
          />

          <Divider />
          <Title level={5}>연관 테이블</Title>
          <RelatedTables tableName={table.physical_name} />
        </div>
      )}
    </Modal>
  );
};

export default LineageModal;
