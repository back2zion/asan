/**
 * SnapshotModal - 검색 조건 스냅샷 저장 모달
 */

import React from 'react';
import { Modal, Input, Card, Space, Typography } from 'antd';
import {
  SaveOutlined, SearchOutlined, DatabaseOutlined, TagOutlined,
} from '@ant-design/icons';

const { Text } = Typography;

interface SnapshotModalProps {
  visible: boolean;
  snapshotName: string;
  searchQuery: string;
  selectedDomains: string[];
  selectedTags: string[];
  selectedSensitivity: string[];
  onSnapshotNameChange: (name: string) => void;
  onSave: () => void;
  onCancel: () => void;
}

const SnapshotModal: React.FC<SnapshotModalProps> = ({
  visible,
  snapshotName,
  searchQuery,
  selectedDomains,
  selectedTags,
  selectedSensitivity,
  onSnapshotNameChange,
  onSave,
  onCancel,
}) => {
  const hasConditions =
    !!searchQuery || selectedDomains.length > 0 || selectedTags.length > 0 || selectedSensitivity.length > 0;

  return (
    <Modal
      title={<><SaveOutlined /> 검색 조건 스냅샷 저장</>}
      open={visible}
      onCancel={onCancel}
      onOk={onSave}
      okText="저장"
      cancelText="취소"
      okButtonProps={{ disabled: !snapshotName.trim(), style: { background: '#006241' } }}
      width={400}
    >
      <Space direction="vertical" style={{ width: '100%', marginTop: 8 }}>
        <Input
          placeholder="스냅샷 이름 (예: 환자 테이블 분석)"
          value={snapshotName}
          onChange={(e) => onSnapshotNameChange(e.target.value)}
          onPressEnter={onSave}
          autoFocus
        />
        <Card size="small" style={{ background: '#f5f5f5' }}>
          <Space direction="vertical" size={2} style={{ fontSize: 12 }}>
            {searchQuery && <Text><SearchOutlined /> 검색어: <Text strong>{searchQuery}</Text></Text>}
            {selectedDomains.length > 0 && <Text><DatabaseOutlined /> 도메인: {selectedDomains.join(', ')}</Text>}
            {selectedTags.length > 0 && <Text><TagOutlined /> 태그: {selectedTags.join(', ')}</Text>}
            {selectedSensitivity.length > 0 && <Text>민감도: {selectedSensitivity.join(', ')}</Text>}
            {!hasConditions && (
              <Text type="secondary">저장할 검색 조건이 없습니다.</Text>
            )}
          </Space>
        </Card>
      </Space>
    </Modal>
  );
};

export default SnapshotModal;
