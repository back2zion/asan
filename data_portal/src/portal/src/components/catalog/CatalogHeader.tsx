/**
 * CatalogHeader - 데이터 카탈로그 페이지 헤더
 * 제목과 데이터 등록 버튼을 표시합니다.
 */

import React from 'react';
import { Card, Typography, Row, Col, Button } from 'antd';
import { DatabaseOutlined, PlusOutlined } from '@ant-design/icons';

const { Title, Paragraph } = Typography;

interface CatalogHeaderProps {
  onRegisterClick: () => void;
}

const CatalogHeader: React.FC<CatalogHeaderProps> = ({ onRegisterClick }) => {
  return (
    <Card style={{ marginBottom: 16 }}>
      <Row align="middle" justify="space-between">
        <Col>
          <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
            <DatabaseOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
            데이터 카탈로그
          </Title>
          <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
            OMOP CDM 기반 메타데이터 탐색 및 데이터 자산 관리
          </Paragraph>
        </Col>
        <Col>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            size="large"
            onClick={onRegisterClick}
            style={{ background: '#006241' }}
          >
            데이터 등록
          </Button>
        </Col>
      </Row>
    </Card>
  );
};

export default CatalogHeader;
