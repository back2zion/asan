import React from 'react';
import { Card, Typography, Alert, Spin } from 'antd';
import { BarChartOutlined } from '@ant-design/icons';

const { Title, Paragraph } = Typography;

const SUPERSET_URL = 'http://localhost:18088/superset/dashboard/list/';

const BI: React.FC = () => {
  const [loading, setLoading] = React.useState(true);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <Card style={{ marginBottom: 16 }}>
        <Title level={3} style={{ margin: 0 }}>
          <BarChartOutlined style={{ marginRight: 8 }} />
          셀프서비스 BI 대시보드
        </Title>
        <Paragraph type="secondary">
          DPR-004: Apache Superset 기반의 No-Code 시각화 및 BI 분석 환경입니다.
        </Paragraph>
        <Alert
          message="내장된 Apache Superset BI 플랫폼"
          description={
            <div>
              <p>
                이 화면에서 직접 데이터 소스를 연결하고, 차트를 생성하며, 인터랙티브 대시보드를 구축할 수 있습니다.
                분석하고자 하는 데이터마트 테이블을 선택하고, 드래그-앤-드롭 인터페이스를 통해 복잡한 분석을 시각적으로 수행하세요.
              </p>
              <p>
                <strong>참고:</strong> 초기 로딩에 시간이 걸릴 수 있습니다. Superset 관리자 계정은 (admin/admin) 입니다.
              </p>
            </div>
          }
          type="info"
          showIcon
        />
      </Card>
      <Card 
        style={{ flex: 1, display: 'flex', flexDirection: 'column' }}
        bodyStyle={{ flex: 1, padding: 0, overflow: 'hidden' }}
      >
        {loading && (
          <div style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            textAlign: 'center'
          }}>
            <Spin size="large" />
            <p style={{ marginTop: 16 }}>BI 대시보드를 불러오는 중...</p>
          </div>
        )}
        <iframe
          src={SUPERSET_URL}
          title="Apache Superset BI Dashboard"
          frameBorder="0"
          onLoad={() => setLoading(false)}
          style={{
            width: '100%',
            height: '100%',
            visibility: loading ? 'hidden' : 'visible'
          }}
        />
      </Card>
    </div>
  );
};

export default BI;
