/**
 * ETL 파이프라인 페이지 (SFR-003 + DIR-002 전체 구현)
 * 16개 탭: Job/Group 관리, 종속관계, 실행 로그, 알림, 파이프라인 대시보드,
 *          이기종 소스 관리, 스키마 버전 관리, 수집 템플릿, 병렬 적재 설정,
 *          매핑 자동생성, 이관 검증, 스키마 변경 관리, CDC 관리, 데이터 설계,
 *          데이터 Pipeline, 마트 운영
 */
import React from 'react';
import { Card, Typography, Row, Col, Tabs } from 'antd';
import {
  SettingOutlined, AppstoreOutlined, ApartmentOutlined,
  FileTextOutlined, BellOutlined, CloudServerOutlined,
  BranchesOutlined, BookOutlined, ThunderboltOutlined,
  SwapOutlined, AuditOutlined, FileSearchOutlined,
  DatabaseOutlined, RocketOutlined, FundOutlined,
} from '@ant-design/icons';
import JobGroupManagement from '../components/etl/JobGroupManagement';
import TableDependencyGraph from '../components/etl/TableDependencyGraph';
import ExecutionLogs from '../components/etl/ExecutionLogs';
import AlertManagement from '../components/etl/AlertManagement';
import CDCManagement from '../components/etl/CDCManagement';
import DataDesignDashboard from '../components/etl/DataDesignDashboard';
import DataMartOps from '../components/etl/DataMartOps';
import PipelineDashboardTab from '../components/etl/PipelineDashboardTab';
import HeterogeneousSourcesTab from '../components/etl/HeterogeneousSourcesTab';
import SchemaVersioningTab from '../components/etl/SchemaVersioningTab';
import IngestionTemplatesTab from '../components/etl/IngestionTemplatesTab';
import ParallelLoadingTab from '../components/etl/ParallelLoadingTab';
import MappingGeneratorTab from '../components/etl/MappingGeneratorTab';
import MigrationVerificationTab from '../components/etl/MigrationVerificationTab';
import SchemaChangeManagementTab from '../components/etl/SchemaChangeManagementTab';
import DataPipelineTab from '../components/etl/DataPipelineTab';

const { Title, Paragraph } = Typography;

const tabItems = [
  { key: 'job-groups', label: <span><AppstoreOutlined /> Job/Group 관리</span>, children: <JobGroupManagement /> },
  { key: 'dependencies', label: <span><ApartmentOutlined /> 테이블 종속관계</span>, children: <TableDependencyGraph /> },
  { key: 'exec-logs', label: <span><FileTextOutlined /> 실행 로그</span>, children: <ExecutionLogs /> },
  { key: 'alerts', label: <span><BellOutlined /> 알림</span>, children: <AlertManagement /> },
  { key: 'pipeline', label: <span><SettingOutlined /> 파이프라인 대시보드</span>, children: <PipelineDashboardTab /> },
  { key: 'sources', label: <span><CloudServerOutlined /> 이기종 소스 관리</span>, children: <HeterogeneousSourcesTab /> },
  { key: 'schema', label: <span><BranchesOutlined /> 스키마 버전 관리</span>, children: <SchemaVersioningTab /> },
  { key: 'templates', label: <span><BookOutlined /> 수집 템플릿</span>, children: <IngestionTemplatesTab /> },
  { key: 'parallel', label: <span><ThunderboltOutlined /> 병렬 적재 설정</span>, children: <ParallelLoadingTab /> },
  { key: 'mapping', label: <span><SwapOutlined /> 매핑 자동생성</span>, children: <MappingGeneratorTab /> },
  { key: 'migration', label: <span><AuditOutlined /> 이관 검증</span>, children: <MigrationVerificationTab /> },
  { key: 'schema-monitor', label: <span><FileSearchOutlined /> 스키마 변경 관리</span>, children: <SchemaChangeManagementTab /> },
  { key: 'cdc', label: <span><ThunderboltOutlined /> CDC 관리</span>, children: <CDCManagement /> },
  { key: 'data-design', label: <span><DatabaseOutlined /> 데이터 설계</span>, children: <DataDesignDashboard /> },
  { key: 'data-pipeline', label: <span><RocketOutlined /> 데이터 Pipeline</span>, children: <DataPipelineTab /> },
  { key: 'mart-ops', label: <span><FundOutlined /> 마트 운영</span>, children: <DataMartOps /> },
];

const ETL: React.FC = () => {
  return (
    <div>
      <Card>
        <Row align="middle" justify="space-between">
          <Col>
            <Title level={3} style={{ margin: 0, color: '#333', fontWeight: '600' }}>
              <SettingOutlined style={{ color: '#006241', marginRight: '12px', fontSize: '28px' }} />
              ETL 파이프라인
            </Title>
            <Paragraph type="secondary" style={{ margin: '8px 0 0 40px', fontSize: '15px', color: '#6c757d' }}>
              Job/Group 관리 · 종속관계 · 실행 로그 · 알림 · 파이프라인 · 이기종 소스 · 병렬 적재 · 매핑 · 이관 검증 · 스키마 변경 관리 · CDC · 데이터 설계 · Pipeline · 마트 운영
            </Paragraph>
          </Col>
        </Row>
      </Card>

      <Card style={{ marginTop: 16 }}>
        <Tabs items={tabItems} defaultActiveKey="job-groups" destroyOnHidden />
      </Card>
    </div>
  );
};

export default ETL;
