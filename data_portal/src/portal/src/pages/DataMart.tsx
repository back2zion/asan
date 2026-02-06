import React, { useState, useMemo } from 'react';
import { 
  Card, 
  Table, 
  Input, 
  Tabs, 
  Typography, 
  Tag, 
  Space, 
  Row, 
  Col, 
  Descriptions,
  Alert,
  Empty,
  Button
} from 'antd';
import { 
  DatabaseOutlined, 
  TableOutlined, 
  FileTextOutlined,
  CodeOutlined,
  ApartmentOutlined,
  UserOutlined,
  TagOutlined
} from '@ant-design/icons';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';

const { Title, Paragraph, Text } = Typography;
const { Search } = Input;
const { TabPane } = Tabs;

// --- Mock Data (as per DIT-002, DPR-002) ---

const mockDataMarts = [
  {
    id: 'dm_001',
    name: '환자 기본정보 마트 (Patient_Master)',
    description: '모든 환자의 인구통계학적 정보, 내원/퇴원 이력, 기본 진단 정보를 포함하는 마스터 데이터마트입니다.',
    owner: '데이터플랫폼팀',
    tags: ['환자', '기본정보', 'CDW', 'Gold'],
    source: 'Silver.Patient_Registered, Silver.Admission_History',
    update_cycle: '매일 (Daily)',
    schema: [
      { key: '1', name: 'patient_id', type: 'VARCHAR(20)', description: '환자 등록번호 (Primary Key)' },
      { key: '2', name: 'birth_date', type: 'DATE', description: '생년월일' },
      { key: '3', name: 'gender', type: 'CHAR(1)', description: '성별 (M/F)' },
      { key: '4', name: 'address', type: 'VARCHAR(200)', description: '주소' },
      { key: '5', name: 'first_visit_date', type: 'DATETIME', description: '최초 내원일' },
      { key: '6', name: 'last_discharge_date', type: 'DATETIME', description: '최종 퇴원일' },
    ],
    sampleData: [
      { patient_id: 'P0012345', birth_date: '1985-03-15', gender: 'M', address: '서울시 강남구', first_visit_date: '2020-01-10', last_discharge_date: '2023-05-22' },
      { patient_id: 'P0067890', birth_date: '1992-11-20', gender: 'F', address: '경기도 성남시', first_visit_date: '2021-08-01', last_discharge_date: '2021-08-15' },
    ],
    lineage: {
      sources: [
        { type: 'Table', name: 'AMIS_OCS.환자정보' },
        { type: 'Table', name: 'AMIS_EMR.진단기록' },
      ],
      transformations: [
        { name: '개인정보 비식별화', tool: 'Airflow' },
        { name: '주소 정제 및 표준화', tool: 'Airflow' },
      ],
      outputs: [
        { type: 'DataMart', name: 'Patient_Master' }
      ]
    },
  },
  {
    id: 'dm_002',
    name: '처방 및 투약정보 마트 (Prescription_Medication)',
    description: '환자별 처방된 약물, 투약 용량, 투약 경로 및 관련 처방 정보를 제공합니다.',
    owner: '임상연구팀',
    tags: ['처방', '투약', 'EDW', 'Gold'],
    source: 'Silver.Prescription_Detail',
    update_cycle: '8시간',
    schema: [
      { key: '1', name: 'prescription_id', type: 'VARCHAR(30)', description: '처방 ID' },
      { key: '2', name: 'patient_id', type: 'VARCHAR(20)', description: '환자 등록번호' },
      { key: '3', name: 'medication_code', type: 'VARCHAR(15)', description: '약물 코드' },
      { key: '4', name: 'medication_name', type: 'VARCHAR(100)', description: '약물명' },
      { key: '5', name: 'dosage', type: 'NUMERIC(10,2)', description: '1회 투여량' },
      { key: '6', name: 'unit', type: 'VARCHAR(10)', description: '단위 (e.g., mg, mL)' },
      { key: '7', name: 'prescribed_at', type: 'DATETIME', description: '처방 시간' },
    ],
    sampleData: [
      { prescription_id: 'PRSC-2023-001', patient_id: 'P0012345', medication_code: 'D00123', medication_name: '타이레놀', dosage: 500, unit: 'mg', prescribed_at: '2023-05-20 10:00' },
      { prescription_id: 'PRSC-2023-002', patient_id: 'P0067890', medication_code: 'A05678', medication_name: '아스피린', dosage: 100, unit: 'mg', prescribed_at: '2021-08-02 09:30' },
    ],
    lineage: {
      sources: [{ type: 'Table', name: 'AMIS_OCS.처방정보' }],
      transformations: [{ name: '약물코드 표준화', tool: 'Airflow' }],
      outputs: [{ type: 'DataMart', name: 'Prescription_Medication' }]
    },
  },
];

const pythonCodeSnippet = (tableName: string) => `
# Python (Pandas)
# !pip install duckdb
import duckdb

# Connect to the Lakehouse
con = duckdb.connect(database=':memory:', read_only=False)

# Example of loading data from the data mart
query = "SELECT * FROM ${tableName} LIMIT 10;"
df = con.execute(query).fetchdf()

print(df)
`;

const rCodeSnippet = (tableName: string) => `
# R (dplyr)
# install.packages("DBI")
# install.packages("duckdb")
library(DBI)
library(duckdb)

# Connect to the Lakehouse
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

# Example of loading data from the data mart
query <- "SELECT * FROM ${tableName} LIMIT 10;"
df <- dbGetQuery(con, query)

print(df)
`;

const DataMart: React.FC = () => {
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedMart, setSelectedMart] = useState<any>(mockDataMarts[0]);

  const filteredDataMarts = useMemo(() => 
    mockDataMarts.filter(mart => 
      mart.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      mart.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
      mart.tags.some(tag => tag.toLowerCase().includes(searchTerm.toLowerCase()))
    ), 
    [searchTerm]
  );

  const martListColumns = [
    {
      title: '데이터마트 이름',
      dataIndex: 'name',
      key: 'name',
      render: (text: string, record: any) => (
        <Button type="link" onClick={() => setSelectedMart(record)} style={{padding: 0}}>
          <Space>
            <DatabaseOutlined />
            <Text strong>{text}</Text>
          </Space>
        </Button>
      ),
    },
    {
      title: '설명',
      dataIndex: 'description',
      key: 'description',
    },
    {
      title: '태그',
      dataIndex: 'tags',
      key: 'tags',
      render: (tags: string[]) => (
        <>
          {tags.map(tag => {
            let color = tag.length > 5 ? 'geekblue' : 'green';
            if (tag === 'CDW') color = 'volcano';
            if (tag === 'EDW') color = 'purple';
            return (
              <Tag color={color} key={tag}>
                {tag.toUpperCase()}
              </Tag>
            );
          })}
        </>
      ),
    },
  ];

  const schemaColumns = [
    { title: '컬럼명', dataIndex: 'name', key: 'name', render: (text:string) => <Text code>{text}</Text> },
    { title: '타입', dataIndex: 'type', key: 'type', render: (text:string) => <Tag color="blue">{text}</Tag> },
    { title: '설명', dataIndex: 'description', key: 'description' },
  ];

  const renderDetailView = () => {
    if (!selectedMart) {
      return (
        <Card style={{ marginTop: 16 }}>
          <Empty description="왼쪽 목록에서 데이터마트를 선택해주세요." />
        </Card>
      );
    }

    const sampleDataColumns = selectedMart.schema.map((col: any) => ({
      title: col.name,
      dataIndex: col.name,
      key: col.name,
    }));

    return (
      <Card
        title={<><DatabaseOutlined /> {selectedMart.name}</>}
        style={{ marginTop: 16 }}
      >
        <Descriptions bordered column={2} size="small">
          <Descriptions.Item label={<><FileTextOutlined /> 설명</>}>{selectedMart.description}</Descriptions.Item>
          <Descriptions.Item label={<><UserOutlined /> 소유자</>}>{selectedMart.owner}</Descriptions.Item>
          <Descriptions.Item label="업데이트 주기">{selectedMart.update_cycle}</Descriptions.Item>
          <Descriptions.Item label="원본 테이블">{selectedMart.source}</Descriptions.Item>
          <Descriptions.Item label={<><TagOutlined/> 태그</>} span={2}>
            {selectedMart.tags.map((tag: string) => <Tag key={tag}>{tag}</Tag>)}
          </Descriptions.Item>
        </Descriptions>
        
        <Tabs defaultActiveKey="1" style={{ marginTop: 20 }}>
          <TabPane tab={<><TableOutlined /> 스키마 정보</>} key="1">
            <Table columns={schemaColumns} dataSource={selectedMart.schema} pagination={false} size="small" />
          </TabPane>
          <TabPane tab="샘플 데이터" key="2">
            <Table columns={sampleDataColumns} dataSource={selectedMart.sampleData} pagination={false} size="small" scroll={{ x: 'max-content' }} />
          </TabPane>
          <TabPane tab={<><ApartmentOutlined /> 데이터 계보 (Lineage)</>} key="3">
            <Paragraph>이 데이터마트가 어떻게 생성되었는지 데이터의 흐름을 보여줍니다.</Paragraph>
            <Row gutter={16}>
              <Col span={8}>
                <Card title="Source" size="small">
                  {selectedMart.lineage.sources.map((s:any) => <Tag color="green">{s.name}</Tag>)}
                </Card>
              </Col>
              <Col span={8}>
                <Card title="Transformation" size="small">
                {selectedMart.lineage.transformations.map((t:any) => <Tag color="cyan">{t.name} ({t.tool})</Tag>)}
                </Card>
              </Col>
              <Col span={8}>
                <Card title="Output" size="small">
                {selectedMart.lineage.outputs.map((o:any) => <Tag color="purple">{o.name}</Tag>)}
                </Card>
              </Col>
            </Row>
          </TabPane>
          <TabPane tab={<><CodeOutlined /> 사용 예제 코드</>} key="4">
            <Tabs defaultActiveKey="python">
                <TabPane tab="Python" key="python">
                    <SyntaxHighlighter language="python" style={oneDark} customStyle={{ borderRadius: '6px' }}>
                        {pythonCodeSnippet(selectedMart.name)}
                    </SyntaxHighlighter>
                </TabPane>
                <TabPane tab="R" key="r">
                    <SyntaxHighlighter language="r" style={oneDark} customStyle={{ borderRadius: '6px' }}>
                        {rCodeSnippet(selectedMart.name)}
                    </SyntaxHighlighter>
                </TabPane>
            </Tabs>
          </TabPane>
        </Tabs>
      </Card>
    );
  };

  return (
    <Space direction="vertical" size="large" style={{ width: '100%' }}>
      <Card>
        <Title level={3}>데이터마트 카탈로그</Title>
        <Paragraph type="secondary">
          CDW/EDW 통합 모델 기반으로 구축된 분석용 데이터마트 목록입니다.
          원하는 데이터를 검색하고, 스키마와 샘플 데이터를 확인하여 연구 및 분석에 활용할 수 있습니다.
        </Paragraph>
        <Alert 
            message="Self-Service 데이터 분석 환경"
            description="데이터 전문가가 사전에 정제하고 구조화한 Gold-Level 데이터를 탐색하여, 즉시 신뢰할 수 있는 분석을 시작할 수 있습니다. 수일이 걸리던 SR 요청 없이 필요한 데이터를 직접 찾고 활용하세요."
            type="info"
            showIcon
        />
      </Card>

      <Row gutter={16}>
        <Col xs={24} lg={10}>
          <Card title="데이터마트 목록">
            <Search
              placeholder="이름, 설명, 태그로 검색..."
              onSearch={value => setSearchTerm(value)}
              onChange={e => setSearchTerm(e.target.value)}
              style={{ marginBottom: 16 }}
              allowClear
            />
            <Table
              columns={martListColumns}
              dataSource={filteredDataMarts}
              rowKey="id"
              size="middle"
              pagination={{ pageSize: 5 }}
              onRow={(record) => ({
                onClick: () => {
                  setSelectedMart(record);
                },
                style: { cursor: 'pointer' }
              })}
            />
          </Card>
        </Col>
        <Col xs={24} lg={14}>
          {renderDetailView()}
        </Col>
      </Row>
    </Space>
  );
};

export default DataMart;