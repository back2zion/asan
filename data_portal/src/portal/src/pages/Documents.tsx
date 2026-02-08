import React, { useState } from 'react';
import {
  Card,
  Upload,
  Button,
  Table,
  Tag,
  Space,
  Typography,
  Progress,
  App,
} from 'antd';
import {
  InboxOutlined,
  UploadOutlined,
  FileTextOutlined,
  EyeOutlined,
  DeleteOutlined
} from '@ant-design/icons';
import type { UploadFile, UploadProps } from 'antd';

const { Title } = Typography;
const { Dragger } = Upload;

interface DocumentItem {
  key: string;
  name: string;
  type: string;
  size: string;
  status: 'processing' | 'completed' | 'failed';
  progress: number;
  uploadDate: string;
}

const Documents: React.FC = () => {
  const { message, modal } = App.useApp();
  const [fileList, setFileList] = useState<UploadFile[]>([]);
  const [documents, setDocuments] = useState<DocumentItem[]>([
    {
      key: '1',
      name: '환자_김철수_진료기록.pdf',
      type: 'PDF',
      size: '2.3 MB',
      status: 'completed',
      progress: 100,
      uploadDate: '2024-11-02 09:30'
    },
    {
      key: '2',
      name: '의료영상_분석보고서.hwp',
      type: 'HWP',
      size: '1.8 MB',
      status: 'processing',
      progress: 65,
      uploadDate: '2024-11-02 09:15'
    },
    {
      key: '3',
      name: '임상연구_데이터.xlsx',
      type: 'EXCEL',
      size: '5.1 MB',
      status: 'completed',
      progress: 100,
      uploadDate: '2024-11-02 08:45'
    }
  ]);

  const uploadProps: UploadProps = {
    name: 'file',
    multiple: true,
    fileList,
    onChange: ({ fileList: newFileList }) => {
      setFileList(newFileList);
    },
    onDrop(e) {
      /* drop event silenced */
    },
    customRequest: ({ file, onSuccess, onProgress }) => {
      // 모의 업로드 프로세스
      const fileName = (file as File).name;
      let progress = 0;
      
      const interval = setInterval(() => {
        progress += Math.random() * 30;
        onProgress?.({ percent: Math.round(progress) }, file);
        
        if (progress >= 100) {
          clearInterval(interval);
          onSuccess?.(file);
          
          // 문서 목록에 추가
          const newDoc: DocumentItem = {
            key: Date.now().toString(),
            name: fileName,
            type: fileName.split('.').pop()?.toUpperCase() || 'UNKNOWN',
            size: `${((file as File).size / 1024 / 1024).toFixed(1)} MB`,
            status: 'processing',
            progress: 0,
            uploadDate: new Date().toLocaleString()
          };
          
          setDocuments(prev => [...prev, newDoc]);
          message.success(`${fileName} 업로드가 완료되었습니다.`);
        }
      }, 500);
    },
  };

  const getStatusTag = (status: DocumentItem['status']) => {
    const statusMap = {
      processing: { color: 'processing', text: '처리중' },
      completed: { color: 'success', text: '완료' },
      failed: { color: 'error', text: '실패' }
    };
    return <Tag color={statusMap[status].color}>{statusMap[status].text}</Tag>;
  };

  const handleView = (record: DocumentItem) => {
    modal.info({
      title: '문서 미리보기',
      content: `${record.name} 문서의 상세 내용을 표시합니다.`,
      width: 600,
    });
  };

  const handleDelete = (record: DocumentItem) => {
    modal.confirm({
      title: '문서 삭제',
      content: `${record.name} 문서를 삭제하시겠습니까?`,
      onOk() {
        setDocuments(prev => prev.filter(doc => doc.key !== record.key));
        message.success('문서가 삭제되었습니다.');
      },
    });
  };

  const columns = [
    {
      title: '문서명',
      dataIndex: 'name',
      key: 'name',
      render: (text: string) => (
        <Space>
          <FileTextOutlined />
          {text}
        </Space>
      ),
    },
    {
      title: '형식',
      dataIndex: 'type',
      key: 'type',
      width: 80,
    },
    {
      title: '크기',
      dataIndex: 'size',
      key: 'size',
      width: 100,
    },
    {
      title: '상태',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: DocumentItem['status']) => getStatusTag(status),
    },
    {
      title: '진행률',
      dataIndex: 'progress',
      key: 'progress',
      width: 120,
      render: (progress: number, record: DocumentItem) => (
        record.status === 'processing' ? 
        <Progress percent={progress} size="small" /> : 
        <Progress percent={100} size="small" status="success" />
      ),
    },
    {
      title: '업로드 일시',
      dataIndex: 'uploadDate',
      key: 'uploadDate',
      width: 150,
    },
    {
      title: '작업',
      key: 'action',
      width: 120,
      render: (_: any, record: DocumentItem) => (
        <Space>
          <Button 
            type="text" 
            icon={<EyeOutlined />} 
            onClick={() => handleView(record)}
          />
          <Button 
            type="text" 
            danger 
            icon={<DeleteOutlined />} 
            onClick={() => handleDelete(record)}
          />
        </Space>
      ),
    },
  ];

  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>문서 관리</Title>
      
      {/* 파일 업로드 영역 */}
      <Card style={{ marginBottom: '24px' }}>
        <Title level={4}>파일 업로드</Title>
        <Dragger {...uploadProps} style={{ marginBottom: '16px' }}>
          <p className="ant-upload-drag-icon">
            <InboxOutlined />
          </p>
          <p className="ant-upload-text">
            파일을 여기로 드래그하거나 클릭하여 업로드하세요
          </p>
          <p className="ant-upload-hint">
            PDF, HWP, DOCX, XLSX 등 다양한 의료 문서를 지원합니다.
          </p>
        </Dragger>
        
        <Upload {...uploadProps} showUploadList={false}>
          <Button icon={<UploadOutlined />}>파일 선택</Button>
        </Upload>
      </Card>

      {/* 문서 목록 */}
      <Card>
        <Title level={4}>업로드된 문서</Title>
        <Table 
          columns={columns} 
          dataSource={documents}
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total) => `총 ${total}개 문서`,
          }}
        />
      </Card>
    </div>
  );
};

export default Documents;