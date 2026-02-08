import React, { useState, useCallback, useEffect } from 'react';
import {
  Card, Row, Col, Tag, Button, Space, Empty, Modal, Form, Input,
  Select, Divider, Badge, List, App, Typography,
} from 'antd';
import {
  MessageOutlined,
  PlusOutlined,
  EditOutlined,
  CheckCircleOutlined,
  TeamOutlined,
} from '@ant-design/icons';

const { Text } = Typography;
const { TextArea } = Input;

const API_BASE = '/api/v1/ai-environment';

interface AnalysisRequest {
  id: string;
  title: string;
  description: string;
  requester: string;
  priority: string;
  status: string;
  assignee: string;
  response: string;
  result_notebook: string;
  created: string;
  updated: string;
}

const PRIORITY_MAP: Record<string, { label: string; color: string }> = {
  low: { label: '낮음', color: 'default' },
  medium: { label: '보통', color: 'blue' },
  high: { label: '높음', color: 'orange' },
  urgent: { label: '긴급', color: 'red' },
};

const STATUS_MAP: Record<string, { label: string; color: string }> = {
  pending: { label: '대기', color: 'default' },
  in_progress: { label: '진행중', color: 'processing' },
  completed: { label: '완료', color: 'success' },
  rejected: { label: '반려', color: 'error' },
};

interface AnalysisRequestManagerProps {
  /** Notebook filenames for the result notebook selector */
  notebookOptions: { value: string; label: string }[];
}

const AnalysisRequestManager: React.FC<AnalysisRequestManagerProps> = ({ notebookOptions }) => {
  const { message } = App.useApp();
  const [requests, setRequests] = useState<AnalysisRequest[]>([]);
  const [requestModalOpen, setRequestModalOpen] = useState(false);
  const [requestForm] = Form.useForm();
  const [requestDetailOpen, setRequestDetailOpen] = useState(false);
  const [selectedRequest, setSelectedRequest] = useState<AnalysisRequest | null>(null);
  const [responseForm] = Form.useForm();

  const fetchRequests = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/shared/requests`);
      const data = await res.json();
      setRequests(data.requests || []);
    } catch {
      console.error('업무 요청 로드 실패');
    }
  }, []);

  useEffect(() => {
    fetchRequests();
  }, [fetchRequests]);

  const handleCreateRequest = async (values: any) => {
    try {
      const res = await fetch(`${API_BASE}/shared/requests`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(values),
      });
      if (!res.ok) throw new Error('요청 등록 실패');
      message.success('분석 요청이 등록되었습니다');
      setRequestModalOpen(false);
      requestForm.resetFields();
      fetchRequests();
    } catch {
      message.error('분석 요청 등록에 실패했습니다');
    }
  };

  const handleUpdateRequest = async (requestId: string, updates: Partial<AnalysisRequest>) => {
    try {
      const res = await fetch(`${API_BASE}/shared/requests/${requestId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates),
      });
      if (!res.ok) throw new Error('요청 업데이트 실패');
      message.success('요청이 업데이트되었습니다');
      fetchRequests();
      if (selectedRequest && selectedRequest.id === requestId) {
        const data = await res.json();
        setSelectedRequest(data.request);
      }
    } catch {
      message.error('요청 업데이트에 실패했습니다');
    }
  };

  const handleResponseSubmit = async (values: any) => {
    if (!selectedRequest) return;
    await handleUpdateRequest(selectedRequest.id, {
      status: 'completed',
      response: values.response,
      result_notebook: values.result_notebook || '',
    });
    setRequestDetailOpen(false);
    responseForm.resetFields();
  };

  return (
    <>
      <Card
        size="small"
        title={<span><MessageOutlined style={{ color: '#fa8c16', marginRight: 8 }} />분석 업무 요청/결과 전달</span>}
        extra={<Button type="primary" icon={<PlusOutlined />} size="small" onClick={() => setRequestModalOpen(true)}>새 요청</Button>}
      >
        {requests.length === 0 ? (
          <Empty description="등록된 분석 요청이 없습니다" />
        ) : (
          <List
            dataSource={requests}
            renderItem={(req) => {
              const priority = PRIORITY_MAP[req.priority] || PRIORITY_MAP.medium;
              const status = STATUS_MAP[req.status] || STATUS_MAP.pending;
              return (
                <List.Item
                  style={{ cursor: 'pointer', padding: '12px 16px', borderRadius: 8, marginBottom: 4, border: '1px solid #f0f0f0' }}
                  onClick={() => { setSelectedRequest(req); setRequestDetailOpen(true); responseForm.resetFields(); }}
                >
                  <List.Item.Meta
                    title={
                      <Space>
                        <Badge status={status.color as any} />
                        <strong>{req.title}</strong>
                        <Tag color={priority.color}>{priority.label}</Tag>
                        <Tag>{status.label}</Tag>
                        {req.assignee && <Tag icon={<TeamOutlined />} color="cyan">{req.assignee}</Tag>}
                      </Space>
                    }
                    description={
                      <Space style={{ fontSize: 12, color: '#999' }}>
                        <span>{req.id}</span>
                        <span>요청자: {req.requester}</span>
                        <span>등록: {req.created}</span>
                        {req.response && <Tag color="green" icon={<CheckCircleOutlined />}>결과 있음</Tag>}
                      </Space>
                    }
                  />
                </List.Item>
              );
            }}
          />
        )}
      </Card>

      {/* New request modal */}
      <Modal
        title={<Space><PlusOutlined />새 분석 요청</Space>}
        open={requestModalOpen}
        onCancel={() => { setRequestModalOpen(false); requestForm.resetFields(); }}
        onOk={() => requestForm.submit()}
        okText="등록"
        cancelText="취소"
      >
        <Form form={requestForm} layout="vertical" onFinish={handleCreateRequest} initialValues={{ priority: 'medium', requester: '데이터분석팀' }}>
          <Form.Item name="title" label="요청 제목" rules={[{ required: true, message: '제목을 입력하세요' }]}>
            <Input placeholder="예: 2025년 당뇨 환자 코호트 분석 요청" />
          </Form.Item>
          <Form.Item name="description" label="요청 내용" rules={[{ required: true, message: '내용을 입력하세요' }]}>
            <TextArea rows={4} placeholder="분석 목적, 대상 데이터, 기대 결과물 등을 상세히 기술하세요" />
          </Form.Item>
          <Row gutter={16}>
            <Col span={12}>
              <Form.Item name="requester" label="요청자">
                <Input placeholder="이름 또는 팀명" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item name="priority" label="우선순위">
                <Select options={[
                  { value: 'low', label: '낮음' },
                  { value: 'medium', label: '보통' },
                  { value: 'high', label: '높음' },
                  { value: 'urgent', label: '긴급' },
                ]} />
              </Form.Item>
            </Col>
          </Row>
        </Form>
      </Modal>

      {/* Request detail / response modal */}
      <Modal
        title={selectedRequest ? <Space><MessageOutlined />{selectedRequest.id}: {selectedRequest.title}</Space> : '요청 상세'}
        open={requestDetailOpen}
        onCancel={() => { setRequestDetailOpen(false); setSelectedRequest(null); responseForm.resetFields(); }}
        footer={null}
        width={700}
      >
        {selectedRequest && (
          <div>
            <Row gutter={[16, 12]}>
              <Col span={8}><Text type="secondary">요청자</Text><br /><Text strong>{selectedRequest.requester}</Text></Col>
              <Col span={8}><Text type="secondary">우선순위</Text><br /><Tag color={(PRIORITY_MAP[selectedRequest.priority] || PRIORITY_MAP.medium).color}>{(PRIORITY_MAP[selectedRequest.priority] || PRIORITY_MAP.medium).label}</Tag></Col>
              <Col span={8}><Text type="secondary">상태</Text><br /><Tag color={(STATUS_MAP[selectedRequest.status] || STATUS_MAP.pending).color}>{(STATUS_MAP[selectedRequest.status] || STATUS_MAP.pending).label}</Tag></Col>
              <Col span={8}><Text type="secondary">담당자</Text><br /><Text>{selectedRequest.assignee || '-'}</Text></Col>
              <Col span={8}><Text type="secondary">등록일</Text><br /><Text>{selectedRequest.created}</Text></Col>
              <Col span={8}><Text type="secondary">수정일</Text><br /><Text>{selectedRequest.updated}</Text></Col>
            </Row>
            <Divider />
            <div style={{ marginBottom: 16 }}>
              <Text type="secondary">요청 내용</Text>
              <div style={{ background: '#fafafa', padding: 12, borderRadius: 6, marginTop: 4 }}>{selectedRequest.description}</div>
            </div>

            {selectedRequest.response && (
              <div style={{ marginBottom: 16 }}>
                <Text type="secondary">분석 결과</Text>
                <div style={{ background: '#f0f8f3', padding: 12, borderRadius: 6, marginTop: 4, border: '1px solid #b7eb8f' }}>{selectedRequest.response}</div>
              </div>
            )}

            <Divider />
            <Space style={{ marginBottom: 16 }}>
              <Text strong>상태 변경:</Text>
              {selectedRequest.status === 'pending' && (
                <Button size="small" type="primary" onClick={() => handleUpdateRequest(selectedRequest.id, { status: 'in_progress', assignee: '데이터분석팀' })}>
                  접수 (진행중)
                </Button>
              )}
              {selectedRequest.status === 'in_progress' && (
                <Button size="small" danger onClick={() => handleUpdateRequest(selectedRequest.id, { status: 'rejected' })}>반려</Button>
              )}
              {selectedRequest.status !== 'completed' && selectedRequest.status !== 'rejected' && (
                <Tag color="blue">아래에서 결과를 전달하면 자동으로 완료 처리됩니다</Tag>
              )}
            </Space>

            {selectedRequest.status !== 'completed' && selectedRequest.status !== 'rejected' && (
              <div style={{ background: '#fafafa', padding: 16, borderRadius: 8, border: '1px solid #f0f0f0' }}>
                <Text strong style={{ display: 'block', marginBottom: 8 }}>
                  <EditOutlined /> 결과 전달
                </Text>
                <Form form={responseForm} layout="vertical" onFinish={handleResponseSubmit}>
                  <Form.Item name="response" label="분석 결과 요약" rules={[{ required: true, message: '결과를 입력하세요' }]}>
                    <TextArea rows={3} placeholder="분석 결과를 요약하세요" />
                  </Form.Item>
                  <Form.Item name="result_notebook" label="결과 노트북 (선택)">
                    <Select allowClear placeholder="결과 노트북 선택" options={notebookOptions} />
                  </Form.Item>
                  <Button type="primary" htmlType="submit" icon={<CheckCircleOutlined />} style={{ background: '#006241', borderColor: '#006241' }}>결과 전달 및 완료</Button>
                </Form>
              </div>
            )}
          </div>
        )}
      </Modal>
    </>
  );
};

export default AnalysisRequestManager;
