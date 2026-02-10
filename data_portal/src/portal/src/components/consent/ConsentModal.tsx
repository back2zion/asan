/**
 * SER-010: 개인정보 수집·이용 동의 모달
 * 로그인 후 미동의 정책이 있으면 자동 표시
 */
import React, { useState, useEffect } from 'react';
import { Modal, Checkbox, Button, Typography, Space, Tag, Divider, Alert, Spin, App, Result } from 'antd';
import { SafetyCertificateOutlined, FileProtectOutlined } from '@ant-design/icons';
import { apiClient } from '../../services/apiUtils';

const { Paragraph, Text } = Typography;

interface ConsentPolicy {
  policy_id: number;
  policy_code: string;
  title: string;
  description: string;
  purpose: string;
  data_items: string;
  retention_days: number;
  is_required: boolean;
  version: string;
  user_agreed?: boolean;
}

interface ConsentModalProps {
  open: boolean;
  userId: string;
  onComplete: () => void;
}

const ConsentModal: React.FC<ConsentModalProps> = ({ open, userId, onComplete }) => {
  const { message: messageApi } = App.useApp();
  const [policies, setPolicies] = useState<ConsentPolicy[]>([]);
  const [checked, setChecked] = useState<Record<number, boolean>>({});
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [fetchError, setFetchError] = useState(false);

  useEffect(() => {
    if (!open || !userId) return;
    setLoading(true);
    setFetchError(false);

    const loadPolicies = async () => {
      try {
        // 1차: 사용자별 동의 현황 조회
        const { data } = await apiClient.get(`/consent/user/${userId}`);
        const list: ConsentPolicy[] = data.policies || [];
        setPolicies(list);
        const init: Record<number, boolean> = {};
        list.forEach(p => { init[p.policy_id] = !!p.user_agreed; });
        setChecked(init);
      } catch {
        try {
          // 2차: 정책 목록만 조회
          const { data: list } = await apiClient.get('/consent/policies');
          const arr = Array.isArray(list) ? list : [];
          setPolicies(arr);
          const init: Record<number, boolean> = {};
          arr.forEach((p: ConsentPolicy) => { init[p.policy_id] = false; });
          setChecked(init);
        } catch {
          setFetchError(true);
        }
      } finally {
        setLoading(false);
      }
    };

    loadPolicies();
  }, [open, userId]);

  const allRequiredChecked = policies
    .filter(p => p.is_required)
    .every(p => checked[p.policy_id]);

  const handleSubmit = async () => {
    setSubmitting(true);
    try {
      const policyIds = policies.filter(p => checked[p.policy_id]).map(p => p.policy_id);
      await apiClient.post('/consent/agree-batch', {
        user_id: userId,
        policy_ids: policyIds,
      });
      messageApi.success('동의가 완료되었습니다');
      onComplete();
    } catch {
      messageApi.error('동의 처리 중 오류가 발생했습니다');
    } finally {
      setSubmitting(false);
    }
  };

  const handleCheckAll = (check: boolean) => {
    const next = { ...checked };
    policies.forEach(p => { next[p.policy_id] = check; });
    setChecked(next);
  };

  return (
    <Modal
      open={open}
      closable={false}
      maskClosable={false}
      width={700}
      title={
        <Space>
          <SafetyCertificateOutlined style={{ color: '#006241' }} />
          <span>개인정보 수집·이용 동의</span>
        </Space>
      }
      footer={[
        <Button key="all" onClick={() => handleCheckAll(true)} disabled={submitting || policies.length === 0}>
          전체 동의
        </Button>,
        <Button
          key="submit"
          type="primary"
          disabled={!allRequiredChecked || submitting || policies.length === 0}
          loading={submitting}
          onClick={handleSubmit}
          style={{ background: '#006241', borderColor: '#006241', color: '#fff' }}
        >
          동의 후 계속
        </Button>,
      ]}
    >
      <div style={{ minHeight: 200 }}>
        {loading ? (
          <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 200 }}>
            <Spin tip="동의 정책 로딩 중..." />
          </div>
        ) : fetchError ? (
          <Result
            status="warning"
            title="동의 정책을 불러올 수 없습니다"
            subTitle="서버 연결을 확인한 후 다시 시도해주세요."
            extra={
              <Button onClick={() => { setLoading(true); setFetchError(false); setTimeout(() => window.location.reload(), 100); }}>
                다시 시도
              </Button>
            }
          />
        ) : (
          <Space direction="vertical" style={{ width: '100%' }} size="middle">
            <Alert
              type="info"
              showIcon
              message="서울아산병원 통합 데이터 플랫폼 이용을 위해 아래 항목에 동의해주세요."
              description="필수 항목에 모두 동의해야 서비스를 이용할 수 있습니다."
            />

            {policies.length === 0 && (
              <Alert type="warning" showIcon message="등록된 동의 정책이 없습니다." />
            )}

            {policies.map(p => (
              <div
                key={p.policy_id}
                style={{
                  border: '1px solid #e8e8e8',
                  borderRadius: 8,
                  padding: '12px 16px',
                  background: checked[p.policy_id] ? '#f6ffed' : '#fff',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                  <Checkbox
                    checked={checked[p.policy_id]}
                    onChange={e => setChecked({ ...checked, [p.policy_id]: e.target.checked })}
                  >
                    <Space>
                      <Text strong>{p.title}</Text>
                      {p.is_required ? (
                        <Tag color="red">필수</Tag>
                      ) : (
                        <Tag color="blue">선택</Tag>
                      )}
                      <Tag>{`v${p.version}`}</Tag>
                    </Space>
                  </Checkbox>
                  <Button
                    type="link"
                    size="small"
                    onClick={() => setExpandedId(expandedId === p.policy_id ? null : p.policy_id)}
                  >
                    {expandedId === p.policy_id ? '접기' : '상세보기'}
                  </Button>
                </div>

                {expandedId === p.policy_id && (
                  <div style={{ marginTop: 12, paddingLeft: 24 }}>
                    <Divider style={{ margin: '8px 0' }} />
                    <Paragraph style={{ fontSize: 13, color: '#555' }}>{p.description}</Paragraph>
                    <Space direction="vertical" size={4}>
                      <Text type="secondary"><FileProtectOutlined /> 수집·이용 목적: {p.purpose}</Text>
                      <Text type="secondary"><FileProtectOutlined /> 수집 항목: {p.data_items}</Text>
                      <Text type="secondary"><FileProtectOutlined /> 보유 기간: {p.retention_days}일</Text>
                    </Space>
                  </div>
                )}
              </div>
            ))}
          </Space>
        )}
      </div>
    </Modal>
  );
};

export default ConsentModal;
