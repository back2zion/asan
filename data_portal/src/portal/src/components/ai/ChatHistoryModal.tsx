/**
 * 대화 기록 Modal 컴포넌트
 * AIAssistantPanel.tsx 에서 분리
 */

import React from 'react';
import { Modal, Space, Spin, List, Avatar, Tag, Button, Timeline, Tooltip, Typography } from 'antd';
import {
  HistoryOutlined,
  MessageOutlined,
  ClockCircleOutlined,
  RollbackOutlined,
  UserOutlined,
  RobotOutlined,
} from '@ant-design/icons';
import { MINT } from './chatConstants';

const { Text } = Typography;

export interface ChatHistoryModalProps {
  visible: boolean;
  onClose: () => void;
  /** 현재 활성 세션 ID (현재 세션 하이라이트용) */
  sessionId: string | null;
  sessions: any[];
  sessionsLoading: boolean;
  selectedSessionId: string | null;
  timelineData: any[];
  timelineLoading: boolean;
  restoringMessageId: string | null;
  onSelectSession: (sid: string) => void;
  onRestore: (sid: string, messageId: string) => void;
  onBackToList: () => void;
}

const ChatHistoryModal: React.FC<ChatHistoryModalProps> = ({
  visible,
  onClose,
  sessionId,
  sessions,
  sessionsLoading,
  selectedSessionId,
  timelineData,
  timelineLoading,
  restoringMessageId,
  onSelectSession,
  onRestore,
  onBackToList,
}) => {
  return (
    <Modal
      title={
        <Space>
          <HistoryOutlined style={{ color: MINT.PRIMARY }} />
          <span>대화 기록</span>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      footer={null}
      width={560}
      styles={{ body: { padding: 0, maxHeight: 480, overflow: 'auto' } }}
    >
      {!selectedSessionId ? (
        // 세션 목록
        <Spin spinning={sessionsLoading}>
          {sessions.length > 0 ? (
            <List
              dataSource={sessions}
              renderItem={(s: any) => {
                const isCurrent = s.session_id === sessionId;
                return (
                  <List.Item
                    style={{
                      padding: '12px 24px',
                      cursor: 'pointer',
                      background: isCurrent ? MINT.LIGHT : undefined,
                    }}
                    onClick={() => onSelectSession(s.session_id)}
                  >
                    <List.Item.Meta
                      avatar={<Avatar style={{ background: isCurrent ? MINT.PRIMARY : '#d9d9d9' }} icon={<MessageOutlined />} />}
                      title={
                        <Space>
                          <span>{s.title || s.session_id?.slice(0, 8) || '세션'}</span>
                          {isCurrent && <Tag color="cyan" style={{ fontSize: 10 }}>현재</Tag>}
                        </Space>
                      }
                      description={
                        <Space size={16}>
                          <Text type="secondary" style={{ fontSize: 12 }}>
                            <ClockCircleOutlined /> {s.updated_at ? new Date(s.updated_at).toLocaleString('ko-KR') : s.created_at ? new Date(s.created_at).toLocaleString('ko-KR') : '-'}
                          </Text>
                          <Text type="secondary" style={{ fontSize: 12 }}>
                            메시지 {s.message_count ?? '?'}개
                          </Text>
                        </Space>
                      }
                    />
                  </List.Item>
                );
              }}
            />
          ) : (
            <div style={{ textAlign: 'center', padding: 40 }}>
              <Text type="secondary">대화 기록이 없습니다.</Text>
            </div>
          )}
        </Spin>
      ) : (
        // 타임라인 뷰
        <div style={{ padding: '16px 24px' }}>
          <Button
            type="link"
            icon={<RollbackOutlined />}
            onClick={onBackToList}
            style={{ marginBottom: 12, padding: 0, color: MINT.PRIMARY }}
          >
            세션 목록으로
          </Button>
          <Spin spinning={timelineLoading}>
            {timelineData.length > 0 ? (
              <Timeline
                items={timelineData.map((msg: any) => {
                  const isUser = msg.role === 'user';
                  const msgId = msg.message_id || msg.id;
                  // 저장된 페이지 상태 조회
                  let savedPageLabel = '';
                  if (isUser) {
                    try {
                      const stateKey = `ai_page_state_${selectedSessionId}`;
                      const states = JSON.parse(localStorage.getItem(stateKey) || '{}');
                      const ps = states[msgId];
                      if (ps?.label) savedPageLabel = ps.label;
                    } catch { /* ignore */ }
                  }
                  return {
                    color: isUser ? MINT.PRIMARY : '#d9d9d9',
                    dot: isUser ? <UserOutlined style={{ fontSize: 14 }} /> : <RobotOutlined style={{ fontSize: 14 }} />,
                    children: (
                      <div style={{ marginBottom: 4 }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                          <div style={{ flex: 1 }}>
                            <Space size={4} style={{ marginBottom: 4 }}>
                              <Tag color={isUser ? 'cyan' : 'default'} style={{ fontSize: 10 }}>
                                {isUser ? '사용자' : 'AI'}
                              </Tag>
                              {savedPageLabel && (
                                <Tag color="geekblue" style={{ fontSize: 9 }}>
                                  {savedPageLabel}
                                </Tag>
                              )}
                            </Space>
                            <Text style={{ fontSize: 12, display: 'block' }}>
                              {(msg.content || '').length > 100 ? `${msg.content.slice(0, 100)}...` : msg.content}
                            </Text>
                            <Text type="secondary" style={{ fontSize: 10 }}>
                              {msg.timestamp ? new Date(msg.timestamp).toLocaleTimeString('ko-KR') : msg.created_at ? new Date(msg.created_at).toLocaleTimeString('ko-KR') : ''}
                            </Text>
                          </div>
                          {isUser && (
                            <Tooltip title={savedPageLabel ? `"${savedPageLabel}" 화면으로 복원` : '이 시점으로 복원'}>
                              <Button
                                size="small"
                                type="link"
                                icon={<RollbackOutlined />}
                                loading={restoringMessageId === msgId}
                                onClick={() => onRestore(selectedSessionId!, msgId)}
                                style={{ color: MINT.PRIMARY, marginLeft: 8 }}
                              >
                                복원
                              </Button>
                            </Tooltip>
                          )}
                        </div>
                      </div>
                    ),
                  };
                })}
              />
            ) : (
              <div style={{ textAlign: 'center', padding: 40 }}>
                <Text type="secondary">타임라인이 없습니다.</Text>
              </div>
            )}
          </Spin>
        </div>
      )}
    </Modal>
  );
};

export default ChatHistoryModal;
