/**
 * MainLayout 모달/드로어 컴포넌트
 * MainLayout.tsx에서 분리하여 파일 크기를 줄임
 */

import React from 'react';
import { Drawer, Modal, List, Switch, Divider, Button, Tag, Space, Typography, Avatar } from 'antd';
import {
  BellOutlined,
  CheckCircleOutlined,
  WarningOutlined,
  InfoCircleOutlined,
  ClockCircleOutlined,
  UserOutlined,
  MailOutlined,
  SafetyCertificateOutlined,
  SettingOutlined,
} from '@ant-design/icons';
import { COLORS, ROLE_LABELS, ROLE_COLORS } from './layoutConstants';
import type { Notification } from './layoutConstants';
import type { AppSettings } from '../../contexts/SettingsContext';

const { Text } = Typography;

/* ------------------------------------------------------------------ */
/*  알림 Drawer                                                        */
/* ------------------------------------------------------------------ */
interface NotificationDrawerProps {
  open: boolean;
  onClose: () => void;
  notifications: Notification[];
}

export const NotificationDrawer: React.FC<NotificationDrawerProps> = ({ open, onClose, notifications }) => (
  <Drawer
    title={<><BellOutlined /> 알림 센터</>}
    placement="right"
    width={380}
    open={open}
    onClose={onClose}
  >
    <List
      dataSource={notifications}
      renderItem={(item) => (
        <List.Item style={{ padding: '12px 0' }}>
          <List.Item.Meta
            avatar={
              item.type === 'success' ? <CheckCircleOutlined style={{ fontSize: 20, color: '#52c41a' }} /> :
              item.type === 'warning' ? <WarningOutlined style={{ fontSize: 20, color: '#faad14' }} /> :
              <InfoCircleOutlined style={{ fontSize: 20, color: '#1890ff' }} />
            }
            title={<span style={{ fontSize: 13 }}>{item.title}</span>}
            description={
              <>
                <div style={{ fontSize: 12, color: '#666' }}>{item.desc}</div>
                <div style={{ fontSize: 11, color: '#aaa', marginTop: 4 }}><ClockCircleOutlined /> {item.time}</div>
              </>
            }
          />
        </List.Item>
      )}
    />
  </Drawer>
);

/* ------------------------------------------------------------------ */
/*  프로필 Modal                                                       */
/* ------------------------------------------------------------------ */
interface ProfileModalProps {
  open: boolean;
  onClose: () => void;
  user: { name?: string; role?: string; email?: string; department?: string } | null;
}

export const ProfileModal: React.FC<ProfileModalProps> = ({ open, onClose, user }) => (
  <Modal
    title={`${user?.name || '사용자'} 프로필`}
    open={open}
    onCancel={onClose}
    footer={<Button onClick={onClose}>닫기</Button>}
    width={420}
  >
    <div style={{ textAlign: 'center', padding: '16px 0' }}>
      <Avatar size={72} style={{ background: COLORS.SECONDARY, fontSize: 28 }}>
        <UserOutlined />
      </Avatar>
      <h3 style={{ marginTop: 12, marginBottom: 4 }}>{user?.name || '사용자'}</h3>
      <Tag color={ROLE_COLORS[user?.role || ''] || 'default'}>{ROLE_LABELS[user?.role || ''] || user?.role || 'user'}</Tag>
    </div>
    <Divider />
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <Text type="secondary"><MailOutlined /> 이메일</Text>
        <Text>{user?.email || '-'}</Text>
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <Text type="secondary"><UserOutlined /> 부서</Text>
        <Text>{user?.department || '-'}</Text>
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <Text type="secondary"><SafetyCertificateOutlined /> 권한</Text>
        <Tag color={ROLE_COLORS[user?.role || ''] || 'default'}>{ROLE_LABELS[user?.role || ''] || user?.role || '일반'}</Tag>
      </div>
      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
        <Text type="secondary"><ClockCircleOutlined /> 마지막 접속</Text>
        <Text>{new Date().toLocaleString('ko-KR')}</Text>
      </div>
    </div>
  </Modal>
);

/* ------------------------------------------------------------------ */
/*  설정 Modal                                                         */
/* ------------------------------------------------------------------ */
interface SettingsModalProps {
  open: boolean;
  onClose: () => void;
  settings: AppSettings;
  updateSetting: <K extends keyof AppSettings>(key: K, value: AppSettings[K]) => void;
  setAiPanelVisible: (visible: boolean) => void;
  message: { info: (content: string) => void };
}

export const SettingsModal: React.FC<SettingsModalProps> = ({ open, onClose, settings, updateSetting, setAiPanelVisible, message }) => (
  <Modal
    title={<><SettingOutlined /> 시스템 설정</>}
    open={open}
    onCancel={onClose}
    footer={<Button onClick={onClose}>닫기</Button>}
    width={480}
  >
    <div style={{ display: 'flex', flexDirection: 'column', gap: 20, padding: '8px 0' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <div style={{ fontWeight: 600 }}>다크 모드</div>
          <div style={{ fontSize: 12, color: '#999' }}>어두운 테마 사용</div>
        </div>
        <Switch size="small" checked={settings.darkMode} onChange={(v) => updateSetting('darkMode', v)} />
      </div>
      <Divider style={{ margin: 0 }} />
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <div style={{ fontWeight: 600 }}>알림 수신</div>
          <div style={{ fontSize: 12, color: '#999' }}>시스템 알림 및 경고 수신</div>
        </div>
        <Switch size="small" checked={settings.notificationsEnabled} onChange={(v) => { updateSetting('notificationsEnabled', v); message.info(v ? '알림 활성화' : '알림 비활성화'); }} />
      </div>
      <Divider style={{ margin: 0 }} />
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <div style={{ fontWeight: 600 }}>자동 새로고침</div>
          <div style={{ fontSize: 12, color: '#999' }}>대시보드 10초 주기 갱신</div>
        </div>
        <Switch size="small" checked={settings.autoRefresh} onChange={(v) => { updateSetting('autoRefresh', v); message.info(v ? '자동 새로고침 켜짐' : '자동 새로고침 꺼짐'); }} />
      </div>
      <Divider style={{ margin: 0 }} />
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <div style={{ fontWeight: 600 }}>AI 도우미 자동 실행</div>
          <div style={{ fontSize: 12, color: '#999' }}>페이지 진입 시 AI 패널 자동 열기</div>
        </div>
        <Switch size="small" checked={settings.aiAutoOpen} onChange={(v) => { updateSetting('aiAutoOpen', v); if (v) setAiPanelVisible(true); }} />
      </div>
    </div>
  </Modal>
);
