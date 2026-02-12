/**
 * 메인 레이아웃 컴포넌트
 * PRD 기반 UI - 상단 회색 헤더 + 청록색 사이드바
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { Layout, Menu, Typography, Button, Tooltip, Space, Badge, Avatar, Dropdown, Tag, App, AutoComplete, Input } from 'antd';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import {
  RobotOutlined,
  BellOutlined,
  UserOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  ClockCircleOutlined,
  SearchOutlined,
  TableOutlined,
  ColumnWidthOutlined,
  LinkOutlined,
  BulbOutlined,
  QuestionCircleOutlined,
  MedicineBoxOutlined,
} from '@ant-design/icons';
import { useQueryClient } from '@tanstack/react-query';
import AIAssistantPanel from '../ai/AIAssistantPanel';
import { useSettings } from '../../contexts/SettingsContext';
import { useAuth } from '../../contexts/AuthContext';
import { semanticApi, sanitizeText } from '../../services/api';
import { catalogExtApi } from '../../services/catalogExtApi';
import { apiClient, getCsrfToken } from '../../services/apiUtils';
import { COLORS, pageShortcuts, getMenuItems, userMenuItems, ROLE_LABELS } from './layoutConstants';
import type { Notification } from './layoutConstants';
import { NotificationDrawer, ProfileModal, SettingsModal } from './MainLayoutModals';
import ResultsOverlay from './ResultsOverlay';
import type { PromotedResults } from './ResultsOverlay';
import ConsentModal from '../consent/ConsentModal';

const { Header, Sider, Content } = Layout;
const { Text } = Typography;

const MainLayout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { settings, updateSetting } = useSettings();
  const { user, logout } = useAuth();
  const { message, modal } = App.useApp();
  const [collapsed, setCollapsed] = useState(false);
  const [aiPanelVisible, setAiPanelVisible] = useState(false);
  const [notificationOpen, setNotificationOpen] = useState(false);
  const [profileOpen, setProfileOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [searchValue, setSearchValue] = useState('');
  const [searchOptions, setSearchOptions] = useState<{ label: React.ReactNode; options: { value: string; label: React.ReactNode }[] }[]>([]);
  const [recentSearches, setRecentSearches] = useState<{ query: string; time: string; results: number }[]>([]);
  const searchRef = useRef<any>(null);
  const debounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // 알림 데이터 — API에서 로딩
  const [notifications, setNotifications] = useState<Notification[]>([]);
  useEffect(() => {
    apiClient.get('/portal-ops/notifications')
      .then(({ data }) => setNotifications(data?.notifications || data || []))
      .catch(() => {});
  }, []);

  // 데모 페이지 API 프리페치 — 로그인 직후 백엔드 캐시 + React Query 캐시 워밍
  const queryClient = useQueryClient();
  useEffect(() => {
    const prefetchEntries: { key: string[]; url: string; extract?: string }[] = [
      { key: ['dashboard-stats'], url: '/api/v1/datamart/dashboard-stats' },
      { key: ['lakehouse-overview'], url: '/api/v1/catalog-ext/lakehouse-overview' },
      { key: ['datamart', 'cdm-summary'], url: '/api/v1/datamart/cdm-summary' },
      { key: ['datamart', 'tables'], url: '/api/v1/datamart/tables', extract: 'tables' },
      { key: ['ontology-graph-prefetch'], url: '/api/v1/ontology/graph?graph_type=schema' },
      { key: ['medical-knowledge-stats'], url: '/api/v1/medical-knowledge/stats' },
      { key: ['system-resources'], url: '/api/v1/ai-environment/resources/system' },
      { key: ['containers'], url: '/api/v1/ai-environment/containers' },
    ];
    prefetchEntries.forEach(({ key, url, extract }, i) => {
      setTimeout(() => {
        queryClient.prefetchQuery({
          queryKey: key,
          queryFn: () => fetch(url).then(r => r.ok ? r.json() : null).then(d => extract && d ? d[extract] : d),
          staleTime: 5 * 60 * 1000,
        });
      }, i * 150); // 150ms 간격으로 분산
    });
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // AI 결과 중앙 화면 표출
  const [promotedResults, setPromotedResults] = useState<PromotedResults | null>(null);

  // SER-010: 개인정보 동의 모달
  const [consentOpen, setConsentOpen] = useState(false);
  const consentUserId = user?.id || '';
  useEffect(() => {
    if (!consentUserId) return;
    apiClient.get(`/consent/user/${consentUserId}`)
      .then(({ data }) => {
        if (data && !data.all_required_agreed) setConsentOpen(true);
      })
      .catch(() => {});
  }, [consentUserId]);

  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail;
      if (detail?.columns && detail?.results) {
        setPromotedResults(detail);
      }
    };
    window.addEventListener('ai:show-results', handler);
    return () => window.removeEventListener('ai:show-results', handler);
  }, []);

  // 최근 검색 이력 로딩
  useEffect(() => {
    catalogExtApi.getRecentSearches()
      .then((data) => setRecentSearches(data.searches || []))
      .catch(() => {});
  }, []);

  // Ctrl+K 단축키
  useEffect(() => {
    const handleGlobalKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault();
        searchRef.current?.focus();
      }
    };
    document.addEventListener('keydown', handleGlobalKeyDown);
    return () => document.removeEventListener('keydown', handleGlobalKeyDown);
  }, []);

  // 검색 debounce 핸들러
  const handleSearchChange = useCallback((value: string) => {
    setSearchValue(value);
    if (debounceTimer.current) clearTimeout(debounceTimer.current);
    if (!value.trim()) {
      // 빈 입력 시 최근 검색 표시
      if (recentSearches.length > 0) {
        setSearchOptions([{
          label: <Text type="secondary" style={{ fontSize: 12 }}>최근 검색</Text>,
          options: recentSearches.slice(0, 5).map((s) => ({
            value: `__recent__${s.query}`,
            label: (
              <Space>
                <ClockCircleOutlined style={{ color: '#8c8c8c' }} />
                <span>{s.query}</span>
                <Text type="secondary" style={{ fontSize: 11 }}>{s.time}</Text>
              </Space>
            ),
          })),
        }]);
      } else {
        setSearchOptions([]);
      }
      return;
    }
    debounceTimer.current = setTimeout(async () => {
      try {
        const query = sanitizeText(value);
        // 페이지 바로가기 매칭
        const matchedPages = pageShortcuts.filter(
          (p) => p.label.toLowerCase().includes(query.toLowerCase())
        );

        // 빠른 API 2개 먼저 (시맨틱 검색 + 오타 보정/AI요약) — ~0.2초
        const [searchResult, suggestResult] = await Promise.all([
          semanticApi.search(query, undefined, 8).catch(() => null),
          catalogExtApi.getSearchSuggest(query).catch(() => null),
        ]);

        // 빠른 결과로 즉시 드롭다운 표시
        const buildGroups = (medicalResult: any = null) => {
          const groups: { label: React.ReactNode; options: { value: string; label: React.ReactNode }[] }[] = [];

          // AI 요약 (최상단)
          const aiSummary = suggestResult?.ai_summary;
          if (aiSummary) {
            groups.push({
              label: <Text type="secondary" style={{ fontSize: 12 }}><BulbOutlined /> AI 요약</Text>,
              options: [{
                value: `__summary__${query}`,
                label: (
                  <div style={{ maxWidth: 370, whiteSpace: 'normal', lineHeight: '1.4', padding: '2px 0' }}>
                    <BulbOutlined style={{ color: '#faad14', marginRight: 6 }} />
                    <Text style={{ fontSize: 13 }}>{aiSummary}</Text>
                  </div>
                ),
              }],
            });
          }

          // 의학 지식 검색 결과 (느린 API — 도착 시 추가)
          const medHits = medicalResult?.results || [];
          if (medHits.length > 0) {
            const docTypeLabels: Record<string, string> = {
              textbook: '교과서', guideline: '가이드라인', journal: '논문',
              online: '온라인', qa_case: 'Q&A', qa_short: 'Q&A', qa_essay: 'Q&A',
            };
            groups.push({
              label: <Text type="secondary" style={{ fontSize: 12 }}><MedicineBoxOutlined /> 의학 지식</Text>,
              options: medHits.slice(0, 3).map((h: any, idx: number) => ({
                value: `__medical__${idx}__${query}`,
                label: (
                  <div style={{ maxWidth: 370, whiteSpace: 'normal', lineHeight: '1.4', padding: '2px 0' }}>
                    <MedicineBoxOutlined style={{ color: '#eb2f96', marginRight: 6 }} />
                    <Tag color="magenta" style={{ fontSize: 11 }}>{docTypeLabels[h.doc_type] || h.doc_type}</Tag>
                    {h.source && <Text type="secondary" style={{ fontSize: 11 }}>{h.source} </Text>}
                    <Text style={{ fontSize: 13 }}>{(h.content || '').slice(0, 80)}...</Text>
                  </div>
                ),
              })),
            });
          }

          if (matchedPages.length > 0) {
            groups.push({
              label: <Text type="secondary" style={{ fontSize: 12 }}>페이지 바로가기</Text>,
              options: matchedPages.map((p) => ({
                value: `__page__${p.path}`,
                label: (
                  <Space>
                    <LinkOutlined style={{ color: '#8c8c8c' }} />
                    <span>{p.label}</span>
                  </Space>
                ),
              })),
            });
          }

          const tables = searchResult?.data?.tables || [];
          const columns = searchResult?.data?.columns || [];

          if (tables.length > 0) {
            groups.push({
              label: <Text type="secondary" style={{ fontSize: 12 }}>테이블</Text>,
              options: tables.slice(0, 5).map((t: any) => ({
                value: `__table__${t.physical_name}`,
                label: (
                  <Space>
                    <TableOutlined style={{ color: '#005BAC' }} />
                    <span><Text strong>{t.business_name}</Text> <Text type="secondary" style={{ fontSize: 12 }}>({t.physical_name})</Text></span>
                  </Space>
                ),
              })),
            });
          }

          if (columns.length > 0) {
            groups.push({
              label: <Text type="secondary" style={{ fontSize: 12 }}>컬럼</Text>,
              options: columns.slice(0, 3).map((c: any) => ({
                value: `__col__${c.table_name || ''}__${c.physical_name}`,
                label: (
                  <Space>
                    <ColumnWidthOutlined style={{ color: '#52c41a' }} />
                    <span>{c.business_name || c.physical_name} <Text type="secondary" style={{ fontSize: 12 }}>({c.physical_name})</Text></span>
                  </Space>
                ),
              })),
            });
          }

          // 오타 보정 제안 (정확한 매칭이 적을 때만)
          if (suggestResult?.has_corrections && tables.length === 0 && matchedPages.length === 0) {
            const corrTables = suggestResult.corrections?.tables || [];
            const corrPages = suggestResult.corrections?.pages || [];
            const suggestions: { value: string; label: React.ReactNode }[] = [];

            corrTables.forEach((ct: any) => {
              suggestions.push({
                value: `__table__${ct.table_name}`,
                label: (
                  <Space>
                    <TableOutlined style={{ color: '#ff7a45' }} />
                    <span>{ct.table_name} <Text type="secondary" style={{ fontSize: 12 }}>({ct.label})</Text></span>
                  </Space>
                ),
              });
            });
            corrPages.forEach((cp: any) => {
              suggestions.push({
                value: `__page__${cp.path}`,
                label: (
                  <Space>
                    <LinkOutlined style={{ color: '#ff7a45' }} />
                    <span>{cp.label}</span>
                  </Space>
                ),
              });
            });

            if (suggestions.length > 0) {
              groups.push({
                label: <Text type="secondary" style={{ fontSize: 12 }}><QuestionCircleOutlined /> 혹시 이것을 찾으셨나요?</Text>,
                options: suggestions,
              });
            }
          }

          return groups;
        };

        // 1단계: 빠른 결과 즉시 표시 (~0.2초)
        setSearchOptions(buildGroups());

        // 2단계: 의학 지식 검색 비동기 추가 (느린 Milvus — 별도 호출)
        fetch('/api/v1/medical-knowledge/search', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...(getCsrfToken() ? { 'X-CSRF-Token': getCsrfToken() } : {}) },
          body: JSON.stringify({ query, top_k: 3 }),
        })
          .then(r => r.ok ? r.json() : null)
          .then(medResult => {
            if (medResult?.results?.length > 0) {
              setSearchOptions(buildGroups(medResult));
            }
          })
          .catch(() => {});
      } catch {
        setSearchOptions([]);
      }
    }, 300);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSearchSelect = (value: string) => {
    if (value.startsWith('__page__')) {
      navigate(value.replace('__page__', ''));
    } else if (value.startsWith('__table__')) {
      const tableName = value.replace('__table__', '');
      const ctx = searchValue.trim();
      const url = ctx && ctx !== tableName
        ? `/catalog?q=${encodeURIComponent(tableName)}&context=${encodeURIComponent(ctx)}`
        : `/catalog?q=${encodeURIComponent(tableName)}`;
      navigate(url);
    } else if (value.startsWith('__col__')) {
      const parts = value.replace('__col__', '').split('__');
      const tableName = parts[0] || parts[1] || '';
      const ctx = searchValue.trim();
      const url = ctx && ctx !== tableName
        ? `/catalog?q=${encodeURIComponent(tableName)}&context=${encodeURIComponent(ctx)}`
        : `/catalog?q=${encodeURIComponent(tableName)}`;
      navigate(url);
    } else if (value.startsWith('__recent__')) {
      const query = value.replace('__recent__', '');
      navigate(`/catalog?q=${encodeURIComponent(query)}`);
    } else if (value.startsWith('__summary__')) {
      const query = value.replace('__summary__', '');
      navigate(`/catalog?q=${encodeURIComponent(query)}`);
    } else if (value.startsWith('__medical__')) {
      const parts = value.split('__');
      const query = parts.slice(3).join('__');
      navigate(`/medical-knowledge?q=${encodeURIComponent(query)}`);
    }
    setSearchValue('');
    setSearchOptions([]);
  };

  // Enter 키로 검색 시 데이터 카탈로그로 이동
  const handleSearchKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && searchValue.trim()) {
      navigate(`/catalog?q=${encodeURIComponent(searchValue.trim())}`);
      setSearchValue('');
      setSearchOptions([]);
    }
  }, [searchValue, navigate]);

  // AI 도우미 자동 실행
  useEffect(() => {
    if (settings.aiAutoOpen) setAiPanelVisible(true);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const handleUserMenuClick = ({ key }: { key: string }) => {
    if (key === 'profile') setProfileOpen(true);
    else if (key === 'settings') setSettingsOpen(true);
    else if (key === 'logout') {
      modal.confirm({
        title: '로그아웃',
        content: '정말 로그아웃 하시겠습니까?',
        okText: '로그아웃',
        cancelText: '취소',
        okButtonProps: { danger: true },
        onOk: () => { logout(); navigate('/login'); },
      });
    }
  };

  const menuItems = getMenuItems(user?.role);

  const handleMenuClick = ({ key }: { key: string }) => {
    if (key.startsWith('/')) {
      navigate(key);
    }
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      {/* ===== 상단 전체 회색 헤더 ===== */}
      <Header
        style={{
          background: COLORS.HEADER_BG,
          padding: '0 24px',
          height: 56,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          zIndex: 1000,
        }}
      >
        {/* 좌측: 로고 */}
        <div style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }} onClick={() => navigate('/dashboard')}>
          <img
            src="/src/assets/asan_logo_full.png"
            alt="서울아산병원"
            style={{
              height: 32,
              objectFit: 'contain',
              filter: 'brightness(0) invert(1)',  // 흰색으로 변환
            }}
          />
        </div>

        {/* 중앙: 통합 검색 */}
        <div style={{ flex: 1, maxWidth: 420, margin: '0 24px' }}>
          <AutoComplete
            ref={searchRef}
            value={searchValue}
            options={searchOptions}
            onSearch={handleSearchChange}
            onSelect={handleSearchSelect}
            style={{ width: '100%' }}
            popupMatchSelectWidth={400}
          >
            <Input
              className="gnb-search"
              prefix={<SearchOutlined style={{ color: 'rgba(255,255,255,0.7)' }} />}
              suffix={
                <Text style={{ color: 'rgba(255,255,255,0.45)', fontSize: 11 }}>Ctrl+K</Text>
              }
              placeholder="테이블, 컬럼, 페이지 검색..."
              style={{
                background: 'rgba(255,255,255,0.15)',
                border: '1px solid rgba(255,255,255,0.25)',
                borderRadius: 6,
                color: 'white',
              }}
              allowClear
              autoComplete="off"
              onKeyDown={handleSearchKeyDown}
            />
          </AutoComplete>
        </div>

        {/* 우측: 버튼들 */}
        <Space size="middle">
          <Tooltip title="AI Assistant">
            <Button
              type={aiPanelVisible ? 'primary' : 'default'}
              icon={<RobotOutlined />}
              onClick={() => setAiPanelVisible(!aiPanelVisible)}
              style={{
                background: aiPanelVisible ? COLORS.SECONDARY : 'transparent',
                borderColor: aiPanelVisible ? COLORS.SECONDARY : 'rgba(255,255,255,0.3)',
                color: 'white',
              }}
            >
              AI Assistant
            </Button>
          </Tooltip>

          <Tooltip title={settings.notificationsEnabled ? '알림' : '알림 꺼짐'}>
            <Badge count={settings.notificationsEnabled ? notifications.length : 0} size="small">
              <Button
                type="text"
                icon={<BellOutlined style={{ fontSize: 18, color: settings.notificationsEnabled ? 'white' : 'rgba(255,255,255,0.3)' }} />}
                onClick={() => {
                  if (settings.notificationsEnabled) setNotificationOpen(true);
                  else message.info('알림 수신이 비활성화 상태입니다. 설정에서 활성화하세요.');
                }}
              />
            </Badge>
          </Tooltip>

          <Dropdown menu={{ items: userMenuItems, onClick: handleUserMenuClick }} placement="bottomRight">
            <Space style={{ cursor: 'pointer' }}>
              <Avatar size={32} style={{ background: COLORS.SECONDARY }}>
                <UserOutlined />
              </Avatar>
              <Text style={{ color: 'white', fontSize: 14 }}>{user?.name || '사용자'}</Text>
            </Space>
          </Dropdown>
        </Space>
      </Header>

      {/* ===== 하단 영역 (사이드바 + 컨텐츠) ===== */}
      <Layout style={{ marginTop: 56 }}>
        {/* 사이드바 - 청록색 */}
        <Sider
          width={220}
          collapsedWidth={64}
          collapsed={collapsed}
          style={{
            background: COLORS.SIDEBAR_BG,
            position: 'fixed',
            left: 0,
            top: 56,
            bottom: 0,
            zIndex: 100,
          }}
        >
          {/* 메뉴 + 접기 버튼을 flex column으로 배치 */}
          <style>
            {`
              .sidebar-menu .ant-menu-sub {
                background: #153d32 !important;
              }
              .sidebar-menu .ant-menu-item:hover,
              .sidebar-menu .ant-menu-submenu-title:hover {
                background: #2a5d4d !important;
              }
              .sidebar-menu .ant-menu-item-selected {
                background: #2a5d4d !important;
              }
              .gnb-search .ant-input::placeholder {
                color: rgba(255,255,255,0.7) !important;
              }
              .gnb-search .ant-input {
                color: white !important;
              }
              .gnb-search .ant-input-clear-icon {
                color: rgba(255,255,255,0.5) !important;
              }
              .ant-layout-sider-children {
                display: flex !important;
                flex-direction: column !important;
              }
            `}
          </style>
          <div style={{ flex: 1, overflowY: 'auto', overflowX: 'hidden' }}>
            <Menu
              className="sidebar-menu"
              mode="inline"
              selectedKeys={[location.pathname]}
              defaultOpenKeys={['data-engineering', 'data-governance', 'data-utilization', 'ai-medical', 'system-ops']}
              items={menuItems}
              onClick={handleMenuClick}
              style={{
                background: 'transparent',
                borderRight: 'none',
                marginTop: 8,
              }}
              theme="dark"
            />
          </div>

          {/* 하단 - 접기 버튼 */}
          <div style={{
            flexShrink: 0,
            padding: '12px 16px',
            borderTop: '1px solid rgba(255,255,255,0.15)',
            background: COLORS.SIDEBAR_BG,
          }}>
            <Button
              type="text"
              icon={collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
              onClick={() => setCollapsed(!collapsed)}
              style={{ color: COLORS.TEXT_MUTED, width: '100%', textAlign: 'left' }}
            >
              {!collapsed && '메뉴 접기'}
            </Button>
          </div>
        </Sider>

        {/* 메인 컨텐츠 */}
        <Content
          style={{
            marginLeft: collapsed ? 64 : 220,
            padding: 24,
            background: '#f0f2f5',
            minHeight: 'calc(100vh - 56px)',
            transition: 'margin-left 0.2s',
          }}
        >
          <Outlet />

          {/* AI 결과 테이블 오버레이 */}
          {promotedResults && (
            <ResultsOverlay
              promotedResults={promotedResults}
              onClose={() => setPromotedResults(null)}
              collapsed={collapsed}
              aiPanelVisible={aiPanelVisible}
            />
          )}
        </Content>
      </Layout>

      {/* AI Assistant 패널 */}
      <AIAssistantPanel
        visible={aiPanelVisible}
        onClose={() => setAiPanelVisible(false)}
        currentContext={{
          currentPage: location.pathname,
          currentTable: new URLSearchParams(location.search).get('q') || undefined,
          currentColumns: undefined,
          userRole: 'admin',
        }}
      />

      {/* 알림 Drawer */}
      <NotificationDrawer
        open={notificationOpen}
        onClose={() => setNotificationOpen(false)}
        notifications={notifications}
      />

      {/* 프로필 Modal */}
      <ProfileModal
        open={profileOpen}
        onClose={() => setProfileOpen(false)}
        user={user}
      />

      {/* SER-010: 개인정보 동의 모달 */}
      <ConsentModal
        open={consentOpen}
        userId={consentUserId}
        onComplete={() => setConsentOpen(false)}
      />

      {/* 설정 Modal */}
      <SettingsModal
        open={settingsOpen}
        onClose={() => setSettingsOpen(false)}
        settings={settings}
        updateSetting={updateSetting}
        setAiPanelVisible={setAiPanelVisible}
        message={message}
      />
    </Layout>
  );
};

export default MainLayout;
