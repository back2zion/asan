/**
 * MainLayout 상수 & 메뉴 정의
 * MainLayout.tsx에서 분리하여 파일 크기를 줄이고 재사용성을 높임
 */

import React from 'react';
import {
  HomeOutlined,
  DatabaseOutlined,
  BarChartOutlined,
  SafetyCertificateOutlined,
  ApiOutlined,
  RobotOutlined,
  ExperimentOutlined,
  UserOutlined,
  SettingOutlined,
  LogoutOutlined,
  AppstoreOutlined,
  FileTextOutlined,
  LinkOutlined,
  ApartmentOutlined,
  DeploymentUnitOutlined,
  ClusterOutlined,
  MedicineBoxOutlined,
} from '@ant-design/icons';
import {
  LayoutDashboard, Workflow, ShieldCheck, BarChart3, Brain, Wrench,
} from 'lucide-react';
import type { MenuProps } from 'antd';

// PRD 기반 컬러
export const COLORS = {
  HEADER_BG: '#4a5568',       // 상단 헤더 짙은 회색
  SIDEBAR_BG: '#1a4d3e',      // 사이드바 짙은 녹색 (아산병원 녹색)
  PRIMARY: '#005BAC',
  SECONDARY: '#00A0B0',
  TEXT_MUTED: '#8fbfaa',
};

// 페이지 바로가기 목록 (5개 대분류 순서)
export const pageShortcuts = [
  { label: '홈 (대시보드)', path: '/dashboard', icon: React.createElement(HomeOutlined) },
  // 데이터 엔지니어링
  { label: 'ETL 파이프라인', path: '/etl', icon: React.createElement(ApiOutlined) },
  { label: '데이터 설계', path: '/data-design', icon: React.createElement(ApartmentOutlined) },
  // 데이터 거버넌스
  { label: '거버넌스 관리', path: '/governance', icon: React.createElement(SafetyCertificateOutlined) },
  { label: '데이터 카탈로그', path: '/catalog', icon: React.createElement(AppstoreOutlined) },
  // 데이터 활용
  { label: '데이터마트', path: '/datamart', icon: React.createElement(DatabaseOutlined) },
  { label: 'BI 대시보드', path: '/bi', icon: React.createElement(BarChartOutlined) },
  { label: 'CDW 연구지원', path: '/cdw', icon: React.createElement(FileTextOutlined) },
  // AI & 의료 지능
  { label: 'AI 분석환경', path: '/ai-environment', icon: React.createElement(RobotOutlined) },
  { label: '비정형 구조화', path: '/ner', icon: React.createElement(ExperimentOutlined) },
  { label: '의료 온톨로지', path: '/ontology', icon: React.createElement(DeploymentUnitOutlined) },
  { label: '의학 지식', path: '/medical-knowledge', icon: React.createElement(MedicineBoxOutlined) },
  // 시스템 운영
  { label: 'AI 운영관리', path: '/ai-ops', icon: React.createElement(SettingOutlined) },
  { label: 'AI 아키텍처', path: '/ai-architecture', icon: React.createElement(ClusterOutlined) },
  { label: '포털 운영관리', path: '/portal-ops', icon: React.createElement(SettingOutlined) },
];

// 알림 데이터
export const notifications = [
  { id: 1, type: 'success', title: 'ETL 파이프라인 완료', desc: 'OMOP CDM 일일 적재 완료 (133만 건)', time: '10분 전' },
  { id: 2, type: 'warning', title: '데이터 품질 경고', desc: 'measurement.value_as_number NULL 비율 100%', time: '1시간 전' },
  { id: 3, type: 'info', title: '시스템 업데이트', desc: 'CDM 변환 요약 대시보드가 추가되었습니다', time: '2시간 전' },
];

// 역할별 한글 라벨 & 태그 색상
export const ROLE_LABELS: Record<string, string> = {
  admin: '관리자',
  doctor: '의사',
  researcher: '연구자',
  patient: '환자',
};
export const ROLE_COLORS: Record<string, string> = {
  admin: 'red',
  doctor: 'blue',
  researcher: 'green',
  patient: 'orange',
};

// 역할별 메뉴 접근 키 (admin은 전체)
const ROLE_MENU_KEYS: Record<string, Set<string>> = {
  admin: new Set(['all']),
  doctor: new Set(['/dashboard', 'data-utilization', 'ai-medical']),
  researcher: new Set(['/dashboard', 'data-engineering', 'data-governance', 'data-utilization', 'ai-medical']),
  patient: new Set(['/dashboard', 'data-utilization']),
};

// 사이드바 메뉴 아이템 (아이콘 크기 통일: lucide 16px)
export function getMenuItems(role?: string): MenuProps['items'] {
  const L = 16;
  const allItems: (NonNullable<MenuProps['items']>[number] & { key: string })[] = [
    {
      key: '/dashboard',
      icon: React.createElement(LayoutDashboard, { size: L }),
      label: '홈',
    },
    {
      key: 'data-engineering',
      icon: React.createElement(Workflow, { size: L }),
      label: '데이터 엔지니어링',
      children: [
        { key: '/etl', icon: React.createElement(ApiOutlined), label: 'ETL 파이프라인' },
        { key: '/data-design', icon: React.createElement(ApartmentOutlined), label: '데이터 설계' },
        { key: '/data-fabric', icon: React.createElement(LinkOutlined), label: '데이터 패브릭' },
      ],
    },
    {
      key: 'data-governance',
      icon: React.createElement(ShieldCheck, { size: L }),
      label: '데이터 거버넌스',
      children: [
        { key: '/governance', icon: React.createElement(SafetyCertificateOutlined), label: '거버넌스 관리' },
        { key: '/catalog', icon: React.createElement(AppstoreOutlined), label: '데이터 카탈로그' },
      ],
    },
    {
      key: 'data-utilization',
      icon: React.createElement(BarChart3, { size: L }),
      label: '데이터 활용',
      children: [
        { key: '/datamart', icon: React.createElement(DatabaseOutlined), label: '데이터마트' },
        { key: '/bi', icon: React.createElement(BarChartOutlined), label: 'BI 대시보드' },
        { key: '/cdw', icon: React.createElement(ExperimentOutlined), label: 'CDW 연구지원' },
      ],
    },
    {
      key: 'ai-medical',
      icon: React.createElement(Brain, { size: L }),
      label: 'AI & 의료 지능',
      children: [
        { key: '/ai-environment', icon: React.createElement(RobotOutlined), label: 'AI 분석환경' },
        { key: '/ner', icon: React.createElement(FileTextOutlined), label: '비정형 구조화' },
        { key: '/ontology', icon: React.createElement(DeploymentUnitOutlined), label: '의료 온톨로지' },
        { key: '/medical-knowledge', icon: React.createElement(MedicineBoxOutlined), label: '의학 지식' },
      ],
    },
    {
      key: 'system-ops',
      icon: React.createElement(Wrench, { size: L }),
      label: '시스템 운영',
      children: [
        { key: '/ai-ops', icon: React.createElement(SettingOutlined), label: 'AI 운영관리' },
        { key: '/ai-architecture', icon: React.createElement(ClusterOutlined), label: 'AI 아키텍처' },
        { key: '/portal-ops', icon: React.createElement(SettingOutlined), label: '포털 운영관리' },
      ],
    },
  ];

  const allowed = ROLE_MENU_KEYS[role || 'admin'] || ROLE_MENU_KEYS.admin;
  if (allowed.has('all')) return allItems;
  return allItems.filter(item => allowed.has(item.key));
}

// 사용자 메뉴 아이템
export const userMenuItems: MenuProps['items'] = [
  { key: 'profile', icon: React.createElement(UserOutlined), label: '프로필' },
  { key: 'settings', icon: React.createElement(SettingOutlined), label: '설정' },
  { type: 'divider' },
  { key: 'logout', icon: React.createElement(LogoutOutlined), label: '로그아웃', danger: true },
];
