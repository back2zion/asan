/**
 * Dashboard 페이지 테스트 (Vitest)
 * 렌더링, 뷰 모드 전환, 시스템 상태
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App as AntApp } from 'antd';

// Mock lucide-react icons
vi.mock('lucide-react', () => ({
  Activity: () => <span data-testid="lucide-icon" />,
  Layers: () => <span data-testid="lucide-icon" />,
  Cpu: () => <span data-testid="lucide-icon" />,
  Network: () => <span data-testid="lucide-icon" />,
  Layout: () => <span data-testid="lucide-icon" />,
  Download: () => <span data-testid="lucide-icon" />,
  Database: () => <span data-testid="lucide-icon" />,
  Server: () => <span data-testid="lucide-icon" />,
  ServerCog: () => <span data-testid="lucide-icon" />,
  ShieldCheck: () => <span data-testid="lucide-icon" />,
  AlertTriangle: () => <span data-testid="lucide-icon" />,
  Wifi: () => <span data-testid="lucide-icon" />,
  ToggleLeft: () => <span data-testid="lucide-icon" />,
  ToggleRight: () => <span data-testid="lucide-icon" />,
  Search: () => <span data-testid="lucide-icon" />,
  X: () => <span data-testid="lucide-icon" />,
}));

// Mock SettingsContext
vi.mock('../contexts/SettingsContext', () => ({
  useSettings: () => ({
    settings: { darkMode: false, notificationsEnabled: true, autoRefresh: false, aiAutoOpen: false },
    updateSetting: vi.fn(),
  }),
}));

// Mock dashboard sub-components to simplify
vi.mock('../components/dashboard', () => ({
  API_BASE: '/api/v1',
  FALLBACK_QUALITY: [{ domain: 'Clinical', score: 95, total: 100, issues: 5 }],
  OperationalView: () => <div data-testid="operational-view">Operational</div>,
  ArchitectureView: () => <div data-testid="architecture-view">Architecture</div>,
  LakehouseView: () => <div data-testid="lakehouse-view">Lakehouse</div>,
  DrillDownModal: () => null,
  LayoutModal: () => null,
  ReportModal: () => null,
  exportReport: vi.fn(),
}));

import { Dashboard } from './Dashboard';

const renderWithAntd = (ui: React.ReactElement) =>
  render(<AntApp>{ui}</AntApp>);

describe('Dashboard Page', () => {
  beforeEach(() => {
    localStorage.clear();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({}),
    }));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    localStorage.clear();
  });

  it('renders page title', () => {
    renderWithAntd(<Dashboard />);
    expect(screen.getByText(/플랫폼 현황/)).toBeInTheDocument();
  });

  it('renders System Online banner', () => {
    renderWithAntd(<Dashboard />);
    expect(screen.getByText('System Online')).toBeInTheDocument();
  });

  it('defaults to LAKEHOUSE view', () => {
    renderWithAntd(<Dashboard />);
    expect(screen.getByTestId('lakehouse-view')).toBeInTheDocument();
  });

  it('switches to OPERATIONAL view on click', async () => {
    const user = userEvent.setup();
    renderWithAntd(<Dashboard />);
    await user.click(screen.getByText('운영 뷰'));
    expect(screen.getByTestId('operational-view')).toBeInTheDocument();
  });

  it('switches to ARCHITECTURE view on click', async () => {
    const user = userEvent.setup();
    renderWithAntd(<Dashboard />);
    await user.click(screen.getByText('아키텍처 뷰'));
    expect(screen.getByTestId('architecture-view')).toBeInTheDocument();
  });

  it('renders layout and report buttons', () => {
    renderWithAntd(<Dashboard />);
    expect(screen.getByText('레이아웃 편집')).toBeInTheDocument();
    expect(screen.getByText('리포트 내보내기')).toBeInTheDocument();
  });

  it('shows CPU and RAM info', () => {
    renderWithAntd(<Dashboard />);
    expect(screen.getByText(/CPU:/)).toBeInTheDocument();
    expect(screen.getByText(/RAM:/)).toBeInTheDocument();
  });

  it('persists view mode to localStorage', async () => {
    const user = userEvent.setup();
    renderWithAntd(<Dashboard />);
    await user.click(screen.getByText('운영 뷰'));
    expect(localStorage.getItem('dashboard_viewMode')).toBe('OPERATIONAL');
  });
});
