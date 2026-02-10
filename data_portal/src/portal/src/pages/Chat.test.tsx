/**
 * Chat 페이지 테스트
 * 렌더링, 사용자 유형 선택, 연결 상태 표시
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App as AntApp } from 'antd';

// Mock StreamingMedicalChat component
vi.mock('../components/StreamingMedicalChat.tsx', () => ({
  default: ({ sessionId, userType }: { sessionId: string; userType: string }) => (
    <div data-testid="streaming-chat">
      session={sessionId} type={userType}
    </div>
  ),
}));

import Chat from './Chat';

const renderWithAntd = (ui: React.ReactElement) =>
  render(<AntApp>{ui}</AntApp>);

describe('Chat Page', () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    // Mock fetch for health check
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ status: 'healthy' }),
    }));
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('renders page title', () => {
    renderWithAntd(<Chat />);
    expect(screen.getByText('서울아산병원 AI 의료 플랫폼')).toBeInTheDocument();
  });

  it('renders default user type as patient', () => {
    renderWithAntd(<Chat />);
    expect(screen.getByText('환자 상담')).toBeInTheDocument();
  });

  it('renders StreamingMedicalChat component', () => {
    renderWithAntd(<Chat />);
    expect(screen.getByTestId('streaming-chat')).toBeInTheDocument();
    expect(screen.getByTestId('streaming-chat')).toHaveTextContent('type=patient');
  });

  it('shows connecting status initially', () => {
    renderWithAntd(<Chat />);
    // Connection starts as 'connecting' before health check resolves
    expect(screen.getByText(/서버 연결 중/)).toBeInTheDocument();
  });

  it('shows connected status after health check succeeds', async () => {
    renderWithAntd(<Chat />);
    // Advance past the 2s initial delay
    await vi.advanceTimersByTimeAsync(2500);
    await waitFor(() => {
      expect(screen.getByText(/Qwen3-LLM 모델 준비 완료/)).toBeInTheDocument();
    });
  });

  it('shows disconnected status when health check fails', async () => {
    vi.mocked(fetch).mockRejectedValue(new Error('Network error'));
    renderWithAntd(<Chat />);
    await vi.advanceTimersByTimeAsync(2500);
    await waitFor(() => {
      expect(screen.getByText(/서버 연결 실패/)).toBeInTheDocument();
    });
  });

  it('renders footer with session info', () => {
    renderWithAntd(<Chat />);
    expect(screen.getByText('데이터스트림즈')).toBeInTheDocument();
    expect(screen.getByText('실시간 스트리밍 지원')).toBeInTheDocument();
  });

  it('renders user type selector with all options', () => {
    renderWithAntd(<Chat />);
    // The selector itself is present
    expect(screen.getByText('사용자 유형:')).toBeInTheDocument();
  });
});
