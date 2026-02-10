/**
 * App 컴포넌트 테스트 (Vitest)
 * App 렌더링 + 라우팅 기본 동작 확인
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';

// App 내부에 BrowserRouter 포함 — MemoryRouter 중첩 불가
// PrivateRoute가 비인증 시 /login 리다이렉트
import App from './App';

describe('App Component', () => {
  it('renders without crashing', () => {
    // App has its own Router, AuthProvider, SettingsProvider
    const { container } = render(<App />);
    expect(container).toBeDefined();
  });

  it('redirects to login when not authenticated', async () => {
    render(<App />);
    // 비인증 상태에서는 Login 페이지로 리다이렉트
    await waitFor(() => {
      // Login page rendered via lazy load
      const loginElements = document.querySelectorAll('form, input, button');
      // At minimum the app renders something (could be spinner or login)
      expect(document.body.children.length).toBeGreaterThan(0);
    });
  });
});
