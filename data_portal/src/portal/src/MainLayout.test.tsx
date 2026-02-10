/**
 * MainLayout 컴포넌트 테스트 (Vitest)
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { App as AntApp } from 'antd';
import { SettingsProvider } from './contexts/SettingsContext';
import { AuthProvider } from './contexts/AuthContext';
import MainLayout from './components/Layout/MainLayout';

// Mock heavy sub-components that cause import issues
vi.mock('./components/ai/AIAssistantPanel', () => ({
  default: () => null,
}));

vi.mock('./components/consent/ConsentModal', () => ({
  default: () => null,
}));

vi.mock('./components/Layout/ResultsOverlay', () => ({
  default: () => null,
}));

// Mock external API calls
vi.mock('./services/api', () => ({
  semanticApi: { search: vi.fn() },
  sanitizeText: (t: string) => t,
}));

vi.mock('./services/catalogExtApi', () => ({
  catalogExtApi: {
    search: vi.fn().mockResolvedValue({ results: [] }),
    getRecentSearches: vi.fn().mockResolvedValue({ searches: [] }),
  },
}));

const renderWithProviders = () =>
  render(
    <MemoryRouter>
      <AuthProvider>
        <SettingsProvider>
          <AntApp>
            <MainLayout />
          </AntApp>
        </SettingsProvider>
      </AuthProvider>
    </MemoryRouter>
  );

describe('MainLayout Component', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({}),
    }));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('renders without crashing', () => {
    const { container } = renderWithProviders();
    expect(container).toBeDefined();
  });

  it('renders the header with logo', () => {
    renderWithProviders();
    // MainLayout renders an img with alt="서울아산병원"
    expect(screen.getByAltText('서울아산병원')).toBeInTheDocument();
  });
});
