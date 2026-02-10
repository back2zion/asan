/**
 * MedicalNER 페이지 테스트
 * NER 분석 탭 렌더링, 분석 버튼 동작, 엔티티 표시, 서비스 장애 처리
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { App as AntApp } from 'antd';

import MedicalNER from './MedicalNER';

const renderWithAntd = (ui: React.ReactElement) =>
  render(<AntApp>{ui}</AntApp>);

describe('MedicalNER Page', () => {
  beforeEach(() => {
    // Mock NER health check — healthy by default
    vi.stubGlobal('fetch', vi.fn().mockImplementation((url: string) => {
      if (typeof url === 'string' && url.includes('/ner/health')) {
        return Promise.resolve({
          ok: true,
          json: async () => ({ status: 'healthy', device: 'cuda:0' }),
        });
      }
      if (typeof url === 'string' && url.includes('/unstructured/jobs')) {
        return Promise.resolve({ ok: true, json: async () => [] });
      }
      if (typeof url === 'string' && url.includes('/unstructured/stats')) {
        return Promise.resolve({ ok: true, json: async () => ({ total_jobs: 0, by_type: {}, by_status: {}, omop_records: {}, recent_jobs: [] }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    }));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('renders page title', () => {
    renderWithAntd(<MedicalNER />);
    expect(screen.getByText('비정형 데이터 구조화')).toBeInTheDocument();
  });

  it('renders all 4 tabs', () => {
    renderWithAntd(<MedicalNER />);
    expect(screen.getByText('NER 분석')).toBeInTheDocument();
    expect(screen.getByText('임상노트 구조화')).toBeInTheDocument();
    expect(screen.getByText('DICOM 파싱')).toBeInTheDocument();
    expect(screen.getByText('처리 이력')).toBeInTheDocument();
  });

  it('shows GPU connection status', async () => {
    renderWithAntd(<MedicalNER />);
    await waitFor(() => {
      expect(screen.getByText('BioClinicalBERT GPU 연결됨')).toBeInTheDocument();
    });
  });

  it('shows GPU disconnected when health fails', async () => {
    vi.mocked(fetch).mockImplementation((url: string) => {
      if (typeof url === 'string' && url.includes('/ner/health')) {
        return Promise.resolve({
          ok: false,
          json: async () => ({}),
        });
      }
      return Promise.resolve({ ok: true, json: async () => ({}) });
    });
    renderWithAntd(<MedicalNER />);
    await waitFor(() => {
      expect(screen.getByText(/GPU 미연결/)).toBeInTheDocument();
    });
  });

  it('renders sample text buttons', () => {
    renderWithAntd(<MedicalNER />);
    expect(screen.getByText('당뇨 진료기록')).toBeInTheDocument();
    expect(screen.getByText('영상의학 소견')).toBeInTheDocument();
    expect(screen.getByText('심장내과 경과기록')).toBeInTheDocument();
    expect(screen.getByText('혈액검사 결과')).toBeInTheDocument();
  });

  it('NER 분석 button is disabled when text is empty', () => {
    renderWithAntd(<MedicalNER />);
    const button = screen.getByRole('button', { name: /NER 분석 실행/ });
    expect(button).toBeDisabled();
  });

  it('renders entity legend', () => {
    renderWithAntd(<MedicalNER />);
    expect(screen.getByText('진단')).toBeInTheDocument();
    expect(screen.getByText('약물')).toBeInTheDocument();
    expect(screen.getByText('검사')).toBeInTheDocument();
    expect(screen.getByText('시술')).toBeInTheDocument();
    expect(screen.getByText('인물')).toBeInTheDocument();
  });

  it('shows placeholder text in NER tab', () => {
    renderWithAntd(<MedicalNER />);
    expect(screen.getByText(/NER 분석 실행" 버튼을 클릭하세요/)).toBeInTheDocument();
  });
});
