import React from 'react';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import MainLayout from './components/Layout/MainLayout';

describe('MainLayout Component Test', () => {
  beforeEach(() => {
    render(
      <MemoryRouter>
        <MainLayout />
      </MemoryRouter>
    );
  });

  test('renders menu items', () => {
    // Check for key menu item labels
    expect(screen.getByText(/데이터 관리/)).toBeInTheDocument();
    expect(screen.getByText(/분석/)).toBeInTheDocument();
  });

  test('renders group titles', () => {
    expect(screen.getByText('데이터 관리')).toBeInTheDocument();
    expect(screen.getByText('분석 & 시각화')).toBeInTheDocument();
    expect(screen.getByText('AI & 연구')).toBeInTheDocument();
  });

  test('renders the main title and user profile section in the header', () => {
    expect(screen.getByText('아산병원 IDP')).toBeInTheDocument();
    expect(screen.getByText('관리자')).toBeInTheDocument();
  });
});
