import React from 'react';
import { render, screen } from '@testing-library/react';
import { Dashboard } from './Dashboard.tsx';

// Mock lucide-react icons
jest.mock('lucide-react', () => ({
  Server: () => <div data-testid="icon" />,
  Database: () => <div data-testid="icon" />,
  Activity: () => <div data-testid="icon" />,
  ShieldCheck: () => <div data-testid="icon" />,
  AlertTriangle: () => <div data-testid="icon" />,
  Cpu: () => <div data-testid="icon" />,
  Network: () => <div data-testid="icon" />,
  Layout: () => <div data-testid="icon" />,
  Download: () => <div data-testid="icon" />,
  ServerCog: () => <div data-testid="icon" />,
  Wifi: () => <div data-testid="icon" />,
  ToggleLeft: () => <div data-testid="icon" />,
  ToggleRight: () => <div data-testid="icon" />,
  Layers: () => <div data-testid="icon" />,
  Search: () => <div data-testid="icon" />,
  X: () => <div data-testid="icon" />,
}));


describe('Dashboard Component', () => {
  it('should render with correct Tailwind CSS classes', () => {
    render(<Dashboard />);

    // Check for a main container with Tailwind classes
    const mainContainer = screen.getByText('플랫폼 현황 (Dashboard)').parentElement?.parentElement;
    expect(mainContainer).toHaveClass('p-6', 'space-y-6');

    // Check for the stats grid container
    const statsGrid = screen.getByText('데이터 레이크').closest('.grid');
    expect(statsGrid).toHaveClass('grid', 'grid-cols-1', 'md:grid-cols-2', 'lg:grid-cols-4');
  });
});
