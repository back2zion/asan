import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import App from './App';

// Mocking the CommercialSolutionWrapper to avoid rendering iframes and external calls
jest.mock('./pages/CommercialSolutionWrapper', () => {
  return ({ solution }: { solution: string }) => (
    <div data-testid="commercial-wrapper">{solution}</div>
  );
});

// Mocking the AIEnvironment page to simplify test
jest.mock('./pages/AIEnvironment', () => {
  return () => <div data-testid="ai-environment-page">AI 분석 환경</div>;
});

// Mocking the CDWResearch page
jest.mock('./pages/CDWResearch', () => {
  return () => <div data-testid="cdw-research-page">CDW 연구</div>;
});

// Mocking the Home page
jest.mock('./pages/Home', () => {
    return () => <div data-testid="home-page">홈</div>;
});


describe('App Routing Test', () => {
  const renderApp = (initialRoute: string) => {
    render(
      <MemoryRouter initialEntries={[initialRoute]}>
        <App />
      </MemoryRouter>
    );
  };

  test('renders Home page on /home route', async () => {
    renderApp('/home');
    await waitFor(() => {
      expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });
  });

  test('renders DataMart page on /datamart route', async () => {
    renderApp('/datamart');
    await waitFor(() => {
        const wrapper = screen.getByTestId('commercial-wrapper');
        expect(wrapper).toBeInTheDocument();
        expect(wrapper).toHaveTextContent('TeraONE');
    });
  });

  test('renders BI page on /bi route', async () => {
    renderApp('/bi');
    await waitFor(() => {
        const wrapper = screen.getByTestId('commercial-wrapper');
        expect(wrapper).toBeInTheDocument();
        expect(wrapper).toHaveTextContent('BIMatrixBI');
    });
  });

  test('renders OLAP page on /olap route', async () => {
    renderApp('/olap');
    await waitFor(() => {
        const wrapper = screen.getByTestId('commercial-wrapper');
        expect(wrapper).toBeInTheDocument();
        expect(wrapper).toHaveTextContent('BIMatrixOLAP');
    });
  });

  test('renders ETL page on /etl route', async () => {
    renderApp('/etl');
    await waitFor(() => {
        const wrapper = screen.getByTestId('commercial-wrapper');
        expect(wrapper).toBeInTheDocument();
        expect(wrapper).toHaveTextContent('Terastream');
    });
  });

  test('renders AI Environment page on /ai-environment route', async () => {
    renderApp('/ai-environment');
    await waitFor(() => {
      expect(screen.getByTestId('ai-environment-page')).toBeInTheDocument();
    });
  });

  test('renders CDW Research page on /cdw route', async () => {
    renderApp('/cdw');
    await waitFor(() => {
      expect(screen.getByTestId('cdw-research-page')).toBeInTheDocument();
    });
  });

  test('redirects from / to /home', async () => {
    renderApp('/');
    await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
    });
  });
});
