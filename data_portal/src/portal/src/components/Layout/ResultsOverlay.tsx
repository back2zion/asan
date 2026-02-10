/**
 * AI 조회 결과 오버레이 컴포넌트
 * MainLayout의 Content 영역 위에 표시되는 AI 쿼리 결과 테이블
 */

import React from 'react';
import { Button, Space, Typography } from 'antd';
import { TableOutlined, CloseOutlined } from '@ant-design/icons';

const { Text } = Typography;

export interface PromotedResults {
  columns: string[];
  results: any[][];
  query: string;
}

interface ResultsOverlayProps {
  promotedResults: PromotedResults;
  onClose: () => void;
  collapsed: boolean;
  aiPanelVisible: boolean;
}

const ResultsOverlay: React.FC<ResultsOverlayProps> = ({
  promotedResults,
  onClose,
  collapsed,
  aiPanelVisible,
}) => {
  return (
    <div
      style={{
        position: 'fixed',
        top: 56 + 24,
        left: (collapsed ? 64 : 220) + 24,
        right: aiPanelVisible ? 380 + 24 : 24,
        zIndex: 900,
        transition: 'left 0.2s, right 0.2s',
      }}
    >
      <div
        style={{
          background: 'white',
          borderRadius: 8,
          boxShadow: '0 4px 24px rgba(0,0,0,0.15)',
          border: '1px solid #e8e8e8',
          maxHeight: 'calc(100vh - 56px - 48px)',
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        {/* 헤더 */}
        <div
          style={{
            padding: '12px 16px',
            borderBottom: '1px solid #f0f0f0',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            background: '#fafafa',
            borderRadius: '8px 8px 0 0',
          }}
        >
          <Space>
            <TableOutlined style={{ color: '#00A0B0' }} />
            <Text strong style={{ fontSize: 14 }}>AI 조회 결과</Text>
            <Text type="secondary" style={{ fontSize: 12 }}>
              ({promotedResults.results.length}건)
            </Text>
          </Space>
          <Button
            type="text"
            size="small"
            icon={<CloseOutlined />}
            onClick={onClose}
          />
        </div>
        {/* 질의 표시 */}
        <div style={{ padding: '8px 16px', background: '#f9fffe', borderBottom: '1px solid #f0f0f0' }}>
          <Text type="secondary" style={{ fontSize: 12 }}>질의: </Text>
          <Text style={{ fontSize: 12 }}>{promotedResults.query}</Text>
        </div>
        {/* 테이블 본체 */}
        <div style={{ overflow: 'auto', flex: 1 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ background: '#fafafa', position: 'sticky', top: 0, zIndex: 1 }}>
                {promotedResults.columns.map((col, ci) => (
                  <th
                    key={ci}
                    style={{
                      padding: '10px 14px',
                      textAlign: 'left',
                      borderBottom: '2px solid #e8e8e8',
                      whiteSpace: 'nowrap',
                      fontWeight: 600,
                    }}
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {promotedResults.results.map((row, ri) => (
                <tr key={ri} style={{ borderBottom: '1px solid #f0f0f0' }}>
                  {promotedResults.columns.map((_, ci) => (
                    <td key={ci} style={{ padding: '8px 14px' }}>
                      {row[ci] != null ? String(row[ci]) : '-'}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default ResultsOverlay;
