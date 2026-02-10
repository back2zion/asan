/**
 * AI 메시지 버블 컴포넌트
 * AIAssistantPanel 의 renderMessage 로직을 분리
 */

import React from 'react';
import { Button, Typography, Space, Collapse } from 'antd';
import {
  SearchOutlined,
  CodeOutlined,
  DatabaseOutlined,
  ExpandOutlined,
} from '@ant-design/icons';
import ReactMarkdown from 'react-markdown';
import { sanitizeHtml } from '../../services/api';
import ImageCell from '../common/ImageCell';
import { MINT, type Message, type ThinkingStep } from './chatConstants';

const { Text } = Typography;

interface MessageBubbleProps {
  message: Message;
  handleSuggestedAction: (action: Record<string, unknown>) => void;
}

const MessageBubble: React.FC<MessageBubbleProps> = ({ message, handleSuggestedAction }) => {
  const isUser = message.role === 'user';
  const isSystem = message.role === 'system';

  if (isSystem) {
    return (
      <div
        key={message.id}
        style={{
          display: 'flex',
          justifyContent: 'center',
          marginBottom: 12,
        }}
      >
        <div
          style={{
            maxWidth: '90%',
            padding: '8px 16px',
            borderRadius: 8,
            backgroundColor: MINT.LIGHT,
            border: `1px solid ${MINT.PRIMARY}`,
            fontSize: 13,
          }}
        >
          <div style={{ color: MINT.DARK, fontStyle: 'italic' }}>
            <ReactMarkdown
              components={{
                p: ({ children }) => <span>{children}</span>,
                strong: ({ children }) => (
                  <strong style={{ color: MINT.DARK }}>{children}</strong>
                ),
              }}
            >
              {message.content}
            </ReactMarkdown>
          </div>
          {message.enhancementConfidence && (
            <Text type="secondary" style={{ fontSize: 10, display: 'block', marginTop: 4 }}>
              신뢰도: {(message.enhancementConfidence * 100).toFixed(0)}%
            </Text>
          )}
        </div>
      </div>
    );
  }

  return (
    <div
      key={message.id}
      style={{
        display: 'flex',
        justifyContent: isUser ? 'flex-end' : 'flex-start',
        marginBottom: 12,
      }}
    >
      <div
        style={{
          maxWidth: '85%',
          padding: '10px 14px',
          borderRadius: isUser ? '16px 16px 4px 16px' : '16px 16px 16px 4px',
          backgroundColor: isUser ? MINT.PRIMARY : 'white',
          color: isUser ? 'white' : 'inherit',
          boxShadow: '0 1px 2px rgba(0,0,0,0.1)',
          overflow: 'hidden',
          wordBreak: 'break-word',
        }}
      >
        {/* 사고 과정 — assistant 메시지 상단에 접힌 상태로 표시 */}
        {!isUser && message.thinkingSteps && message.thinkingSteps.length > 0 && (
          <Collapse
            size="small"
            ghost
            style={{
              marginBottom: 8,
              marginLeft: -8,
              marginRight: -8,
              background: '#e8f4fd',
              borderRadius: 6,
              border: '1px solid #b3d9f2',
            }}
            items={[{
              key: 'thinking',
              label: (
                <span style={{ fontSize: 12, color: '#1677ff', fontWeight: 500, cursor: 'pointer' }}>
                  💭 사고 과정 보기
                </span>
              ),
              children: (
                <div style={{ fontSize: 12, lineHeight: 1.8 }}>
                  {message.thinkingSteps.map((step: ThinkingStep, si: number) => {
                    const iconMap: Record<string, React.ReactNode> = { search: <SearchOutlined />, code: <CodeOutlined />, database: <DatabaseOutlined /> };
                    return (
                      <div key={si} style={{ marginBottom: 10 }}>
                        <div style={{ fontWeight: 600, color: '#333', marginBottom: 2 }}>
                          {iconMap[step.icon]} {step.label}
                        </div>
                        <pre style={{
                          fontSize: 11,
                          background: '#f5f5f5',
                          padding: '6px 8px',
                          borderRadius: 4,
                          margin: 0,
                          whiteSpace: 'pre-wrap',
                          wordBreak: 'break-all',
                          color: '#555',
                        }}>
                          {step.detail}
                        </pre>
                      </div>
                    );
                  })}
                </div>
              ),
            }]}
          />
        )}
        {isUser ? (
          <Text style={{ color: 'white' }}>{message.content}</Text>
        ) : (
          <div className="ai-message-content" style={{ overflow: 'hidden', wordBreak: 'break-word' }}>
            <ReactMarkdown
              components={{
                a: ({ href, children }) => (
                  <a
                    href={href}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{ color: MINT.PRIMARY }}
                  >
                    {children}
                  </a>
                ),
                img: ({ src, alt }) => (
                  <img
                    src={src}
                    alt={alt || 'image'}
                    style={{ maxWidth: '100%', borderRadius: 8, margin: '8px 0', display: 'block' }}
                    onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }}
                  />
                ),
              }}
            >
              {sanitizeHtml(message.content)}
            </ReactMarkdown>
          </div>
        )}

        {/* tool_results 렌더링 — SQL 결과 미니 테이블 + 중앙 화면 전송 */}
        {message.toolResults && message.toolResults.length > 0 && (
          <div style={{ marginTop: 8 }}>
            {message.toolResults.map((tr, trIdx) => {
              const columns = (tr as any).columns as string[] | undefined;
              const results = (tr as any).results as any[][] | undefined;
              if (!columns || !results || results.length === 0) return null;
              return (
                <div key={trIdx} style={{ maxHeight: 200, overflow: 'auto', marginBottom: 8, maxWidth: '100%' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 11, tableLayout: 'auto' }}>
                    <thead>
                      <tr style={{ background: '#fafafa', borderBottom: '1px solid #d9d9d9' }}>
                        {columns.map((col, ci) => (
                          <th key={ci} style={{ padding: '4px 6px', textAlign: 'left', border: '1px solid #e8e8e8', whiteSpace: 'nowrap' }}>
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {results.slice(0, 5).map((row, ri) => (
                        <tr key={ri}>
                          {columns.map((_, ci) => (
                            <td key={ci} style={{ padding: '4px 6px', border: '1px solid #e8e8e8' }}>
                              <ImageCell value={row[ci]} />
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {results.length > 5 && (
                    <div style={{ textAlign: 'center', fontSize: 11, color: '#999', marginTop: 4 }}>
                      ... 외 {results.length - 5}건
                    </div>
                  )}
                  {/* 중앙 화면에 표출 버튼 */}
                  <Button
                    size="small"
                    type="link"
                    icon={<ExpandOutlined />}
                    style={{ padding: 0, fontSize: 11, color: MINT.PRIMARY, marginTop: 4 }}
                    onClick={() => {
                      window.dispatchEvent(new CustomEvent('ai:show-results', {
                        detail: { columns, results, query: message.content },
                      }));
                    }}
                  >
                    중앙 화면에 표출
                  </Button>
                </div>
              );
            })}
          </div>
        )}

        {message.suggestedActions && message.suggestedActions.length > 0 && (
          <div style={{ marginTop: 8 }}>
            <Space wrap>
              {message.suggestedActions.map((action, index) => (
                <Button
                  key={index}
                  size="small"
                  type="link"
                  onClick={() => handleSuggestedAction(action)}
                  style={{ padding: '0 4px', height: 'auto', color: MINT.PRIMARY }}
                >
                  {(action.label as string) || (action.action as string)}
                </Button>
              ))}
            </Space>
          </div>
        )}

        <Text
          type="secondary"
          style={{
            display: 'block',
            fontSize: 11,
            marginTop: 4,
            textAlign: isUser ? 'right' : 'left',
            color: isUser ? 'rgba(255,255,255,0.7)' : undefined,
          }}
        >
          {message.timestamp.toLocaleTimeString('ko-KR', {
            hour: '2-digit',
            minute: '2-digit',
          })}
        </Text>
      </div>
    </div>
  );
};

export default MessageBubble;
