import React from 'react';

export interface StreamEvent {
  event_type: string;
  data: any;
  timestamp: string;
  session_id: string;
}

export interface Message {
  id: string;
  content: string;
  sender: 'user' | 'ai';
  timestamp: string;
  metadata?: any;
  tool_results?: any[];
}

export interface StreamingMedicalChatProps {
  sessionId?: string;
  patientId?: string;
  userType?: 'patient' | 'doctor' | 'researcher';
  onSessionUpdate?: (sessionData: any) => void;
}

export interface StreamChatInputProps {
  onSend: (msg: string) => void;
  isStreaming: boolean;
}

export const STORAGE_KEY = 'asan_chat_messages';
export const SESSION_KEY = 'asan_chat_session_id';

// Shared markdown component renderers
export const markdownComponents: any = {
  p: ({ children }: any) => React.createElement('p', { style: { marginBottom: '8px', lineHeight: '1.6' } }, children),
  code: ({ children, className }: any) => (
    className ? (
      React.createElement('pre', {
        style: {
          background: '#f5f5f5',
          padding: '8px 12px',
          borderRadius: '6px',
          fontSize: '12px',
          overflowX: 'auto',
          border: '1px solid #e0e0e0',
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-all',
          maxWidth: '100%'
        }
      }, React.createElement('code', null, children))
    ) : (
      React.createElement('code', {
        style: {
          background: '#f5f5f5',
          padding: '2px 6px',
          borderRadius: '4px',
          fontSize: '13px',
          border: '1px solid #e0e0e0'
        }
      }, children)
    )
  ),
  ul: ({ children }: any) => React.createElement('ul', { style: { marginBottom: '8px', paddingLeft: '20px' } }, children),
  ol: ({ children }: any) => React.createElement('ol', { style: { marginBottom: '8px', paddingLeft: '20px' } }, children),
  li: ({ children }: any) => React.createElement('li', { style: { marginBottom: '4px' } }, children),
  h1: ({ children }: any) => React.createElement('h3', { style: { color: '#1a5d3a', marginBottom: '8px' } }, children),
  h2: ({ children }: any) => React.createElement('h4', { style: { color: '#1a5d3a', marginBottom: '6px' } }, children),
  h3: ({ children }: any) => React.createElement('h5', { style: { color: '#1a5d3a', marginBottom: '6px' } }, children),
  strong: ({ children }: any) => React.createElement('strong', { style: { color: '#1a5d3a' } }, children),
  blockquote: ({ children }: any) => (
    React.createElement('blockquote', {
      style: {
        borderLeft: '4px solid #1a5d3a',
        paddingLeft: '12px',
        margin: '8px 0',
        fontStyle: 'italic',
        background: '#f9f9f9',
        padding: '8px 12px',
        borderRadius: '0 4px 4px 0'
      }
    }, children)
  ),
  table: ({ children }: any) => (
    React.createElement('div', { style: { overflowX: 'auto', marginBottom: '12px' } },
      React.createElement('table', {
        style: {
          width: '100%',
          borderCollapse: 'collapse',
          fontSize: '13px',
          border: '1px solid #e0e0e0',
          borderRadius: '6px',
        }
      }, children)
    )
  ),
  thead: ({ children }: any) => (
    React.createElement('thead', { style: { background: '#f0f9f4' } }, children)
  ),
  tbody: ({ children }: any) => React.createElement('tbody', null, children),
  tr: ({ children }: any) => (
    React.createElement('tr', { style: { borderBottom: '1px solid #e8e8e8' } }, children)
  ),
  th: ({ children }: any) => (
    React.createElement('th', {
      style: {
        padding: '8px 12px',
        textAlign: 'left' as const,
        fontWeight: 600,
        color: '#1a5d3a',
        borderBottom: '2px solid #1a5d3a',
        whiteSpace: 'nowrap'
      }
    }, children)
  ),
  td: ({ children }: any) => (
    React.createElement('td', {
      style: {
        padding: '6px 12px',
        borderBottom: '1px solid #f0f0f0'
      }
    }, children)
  ),
};
