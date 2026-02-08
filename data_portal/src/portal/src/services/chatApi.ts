/**
 * AI Assistant (Chat) API
 */

import { apiClient, sanitizeText } from './apiUtils';
import type { ChatRequest, ChatResponse } from './apiUtils';

export const chatApi = {
  // 대화 전송
  sendMessage: async (request: ChatRequest): Promise<ChatResponse> => {
    const sanitizedRequest = {
      ...request,
      message: sanitizeText(request.message),
    };
    const response = await apiClient.post<ChatResponse>('/chat', sanitizedRequest);
    return response.data;
  },

  // 세션 목록 조회
  getSessions: async (userId: string) => {
    const response = await apiClient.get('/chat/sessions', {
      params: { user_id: sanitizeText(userId) },
    });
    return response.data;
  },

  // 세션 상세 조회
  getSession: async (sessionId: string) => {
    const response = await apiClient.get(`/chat/sessions/${encodeURIComponent(sessionId)}`);
    return response.data;
  },

  // 타임라인 조회
  getTimeline: async (sessionId: string) => {
    const response = await apiClient.get(`/chat/sessions/${encodeURIComponent(sessionId)}/timeline`);
    return response.data;
  },

  // 상태 복원
  restoreState: async (sessionId: string, messageId: string) => {
    const response = await apiClient.post(
      `/chat/sessions/${encodeURIComponent(sessionId)}/restore/${encodeURIComponent(messageId)}`
    );
    return response.data;
  },
};
