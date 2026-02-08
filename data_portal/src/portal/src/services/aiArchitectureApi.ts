/**
 * AI Architecture API (AAR-002) — 운영 관리
 */

import { apiClient } from './apiUtils';

export interface SwComponent {
  id: string;
  name: string;
  type: string;
  description: string;
  version?: string;
  tools_count?: number;
  endpoint?: string;
  components?: Record<string, string>;
  nodes?: string[];
  checkpointer?: string;
  model?: string;
  runtime?: string;
  providers?: string[];
}

export interface GpuResource {
  id: string;
  name: string;
  type: string;
  device: string;
  model_loaded: string;
  memory_gb: number;
  port: number | string;
}

export interface ContainerStack {
  stack: string;
  compose: string;
  services: string[];
}

export interface ArchitectureOverview {
  sw_architecture: {
    components: SwComponent[];
    patterns: string[];
  };
  hw_infrastructure: {
    gpu_resources: GpuResource[];
    total_gpu_memory_gb: number;
  };
  container_infrastructure: {
    stacks: ContainerStack[];
    total_services: number;
    orchestration: string;
  };
}

export interface HealthCheck {
  overall: string;
  components: Record<string, { status: string; [key: string]: any }>;
  checked_at: string;
}

export interface ContainerInfo {
  name: string;
  status: string;
  ports: string;
  image: string;
}

export interface McpTool {
  id: number;
  name: string;
  category: string;
  description: string;
  backend_service: string;
  data_source: string;
  endpoint: string;
  enabled: boolean;
  status?: string;
  priority?: string;   // high / medium / low
  reference?: string;  // 참고 URL
  phase?: string;      // deployed / phase1 / phase2 / future
}

export interface McpSummary {
  total: number;
  enabled: number;
  by_phase: Record<string, number>;
  by_category: Record<string, number>;
}

export const aiArchitectureApi = {
  // ── 읽기 ──
  getOverview: async () => {
    const response = await apiClient.get('/ai-architecture/overview');
    return response.data as ArchitectureOverview;
  },
  getHealth: async () => {
    const response = await apiClient.get('/ai-architecture/health');
    return response.data as HealthCheck;
  },
  getContainers: async () => {
    const response = await apiClient.get('/ai-architecture/containers');
    return response.data as { containers: ContainerInfo[]; count: number };
  },
  getMcpTopology: async () => {
    const response = await apiClient.get('/ai-architecture/mcp-topology');
    return response.data;
  },

  // ── MCP 도구 CRUD ──
  createMcpTool: async (data: Partial<McpTool>) => {
    const response = await apiClient.post('/ai-architecture/mcp-tools', data);
    return response.data as McpTool;
  },
  updateMcpTool: async (toolId: number, data: Partial<McpTool>) => {
    const response = await apiClient.put(`/ai-architecture/mcp-tools/${toolId}`, data);
    return response.data as McpTool;
  },
  deleteMcpTool: async (toolId: number) => {
    const response = await apiClient.delete(`/ai-architecture/mcp-tools/${toolId}`);
    return response.data;
  },
  testMcpTool: async (toolId: number) => {
    const response = await apiClient.post(`/ai-architecture/mcp-tools/${toolId}/test`);
    return response.data as { success: boolean; latency_ms: number; error?: string; tool: string };
  },

  // ── 서비스 관리 ──
  testService: async (serviceId: string) => {
    const response = await apiClient.post(`/ai-architecture/services/${serviceId}/test`);
    return response.data as { success: boolean; latency_ms: number; error?: string; service: string };
  },
  updateService: async (serviceId: string, data: Partial<SwComponent>) => {
    const response = await apiClient.put(`/ai-architecture/services/${serviceId}`, data);
    return response.data;
  },

  // ── 컨테이너 관리 ──
  containerAction: async (name: string, action: 'restart' | 'stop' | 'start') => {
    const response = await apiClient.post(`/ai-architecture/containers/${name}/action?action=${action}`);
    return response.data as { success: boolean; container: string; action: string; output?: string; error?: string };
  },
  getContainerLogs: async (name: string, tail = 100) => {
    const response = await apiClient.get(`/ai-architecture/containers/${name}/logs?tail=${tail}`);
    return response.data as { container: string; logs: string; lines: number };
  },
};
