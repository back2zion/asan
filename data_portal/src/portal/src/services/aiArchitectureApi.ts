/**
 * AI Architecture API (AAR-002)
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
  name: string;
  category: string;
  backend_service: string;
  data_source: string;
  status: string;
}

export const aiArchitectureApi = {
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
};
