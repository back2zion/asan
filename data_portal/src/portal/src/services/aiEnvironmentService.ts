/**
 * SFR-006 AI 분석환경 API 서비스
 * 백엔드 API와의 통신을 담당
 */

import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/v1';

// API 클라이언트 설정
const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  }
});

// 요청 인터셉터 (인증 토큰 + CSRF 토큰 추가)
apiClient.interceptors.request.use((config) => {
  const token = localStorage.getItem('idp_access_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  const csrfToken = document.cookie
    .split('; ')
    .find((row) => row.startsWith('csrf_token='))
    ?.split('=')[1];
  if (csrfToken) {
    config.headers['X-CSRF-Token'] = csrfToken;
  }
  return config;
});

// === 타입 정의 ===

export interface ResourceSpec {
  cpu_cores: number;
  memory_gb: number;
  gpu_count: number;
  disk_gb: number;
}

export interface ContainerCreateRequest {
  name: string;
  description?: string;
  resources: ResourceSpec;
  template_id?: string;
  environment_type?: string;
}

export interface ContainerResponse {
  id: string;
  name: string;
  description?: string;
  status: 'creating' | 'running' | 'stopped' | 'error' | 'terminating';
  resources: ResourceSpec;
  created_at: string;
  updated_at: string;
  uptime_seconds: number;
  access_url?: string;
  user_id: string;
  template_id?: string;
}

export interface ResourceMetrics {
  timestamp: string;
  cpu_usage_percent: number;
  memory_usage_gb: number;
  memory_usage_percent: number;
  gpu_usage_percent?: number;
  gpu_memory_usage_gb?: number;
  disk_usage_gb: number;
  disk_usage_percent: number;
}

export interface SystemResourceMetrics {
  cpu: {
    total_cores: number;
    used_cores: number;
    utilization_percent: number;
  };
  memory: {
    total_gb: number;
    used_gb: number;
    utilization_percent: number;
  };
  gpu: {
    total_count: number;
    used_count: number;
    utilization_percent: number;
  };
  timestamp: string;
}

export interface AnalysisTemplate {
  id: string;
  name: string;
  description: string;
  category: string;
  notebook_path: string;
  required_packages: string[];
  estimated_runtime_minutes: number;
  created_at: string;
}

// === API 서비스 클래스 ===

export class AIEnvironmentService {
  private static instance: AIEnvironmentService;

  static getInstance(): AIEnvironmentService {
    if (!AIEnvironmentService.instance) {
      AIEnvironmentService.instance = new AIEnvironmentService();
    }
    return AIEnvironmentService.instance;
  }

  // === Container Management ===

  /**
   * 사용자의 컨테이너 목록 조회
   */
  async getContainers(): Promise<ContainerResponse[]> {
    try {
      const response = await apiClient.get('/ai-environment/containers');
      return response.data;
    } catch (error) {
      throw new Error('컨테이너 목록을 불러오는데 실패했습니다.');
    }
  }

  /**
   * 새로운 분석 환경 컨테이너 생성
   */
  async createContainer(request: ContainerCreateRequest): Promise<ContainerResponse> {
    try {
      const response = await apiClient.post('/ai-environment/containers', request);
      return response.data;
    } catch (error: any) {
      if (error.response?.status === 400) {
        throw new Error(error.response.data.detail || '리소스가 부족합니다.');
      }
      
      throw new Error('컨테이너 생성에 실패했습니다.');
    }
  }

  /**
   * 특정 컨테이너 상세 조회
   */
  async getContainer(containerId: string): Promise<ContainerResponse> {
    try {
      const response = await apiClient.get(`/ai-environment/containers/${containerId}`);
      return response.data;
    } catch (error: any) {
      if (error.response?.status === 404) {
        throw new Error('컨테이너를 찾을 수 없습니다.');
      }
      
      throw new Error('컨테이너 정보를 불러오는데 실패했습니다.');
    }
  }

  /**
   * 컨테이너 시작
   */
  async startContainer(containerId: string): Promise<{ message: string; access_url: string }> {
    try {
      const response = await apiClient.post(`/ai-environment/containers/${containerId}/start`);
      return response.data;
    } catch (error: any) {
      if (error.response?.status === 400) {
        throw new Error(error.response.data.detail || '컨테이너를 시작할 수 없습니다.');
      }
      
      throw new Error('컨테이너 시작에 실패했습니다.');
    }
  }

  /**
   * 컨테이너 중지
   */
  async stopContainer(containerId: string): Promise<{ message: string }> {
    try {
      const response = await apiClient.post(`/ai-environment/containers/${containerId}/stop`);
      return response.data;
    } catch (error: any) {
      throw new Error('컨테이너 중지에 실패했습니다.');
    }
  }

  /**
   * 컨테이너 삭제
   */
  async deleteContainer(containerId: string): Promise<{ message: string }> {
    try {
      const response = await apiClient.delete(`/ai-environment/containers/${containerId}`);
      return response.data;
    } catch (error: any) {
      throw new Error('컨테이너 삭제에 실패했습니다.');
    }
  }

  // === Resource Monitoring ===

  /**
   * 실제 시스템 리소스 정보 조회
   */
  async getRealSystemInfo(): Promise<any> {
    try {
      const response = await apiClient.get('/ai-environment/resources/system-info');
      return response.data;
    } catch (error) {
      throw new Error('실제 시스템 리소스 정보를 불러오는데 실패했습니다.');
    }
  }

  /**
   * 시스템 리소스 현황 조회 (기존)
   */
  async getSystemResources(): Promise<SystemResourceMetrics> {
    try {
      const response = await apiClient.get('/ai-environment/resources/metrics');
      return response.data;
    } catch (error) {
      throw new Error('시스템 리소스 정보를 불러오는데 실패했습니다.');
    }
  }

  /**
   * 특정 컨테이너의 리소스 메트릭
   */
  async getContainerMetrics(containerId: string): Promise<ResourceMetrics> {
    try {
      const response = await apiClient.get(`/ai-environment/containers/${containerId}/metrics`);
      return response.data;
    } catch (error: any) {
      if (error.response?.status === 404) {
        throw new Error('컨테이너를 찾을 수 없습니다.');
      }
      
      throw new Error('컨테이너 메트릭을 불러오는데 실패했습니다.');
    }
  }

  // === Template Management ===

  /**
   * 분석 템플릿 목록 조회
   */
  async getTemplates(category?: string): Promise<AnalysisTemplate[]> {
    try {
      const params = category ? { category } : {};
      const response = await apiClient.get('/ai-environment/templates', { params });
      return response.data;
    } catch (error) {
      throw new Error('템플릿 목록을 불러오는데 실패했습니다.');
    }
  }

  /**
   * 특정 템플릿 상세 조회
   */
  async getTemplate(templateId: string): Promise<AnalysisTemplate> {
    try {
      const response = await apiClient.get(`/ai-environment/templates/${templateId}`);
      return response.data;
    } catch (error: any) {
      if (error.response?.status === 404) {
        throw new Error('템플릿을 찾을 수 없습니다.');
      }
      
      throw new Error('템플릿 정보를 불러오는데 실패했습니다.');
    }
  }

  // === Helper Methods ===

  /**
   * 컨테이너 상태를 한국어로 변환
   */
  getStatusText(status: string): string {
    const statusMap: { [key: string]: string } = {
      creating: '생성중',
      running: '실행중', 
      stopped: '중지됨',
      error: '오류',
      terminating: '종료중'
    };
    return statusMap[status] || status;
  }

  /**
   * 업타임을 사람이 읽기 쉬운 형태로 변환
   */
  formatUptime(uptimeSeconds: number): string {
    if (uptimeSeconds < 60) {
      return `${uptimeSeconds}초`;
    } else if (uptimeSeconds < 3600) {
      const minutes = Math.floor(uptimeSeconds / 60);
      return `${minutes}분`;
    } else if (uptimeSeconds < 86400) {
      const hours = Math.floor(uptimeSeconds / 3600);
      const minutes = Math.floor((uptimeSeconds % 3600) / 60);
      return `${hours}시간 ${minutes}분`;
    } else {
      const days = Math.floor(uptimeSeconds / 86400);
      const hours = Math.floor((uptimeSeconds % 86400) / 3600);
      return `${days}일 ${hours}시간`;
    }
  }

  /**
   * 바이트를 GB로 변환
   */
  bytesToGB(bytes: number): string {
    return (bytes / (1024 * 1024 * 1024)).toFixed(1);
  }

  /**
   * 리소스 사용률에 따른 색상 반환
   */
  getUtilizationColor(utilization: number): string {
    if (utilization >= 90) return '#ff4d4f';  // 위험 (빨간색)
    if (utilization >= 70) return '#fa8c16';  // 주의 (주황색)
    if (utilization >= 50) return '#fadb14';  // 보통 (노란색)
    return '#52c41a';  // 좋음 (녹색)
  }
}

// 싱글톤 인스턴스 내보내기
export const aiEnvironmentService = AIEnvironmentService.getInstance();