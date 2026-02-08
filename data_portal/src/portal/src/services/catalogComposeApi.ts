/**
 * Catalog Compose API (DPR-002: 드래그앤드롭 데이터 조합)
 */

import { apiClient } from './apiUtils';

export interface FkRelationship {
  from_table: string;
  from_column: string;
  to_table: string;
  to_column: string;
}

export interface JoinInfo {
  left_table: string;
  left_column: string;
  right_table: string;
  right_column: string;
  join_type: string;
}

export const catalogComposeApi = {
  getFkRelationships: async () => {
    const response = await apiClient.get('/catalog-compose/fk-relationships');
    return response.data;
  },

  detectJoins: async (tables: string[]) => {
    const response = await apiClient.post('/catalog-compose/detect-joins', { tables });
    return response.data;
  },

  generateSql: async (data: {
    tables: string[];
    selected_columns?: Record<string, string[]>;
    joins?: JoinInfo[];
    limit?: number;
  }) => {
    const response = await apiClient.post('/catalog-compose/generate-sql', data);
    return response.data;
  },

  preview: async (sql: string, limit = 20) => {
    const response = await apiClient.post('/catalog-compose/preview', { sql, limit });
    return response.data;
  },
};
