/**
 * 데이터 거버넌스 공용 헬퍼
 */

const API_BASE = '/api/v1';

export async function executeSQL(sql: string): Promise<{ columns: string[]; results: any[][]; row_count: number }> {
  const resp = await fetch(`${API_BASE}/text2sql/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => null);
    throw new Error(data?.detail || `HTTP ${resp.status}`);
  }
  const data = await resp.json();
  if (data.success && data.result) return data.result;
  throw new Error('쿼리 실행 실패');
}
