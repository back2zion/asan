/**
 * 데이터 카탈로그 공용 상수
 */
import type { TableInfo } from '../../services/api';

// 민감도 레벨 색상
export const sensitivityColors: Record<string, string> = {
  PUBLIC: 'green',
  INTERNAL: 'blue',
  CONFIDENTIAL: 'orange',
  RESTRICTED: 'red',
};

// 민감도 레벨 라벨
export const sensitivityLabels: Record<string, string> = {
  PUBLIC: '공개',
  INTERNAL: '내부',
  CONFIDENTIAL: '기밀',
  RESTRICTED: '제한',
};

// 코드 생성 헬퍼
export function generateSqlCode(table: TableInfo): string {
  const columns = table.columns?.map(c => c.physical_name).join(',\n       ') || '*';
  return `SELECT ${columns}
FROM ${table.physical_name}
LIMIT 100;`;
}

export function generatePythonCode(table: TableInfo): string {
  return `import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://user:pass@host:5432/db')
df = pd.read_sql('SELECT * FROM ${table.physical_name} LIMIT 100', engine)
print(df.head())`;
}

export function generateRCode(table: TableInfo): string {
  return `library(DBI)
library(RPostgres)

con <- dbConnect(RPostgres::Postgres(),
  host = "host", port = 5432,
  dbname = "db", user = "user", password = "pass")

df <- dbGetQuery(con, "SELECT * FROM ${table.physical_name} LIMIT 100")
head(df)
dbDisconnect(con)`;
}

export function generateApiEndpoint(table: TableInfo): string {
  return `GET /api/v1/data/${table.physical_name.toLowerCase()}?limit=100

# curl 예시
curl -X GET "https://idp.amc.seoul.kr/api/v1/data/${table.physical_name.toLowerCase()}?limit=100" \\
  -H "Authorization: Bearer <TOKEN>"`;
}
