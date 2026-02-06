import React, { useMemo } from 'react';
import {
  ResponsiveContainer,
  LineChart, Line,
  BarChart, Bar,
  PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts';

const COLORS = ['#006241', '#005BAC', '#52A67D', '#FF6F00', '#00A0B0', '#E8927C', '#7B61FF', '#2D9CDB'];

interface ResultChartProps {
  columns: string[];
  results: any[][];
}

type ChartType = 'line' | 'groupedBar' | 'pie' | 'bar' | 'pivotBar' | 'pivotLine';

function isNumeric(value: any): boolean {
  if (value == null || value === '') return false;
  return !isNaN(Number(value));
}

function isAggregateColumn(col: string): boolean {
  return /count|sum|avg|total|평균|합계|건수|비율|percent|ratio|min|max/i.test(col);
}

function isTimeColumn(col: string): boolean {
  return /year|연도|월|month|date|날짜|기간|quarter|분기/i.test(col);
}

function isIdColumn(col: string): boolean {
  if (isAggregateColumn(col)) return false;
  return /(_id|_key|_url|_filename|image_url|image_filename|person_source|gender_source|source_value)$/i.test(col)
    || /^(person_id|patient_id|visit_occurrence_id|imaging_study_id)$/i.test(col);
}

interface ColumnAnalysis {
  valueIndices: number[];
  categoryIndices: number[];
  timeIndices: number[];
  isAggregate: boolean;
}

function analyzeColumns(columns: string[], results: any[][]): ColumnAnalysis {
  const valueIndices: number[] = [];
  const categoryIndices: number[] = [];
  const timeIndices: number[] = [];
  let isAggregate = false;

  columns.forEach((col, idx) => {
    const isTime = isTimeColumn(col);
    const isId = isIdColumn(col);
    const isAgg = isAggregateColumn(col);

    if (isAgg) isAggregate = true;

    const nonNull = results.filter(row => row[idx] != null && row[idx] !== '');
    const numRatio = nonNull.length > 0
      ? nonNull.filter(row => isNumeric(row[idx])).length / nonNull.length
      : 0;
    const isNum = numRatio >= 0.8;

    if (isTime) {
      timeIndices.push(idx);
    } else if (isId) {
      // skip
    } else if (isNum) {
      valueIndices.push(idx);
    } else {
      categoryIndices.push(idx);
    }
  });

  if (!isAggregate && results.length <= 50 && valueIndices.length > 0 && (categoryIndices.length > 0 || timeIndices.length > 0)) {
    isAggregate = true;
  }

  return { valueIndices, categoryIndices, timeIndices, isAggregate };
}

/** 고유값 수 계산 */
function uniqueCount(results: any[][], idx: number): number {
  return new Set(results.map(row => row[idx])).size;
}

interface ChartDecision {
  type: ChartType;
  xIdx: number;           // X축 컬럼 인덱스
  seriesIdx?: number;     // 피벗 시리즈 컬럼 인덱스 (pivotBar/pivotLine용)
  valueIndices: number[]; // 값 컬럼 인덱스들
}

function decideChart(analysis: ColumnAnalysis, columns: string[], results: any[][]): ChartDecision | null {
  const { valueIndices, categoryIndices, timeIndices, isAggregate } = analysis;

  if (valueIndices.length === 0) return null;
  if (!isAggregate) return null;

  // === Case 1: 시계열 + 카테고리 + 값 1개 → pivotLine (연도별 방문유형 비교 등) ===
  if (timeIndices.length > 0 && categoryIndices.length >= 1 && valueIndices.length === 1) {
    const timeIdx = timeIndices[0];
    const catIdx = categoryIndices[0];
    const catUnique = uniqueCount(results, catIdx);
    // 시리즈(카테고리)의 고유값이 적당한 경우만 피벗
    if (catUnique >= 2 && catUnique <= 10) {
      return { type: 'pivotLine', xIdx: timeIdx, seriesIdx: catIdx, valueIndices };
    }
  }

  // === Case 2: 시계열 + 여러 값 컬럼 → line ===
  if (timeIndices.length > 0 && valueIndices.length > 0) {
    return { type: 'line', xIdx: timeIndices[0], valueIndices };
  }

  // === Case 3: 카테고리 2개 + 값 1개 → pivotBar (성별×연령대 등) ===
  if (categoryIndices.length >= 2 && valueIndices.length === 1) {
    const cat0Unique = uniqueCount(results, categoryIndices[0]);
    const cat1Unique = uniqueCount(results, categoryIndices[1]);

    // 고유값이 더 많은 쪽을 X축, 적은 쪽을 시리즈로
    let xIdx: number, seriesIdx: number;
    if (cat0Unique >= cat1Unique) {
      xIdx = categoryIndices[0];
      seriesIdx = categoryIndices[1];
    } else {
      xIdx = categoryIndices[1];
      seriesIdx = categoryIndices[0];
    }

    const seriesUnique = uniqueCount(results, seriesIdx);
    if (seriesUnique >= 2 && seriesUnique <= 10) {
      return { type: 'pivotBar', xIdx, seriesIdx, valueIndices };
    }
    // 시리즈 값이 너무 많으면 첫 카테고리만 X축으로 사용
  }

  // === Case 4: 카테고리 1개 + 여러 값 → groupedBar ===
  if (categoryIndices.length > 0 && valueIndices.length >= 2) {
    return { type: 'groupedBar', xIdx: categoryIndices[0], valueIndices };
  }

  // === Case 5: 카테고리 1개 + 값 1개 → pie(≤8) or bar ===
  if (categoryIndices.length >= 1 && valueIndices.length === 1) {
    const xIdx = categoryIndices[0];
    const xUnique = uniqueCount(results, xIdx);
    const chartType = xUnique <= 8 ? 'pie' as ChartType : 'bar' as ChartType;
    return { type: chartType, xIdx, valueIndices };
  }

  // === Case 6: 값만 여러개 → groupedBar (row index as X) ===
  if (valueIndices.length >= 2 && timeIndices.length === 0 && categoryIndices.length === 0) {
    return null; // 의미 없는 차트 방지
  }

  return null;
}

/** 데이터 피벗: rows → {xValue, series1Val, series2Val, ...} */
function pivotData(
  columns: string[],
  results: any[][],
  xIdx: number,
  seriesIdx: number,
  valueIdx: number,
): { data: Record<string, any>[]; seriesKeys: string[] } {
  const xKey = columns[xIdx];
  const seriesValues = [...new Set(results.map(row => String(row[seriesIdx])))];

  // x값 기준으로 그룹핑
  const grouped = new Map<string, Record<string, any>>();
  for (const row of results) {
    const xVal = String(row[xIdx]);
    const sVal = String(row[seriesIdx]);
    const numVal = isNumeric(row[valueIdx]) ? Number(row[valueIdx]) : 0;

    if (!grouped.has(xVal)) {
      const obj: Record<string, any> = { [xKey]: xVal };
      // 모든 시리즈 초기값 0
      for (const sv of seriesValues) obj[sv] = 0;
      grouped.set(xVal, obj);
    }
    grouped.get(xVal)![sVal] = numVal;
  }

  // X축 값 정렬 (숫자면 숫자순, 아니면 문자순)
  const data = [...grouped.values()].sort((a, b) => {
    const av = a[xKey], bv = b[xKey];
    if (isNumeric(av) && isNumeric(bv)) return Number(av) - Number(bv);
    return String(av).localeCompare(String(bv));
  });

  return { data, seriesKeys: seriesValues };
}

function buildChartData(columns: string[], results: any[][]): Record<string, any>[] {
  return results.map(row => {
    const obj: Record<string, any> = {};
    columns.forEach((col, idx) => {
      obj[col] = isNumeric(row[idx]) ? Number(row[idx]) : row[idx];
    });
    return obj;
  });
}

/** 차트 검증: 데이터가 차트로 의미 있는지 확인 */
function validateChart(
  decision: ChartDecision,
  columns: string[],
  results: any[][],
): boolean {
  // 데이터가 비었으면 무효
  if (results.length === 0) return false;

  const { type, xIdx, valueIndices } = decision;

  // X축 고유값이 1개뿐이면 차트 불필요
  const xUnique = uniqueCount(results, xIdx);
  if (xUnique < 2 && type !== 'pie') return false;

  // 값이 전부 0이거나 같으면 차트 불필요
  for (const vIdx of valueIndices) {
    const vals = results.map(r => (isNumeric(r[vIdx]) ? Number(r[vIdx]) : 0));
    const allSame = vals.every(v => v === vals[0]);
    if (allSame && type !== 'pie') continue; // 다른 값 컬럼 확인
    if (!allSame) return true; // 하나라도 다양하면 OK
  }

  // pie는 비율 보여주니 값이 다양하면 OK
  if (type === 'pie') {
    const vIdx = valueIndices[0];
    const vals = results.map(r => (isNumeric(r[vIdx]) ? Number(r[vIdx]) : 0));
    return !vals.every(v => v === vals[0]);
  }

  return false;
}

const RADIAN = Math.PI / 180;
const renderPieLabel = ({ cx, cy, midAngle, innerRadius, outerRadius, percent }: any) => {
  const radius = innerRadius + (outerRadius - innerRadius) * 0.5;
  const x = cx + radius * Math.cos(-midAngle * RADIAN);
  const y = cy + radius * Math.sin(-midAngle * RADIAN);
  if (percent < 0.05) return null;
  return (
    <text x={x} y={y} fill="white" textAnchor="middle" dominantBaseline="central" fontSize={12} fontWeight={600}>
      {`${(percent * 100).toFixed(0)}%`}
    </text>
  );
};

const ResultChart: React.FC<ResultChartProps> = ({ columns, results }) => {
  const chart = useMemo(() => {
    if (!columns || columns.length === 0 || !results || results.length === 0) return null;

    const analysis = analyzeColumns(columns, results);
    const decision = decideChart(analysis, columns, results);
    if (!decision) return null;

    // 검증
    if (!validateChart(decision, columns, results)) return null;

    const { type, xIdx, seriesIdx, valueIndices: valIdxs } = decision;
    const xKey = columns[xIdx];
    const valueKeys = valIdxs.map(idx => columns[idx]);

    switch (type) {
      case 'pivotLine': {
        if (seriesIdx == null) return null;
        const { data, seriesKeys } = pivotData(columns, results, xIdx, seriesIdx, valIdxs[0]);
        return (
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey={xKey} tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Legend />
              {seriesKeys.map((sk, i) => (
                <Line
                  key={sk}
                  type="monotone"
                  dataKey={sk}
                  stroke={COLORS[i % COLORS.length]}
                  strokeWidth={2}
                  dot={{ r: 4 }}
                  activeDot={{ r: 6 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        );
      }

      case 'pivotBar': {
        if (seriesIdx == null) return null;
        const { data, seriesKeys } = pivotData(columns, results, xIdx, seriesIdx, valIdxs[0]);
        return (
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey={xKey} tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Legend />
              {seriesKeys.map((sk, i) => (
                <Bar key={sk} dataKey={sk} fill={COLORS[i % COLORS.length]} radius={[4, 4, 0, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        );
      }

      case 'line': {
        const data = buildChartData(columns, results);
        return (
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey={xKey} tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Legend />
              {valueKeys.map((key, i) => (
                <Line
                  key={key}
                  type="monotone"
                  dataKey={key}
                  stroke={COLORS[i % COLORS.length]}
                  strokeWidth={2}
                  dot={{ r: 4 }}
                  activeDot={{ r: 6 }}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        );
      }

      case 'groupedBar': {
        const data = buildChartData(columns, results);
        return (
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={data} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey={xKey} tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Legend />
              {valueKeys.map((key, i) => (
                <Bar key={key} dataKey={key} fill={COLORS[i % COLORS.length]} radius={[4, 4, 0, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        );
      }

      case 'pie': {
        const data = buildChartData(columns, results);
        const valKey = valueKeys[0];
        return (
          <ResponsiveContainer width="100%" height={350}>
            <PieChart>
              <Pie
                data={data}
                dataKey={valKey}
                nameKey={xKey}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={120}
                labelLine={false}
                label={renderPieLabel}
              >
                {data.map((_, i) => (
                  <Cell key={`cell-${i}`} fill={COLORS[i % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        );
      }

      case 'bar': {
        const data = buildChartData(columns, results);
        return (
          <ResponsiveContainer width="100%" height={Math.max(300, results.length * 36)}>
            <BarChart data={data} layout="vertical" margin={{ top: 5, right: 30, left: 120, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" tick={{ fontSize: 12 }} />
              <YAxis type="category" dataKey={xKey} tick={{ fontSize: 12 }} width={110} />
              <Tooltip />
              <Legend />
              {valueKeys.map((key, i) => (
                <Bar key={key} dataKey={key} fill={COLORS[i % COLORS.length]} radius={[0, 4, 4, 0]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        );
      }

      default:
        return null;
    }
  }, [columns, results]);

  if (!chart) return null;

  return (
    <div style={{
      marginTop: 16,
      padding: '16px 8px',
      background: '#fafafa',
      borderRadius: 8,
      border: '1px solid #f0f0f0',
    }}>
      {chart}
    </div>
  );
};

export default ResultChart;
