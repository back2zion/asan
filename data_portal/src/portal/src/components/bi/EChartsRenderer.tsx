/**
 * EChartsRenderer — Apache ECharts 기반 고급 차트 렌더러
 * 12개 차트 타입, toolbox, 애니메이션, 병원 테마
 */
import React, { useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import * as echarts from 'echarts/core';
import { GraphicComponent } from 'echarts/components';
echarts.use([GraphicComponent]);

// ─── 병원 테마 컬러 ───
const C = [
  '#006241', '#005BAC', '#00A0B0', '#52C41A', '#FAAD14',
  '#FF4D4F', '#722ED1', '#EB2F96', '#13C2C2', '#2D9CDB',
  '#7B61FF', '#E8927C',
];

// ─── Helpers ───
function isNum(v: any): boolean {
  if (v == null || v === '') return false;
  return !isNaN(Number(v));
}
function isAggCol(c: string) { return /count|sum|avg|total|평균|합계|건수|비율|percent|ratio|min|max/i.test(c); }
function isTimeCol(c: string) { return /year|연도|월|month|date|날짜|기간|quarter|분기/i.test(c); }
function isIdCol(c: string) {
  if (isAggCol(c)) return false;
  return /(_id|_key|_url|_filename|source_value)$/i.test(c)
    || /^(person_id|patient_id|visit_occurrence_id)$/i.test(c);
}

// ─── Column Analysis ───
interface ColA { valIdx: number[]; catIdx: number[]; timeIdx: number[]; isAgg: boolean; }

function analyzeCols(cols: string[], rows: any[][]): ColA {
  const valIdx: number[] = [], catIdx: number[] = [], timeIdx: number[] = [];
  let isAgg = false;
  cols.forEach((col, i) => {
    const agg = isAggCol(col), time = isTimeCol(col), id = isIdCol(col);
    if (agg) isAgg = true;
    const nn = rows.filter(r => r[i] != null && r[i] !== '');
    const ratio = nn.length > 0 ? nn.filter(r => isNum(r[i])).length / nn.length : 0;
    const numeric = ratio >= 0.8;
    if (agg && numeric) valIdx.push(i);
    else if (time) timeIdx.push(i);
    else if (id) { /* skip */ }
    else if (numeric) valIdx.push(i);
    else catIdx.push(i);
  });
  if (!isAgg && rows.length <= 50 && valIdx.length > 0 && (catIdx.length > 0 || timeIdx.length > 0)) isAgg = true;
  return { valIdx, catIdx, timeIdx, isAgg };
}

function uniq(rows: any[][], i: number) { return [...new Set(rows.map(r => r[i]))]; }
function filterConst(vi: number[], rows: any[][]) {
  if (rows.length <= 1) return vi;
  return vi.filter(i => { const f = rows[0][i]; return !rows.every(r => r[i] === f); });
}
function pickPrimary(vi: number[], cols: string[]) {
  const p = vi.filter(i => /^(count|sum|avg|cnt|건수|합계|평균|patient_count|record_count)/i.test(cols[i]));
  return p.length > 0 ? p : vi;
}

// ─── Auto-Detect ───
type AutoType = 'line' | 'bar' | 'hbar' | 'groupedBar' | 'pie' | 'pivotLine' | 'pivotBar';
interface AutoD { type: AutoType; xIdx: number; seriesIdx?: number; valIdx: number[]; }

function autoDetect(a: ColA, cols: string[], rows: any[][]): AutoD | null {
  const { valIdx: raw, catIdx, timeIdx, isAgg } = a;
  if (raw.length === 0 || !isAgg) return null;
  const nc = filterConst(raw, rows);
  const vi = (nc.length >= 3 && catIdx.length > 0) ? pickPrimary(nc, cols) : nc.length > 0 ? nc : raw;

  if (timeIdx.length > 0 && catIdx.length >= 1 && vi.length === 1) {
    const cu = uniq(rows, catIdx[0]).length;
    if (cu >= 2 && cu <= 10) return { type: 'pivotLine', xIdx: timeIdx[0], seriesIdx: catIdx[0], valIdx: vi };
  }
  if (timeIdx.length > 0 && vi.length > 0) return { type: 'line', xIdx: timeIdx[0], valIdx: vi };
  if (catIdx.length >= 2 && vi.length === 1) {
    const u0 = uniq(rows, catIdx[0]).length, u1 = uniq(rows, catIdx[1]).length;
    const [xI, sI] = u0 >= u1 ? [catIdx[0], catIdx[1]] : [catIdx[1], catIdx[0]];
    if (uniq(rows, sI).length >= 2 && uniq(rows, sI).length <= 10)
      return { type: 'pivotBar', xIdx: xI, seriesIdx: sI, valIdx: vi };
  }
  if (catIdx.length > 0 && vi.length >= 2) return { type: 'groupedBar', xIdx: catIdx[0], valIdx: vi };
  if (catIdx.length >= 1 && vi.length === 1) {
    const xu = uniq(rows, catIdx[0]).length;
    return { type: xu <= 8 ? 'pie' : 'hbar', xIdx: catIdx[0], valIdx: vi };
  }
  return null;
}

// ─── Pivot ───
function pivot(cols: string[], rows: any[][], xI: number, sI: number, vI: number) {
  const sKeys = [...new Set(rows.map(r => String(r[sI])))];
  const grp = new Map<string, Record<string, number>>();
  for (const r of rows) {
    const x = String(r[xI]), s = String(r[sI]), v = isNum(r[vI]) ? Number(r[vI]) : 0;
    if (!grp.has(x)) grp.set(x, Object.fromEntries(sKeys.map(k => [k, 0])));
    grp.get(x)![s] = v;
  }
  const sorted = [...grp.entries()].sort((a, b) =>
    isNum(a[0]) && isNum(b[0]) ? Number(a[0]) - Number(b[0]) : a[0].localeCompare(b[0])
  );
  const xLabels = sorted.map(([x]) => x);
  const sData: Record<string, number[]> = {};
  for (const sk of sKeys) sData[sk] = sorted.map(([, v]) => v[sk] || 0);
  return { xLabels, sKeys, sData };
}

// ─── Toolbox & Common ───
const ANIM = { animationDuration: 800, animationEasing: 'cubicOut' as const };
function tbx(show: boolean) {
  if (!show) return {};
  return {
    toolbox: {
      show: true, right: 12, top: 4, iconStyle: { borderColor: '#999' },
      feature: {
        saveAsImage: { title: '이미지 저장', pixelRatio: 2 },
        dataView: { title: '데이터', readOnly: true, lang: ['데이터', '닫기', '새로고침'] },
        magicType: { type: ['line', 'bar'], title: { line: '라인', bar: '바' } },
        restore: { title: '초기화' },
      },
    },
  };
}
function grid(bottom = 30) {
  return { grid: { left: 60, right: 30, top: 40, bottom, containLabel: true } };
}
function numFmt(n: number) {
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return String(n);
}

// ─── Auto Option Builder ───
function buildAutoOpt(d: AutoD, cols: string[], rows: any[][], st: boolean): any {
  const t = tbx(st);
  switch (d.type) {
    case 'line':
    case 'groupedBar': {
      const xl = rows.map(r => String(r[d.xIdx]));
      const isLine = d.type === 'line';
      return {
        color: C, ...t, ...grid(d.valIdx.length > 1 ? 50 : 30),
        tooltip: { trigger: 'axis', confine: true },
        legend: d.valIdx.length > 1 ? { bottom: 0, type: 'scroll' } : undefined,
        xAxis: { type: 'category', data: xl, axisLabel: { rotate: xl.length > 12 ? 35 : 0, fontSize: 11 }, axisTick: { alignWithLabel: true } },
        yAxis: { type: 'value', axisLabel: { fontSize: 11, formatter: (v: number) => numFmt(v) }, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
        series: d.valIdx.map((vi, i) => ({
          name: cols[vi], type: isLine ? 'line' : 'bar',
          data: rows.map(r => isNum(r[vi]) ? Number(r[vi]) : 0),
          smooth: true,
          ...(isLine
            ? { areaStyle: { opacity: 0.12 }, lineStyle: { width: 2.5 }, symbol: 'circle', symbolSize: 6 }
            : { barMaxWidth: 40, itemStyle: { borderRadius: [4, 4, 0, 0] } }),
        })),
        dataZoom: xl.length > 20 ? [{ type: 'inside' }, { type: 'slider', bottom: d.valIdx.length > 1 ? 30 : 8, height: 18 }] : undefined,
        ...ANIM,
      };
    }
    case 'hbar': {
      const xl = rows.map(r => String(r[d.xIdx]));
      const vi = d.valIdx[0];
      const vals = rows.map(r => isNum(r[vi]) ? Number(r[vi]) : 0);
      return {
        color: C, ...t,
        tooltip: { trigger: 'axis', confine: true },
        grid: { left: 20, right: 40, top: 20, bottom: 20, containLabel: true },
        xAxis: { type: 'value', axisLabel: { fontSize: 11, formatter: (v: number) => numFmt(v) }, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
        yAxis: { type: 'category', data: [...xl].reverse(), axisLabel: { fontSize: 11, width: 100, overflow: 'truncate' } },
        series: [{
          type: 'bar', data: [...vals].reverse(), barMaxWidth: 28,
          itemStyle: {
            borderRadius: [0, 4, 4, 0],
            color: { type: 'linear', x: 0, y: 0, x2: 1, y2: 0, colorStops: [{ offset: 0, color: C[0] }, { offset: 1, color: C[2] }] },
          },
          label: { show: true, position: 'right', fontSize: 11, formatter: (p: any) => numFmt(p.value) },
        }],
        ...ANIM,
      };
    }
    case 'pie': {
      const vi = d.valIdx[0];
      const data = rows.map((r, i) => ({
        name: String(r[d.xIdx]), value: isNum(r[vi]) ? Number(r[vi]) : 0,
        itemStyle: { color: C[i % C.length] },
      }));
      return {
        color: C, ...t,
        tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
        legend: { bottom: 0, type: 'scroll', textStyle: { fontSize: 11 } },
        series: [{
          type: 'pie', radius: ['38%', '68%'], center: ['50%', '45%'],
          data,
          label: { formatter: '{b}\n{d}%', fontSize: 11 },
          emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.25)' }, scale: true, scaleSize: 6 },
          itemStyle: { borderRadius: 6, borderColor: '#fff', borderWidth: 2 },
        }],
        ...ANIM,
      };
    }
    case 'pivotLine':
    case 'pivotBar': {
      if (d.seriesIdx == null) return {};
      const { xLabels, sKeys, sData } = pivot(cols, rows, d.xIdx, d.seriesIdx, d.valIdx[0]);
      const isLine = d.type === 'pivotLine';
      return {
        color: C, ...t, ...grid(50),
        tooltip: { trigger: 'axis', confine: true },
        legend: { bottom: 0, type: 'scroll', textStyle: { fontSize: 11 } },
        xAxis: { type: 'category', data: xLabels, axisLabel: { rotate: xLabels.length > 12 ? 35 : 0, fontSize: 11 }, axisTick: { alignWithLabel: true } },
        yAxis: { type: 'value', axisLabel: { fontSize: 11, formatter: (v: number) => numFmt(v) }, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
        series: sKeys.map((sk) => ({
          name: sk, type: isLine ? 'line' : 'bar',
          data: sData[sk], smooth: true,
          ...(isLine
            ? { areaStyle: { opacity: 0.08 }, lineStyle: { width: 2.5 }, symbol: 'circle', symbolSize: 5 }
            : { barMaxWidth: 30, itemStyle: { borderRadius: [4, 4, 0, 0] } }),
        })),
        dataZoom: xLabels.length > 20 ? [{ type: 'inside' }, { type: 'slider', bottom: 28, height: 18 }] : undefined,
        ...ANIM,
      };
    }
    default: return {};
  }
}

// ─── Explicit Chart Type Builder ───
function buildExplicit(
  chartType: string, cols: string[], rows: any[][],
  xField?: string, yField?: string, groupField?: string, st = true,
): any {
  const t = tbx(st);
  const xI = xField ? cols.indexOf(xField) : 0;
  const yI = yField ? cols.indexOf(yField) : (cols.length > 1 ? 1 : 0);
  const gI = groupField ? cols.indexOf(groupField) : -1;
  const labels = rows.map(r => String(r[xI] ?? ''));
  const values = rows.map(r => isNum(r[yI]) ? Number(r[yI]) : 0);

  switch (chartType) {
    case 'bar': {
      if (gI >= 0) {
        const { xLabels, sKeys, sData } = pivot(cols, rows, xI, gI, yI);
        return {
          color: C, ...t, ...grid(50),
          tooltip: { trigger: 'axis' },
          legend: { bottom: 0, type: 'scroll' },
          xAxis: { type: 'category', data: xLabels, axisLabel: { rotate: xLabels.length > 12 ? 35 : 0, fontSize: 11 }, axisTick: { alignWithLabel: true } },
          yAxis: { type: 'value', axisLabel: { formatter: (v: number) => numFmt(v) }, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
          series: sKeys.map(sk => ({ name: sk, type: 'bar', data: sData[sk], barMaxWidth: 40, itemStyle: { borderRadius: [4, 4, 0, 0] } })),
          dataZoom: xLabels.length > 20 ? [{ type: 'inside' }, { type: 'slider', bottom: 28, height: 18 }] : undefined,
          ...ANIM,
        };
      }
      return {
        color: C, ...t, ...grid(),
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'category', data: labels, axisLabel: { rotate: labels.length > 12 ? 35 : 0, fontSize: 11 }, axisTick: { alignWithLabel: true } },
        yAxis: { type: 'value', axisLabel: { fontSize: 11, formatter: (v: number) => numFmt(v) }, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
        series: [{
          type: 'bar', data: values, barMaxWidth: 50,
          itemStyle: {
            borderRadius: [4, 4, 0, 0],
            color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: C[0] }, { offset: 1, color: C[2] }] },
          },
          label: { show: values.length <= 15, position: 'top', fontSize: 10, formatter: (p: any) => numFmt(p.value) },
        }],
        dataZoom: labels.length > 20 ? [{ type: 'inside' }, { type: 'slider', bottom: 8, height: 18 }] : undefined,
        ...ANIM,
      };
    }

    case 'line': {
      if (gI >= 0) {
        const { xLabels, sKeys, sData } = pivot(cols, rows, xI, gI, yI);
        return {
          color: C, ...t, ...grid(50),
          tooltip: { trigger: 'axis' },
          legend: { bottom: 0, type: 'scroll' },
          xAxis: { type: 'category', data: xLabels, axisTick: { alignWithLabel: true } },
          yAxis: { type: 'value', axisLabel: { formatter: (v: number) => numFmt(v) }, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
          series: sKeys.map(sk => ({
            name: sk, type: 'line', data: sData[sk], smooth: true,
            areaStyle: { opacity: 0.08 }, symbol: 'circle', symbolSize: 5,
          })),
          ...ANIM,
        };
      }
      return {
        color: C, ...t, ...grid(),
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'category', data: labels, axisLabel: { fontSize: 11 }, axisTick: { alignWithLabel: true } },
        yAxis: { type: 'value', axisLabel: { fontSize: 11, formatter: (v: number) => numFmt(v) }, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
        series: [{
          type: 'line', data: values, smooth: true,
          areaStyle: {
            opacity: 0.18,
            color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: C[0] }, { offset: 1, color: 'rgba(0,98,65,0.02)' }] },
          },
          lineStyle: { width: 3 }, symbol: 'circle', symbolSize: 6,
          itemStyle: { color: C[0] },
        }],
        dataZoom: labels.length > 20 ? [{ type: 'inside' }, { type: 'slider', bottom: 8, height: 18 }] : undefined,
        ...ANIM,
      };
    }

    case 'area': {
      if (gI >= 0) {
        const { xLabels, sKeys, sData } = pivot(cols, rows, xI, gI, yI);
        return {
          color: C, ...t, ...grid(50),
          tooltip: { trigger: 'axis' },
          legend: { bottom: 0, type: 'scroll' },
          xAxis: { type: 'category', data: xLabels, axisTick: { alignWithLabel: true } },
          yAxis: { type: 'value', axisLabel: { formatter: (v: number) => numFmt(v) }, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
          series: sKeys.map((sk, i) => ({
            name: sk, type: 'line', data: sData[sk], smooth: true, stack: 'total',
            areaStyle: { opacity: 0.45 }, lineStyle: { width: 1.5 },
          })),
          ...ANIM,
        };
      }
      return {
        color: C, ...t, ...grid(),
        tooltip: { trigger: 'axis' },
        xAxis: { type: 'category', data: labels, axisTick: { alignWithLabel: true } },
        yAxis: { type: 'value', axisLabel: { formatter: (v: number) => numFmt(v) }, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
        series: [{
          type: 'line', data: values, smooth: true,
          areaStyle: {
            opacity: 0.45,
            color: { type: 'linear', x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: C[0] }, { offset: 1, color: 'rgba(0,98,65,0.05)' }] },
          },
          lineStyle: { width: 2 }, symbol: 'circle', symbolSize: 5,
        }],
        ...ANIM,
      };
    }

    case 'pie': {
      const data = rows.map((r) => ({
        name: String(r[xI] ?? ''), value: isNum(r[yI]) ? Number(r[yI]) : 0,
      }));
      return {
        color: C, ...t,
        tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
        legend: { bottom: 0, type: 'scroll', textStyle: { fontSize: 11 } },
        series: [{
          type: 'pie', radius: ['38%', '68%'], center: ['50%', '44%'],
          data,
          label: { formatter: '{b}\n{d}%', fontSize: 11 },
          emphasis: { itemStyle: { shadowBlur: 12, shadowColor: 'rgba(0,0,0,0.25)' }, scale: true, scaleSize: 6 },
          itemStyle: { borderRadius: 6, borderColor: '#fff', borderWidth: 2 },
        }],
        ...ANIM,
      };
    }

    case 'doughnut': {
      const data = rows.map((r) => ({
        name: String(r[xI] ?? ''), value: isNum(r[yI]) ? Number(r[yI]) : 0,
      }));
      const total = data.reduce((s, d) => s + d.value, 0);
      return {
        color: C, ...t,
        tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
        legend: { bottom: 0, type: 'scroll', textStyle: { fontSize: 11 } },
        graphic: [{
          type: 'text', left: 'center', top: '42%',
          style: { text: numFmt(total), fontSize: 22, fontWeight: 'bold', fill: '#333', textAlign: 'center' },
        }, {
          type: 'text', left: 'center', top: '50%',
          style: { text: '합계', fontSize: 12, fill: '#999', textAlign: 'center' },
        }],
        series: [{
          type: 'pie', radius: ['48%', '72%'], center: ['50%', '46%'],
          data,
          label: { formatter: '{b}\n{d}%', fontSize: 11 },
          emphasis: { itemStyle: { shadowBlur: 12, shadowColor: 'rgba(0,0,0,0.25)' }, scale: true, scaleSize: 6 },
          itemStyle: { borderRadius: 8, borderColor: '#fff', borderWidth: 3 },
        }],
        ...ANIM,
      };
    }

    case 'scatter': {
      const data = rows.map(r => [
        isNum(r[xI]) ? Number(r[xI]) : 0,
        isNum(r[yI]) ? Number(r[yI]) : 0,
      ]);
      return {
        color: C, ...t, ...grid(),
        tooltip: {
          trigger: 'item',
          formatter: (p: any) => `${cols[xI]}: ${numFmt(p.value[0])}<br/>${cols[yI]}: ${numFmt(p.value[1])}`,
        },
        xAxis: { type: 'value', name: cols[xI], nameLocation: 'center', nameGap: 30, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
        yAxis: { type: 'value', name: cols[yI], nameLocation: 'center', nameGap: 45, splitLine: { lineStyle: { type: 'dashed', color: '#eee' } } },
        series: [{
          type: 'scatter', data, symbolSize: 12,
          itemStyle: { color: C[0], opacity: 0.7, borderColor: '#fff', borderWidth: 1 },
          emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.3)', opacity: 1 } },
        }],
        ...ANIM,
      };
    }

    case 'gauge': {
      const maxVal = Math.max(...values, 1);
      const lastVal = values[values.length - 1] || 0;
      return {
        ...t,
        series: [{
          type: 'gauge', startAngle: 210, endAngle: -30,
          min: 0, max: maxVal, splitNumber: 5, alignTicks: false,
          data: [{ value: lastVal, name: yField || cols[yI] || '값' }],
          axisLine: {
            lineStyle: {
              width: 22,
              color: [[0.25, '#FF4D4F'], [0.5, '#FAAD14'], [0.75, '#52C41A'], [1, '#006241']],
            },
          },
          pointer: { width: 5, length: '55%', itemStyle: { color: '#333' } },
          axisTick: { distance: -22, length: 8, lineStyle: { color: '#fff', width: 2 } },
          splitLine: { distance: -26, length: 18, lineStyle: { color: '#fff', width: 3 } },
          axisLabel: { distance: 32, fontSize: 11, formatter: (v: number) => numFmt(v) },
          detail: {
            fontSize: 30, fontWeight: 'bold', color: C[0],
            offsetCenter: [0, '68%'], formatter: (v: number) => numFmt(v),
          },
          title: { offsetCenter: [0, '88%'], fontSize: 14, color: '#666' },
        }],
        ...ANIM,
      };
    }

    case 'radar': {
      const numIdx = cols.map((_, i) => i).filter(i => {
        const nn = rows.filter(r => r[i] != null && r[i] !== '');
        return nn.length > 0 && nn.filter(r => isNum(r[i])).length / nn.length >= 0.8;
      });
      const catI = cols.findIndex((_, i) => !numIdx.includes(i));
      const indicators = numIdx.map(i => ({
        name: cols[i],
        max: Math.max(...rows.map(r => isNum(r[i]) ? Number(r[i]) : 0), 1) * 1.2,
      }));
      const display = rows.slice(0, 6);
      const sData = display.map((r, ri) => ({
        name: catI >= 0 ? String(r[catI]) : `항목 ${ri + 1}`,
        value: numIdx.map(i => isNum(r[i]) ? Number(r[i]) : 0),
        areaStyle: { opacity: 0.15 },
        lineStyle: { width: 2 },
        symbol: 'circle', symbolSize: 5,
      }));
      return {
        color: C, ...t,
        tooltip: { trigger: 'item' },
        legend: { bottom: 0, type: 'scroll', textStyle: { fontSize: 11 } },
        radar: {
          indicator: indicators, shape: 'polygon',
          axisName: { fontSize: 11, color: '#666' },
          splitArea: { areaStyle: { color: ['rgba(0,98,65,0.02)', 'rgba(0,98,65,0.05)'] } },
        },
        series: [{ type: 'radar', data: sData, emphasis: { areaStyle: { opacity: 0.35 } } }],
        ...ANIM,
      };
    }

    case 'heatmap': {
      const gIH = gI >= 0 ? gI : (cols.length >= 3 ? (xI === 0 ? 1 : 0) : -1);
      if (gIH < 0) return buildExplicit('bar', cols, rows, xField, yField, groupField, st);
      const xVals = [...new Set(rows.map(r => String(r[xI])))];
      const yVals = [...new Set(rows.map(r => String(r[gIH])))];
      const data: [number, number, number][] = [];
      let mx = 0;
      for (const r of rows) {
        const xi = xVals.indexOf(String(r[xI])), yi = yVals.indexOf(String(r[gIH]));
        const v = isNum(r[yI]) ? Number(r[yI]) : 0;
        if (xi >= 0 && yi >= 0) { data.push([xi, yi, v]); if (v > mx) mx = v; }
      }
      return {
        ...t,
        tooltip: { position: 'top', formatter: (p: any) => `${xVals[p.value[0]]} × ${yVals[p.value[1]]}: ${numFmt(p.value[2])}` },
        grid: { left: 100, right: 60, top: 20, bottom: 50 },
        xAxis: { type: 'category', data: xVals, splitArea: { show: true }, axisLabel: { fontSize: 11, rotate: xVals.length > 8 ? 30 : 0 } },
        yAxis: { type: 'category', data: yVals, splitArea: { show: true }, axisLabel: { fontSize: 11 } },
        visualMap: {
          min: 0, max: mx || 1, calculable: true, orient: 'vertical', right: 10, top: 20, bottom: 50,
          inRange: { color: ['#e8f5e9', '#a5d6a7', '#66bb6a', '#2e7d32', '#1b5e20'] },
        },
        series: [{
          type: 'heatmap', data,
          label: { show: data.length <= 100, fontSize: 10, formatter: (p: any) => numFmt(p.value[2]) },
          emphasis: { itemStyle: { shadowBlur: 10, shadowColor: 'rgba(0,0,0,0.3)' } },
        }],
        ...ANIM,
      };
    }

    case 'treemap': {
      const data = rows.map((r, i) => ({
        name: String(r[xI] ?? `항목 ${i + 1}`),
        value: isNum(r[yI]) ? Number(r[yI]) : 0,
        itemStyle: { color: C[i % C.length] },
      })).sort((a, b) => b.value - a.value);
      return {
        ...t,
        tooltip: { trigger: 'item', formatter: (p: any) => `${p.name}: ${numFmt(p.value)}` },
        series: [{
          type: 'treemap', data, leafDepth: 1,
          label: { show: true, formatter: (p: any) => `${p.name}\n${numFmt(p.value)}`, fontSize: 12, color: '#fff' },
          breadcrumb: { show: false },
          itemStyle: { borderColor: '#fff', borderWidth: 2, gapWidth: 2 },
          upperLabel: { show: true, height: 24, fontSize: 12, color: '#fff' },
          levels: [{
            itemStyle: { borderColor: '#fff', borderWidth: 3, gapWidth: 3 },
            upperLabel: { show: false },
          }],
        }],
        ...ANIM,
      };
    }

    case 'funnel': {
      const data = rows.map((r) => ({
        name: String(r[xI] ?? ''),
        value: isNum(r[yI]) ? Number(r[yI]) : 0,
      })).sort((a, b) => b.value - a.value);
      return {
        color: C, ...t,
        tooltip: { trigger: 'item', formatter: (p: any) => `${p.name}: ${numFmt(p.value)}` },
        legend: { bottom: 0, type: 'scroll', textStyle: { fontSize: 11 } },
        series: [{
          type: 'funnel', left: '10%', top: 30, bottom: 50, width: '80%',
          min: 0, max: Math.max(...data.map(d => d.value), 1),
          sort: 'descending', gap: 4, data,
          label: { show: true, position: 'inside', formatter: (p: any) => `${p.name}\n${numFmt(p.value)}`, fontSize: 12, color: '#fff' },
          itemStyle: { borderColor: '#fff', borderWidth: 2 },
          emphasis: { label: { fontSize: 14 } },
        }],
        ...ANIM,
      };
    }

    default: return {};
  }
}

// ─── Main Component ───
export interface EChartsRendererProps {
  columns: string[];
  results: any[][];
  chartType?: string;
  xField?: string;
  yField?: string;
  groupField?: string;
  height?: number;
  showToolbox?: boolean;
}

const EChartsRenderer: React.FC<EChartsRendererProps> = ({
  columns, results, chartType, xField, yField, groupField,
  height = 380, showToolbox = true,
}) => {
  const option = useMemo(() => {
    if (!columns?.length || !results?.length) return null;

    // Explicit chart type
    if (chartType && chartType !== 'table') {
      return buildExplicit(chartType, columns, results, xField, yField, groupField, showToolbox);
    }

    // Auto-detect
    const analysis = analyzeCols(columns, results);
    const decision = autoDetect(analysis, columns, results);
    if (!decision) return null;

    // Validate
    const xu = uniq(results, decision.xIdx).length;
    if (xu < 2 && decision.type !== 'pie') return null;
    const hasVar = decision.valIdx.some(vi => {
      const vs = results.map(r => isNum(r[vi]) ? Number(r[vi]) : 0);
      return !vs.every(v => v === vs[0]);
    });
    if (!hasVar && decision.type !== 'pie') return null;

    return buildAutoOpt(decision, columns, results, showToolbox);
  }, [columns, results, chartType, xField, yField, groupField, showToolbox]);

  if (!option) return null;

  return (
    <div style={{ marginTop: 8, borderRadius: 8, overflow: 'hidden' }}>
      <ReactECharts
        option={option}
        style={{ height, width: '100%' }}
        opts={{ renderer: 'svg' }}
        notMerge
        lazyUpdate
      />
    </div>
  );
};

export default EChartsRenderer;
