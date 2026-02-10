import React, { useState, useEffect } from 'react';
import { Card, Steps, Button, Radio, Checkbox, Space, Typography, Spin, Empty, Tag, Row, Col, App } from 'antd';
import {
  FileExcelOutlined, FilePdfOutlined, FilePptOutlined, FileTextOutlined,
  DownloadOutlined, CheckCircleOutlined,
} from '@ant-design/icons';
import { biApi } from '../../services/biApi';
import { saveAs } from 'file-saver';

const { Text, Title } = Typography;

const FORMATS = [
  { value: 'csv', label: 'CSV', icon: <FileTextOutlined />, desc: 'Excel 호환 CSV (UTF-8 BOM)' },
  { value: 'json', label: 'JSON', icon: <FileExcelOutlined />, desc: '구조화된 JSON 데이터' },
  { value: 'pdf', label: 'PDF (Print)', icon: <FilePdfOutlined />, desc: '브라우저 인쇄로 PDF 생성' },
  { value: 'pptx', label: 'PowerPoint', icon: <FilePptOutlined />, desc: '프레젠테이션 슬라이드' },
];

const ReportExporter: React.FC = () => {
  const { message, modal } = App.useApp();
  const [step, setStep] = useState(0);
  const [format, setFormat] = useState('csv');
  const [charts, setCharts] = useState<any[]>([]);
  const [dashboards, setDashboards] = useState<any[]>([]);
  const [selectedChartIds, setSelectedChartIds] = useState<number[]>([]);
  const [loading, setLoading] = useState(true);
  const [exporting, setExporting] = useState(false);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const [c, d] = await Promise.all([biApi.getCharts(), biApi.getDashboards()]);
        setCharts(c);
        setDashboards(d);
      } catch { /* ignore */ }
      setLoading(false);
    };
    load();
  }, []);

  const doExport = async () => {
    if (format === 'pdf') {
      // Browser print
      window.print();
      message.success('인쇄 대화상자가 열렸습니다');
      return;
    }

    if (format === 'pptx') {
      setExporting(true);
      try {
        const PptxGenJS = (await import('pptxgenjs')).default;
        const pptx = new PptxGenJS();
        pptx.title = 'BI 보고서';
        pptx.author = 'IDP BI';
        pptx.layout = 'LAYOUT_16x9';

        // 차트 유형 매핑
        const chartTypeMap: Record<string, any> = {
          bar: pptx.ChartType.bar,
          line: pptx.ChartType.line,
          pie: pptx.ChartType.pie,
          doughnut: pptx.ChartType.doughnut,
          area: pptx.ChartType.area,
          scatter: pptx.ChartType.scatter,
        };

        // 차트 색상 팔레트
        const CHART_COLORS = ['005BAC', '00A0B0', '52C41A', 'FAAD14', 'FF4D4F', '722ED1', 'EB2F96', '13C2C2'];

        // Title slide
        const titleSlide = pptx.addSlide();
        titleSlide.addText('서울아산병원 IDP\nBI 보고서', {
          x: 1, y: 1, w: 8, h: 2, fontSize: 36, bold: true, color: '005BAC', lineSpacing: 48,
        });
        titleSlide.addText(`차트 ${selectedChartIds.length}개 | ${new Date().toLocaleDateString('ko-KR')}`, {
          x: 1, y: 3.2, w: 8, h: 0.5, fontSize: 16, color: '666666',
        });
        titleSlide.addShape(pptx.ShapeType.rect, {
          x: 0, y: 4.8, w: 10, h: 0.04, fill: { color: '005BAC' },
        });

        // Chart slides
        for (const cid of selectedChartIds) {
          const chart = charts.find(c => c.chart_id === cid);
          if (!chart) continue;

          const slide = pptx.addSlide();
          slide.addText(chart.name, {
            x: 0.5, y: 0.2, w: 9, h: 0.5, fontSize: 20, bold: true, color: '333333',
          });

          // 데이터 가져오기
          let rawData: any = null;
          try {
            rawData = await biApi.getChartRawData(cid, 100);
          } catch { /* 데이터 로드 실패 시 텍스트만 표시 */ }

          if (rawData?.rows?.length > 0 && rawData.columns?.length >= 2) {
            const cols: string[] = rawData.columns;
            // API는 rows를 배열의 배열로 반환 — 인덱스 기반 접근
            const rawRows: any[] = rawData.rows;
            const labels = rawRows.map(r => String(Array.isArray(r) ? r[0] : r[cols[0]] ?? '').slice(0, 20));

            const pptxChartType = chartTypeMap[chart.chart_type] || pptx.ChartType.bar;

            // 값 추출 헬퍼: 배열/객체 모두 지원
            const getVal = (r: any, colIdx: number) => {
              const v = Array.isArray(r) ? r[colIdx] : r[cols[colIdx]];
              return Number(v) || 0;
            };

            if (chart.chart_type === 'pie' || chart.chart_type === 'doughnut') {
              const values = rawRows.map(r => getVal(r, 1));
              slide.addChart(pptxChartType, [{ name: cols[1], labels, values }], {
                x: 0.5, y: 0.9, w: 9, h: 4.2,
                showLegend: true, legendPos: 'b', legendFontSize: 10,
                showPercent: true, showTitle: false,
                chartColors: CHART_COLORS.slice(0, labels.length),
              });
            } else {
              // 바/라인/에어리어: 값 컬럼은 인덱스 1부터
              const seriesIndices = cols.slice(1).map((_, i) => i + 1).filter(idx => {
                return rawRows.some(r => {
                  const v = Array.isArray(r) ? r[idx] : r[cols[idx]];
                  return typeof v === 'number' || !isNaN(Number(v));
                });
              });
              const chartData = seriesIndices.map((si, i) => ({
                name: cols[si],
                labels,
                values: rawRows.map(r => getVal(r, si)),
                color: CHART_COLORS[i % CHART_COLORS.length],
              }));

              if (chartData.length > 0) {
                slide.addChart(pptxChartType, chartData, {
                  x: 0.5, y: 0.9, w: 9, h: 4.2,
                  showLegend: seriesIndices.length > 1, legendPos: 'b', legendFontSize: 10,
                  showTitle: false,
                  catAxisLabelFontSize: 9, valAxisLabelFontSize: 9,
                  catAxisOrientation: 'minMax' as const,
                  catAxisLabelRotate: labels.length > 8 ? 45 : 0,
                  chartColors: CHART_COLORS,
                });
              }
            }

            // 데이터 요약 (하단)
            slide.addText(`데이터: ${rawRows.length}행 × ${cols.length}열`, {
              x: 0.5, y: 5.2, w: 9, h: 0.3, fontSize: 9, color: 'AAAAAA',
            });
          } else {
            // 데이터 없음 — 설명만 표시
            slide.addText(chart.description || '데이터를 로드할 수 없습니다', {
              x: 0.5, y: 1.5, w: 9, h: 1, fontSize: 14, color: '999999',
            });
            const sqlPreview = (chart.sql_query || '').substring(0, 300);
            slide.addText(`SQL: ${sqlPreview}`, {
              x: 0.5, y: 3, w: 9, h: 1, fontSize: 8, color: 'AAAAAA', fontFace: 'Courier New',
            });
          }
        }

        const data = await pptx.write({ outputType: 'blob' }) as Blob;
        saveAs(data, 'BI_보고서.pptx');
        message.success('PowerPoint 파일이 생성되었습니다');
      } catch (err) {
        message.error('PPT 생성 실패');
      }
      setExporting(false);
      return;
    }

    // CSV / JSON via API
    setExporting(true);
    try {
      const blob = await biApi.exportReport(format, {
        chart_ids: selectedChartIds,
        title: 'BI_보고서',
        include_data: true,
      });
      const ext = format === 'csv' ? 'csv' : 'json';
      saveAs(new Blob([blob]), `BI_보고서.${ext}`);
      message.success(`${format.toUpperCase()} 파일이 다운로드되었습니다`);
    } catch {
      message.error('내보내기 실패');
    }
    setExporting(false);
  };

  const handleExport = () => {
    if (selectedChartIds.length === 0) { message.warning('내보낼 차트를 선택하세요'); return; }
    modal.confirm({
      title: '보고서 내보내기',
      content: `${selectedChartIds.length}개 차트를 ${format.toUpperCase()} 형식으로 내보냅니다. 데이터 양에 따라 10초 이상 소요될 수 있습니다. 진행하시겠습니까?`,
      okText: '내보내기',
      cancelText: '취소',
      onOk: doExport,
    });
  };

  const handleSelectDashboard = (dbId: number) => {
    const db = dashboards.find(d => d.dashboard_id === dbId);
    if (db?.chart_ids) {
      setSelectedChartIds(db.chart_ids);
    }
  };

  const stepContent = [
    // Step 0: Format
    <Row gutter={[12, 12]} key="s0">
      {FORMATS.map(f => (
        <Col span={12} key={f.value}>
          <Card
            hoverable size="small"
            onClick={() => setFormat(f.value)}
            style={{
              border: format === f.value ? '2px solid #006241' : '1px solid #d9d9d9',
              cursor: 'pointer',
            }}
          >
            <Space>
              <div style={{ fontSize: 24, color: format === f.value ? '#006241' : '#999' }}>{f.icon}</div>
              <div>
                <Text strong={format === f.value}>{f.label}</Text>
                <br />
                <Text type="secondary" style={{ fontSize: 11 }}>{f.desc}</Text>
              </div>
            </Space>
          </Card>
        </Col>
      ))}
    </Row>,

    // Step 1: Content selection
    <div key="s1">
      {loading ? <Spin /> : (
        <Space direction="vertical" style={{ width: '100%' }}>
          {dashboards.length > 0 && (
            <div style={{ marginBottom: 8 }}>
              <Text type="secondary" style={{ fontSize: 12, marginBottom: 4, display: 'block' }}>대시보드에서 선택:</Text>
              <Space wrap>
                {dashboards.map(db => (
                  <Tag key={db.dashboard_id} style={{ cursor: 'pointer', padding: '4px 8px' }}
                    onClick={() => handleSelectDashboard(db.dashboard_id)}
                  >{db.name} ({db.chart_ids?.length || 0}개)</Tag>
                ))}
              </Space>
            </div>
          )}
          <Text type="secondary" style={{ fontSize: 12 }}>차트 개별 선택:</Text>
          <Checkbox.Group
            value={selectedChartIds}
            onChange={(vals) => setSelectedChartIds(vals as number[])}
          >
            <Space direction="vertical">
              {charts.map(c => (
                <Checkbox key={c.chart_id} value={c.chart_id}>
                  {c.name} <Tag style={{ marginLeft: 4 }}>{c.chart_type}</Tag>
                </Checkbox>
              ))}
            </Space>
          </Checkbox.Group>
          {charts.length === 0 && <Empty description="저장된 차트가 없습니다" />}
        </Space>
      )}
    </div>,

    // Step 2: Generate
    <div key="s2" style={{ textAlign: 'center', padding: '20px 0' }}>
      <div style={{ marginBottom: 16 }}>
        <Text strong style={{ fontSize: 16 }}>내보내기 요약</Text>
      </div>
      <Space direction="vertical" size="middle">
        <div>
          <Tag color="blue" style={{ fontSize: 14, padding: '4px 12px' }}>{FORMATS.find(f => f.value === format)?.label}</Tag>
          <Text style={{ marginLeft: 8 }}>{selectedChartIds.length}개 차트</Text>
        </div>
        <Button
          type="primary"
          size="large"
          icon={exporting ? undefined : <DownloadOutlined />}
          loading={exporting}
          onClick={handleExport}
        >
          {exporting ? '생성 중...' : '보고서 생성'}
        </Button>
      </Space>
    </div>,
  ];

  return (
    <Card size="small">
      <Steps
        current={step}
        size="small"
        items={[
          { title: '포맷 선택' },
          { title: '콘텐츠 선택' },
          { title: '생성' },
        ]}
        style={{ marginBottom: 20 }}
      />
      <div style={{ minHeight: 200 }}>{stepContent[step]}</div>
      <div style={{ marginTop: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Button disabled={step === 0} onClick={() => setStep(s => s - 1)}>이전</Button>
        {step < 2 && (
          <Button type="primary" onClick={() => setStep(s => s + 1)}>다음</Button>
        )}
      </div>
    </Card>
  );
};

export default ReportExporter;
