import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  Typography, Space, Tag, Input, Spin, Pagination, Select, Empty,
  Descriptions, Drawer, Slider, Switch, Tooltip, Badge,
} from 'antd';
import {
  SearchOutlined, EyeOutlined, FileTextOutlined, ExperimentOutlined,
  PictureOutlined, MenuFoldOutlined, MenuUnfoldOutlined,
  ZoomInOutlined, ZoomOutOutlined, ExpandOutlined,
  InfoCircleOutlined, BgColorsOutlined, AimOutlined,
  ColumnWidthOutlined,
} from '@ant-design/icons';
import { Microscope, Layers, Move, Crosshair, Ruler } from 'lucide-react';

const { Text } = Typography;
const { Search } = Input;

const API = '/api/v1/pathology';

// ── Annotation label config ────────────────────────────────
const LABEL_COLORS: Record<string, string> = {
  Tumor: 'rgba(255, 77, 79, 0.45)',
  Normal: 'rgba(82, 196, 26, 0.40)',
  Stroma: 'rgba(24, 144, 255, 0.30)',
  Immune: 'rgba(250, 173, 20, 0.45)',
};
const LABEL_BORDER: Record<string, string> = {
  Tumor: '#ff4d4f', Normal: '#52c41a', Stroma: '#1890ff', Immune: '#faad14',
};
const LABEL_KO: Record<string, string> = {
  Tumor: '종양', Normal: '정상 조직', Stroma: '기질', Immune: '면역세포',
};

// ── Types ──────────────────────────────────────────────────
interface CaseItem {
  case_id: string; cancer_type: string; category: string;
  category_label: string; tumor_category: string; has_image: boolean;
  file_name: string;
}
interface CaseDetail {
  case_id: string; cancer_type: string; category: string;
  category_label: string; tumor_category: string; has_image: boolean;
  clinical: Record<string, unknown>;
  file: {
    file_name?: string; patch_id?: string; format?: string;
    mpp?: number; filesize?: string; width?: number; height?: number;
    class?: string; patch_discription?: string;
    object?: { type: string; label: string; coordinate: number[][] }[];
  };
}
interface Stats {
  total: number;
  stomach: { total: number; with_image: number; categories: Record<string, { count: number; label: string; with_image: number }> };
  breast: { total: number; with_image: number; categories: Record<string, { count: number; label: string; with_image: number }> };
}

// ── Styles ─────────────────────────────────────────────────
const S = {
  root: { display: 'flex', height: 'calc(100vh - 56px)', overflow: 'hidden', background: '#111' } as React.CSSProperties,
  sidebar: (open: boolean): React.CSSProperties => ({
    width: open ? 280 : 0, minWidth: open ? 280 : 0,
    background: '#1a1d23', borderRight: '1px solid #2a2d35',
    display: 'flex', flexDirection: 'column', overflow: 'hidden',
    transition: 'width 0.25s ease, min-width 0.25s ease',
  }),
  sidebarHeader: {
    padding: '12px 14px', borderBottom: '1px solid #2a2d35',
    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
  } as React.CSSProperties,
  sidebarBody: {
    flex: 1, overflowY: 'auto', padding: '8px',
  } as React.CSSProperties,
  viewer: {
    flex: 1, position: 'relative', overflow: 'hidden', background: '#0d0d0d',
    cursor: 'grab',
  } as React.CSSProperties,
  toolbar: {
    position: 'absolute', top: 12, left: 12, zIndex: 20,
    display: 'flex', flexDirection: 'column', gap: 2,
    background: 'rgba(26,29,35,0.92)', borderRadius: 8,
    padding: '6px 4px', backdropFilter: 'blur(8px)',
    border: '1px solid rgba(255,255,255,0.08)',
  } as React.CSSProperties,
  toolBtn: (active?: boolean): React.CSSProperties => ({
    width: 36, height: 36, display: 'flex', alignItems: 'center', justifyContent: 'center',
    borderRadius: 6, border: 'none', cursor: 'pointer',
    background: active ? 'rgba(0,91,172,0.5)' : 'transparent',
    color: active ? '#69b1ff' : '#aab',
    transition: 'all 0.15s',
  }),
  minimap: {
    position: 'absolute', bottom: 12, right: 12, zIndex: 20,
    width: 160, height: 120, borderRadius: 6, overflow: 'hidden',
    border: '1px solid rgba(255,255,255,0.15)',
    background: 'rgba(0,0,0,0.7)',
  } as React.CSSProperties,
  statusBar: {
    position: 'absolute', bottom: 0, left: 0, right: 0, zIndex: 15,
    height: 28, background: 'rgba(26,29,35,0.9)', borderTop: '1px solid #2a2d35',
    display: 'flex', alignItems: 'center', padding: '0 12px', gap: 16,
  } as React.CSSProperties,
  legend: {
    position: 'absolute', bottom: 40, left: 12, zIndex: 15,
    background: 'rgba(26,29,35,0.9)', borderRadius: 8, padding: '8px 12px',
    border: '1px solid rgba(255,255,255,0.08)',
  } as React.CSSProperties,
  zoomInfo: {
    position: 'absolute', top: 12, right: 12, zIndex: 15,
    background: 'rgba(26,29,35,0.9)', borderRadius: 6, padding: '6px 12px',
    border: '1px solid rgba(255,255,255,0.08)',
  } as React.CSSProperties,
  enhancePanel: {
    position: 'absolute', top: 12, right: 60, zIndex: 20,
    background: 'rgba(26,29,35,0.95)', borderRadius: 8, padding: '12px 16px',
    border: '1px solid rgba(255,255,255,0.1)', width: 200,
    backdropFilter: 'blur(8px)',
  } as React.CSSProperties,
  caseItem: (selected: boolean): React.CSSProperties => ({
    padding: '8px 10px', marginBottom: 3, borderRadius: 6, cursor: 'pointer',
    border: `1px solid ${selected ? '#005BAC' : '#2a2d35'}`,
    background: selected ? 'rgba(0,91,172,0.15)' : '#1e2128',
    transition: 'all 0.15s',
  }),
};

// ── Component ──────────────────────────────────────────────
const DigitalPathology: React.FC = () => {
  // ── State: sidebar & data ──
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [stats, setStats] = useState<Stats | null>(null);
  const [cases, setCases] = useState<CaseItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [cancerFilter, setCancerFilter] = useState<string>('all');
  const [categoryFilter, setCategoryFilter] = useState<string>('');
  const [searchText, setSearchText] = useState('');
  const [page, setPage] = useState(1);
  const pageSize = 20;

  const [selectedCase, setSelectedCase] = useState<CaseDetail | null>(null);
  const [caseLoading, setCaseLoading] = useState(false);

  // ── State: viewer ──
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const dragStart = useRef({ x: 0, y: 0, panX: 0, panY: 0 });
  const viewerRef = useRef<HTMLDivElement>(null);
  const imgRef = useRef<HTMLImageElement>(null);

  // ── State: tools ──
  type Tool = 'move' | 'crosshair' | 'measure';
  const [activeTool, setActiveTool] = useState<Tool>('move');
  const [showAnnotations, setShowAnnotations] = useState(true);
  const [showEnhance, setShowEnhance] = useState(false);
  const [brightness, setBrightness] = useState(100);
  const [contrast, setContrast] = useState(100);
  const [infoDrawer, setInfoDrawer] = useState(false);

  // ── State: measure (screen px coordinates relative to viewer) ──
  const [measureStart, setMeasureStart] = useState<{ x: number; y: number } | null>(null);
  const [measureEnd, setMeasureEnd] = useState<{ x: number; y: number } | null>(null);
  const [measureLive, setMeasureLive] = useState<{ x: number; y: number } | null>(null);

  // ── State: image loaded dimensions ──
  const [imgNatural, setImgNatural] = useState({ w: 0, h: 0 });

  // ── Data fetching ──
  useEffect(() => {
    fetch(`${API}/stats`).then(r => r.json()).then(setStats).catch(() => {});
  }, []);

  const fetchCases = useCallback(() => {
    setLoading(true);
    const params = new URLSearchParams();
    if (cancerFilter !== 'all') params.set('cancer_type', cancerFilter);
    if (categoryFilter) params.set('category', categoryFilter);
    if (searchText) params.set('search', searchText);
    params.set('offset', String((page - 1) * pageSize));
    params.set('limit', String(pageSize));
    fetch(`${API}/cases?${params}`)
      .then(r => r.json())
      .then(d => { setCases(d.items || []); setTotal(d.total || 0); })
      .catch(() => { setCases([]); setTotal(0); })
      .finally(() => setLoading(false));
  }, [cancerFilter, categoryFilter, searchText, page]);

  useEffect(() => { fetchCases(); }, [fetchCases]);

  const selectCase = (caseId: string) => {
    setCaseLoading(true);
    // Reset viewer
    setZoom(1); setPan({ x: 0, y: 0 });
    setBrightness(100); setContrast(100);
    setMeasureStart(null); setMeasureEnd(null); setMeasureLive(null);
    fetch(`${API}/cases/${caseId}`)
      .then(r => r.json())
      .then(d => setSelectedCase(d))
      .catch(() => {})
      .finally(() => setCaseLoading(false));
  };

  // ── Derived ──
  const imageUrl = selectedCase?.has_image
    ? `${API}/images/${selectedCase.cancer_type}/${selectedCase.file?.file_name}`
    : null;

  const annotationSummary = useMemo(() => {
    if (!selectedCase?.file?.object) return {};
    const s: Record<string, number> = {};
    for (const obj of selectedCase.file.object) s[obj.label] = (s[obj.label] || 0) + 1;
    return s;
  }, [selectedCase]);

  const categoryOptions = useMemo(() => {
    if (!stats) return [];
    const opts: { value: string; label: string }[] = [];
    const add = (cats: Record<string, { count: number; label: string }>) => {
      for (const [k, v] of Object.entries(cats))
        opts.push({ value: k, label: `${k} — ${v.label} (${v.count})` });
    };
    if (cancerFilter === 'all' || cancerFilter === 'stomach') add(stats.stomach?.categories || {});
    if (cancerFilter === 'all' || cancerFilter === 'breast') add(stats.breast?.categories || {});
    return opts;
  }, [stats, cancerFilter]);

  // ── Viewer: zoom & pan handlers ──
  const handleWheel = useCallback((e: React.WheelEvent) => {
    e.preventDefault();
    const factor = e.deltaY < 0 ? 1.15 : 0.87;
    setZoom(z => Math.max(0.1, Math.min(20, z * factor)));
  }, []);

  // Convert screen click to normalized image coords (0~1)
  const screenToImgCoord = useCallback((e: React.MouseEvent) => {
    if (!imgRef.current) return null;
    const rect = imgRef.current.getBoundingClientRect();
    const x = (e.clientX - rect.left) / rect.width;
    const y = (e.clientY - rect.top) / rect.height;
    if (x < 0 || x > 1 || y < 0 || y > 1) return null; // outside image
    return { x, y };
  }, []);

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    if (activeTool === 'move') {
      setIsDragging(true);
      dragStart.current = { x: e.clientX, y: e.clientY, panX: pan.x, panY: pan.y };
    } else if (activeTool === 'measure') {
      const pt = screenToImgCoord(e);
      if (!pt) return;
      if (!measureStart || measureEnd) {
        // First click: set start, clear end
        setMeasureStart(pt); setMeasureEnd(null); setMeasureLive(pt);
      } else {
        // Second click: set end
        setMeasureEnd(pt); setMeasureLive(null);
      }
    }
  }, [activeTool, pan, measureStart, measureEnd, screenToImgCoord]);

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    if (isDragging) {
      const dx = e.clientX - dragStart.current.x;
      const dy = e.clientY - dragStart.current.y;
      setPan({ x: dragStart.current.panX + dx, y: dragStart.current.panY + dy });
    } else if (activeTool === 'measure' && measureStart && !measureEnd) {
      // Live tracking for measure line
      const pt = screenToImgCoord(e);
      if (pt) setMeasureLive(pt);
    }
  }, [isDragging, activeTool, measureStart, measureEnd, screenToImgCoord]);

  const handleMouseUp = useCallback(() => { setIsDragging(false); }, []);

  const resetView = useCallback(() => {
    setZoom(1); setPan({ x: 0, y: 0 });
  }, []);

  // ── Keyboard shortcuts ──
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      switch (e.key) {
        case '+': case '=': e.preventDefault(); setZoom(z => Math.min(20, z * 1.3)); break;
        case '-': e.preventDefault(); setZoom(z => Math.max(0.1, z * 0.77)); break;
        case '0': e.preventDefault(); resetView(); break;
        case 'a': setShowAnnotations(v => !v); break;
        case 's': setSidebarOpen(v => !v); break;
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [resetView]);

  // ── Measure distance (using mpp if available) ──
  // Measure distance: show for both confirmed end and live tracking
  const measureEndPoint = measureEnd || measureLive;
  const measureDistance = useMemo(() => {
    if (!measureStart || !measureEndPoint || !imgNatural.w) return null;
    const dx = (measureEndPoint.x - measureStart.x) * imgNatural.w;
    const dy = (measureEndPoint.y - measureStart.y) * imgNatural.h;
    const px = Math.sqrt(dx * dx + dy * dy);
    if (px < 1) return null; // too small
    const mpp = selectedCase?.file?.mpp;
    if (mpp) return { px, um: (px * mpp).toFixed(1), confirmed: !!measureEnd };
    return { px, um: null, confirmed: !!measureEnd };
  }, [measureStart, measureEndPoint, imgNatural, selectedCase, measureEnd]);

  // ── Render ───────────────────────────────────────────────
  return (
    <div style={S.root}>
      {/* ━━ LEFT SIDEBAR ━━ */}
      <div style={S.sidebar(sidebarOpen)}>
        {sidebarOpen && (
          <>
            {/* Sidebar header */}
            <div style={S.sidebarHeader}>
              <Space size={8}>
                <Microscope size={18} color="#69b1ff" />
                <Text strong style={{ color: '#e0e0e0', fontSize: 14 }}>케이스 목록</Text>
              </Space>
              <Space size={8}>
                <Badge count={total} overflowCount={9999} style={{ backgroundColor: '#005BAC' }} />
                <MenuFoldOutlined
                  style={{ color: '#aab', cursor: 'pointer', fontSize: 14 }}
                  onClick={() => setSidebarOpen(false)}
                />
              </Space>
            </div>

            {/* Filters */}
            <div style={{ padding: '8px 10px', borderBottom: '1px solid #2a2d35' }}>
              <Space direction="vertical" style={{ width: '100%' }} size={6}>
                <Select
                  value={cancerFilter} size="small" style={{ width: '100%' }}
                  onChange={v => { setCancerFilter(v); setCategoryFilter(''); setPage(1); }}
                  options={[
                    { value: 'all', label: '전체 암종' },
                    { value: 'stomach', label: '위암 (Stomach)' },
                    { value: 'breast', label: '유방암 (Breast)' },
                  ]}
                  classNames={{ popup: { root: 'dark-select' } }}
                />
                <Select
                  value={categoryFilter} size="small" style={{ width: '100%' }}
                  onChange={v => { setCategoryFilter(v); setPage(1); }}
                  allowClear placeholder="세부 분류" options={categoryOptions}
                />
                <Search
                  placeholder="Case ID 검색" size="small" allowClear
                  onSearch={v => { setSearchText(v); setPage(1); }}
                  prefix={<SearchOutlined style={{ color: '#666' }} />}
                />
              </Space>
            </div>

            {/* Stats row */}
            {stats && (
              <div style={{
                padding: '6px 10px', borderBottom: '1px solid #2a2d35',
                display: 'flex', gap: 8, fontSize: 11, color: '#8a8a9a',
              }}>
                <span>위암 <b style={{ color: '#69b1ff' }}>{stats.stomach?.total || 0}</b></span>
                <span>|</span>
                <span>유방암 <b style={{ color: '#ff85c0' }}>{stats.breast?.total || 0}</b></span>
                <span>|</span>
                <span>전체 <b style={{ color: '#e0e0e0' }}>{stats.total}</b></span>
              </div>
            )}

            {/* Case list */}
            <div style={S.sidebarBody}>
              <Spin spinning={loading} size="small">
                {cases.map(c => (
                  <div
                    key={c.case_id}
                    onClick={() => selectCase(c.case_id)}
                    style={S.caseItem(selectedCase?.case_id === c.case_id)}
                    onMouseEnter={e => {
                      if (selectedCase?.case_id !== c.case_id)
                        (e.currentTarget as HTMLDivElement).style.background = '#262a33';
                    }}
                    onMouseLeave={e => {
                      if (selectedCase?.case_id !== c.case_id)
                        (e.currentTarget as HTMLDivElement).style.background = '#1e2128';
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <Text style={{ color: '#d0d0d0', fontSize: 12, fontFamily: 'monospace', maxWidth: 150, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {c.case_id}
                      </Text>
                      <Space size={3}>
                        {c.has_image && <PictureOutlined style={{ color: '#52c41a', fontSize: 11 }} />}
                        <Tag
                          color={c.cancer_type === 'stomach' ? 'blue' : 'magenta'}
                          style={{ fontSize: 10, lineHeight: '16px', margin: 0, padding: '0 4px' }}
                        >
                          {c.cancer_type === 'stomach' ? '위' : '유방'}
                        </Tag>
                      </Space>
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 2 }}>
                      <Text style={{ color: '#7a7a8a', fontSize: 10 }}>{c.category_label}</Text>
                      <Tag
                        color={c.tumor_category === 'normal' ? 'green' : 'orange'}
                        style={{ fontSize: 9, lineHeight: '14px', margin: 0, padding: '0 3px' }}
                      >
                        {c.tumor_category}
                      </Tag>
                    </div>
                  </div>
                ))}
                {cases.length === 0 && !loading && (
                  <Empty description={<Text style={{ color: '#666' }}>결과 없음</Text>} style={{ marginTop: 60 }} />
                )}
              </Spin>
            </div>

            {/* Pagination */}
            <div style={{ padding: '6px 10px', borderTop: '1px solid #2a2d35', textAlign: 'center' }}>
              <Pagination
                current={page} total={total} pageSize={pageSize}
                onChange={p => setPage(p)} size="small" showSizeChanger={false} simple
              />
            </div>
          </>
        )}
      </div>

      {/* ━━ MAIN VIEWER AREA ━━ */}
      <div
        ref={viewerRef}
        style={{
          ...S.viewer,
          cursor: activeTool === 'move' ? (isDragging ? 'grabbing' : 'grab')
            : activeTool === 'crosshair' ? 'crosshair'
            : activeTool === 'measure' ? 'crosshair' : 'default',
        }}
        onWheel={handleWheel}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
      >
        {/* Toggle sidebar button (when collapsed) */}
        {!sidebarOpen && (
          <Tooltip title="케이스 목록 (S)" placement="right">
            <button
              onClick={() => setSidebarOpen(true)}
              style={{
                ...S.toolBtn(false),
                position: 'absolute', top: 12, left: 12, zIndex: 25,
                background: 'rgba(26,29,35,0.92)', border: '1px solid rgba(255,255,255,0.08)',
                borderRadius: 8,
              }}
            >
              <MenuUnfoldOutlined style={{ fontSize: 16 }} />
            </button>
          </Tooltip>
        )}

        {/* ── Floating Toolbar ── */}
        <div style={{ ...S.toolbar, left: sidebarOpen ? 12 : 56 }}>
          <Tooltip title="이동 (드래그)" placement="right">
            <button style={S.toolBtn(activeTool === 'move')} onClick={() => setActiveTool('move')}>
              <Move size={16} />
            </button>
          </Tooltip>
          <Tooltip title="십자선" placement="right">
            <button style={S.toolBtn(activeTool === 'crosshair')} onClick={() => setActiveTool('crosshair')}>
              <Crosshair size={16} />
            </button>
          </Tooltip>
          <Tooltip title="측정" placement="right">
            <button style={S.toolBtn(activeTool === 'measure')} onClick={() => { setActiveTool('measure'); setMeasureStart(null); setMeasureEnd(null); setMeasureLive(null); }}>
              <Ruler size={16} />
            </button>
          </Tooltip>
          <div style={{ height: 1, background: '#333', margin: '4px 6px' }} />
          <Tooltip title="확대" placement="right">
            <button style={S.toolBtn()} onClick={() => setZoom(z => Math.min(20, z * 1.4))}>
              <ZoomInOutlined style={{ fontSize: 16 }} />
            </button>
          </Tooltip>
          <Tooltip title="축소" placement="right">
            <button style={S.toolBtn()} onClick={() => setZoom(z => Math.max(0.1, z * 0.7))}>
              <ZoomOutOutlined style={{ fontSize: 16 }} />
            </button>
          </Tooltip>
          <Tooltip title="전체 보기 (0)" placement="right">
            <button style={S.toolBtn()} onClick={resetView}>
              <ExpandOutlined style={{ fontSize: 16 }} />
            </button>
          </Tooltip>
          <div style={{ height: 1, background: '#333', margin: '4px 6px' }} />
          <Tooltip title="어노테이션 (A)" placement="right">
            <button style={S.toolBtn(showAnnotations)} onClick={() => setShowAnnotations(v => !v)}>
              <Layers size={16} />
            </button>
          </Tooltip>
          <Tooltip title="이미지 보정" placement="right">
            <button style={S.toolBtn(showEnhance)} onClick={() => setShowEnhance(v => !v)}>
              <BgColorsOutlined style={{ fontSize: 16 }} />
            </button>
          </Tooltip>
          <Tooltip title="케이스 정보" placement="right">
            <button style={S.toolBtn(infoDrawer)} onClick={() => setInfoDrawer(v => !v)}>
              <InfoCircleOutlined style={{ fontSize: 16 }} />
            </button>
          </Tooltip>
        </div>

        {/* ── Enhancement Panel ── */}
        {showEnhance && (
          <div style={S.enhancePanel}>
            <Text style={{ color: '#ccc', fontSize: 12, fontWeight: 600 }}>이미지 보정</Text>
            <div style={{ marginTop: 10 }}>
              <Text style={{ color: '#999', fontSize: 11 }}>밝기: {brightness}%</Text>
              <Slider min={20} max={300} value={brightness} onChange={setBrightness}
                styles={{ track: { background: '#005BAC' }, rail: { background: '#333' } }} />
            </div>
            <div style={{ marginTop: 4 }}>
              <Text style={{ color: '#999', fontSize: 11 }}>대비: {contrast}%</Text>
              <Slider min={20} max={300} value={contrast} onChange={setContrast}
                styles={{ track: { background: '#005BAC' }, rail: { background: '#333' } }} />
            </div>
            <div style={{ marginTop: 8, textAlign: 'right' }}>
              <a style={{ color: '#69b1ff', fontSize: 11, cursor: 'pointer' }}
                onClick={() => { setBrightness(100); setContrast(100); }}>
                초기화
              </a>
            </div>
          </div>
        )}

        {/* ── Zoom info badge ── */}
        <div style={S.zoomInfo}>
          <Text style={{ color: '#aab', fontSize: 12, fontFamily: 'monospace' }}>
            {(zoom * 100).toFixed(0)}%
          </Text>
        </div>

        {/* ── Image + Annotations ── */}
        {caseLoading ? (
          <div style={{ display: 'flex', height: '100%', justifyContent: 'center', alignItems: 'center' }}>
            <Spin size="large" />
          </div>
        ) : selectedCase ? (
          imageUrl ? (
            <div style={{
              position: 'absolute', inset: 0,
              display: 'flex', justifyContent: 'center', alignItems: 'center',
            }}>
              <div style={{
                transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
                transformOrigin: 'center center',
                transition: isDragging ? 'none' : 'transform 0.1s ease-out',
                position: 'relative',
                filter: `brightness(${brightness}%) contrast(${contrast}%)`,
              }}>
                <img
                  ref={imgRef}
                  src={imageUrl}
                  alt={selectedCase.case_id}
                  draggable={false}
                  onLoad={() => {
                    if (imgRef.current) {
                      setImgNatural({
                        w: imgRef.current.naturalWidth,
                        h: imgRef.current.naturalHeight,
                      });
                    }
                  }}
                  style={{
                    maxWidth: '85vw', maxHeight: '85vh',
                    display: 'block', userSelect: 'none',
                    borderRadius: 2,
                  }}
                />
                {/* SVG annotation overlay */}
                {showAnnotations && selectedCase.file?.object && imgNatural.w > 0 && (
                  <svg
                    style={{
                      position: 'absolute', top: 0, left: 0,
                      width: '100%', height: '100%',
                      pointerEvents: 'none',
                    }}
                    viewBox={`0 0 ${imgNatural.w} ${imgNatural.h}`}
                    preserveAspectRatio="none"
                  >
                    {selectedCase.file.object.map((obj, i) => {
                      if (!obj.coordinate || obj.coordinate.length < 3) return null;
                      const d = obj.coordinate.map((c, j) =>
                        `${j === 0 ? 'M' : 'L'}${c[0]},${c[1]}`
                      ).join(' ') + ' Z';
                      return (
                        <path
                          key={i} d={d}
                          fill={LABEL_COLORS[obj.label] || 'rgba(128,128,128,0.3)'}
                          stroke={LABEL_BORDER[obj.label] || '#888'}
                          strokeWidth={Math.max(1, imgNatural.w / 500)}
                        />
                      );
                    })}
                  </svg>
                )}
                {/* Measure line is rendered outside zoom container */}
              </div>
            </div>
          ) : (
            <div style={{
              display: 'flex', height: '100%', flexDirection: 'column',
              justifyContent: 'center', alignItems: 'center',
            }}>
              <PictureOutlined style={{ fontSize: 48, color: '#444' }} />
              <Text style={{ color: '#666', marginTop: 12 }}>이미지 없음</Text>
            </div>
          )
        ) : (
          <div style={{
            display: 'flex', height: '100%', flexDirection: 'column',
            justifyContent: 'center', alignItems: 'center', gap: 12,
          }}>
            <Microscope size={56} color="#333" />
            <Text style={{ color: '#555', fontSize: 15 }}>좌측 목록에서 케이스를 선택하세요</Text>
            <Text style={{ color: '#444', fontSize: 12 }}>
              마우스 휠: 확대/축소 | 드래그: 이동 | A: 어노테이션 토글
            </Text>
          </div>
        )}

        {/* ── Measure line (outside zoom container — constant thickness) ── */}
        {measureStart && measureEndPoint && imgRef.current && (() => {
          const rect = imgRef.current!.getBoundingClientRect();
          const viewerRect = viewerRef.current?.getBoundingClientRect();
          if (!viewerRect) return null;
          const ox = rect.left - viewerRect.left;
          const oy = rect.top - viewerRect.top;
          const toX = (pt: { x: number; y: number }) => ox + pt.x * rect.width;
          const toY = (pt: { x: number; y: number }) => oy + pt.y * rect.height;
          return (
            <svg style={{
              position: 'absolute', inset: 0, zIndex: 18,
              pointerEvents: 'none', width: '100%', height: '100%',
            }}>
              <line
                x1={toX(measureStart)} y1={toY(measureStart)}
                x2={toX(measureEndPoint)} y2={toY(measureEndPoint)}
                stroke="#00ff88" strokeWidth={1.5}
                strokeDasharray="6 4"
                opacity={measureEnd ? 1 : 0.6}
              />
              {/* Start point */}
              <circle cx={toX(measureStart)} cy={toY(measureStart)} r={4}
                fill="#00ff88" stroke="#003311" strokeWidth={1} />
              {/* End point (solid if confirmed, hollow if live) */}
              <circle cx={toX(measureEndPoint)} cy={toY(measureEndPoint)} r={4}
                fill={measureEnd ? '#00ff88' : 'none'}
                stroke="#00ff88" strokeWidth={1.5} />
              {/* Distance label at midpoint */}
              {measureDistance && (
                <text
                  x={(toX(measureStart) + toX(measureEndPoint)) / 2}
                  y={(toY(measureStart) + toY(measureEndPoint)) / 2 - 10}
                  fill="#00ff88" fontSize={12} fontFamily="monospace"
                  textAnchor="middle"
                  style={{ textShadow: '0 0 4px rgba(0,0,0,0.9)' }}
                >
                  {measureDistance.px.toFixed(1)}px
                  {measureDistance.um ? ` (${measureDistance.um}µm)` : ''}
                </text>
              )}
            </svg>
          );
        })()}

        {/* ── Annotation Legend ── */}
        {showAnnotations && Object.keys(annotationSummary).length > 0 && (
          <div style={S.legend}>
            <Text style={{ color: '#888', fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: 1 }}>
              Annotations
            </Text>
            <div style={{ marginTop: 6, display: 'flex', flexDirection: 'column', gap: 4 }}>
              {Object.entries(annotationSummary).map(([label, count]) => (
                <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div style={{
                    width: 12, height: 12, borderRadius: 2,
                    background: LABEL_COLORS[label] || 'rgba(128,128,128,0.3)',
                    border: `2px solid ${LABEL_BORDER[label] || '#888'}`,
                  }} />
                  <Text style={{ color: '#ccc', fontSize: 11 }}>
                    {LABEL_KO[label] || label}
                  </Text>
                  <Text style={{ color: '#777', fontSize: 10 }}>({count})</Text>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* ── Measure result ── */}
        {measureDistance && (
          <div style={{
            position: 'absolute', bottom: 40, right: 12, zIndex: 15,
            background: 'rgba(0,30,0,0.85)', borderRadius: 6, padding: '6px 12px',
            border: '1px solid rgba(0,255,136,0.3)',
          }}>
            <Space size={8}>
              <ColumnWidthOutlined style={{ color: '#00ff88', fontSize: 14 }} />
              <Text style={{ color: '#00ff88', fontSize: 12, fontFamily: 'monospace' }}>
                {measureDistance.px.toFixed(1)} px
                {measureDistance.um && ` = ${measureDistance.um} \u00B5m`}
              </Text>
            </Space>
          </div>
        )}

        {/* ── Minimap ── */}
        {imageUrl && imgNatural.w > 0 && (
          <div style={S.minimap}>
            <img
              src={imageUrl}
              alt="minimap"
              style={{ width: '100%', height: '100%', objectFit: 'contain', opacity: 0.7 }}
              draggable={false}
            />
            {/* Viewport indicator */}
            {viewerRef.current && (() => {
              const vw = viewerRef.current!.clientWidth;
              const vh = viewerRef.current!.clientHeight;
              // Compute displayed image size
              const imgRatio = imgNatural.w / imgNatural.h;
              const maxW = vw * 0.85;
              const maxH = vh * 0.85;
              let dispW = maxW, dispH = maxW / imgRatio;
              if (dispH > maxH) { dispH = maxH; dispW = maxH * imgRatio; }
              // Viewport in image coordinates
              const vpW = vw / (zoom * dispW / imgNatural.w);
              const vpH = vh / (zoom * dispH / imgNatural.h);
              const vpX = imgNatural.w / 2 - pan.x / (zoom * dispW / imgNatural.w) - vpW / 2;
              const vpY = imgNatural.h / 2 - pan.y / (zoom * dispH / imgNatural.h) - vpH / 2;
              // Minimap scale
              const mmW = 160, mmH = 120;
              const mmImgRatio = imgNatural.w / imgNatural.h;
              let mmImgW = mmW, mmImgH = mmW / mmImgRatio;
              if (mmImgH > mmH) { mmImgH = mmH; mmImgW = mmH * mmImgRatio; }
              const mmOffX = (mmW - mmImgW) / 2;
              const mmOffY = (mmH - mmImgH) / 2;
              const sx = mmImgW / imgNatural.w;
              const sy = mmImgH / imgNatural.h;
              return (
                <div style={{
                  position: 'absolute',
                  left: mmOffX + vpX * sx, top: mmOffY + vpY * sy,
                  width: Math.min(vpW * sx, mmImgW), height: Math.min(vpH * sy, mmImgH),
                  border: '1.5px solid #69b1ff',
                  background: 'rgba(0,91,172,0.12)',
                  borderRadius: 1,
                  transition: isDragging ? 'none' : 'all 0.15s ease-out',
                  pointerEvents: 'none',
                }} />
              );
            })()}
          </div>
        )}

        {/* ── Status Bar ── */}
        <div style={S.statusBar}>
          {selectedCase && (
            <>
              <Text style={{ color: '#888', fontSize: 11 }}>
                <Tag color={selectedCase.cancer_type === 'stomach' ? 'blue' : 'magenta'} style={{ fontSize: 10, lineHeight: '16px', margin: 0, padding: '0 4px' }}>
                  {selectedCase.cancer_type === 'stomach' ? '위암' : '유방암'}
                </Tag>
              </Text>
              <Text style={{ color: '#777', fontSize: 11, fontFamily: 'monospace' }}>
                {selectedCase.case_id}
              </Text>
              {selectedCase.file?.width && (
                <Text style={{ color: '#666', fontSize: 11 }}>
                  {selectedCase.file.width} x {selectedCase.file.height}
                </Text>
              )}
              {selectedCase.file?.mpp && (
                <Text style={{ color: '#666', fontSize: 11 }}>
                  MPP: {selectedCase.file.mpp}
                </Text>
              )}
              <Text style={{ color: '#666', fontSize: 11 }}>
                {selectedCase.file?.format}
              </Text>
              {selectedCase.file?.object && (
                <Text style={{ color: '#666', fontSize: 11 }}>
                  <AimOutlined /> {selectedCase.file.object.length} annotations
                </Text>
              )}
              <div style={{ flex: 1 }} />
              <Text style={{ color: '#555', fontSize: 10 }}>
                Zoom: {(zoom * 100).toFixed(0)}% | 마우스 휠 확대/축소 | A: 어노테이션
              </Text>
            </>
          )}
          {!selectedCase && (
            <Text style={{ color: '#555', fontSize: 11 }}>
              AI-Hub 위암/유방암 병리 이미지 및 판독문 합성데이터
            </Text>
          )}
        </div>
      </div>

      {/* ━━ INFO DRAWER ━━ */}
      <Drawer
        title={
          <Space>
            <EyeOutlined style={{ color: '#005BAC' }} />
            <span>케이스 상세 정보</span>
          </Space>
        }
        placement="right" width={380} open={infoDrawer}
        onClose={() => setInfoDrawer(false)}
        styles={{
          header: { background: '#f5f7fa', borderBottom: '1px solid #e8e8e8' },
          body: { padding: '16px' },
        }}
      >
        {selectedCase ? (
          <Space direction="vertical" style={{ width: '100%' }} size={16}>
            {/* Clinical */}
            <div>
              <Text strong style={{ fontSize: 13, color: '#005BAC' }}>
                <FileTextOutlined /> 임상 정보
              </Text>
              <Descriptions column={1} size="small" style={{ marginTop: 8 }}
                styles={{ label: { fontSize: 12, width: 90 }, content: { fontSize: 12 } }}>
                <Descriptions.Item label="암종">
                  {selectedCase.cancer_type === 'stomach' ? '위암' : '유방암'}
                </Descriptions.Item>
                <Descriptions.Item label="분류">{selectedCase.category_label}</Descriptions.Item>
                <Descriptions.Item label="종양 코드">{selectedCase.clinical?.tumor_code as string}</Descriptions.Item>
                <Descriptions.Item label="카테고리">{selectedCase.category}</Descriptions.Item>
                <Descriptions.Item label="종양 판정">
                  <Tag color={selectedCase.tumor_category === 'normal' ? 'green' : 'red'} style={{ fontSize: 11 }}>
                    {selectedCase.tumor_category === 'normal' ? '정상' : '비정상'}
                  </Tag>
                </Descriptions.Item>
              </Descriptions>
            </div>

            {/* File info */}
            <div>
              <Text strong style={{ fontSize: 13, color: '#005BAC' }}>
                <ExperimentOutlined /> 파일 정보
              </Text>
              <Descriptions column={1} size="small" style={{ marginTop: 8 }}
                styles={{ label: { fontSize: 12, width: 90 }, content: { fontSize: 12 } }}>
                <Descriptions.Item label="파일명">{selectedCase.file?.file_name}</Descriptions.Item>
                <Descriptions.Item label="포맷">
                  {selectedCase.file?.format} ({selectedCase.file?.width}x{selectedCase.file?.height})
                </Descriptions.Item>
                <Descriptions.Item label="MPP">{selectedCase.file?.mpp}</Descriptions.Item>
                <Descriptions.Item label="파일 크기">{selectedCase.file?.filesize}</Descriptions.Item>
                <Descriptions.Item label="조직 분류">{selectedCase.file?.class}</Descriptions.Item>
                <Descriptions.Item label="어노테이션">
                  {selectedCase.file?.object?.length || 0}개 영역
                </Descriptions.Item>
              </Descriptions>
            </div>

            {/* Annotation summary */}
            {Object.keys(annotationSummary).length > 0 && (
              <div>
                <Text strong style={{ fontSize: 13, color: '#005BAC' }}>
                  <Layers size={13} /> 어노테이션 요약
                </Text>
                <div style={{ marginTop: 8 }}>
                  {Object.entries(annotationSummary).map(([label, count]) => (
                    <div key={label} style={{
                      display: 'flex', alignItems: 'center', gap: 8,
                      padding: '4px 0', borderBottom: '1px solid #f0f0f0',
                    }}>
                      <div style={{
                        width: 14, height: 14, borderRadius: 3,
                        background: LABEL_COLORS[label] || 'rgba(128,128,128,0.3)',
                        border: `2px solid ${LABEL_BORDER[label] || '#888'}`,
                      }} />
                      <Text style={{ flex: 1, fontSize: 12 }}>{LABEL_KO[label] || label}</Text>
                      <Tag style={{ fontSize: 11 }}>{count}개</Tag>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Patch description */}
            {selectedCase.file?.patch_discription && (
              <div>
                <Text strong style={{ fontSize: 13, color: '#005BAC' }}>병리 소견</Text>
                <div style={{
                  marginTop: 8, padding: '10px 12px',
                  background: '#f9f9f9', borderRadius: 6, border: '1px solid #eee',
                }}>
                  <Text style={{ fontSize: 12, lineHeight: 1.7 }}>
                    {selectedCase.file.patch_discription}
                  </Text>
                </div>
              </div>
            )}
          </Space>
        ) : (
          <Empty description="케이스를 선택하세요" />
        )}
      </Drawer>
    </div>
  );
};

export default DigitalPathology;
