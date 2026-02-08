import React from 'react';
import { Modal, Radio } from 'antd';
import { FilePdfOutlined } from '@ant-design/icons';
import { Search, X } from 'lucide-react';
import type { PipelineInfo } from './dashboardConstants';

// --- Drill-Down Modal ---

interface DrillDownModalProps {
  drillDownData: { title: string; data: any } | null;
  onClose: () => void;
}

export const DrillDownModal: React.FC<DrillDownModalProps> = ({ drillDownData, onClose }) => {
  if (!drillDownData) return null;

  return (
    <div className="fixed inset-0 bg-black/40 z-[60] flex items-center justify-center p-4 backdrop-blur-sm animate-in fade-in duration-200">
       <div className="bg-white rounded-xl shadow-2xl w-full max-w-lg overflow-hidden border border-gray-100">
          <div className="bg-[#006241] p-4 flex justify-between items-center text-white">
             <h3 className="font-bold flex items-center gap-2">
                <Search size={18} /> {'상세 분석'}: {drillDownData.title}
             </h3>
             <button onClick={onClose} className="hover:bg-white/20 p-1 rounded transition-colors">
                <X size={20} />
             </button>
          </div>
          <div className="p-6 bg-[#F5F0E8]/30">
             <div className="mb-4">
                <div className="text-sm text-[#A8A8A8] font-bold uppercase mb-1">{'선택된 항목'}</div>
                <div className="text-2xl font-bold text-[#53565A] font-mono">
                   {drillDownData.data.activeLabel || drillDownData.data.domain || drillDownData.data.time || 'N/A'}
                </div>
             </div>

             <div className="space-y-3 bg-white p-4 rounded-lg border border-gray-200">
                <div className="flex justify-between items-center text-sm border-b border-gray-100 pb-2">
                   <span className="text-gray-500">{'주요 지표'} (Value)</span>
                   <span className="font-mono font-bold text-[#006241]">
                      {drillDownData.data.count?.toLocaleString() || drillDownData.data.volume || drillDownData.data.score || 'N/A'}
                   </span>
                </div>
                {drillDownData.data.errorRate !== undefined && (
                    <div className="flex justify-between items-center text-sm border-b border-gray-100 pb-2">
                       <span className="text-gray-500">{'오류율'} (Error Rate)</span>
                       <span className={`font-mono font-bold ${drillDownData.data.errorRate > 0.05 ? 'text-red-500' : 'text-[#52A67D]'}`}>
                          {(drillDownData.data.errorRate * 100).toFixed(1)}%
                       </span>
                    </div>
                )}
                 {drillDownData.data.issues !== undefined && (
                    <div className="flex justify-between items-center text-sm">
                       <span className="text-gray-500">{'감지된 이슈'} (Issues)</span>
                       <span className="font-mono font-bold text-[#FF6F00]">
                          {drillDownData.data.issues} {'건'}
                       </span>
                    </div>
                )}
             </div>

             <div className="mt-6 flex justify-end gap-2">
                <button onClick={onClose} className="px-4 py-2 text-sm font-medium text-gray-500 hover:bg-gray-100 rounded-lg transition-colors">{'닫기'}</button>
                <button className="px-4 py-2 text-sm font-bold text-white bg-[#006241] hover:bg-[#004e32] rounded-lg shadow-sm transition-colors">{'심층 리포트 생성'}</button>
             </div>
          </div>
       </div>
    </div>
  );
};

// --- Layout Edit Modal ---

interface LayoutModalProps {
  open: boolean;
  layoutChoice: string;
  onLayoutChange: (value: string) => void;
  onCancel: () => void;
  onOk: () => void;
}

export const LayoutModal: React.FC<LayoutModalProps> = ({ open, layoutChoice, onLayoutChange, onCancel, onOk }) => (
  <Modal
    title={'대시보드 레이아웃 편집'}
    open={open}
    onCancel={onCancel}
    onOk={onOk}
    okText={'적용'}
    cancelText={'취소'}
  >
    <div style={{ padding: '12px 0' }}>
      <p style={{ color: '#666', marginBottom: 16 }}>{'대시보드 위젯 배치를 선택하세요'}:</p>
      <Radio.Group value={layoutChoice} onChange={e => onLayoutChange(e.target.value)} style={{ width: '100%' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <Radio value="default" style={{ padding: '8px 12px', border: '1px solid #d9d9d9', borderRadius: 8 }}>
            <span style={{ fontWeight: 600 }}>{'기본 레이아웃'}</span>
            <span style={{ color: '#999', marginLeft: 8, fontSize: 12 }}>KPI + {'차트'} + {'인프라'} ({'현재'})</span>
          </Radio>
          <Radio value="compact" style={{ padding: '8px 12px', border: '1px solid #d9d9d9', borderRadius: 8 }}>
            <span style={{ fontWeight: 600 }}>{'컴팩트 뷰'}</span>
            <span style={{ color: '#999', marginLeft: 8, fontSize: 12 }}>KPI {'중심'}, {'차트 축소'}</span>
          </Radio>
          <Radio value="analytics" style={{ padding: '8px 12px', border: '1px solid #d9d9d9', borderRadius: 8 }}>
            <span style={{ fontWeight: 600 }}>{'분석 뷰'}</span>
            <span style={{ color: '#999', marginLeft: 8, fontSize: 12 }}>{'차트 확대'}, KPI {'상단 고정'}</span>
          </Radio>
        </div>
      </Radio.Group>
    </div>
  </Modal>
);

// --- Report Export Modal & Logic ---

interface ReportData {
  totalRecords: number;
  systemInfo: { cpuPercent: number; memPercent: number; memUsedGb: number; memTotalGb: number };
  qualityData: { domain: string; score: number; issues: number }[];
  activityData: { month: string; count: number }[];
  pipelineInfo: PipelineInfo | null;
  securityScore: number | null;
  containers: any[];
}

export const generateReportContent = (data: ReportData) => {
  const now = new Date().toLocaleString('ko-KR');
  return {
    title: '서울아산병원 통합 데이터 플랫폼 현황 리포트',
    generated: now,
    system: {
      cpu: data.systemInfo.cpuPercent.toFixed(1),
      memUsed: data.systemInfo.memUsedGb,
      memTotal: data.systemInfo.memTotalGb,
      memPercent: data.systemInfo.memPercent.toFixed(0),
    },
    data: { totalRecords: data.totalRecords },
    quality: data.qualityData,
    activity: data.activityData,
    pipeline: data.pipelineInfo,
    security: data.securityScore,
    containers: data.containers.filter(c => c.is_protected),
  };
};

export const exportReport = (reportData: ReportData, format: 'csv' | 'txt', onSuccess: () => void) => {
  const r = generateReportContent(reportData);
  let blob: Blob;
  let filename: string;

  if (format === 'csv') {
    const BOM = '\uFEFF';
    const rows: string[] = [];
    rows.push('섹션,항목,값,단위');
    rows.push(`시스템,생성일시,"${r.generated}",`);
    rows.push(`시스템,CPU 사용률,${r.system.cpu},%`);
    rows.push(`시스템,메모리 사용,${r.system.memUsed},GB`);
    rows.push(`시스템,메모리 전체,${r.system.memTotal},GB`);
    rows.push(`시스템,메모리 사용률,${r.system.memPercent},%`);
    rows.push(`데이터,총 레코드,"${r.data.totalRecords.toLocaleString()}",건`);
    if (r.pipeline) {
      rows.push(`파이프라인,전체 DAG,${r.pipeline.total_dags},개`);
      rows.push(`파이프라인,Active,${r.pipeline.active},개`);
      rows.push(`파이프라인,Running,${r.pipeline.recent_running},개`);
      rows.push(`파이프라인,Failed,${r.pipeline.recent_failed},개`);
    }
    if (r.security != null) rows.push(`보안,준수율,${r.security.toFixed(1)},%`);
    rows.push('');
    rows.push('도메인,품질점수,이슈건수');
    r.quality.forEach(q => rows.push(`${q.domain},${q.score},${q.issues}`));
    rows.push('');
    rows.push('기간,검사건수');
    r.activity.forEach(d => rows.push(`${d.month},"${d.count.toLocaleString()}"`));
    rows.push('');
    rows.push('컨테이너,상태');
    r.containers.forEach(c => rows.push(`${c.name},${c.status}`));
    blob = new Blob([BOM + rows.join('\n')], { type: 'text/csv;charset=utf-8' });
    filename = `asan_idp_report_${new Date().toISOString().slice(0,10)}.csv`;
  } else {
    const lines = [
      '═══════════════════════════════════════════════════════════════',
      '  서울아산병원 통합 데이터 플랫폼 - 현황 리포트',
      '═══════════════════════════════════════════════════════════════',
      `  생성일시: ${r.generated}`,
      '',
      '■ 시스템 상태',
      `  CPU 사용률: ${r.system.cpu}%`,
      `  메모리: ${r.system.memUsed}/${r.system.memTotal} GB (${r.system.memPercent}%)`,
      '',
      '■ 데이터 현황',
      `  총 레코드: ${r.data.totalRecords.toLocaleString()}건`,
      '',
    ];
    if (r.pipeline) {
      lines.push('■ 파이프라인 현황');
      lines.push(`  DAG: ${r.pipeline.total_dags}개 (Active ${r.pipeline.active}, Running ${r.pipeline.recent_running}, Failed ${r.pipeline.recent_failed})`);
      lines.push('');
    }
    if (r.security != null) { lines.push(`■ 보안 준수율: ${r.security.toFixed(1)}%`); lines.push(''); }
    lines.push('■ 데이터 품질 지수');
    r.quality.forEach(q => lines.push(`  ${q.domain}: ${q.score}점 (이슈 ${q.issues}건)`));
    lines.push('');
    lines.push('■ 검사 활동 추이');
    r.activity.forEach(d => lines.push(`  ${d.month}: ${d.count.toLocaleString()}건`));
    lines.push('');
    lines.push('■ 인프라 컨테이너');
    r.containers.forEach(c => lines.push(`  ${c.status === 'running' ? '●' : '○'} ${c.name} (${c.status})`));
    lines.push('');
    lines.push('═══════════════════════════════════════════════════════════════');
    lines.push('  Generated by Asan IDP Dashboard');
    lines.push('═══════════════════════════════════════════════════════════════');
    blob = new Blob([lines.join('\n')], { type: 'text/plain;charset=utf-8' });
    filename = `asan_idp_report_${new Date().toISOString().slice(0,10)}.txt`;
  }

  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
  onSuccess();
};

interface ReportModalProps {
  open: boolean;
  reportFormat: 'csv' | 'txt';
  onFormatChange: (value: 'csv' | 'txt') => void;
  onCancel: () => void;
  onOk: () => void;
}

export const ReportModal: React.FC<ReportModalProps> = ({ open, reportFormat, onFormatChange, onCancel, onOk }) => (
  <Modal
    title={<><FilePdfOutlined /> {'리포트 내보내기'}</>}
    open={open}
    onCancel={onCancel}
    onOk={onOk}
    okText={'다운로드'}
    cancelText={'취소'}
  >
    <div style={{ padding: '12px 0' }}>
      <p style={{ color: '#666', marginBottom: 12 }}>{'현재 대시보드 현황을 리포트로 내보냅니다'}.</p>
      <div style={{ marginBottom: 16 }}>
        <p style={{ fontWeight: 600, marginBottom: 8, fontSize: 13 }}>{'내보내기 형식'}:</p>
        <Radio.Group value={reportFormat} onChange={e => onFormatChange(e.target.value)} style={{ width: '100%' }}>
          <div style={{ display: 'flex', gap: 8 }}>
            <Radio.Button value="csv" style={{ flex: 1, textAlign: 'center' }}>CSV (Excel {'호환'})</Radio.Button>
            <Radio.Button value="txt" style={{ flex: 1, textAlign: 'center' }}>TXT ({'텍스트'})</Radio.Button>
          </div>
        </Radio.Group>
      </div>
      <div style={{ background: '#f6ffed', border: '1px solid #b7eb8f', borderRadius: 8, padding: '12px 16px' }}>
        <p style={{ fontWeight: 600, marginBottom: 8 }}>{'포함 항목'}:</p>
        <ul style={{ margin: 0, paddingLeft: 20, color: '#555', fontSize: 13 }}>
          <li>{'시스템 상태'} (CPU, {'메모리'})</li>
          <li>{'데이터 현황'} ({'총 레코드 수'})</li>
          <li>{'파이프라인 현황'} (Airflow DAG)</li>
          <li>{'보안 준수율'}</li>
          <li>{'도메인별 품질 지수'}</li>
          <li>{'검사 활동 추이'}</li>
          <li>{'인프라 컨테이너 목록'}</li>
        </ul>
      </div>
    </div>
  </Modal>
);
