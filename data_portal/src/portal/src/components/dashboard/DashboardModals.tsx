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
                <Search size={18} /> {'\uC0C1\uC138 \uBD84\uC11D'}: {drillDownData.title}
             </h3>
             <button onClick={onClose} className="hover:bg-white/20 p-1 rounded transition-colors">
                <X size={20} />
             </button>
          </div>
          <div className="p-6 bg-[#F5F0E8]/30">
             <div className="mb-4">
                <div className="text-sm text-[#A8A8A8] font-bold uppercase mb-1">{'\uC120\uD0DD\uB41C \uD56D\uBAA9'}</div>
                <div className="text-2xl font-bold text-[#53565A] font-mono">
                   {drillDownData.data.activeLabel || drillDownData.data.domain || drillDownData.data.time || 'N/A'}
                </div>
             </div>

             <div className="space-y-3 bg-white p-4 rounded-lg border border-gray-200">
                <div className="flex justify-between items-center text-sm border-b border-gray-100 pb-2">
                   <span className="text-gray-500">{'\uC8FC\uC694 \uC9C0\uD45C'} (Value)</span>
                   <span className="font-mono font-bold text-[#006241]">
                      {drillDownData.data.count?.toLocaleString() || drillDownData.data.volume || drillDownData.data.score || 'N/A'}
                   </span>
                </div>
                {drillDownData.data.errorRate !== undefined && (
                    <div className="flex justify-between items-center text-sm border-b border-gray-100 pb-2">
                       <span className="text-gray-500">{'\uC624\uB958\uC728'} (Error Rate)</span>
                       <span className={`font-mono font-bold ${drillDownData.data.errorRate > 0.05 ? 'text-red-500' : 'text-[#52A67D]'}`}>
                          {(drillDownData.data.errorRate * 100).toFixed(1)}%
                       </span>
                    </div>
                )}
                 {drillDownData.data.issues !== undefined && (
                    <div className="flex justify-between items-center text-sm">
                       <span className="text-gray-500">{'\uAC10\uC9C0\uB41C \uC774\uC288'} (Issues)</span>
                       <span className="font-mono font-bold text-[#FF6F00]">
                          {drillDownData.data.issues} {'\uAC74'}
                       </span>
                    </div>
                )}
             </div>

             <div className="mt-6 flex justify-end gap-2">
                <button onClick={onClose} className="px-4 py-2 text-sm font-medium text-gray-500 hover:bg-gray-100 rounded-lg transition-colors">{'\uB2EB\uAE30'}</button>
                <button className="px-4 py-2 text-sm font-bold text-white bg-[#006241] hover:bg-[#004e32] rounded-lg shadow-sm transition-colors">{'\uC2EC\uCE35 \uB9AC\uD3EC\uD2B8 \uC0DD\uC131'}</button>
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
    title={'\uB300\uC2DC\uBCF4\uB4DC \uB808\uC774\uC544\uC6C3 \uD3B8\uC9D1'}
    open={open}
    onCancel={onCancel}
    onOk={onOk}
    okText={'\uC801\uC6A9'}
    cancelText={'\uCDE8\uC18C'}
  >
    <div style={{ padding: '12px 0' }}>
      <p style={{ color: '#666', marginBottom: 16 }}>{'\uB300\uC2DC\uBCF4\uB4DC \uC704\uC82F \uBC30\uCE58\uB97C \uC120\uD0DD\uD558\uC138\uC694'}:</p>
      <Radio.Group value={layoutChoice} onChange={e => onLayoutChange(e.target.value)} style={{ width: '100%' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
          <Radio value="default" style={{ padding: '8px 12px', border: '1px solid #d9d9d9', borderRadius: 8 }}>
            <span style={{ fontWeight: 600 }}>{'\uAE30\uBCF8 \uB808\uC774\uC544\uC6C3'}</span>
            <span style={{ color: '#999', marginLeft: 8, fontSize: 12 }}>KPI + {'\uCC28\uD2B8'} + {'\uC778\uD504\uB77C'} ({'\uD604\uC7AC'})</span>
          </Radio>
          <Radio value="compact" style={{ padding: '8px 12px', border: '1px solid #d9d9d9', borderRadius: 8 }}>
            <span style={{ fontWeight: 600 }}>{'\uCEF4\uD329\uD2B8 \uBDF0'}</span>
            <span style={{ color: '#999', marginLeft: 8, fontSize: 12 }}>KPI {'\uC911\uC2EC'}, {'\uCC28\uD2B8 \uCD95\uC18C'}</span>
          </Radio>
          <Radio value="analytics" style={{ padding: '8px 12px', border: '1px solid #d9d9d9', borderRadius: 8 }}>
            <span style={{ fontWeight: 600 }}>{'\uBD84\uC11D \uBDF0'}</span>
            <span style={{ color: '#999', marginLeft: 8, fontSize: 12 }}>{'\uCC28\uD2B8 \uD655\uB300'}, KPI {'\uC0C1\uB2E8 \uACE0\uC815'}</span>
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
    title: '\uC11C\uC6B8\uC544\uC0B0\uBCD1\uC6D0 \uD1B5\uD569 \uB370\uC774\uD130 \uD50C\uB7AB\uD3FC \uD604\uD669 \uB9AC\uD3EC\uD2B8',
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
    rows.push('\uC139\uC158,\uD56D\uBAA9,\uAC12,\uB2E8\uC704');
    rows.push(`\uC2DC\uC2A4\uD15C,\uC0DD\uC131\uC77C\uC2DC,"${r.generated}",`);
    rows.push(`\uC2DC\uC2A4\uD15C,CPU \uC0AC\uC6A9\uB960,${r.system.cpu},%`);
    rows.push(`\uC2DC\uC2A4\uD15C,\uBA54\uBAA8\uB9AC \uC0AC\uC6A9,${r.system.memUsed},GB`);
    rows.push(`\uC2DC\uC2A4\uD15C,\uBA54\uBAA8\uB9AC \uC804\uCCB4,${r.system.memTotal},GB`);
    rows.push(`\uC2DC\uC2A4\uD15C,\uBA54\uBAA8\uB9AC \uC0AC\uC6A9\uB960,${r.system.memPercent},%`);
    rows.push(`\uB370\uC774\uD130,\uCD1D \uB808\uCF54\uB4DC,"${r.data.totalRecords.toLocaleString()}",\uAC74`);
    if (r.pipeline) {
      rows.push(`\uD30C\uC774\uD504\uB77C\uC778,\uC804\uCCB4 DAG,${r.pipeline.total_dags},\uAC1C`);
      rows.push(`\uD30C\uC774\uD504\uB77C\uC778,Active,${r.pipeline.active},\uAC1C`);
      rows.push(`\uD30C\uC774\uD504\uB77C\uC778,Running,${r.pipeline.recent_running},\uAC1C`);
      rows.push(`\uD30C\uC774\uD504\uB77C\uC778,Failed,${r.pipeline.recent_failed},\uAC1C`);
    }
    if (r.security != null) rows.push(`\uBCF4\uC548,\uC900\uC218\uC728,${r.security.toFixed(1)},%`);
    rows.push('');
    rows.push('\uB3C4\uBA54\uC778,\uD488\uC9C8\uC810\uC218,\uC774\uC288\uAC74\uC218');
    r.quality.forEach(q => rows.push(`${q.domain},${q.score},${q.issues}`));
    rows.push('');
    rows.push('\uAE30\uAC04,\uAC80\uC0AC\uAC74\uC218');
    r.activity.forEach(d => rows.push(`${d.month},"${d.count.toLocaleString()}"`));
    rows.push('');
    rows.push('\uCEE8\uD14C\uC774\uB108,\uC0C1\uD0DC');
    r.containers.forEach(c => rows.push(`${c.name},${c.status}`));
    blob = new Blob([BOM + rows.join('\n')], { type: 'text/csv;charset=utf-8' });
    filename = `asan_idp_report_${new Date().toISOString().slice(0,10)}.csv`;
  } else {
    const lines = [
      '\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550',
      '  \uC11C\uC6B8\uC544\uC0B0\uBCD1\uC6D0 \uD1B5\uD569 \uB370\uC774\uD130 \uD50C\uB7AB\uD3FC - \uD604\uD669 \uB9AC\uD3EC\uD2B8',
      '\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550',
      `  \uC0DD\uC131\uC77C\uC2DC: ${r.generated}`,
      '',
      '\u25A0 \uC2DC\uC2A4\uD15C \uC0C1\uD0DC',
      `  CPU \uC0AC\uC6A9\uB960: ${r.system.cpu}%`,
      `  \uBA54\uBAA8\uB9AC: ${r.system.memUsed}/${r.system.memTotal} GB (${r.system.memPercent}%)`,
      '',
      '\u25A0 \uB370\uC774\uD130 \uD604\uD669',
      `  \uCD1D \uB808\uCF54\uB4DC: ${r.data.totalRecords.toLocaleString()}\uAC74`,
      '',
    ];
    if (r.pipeline) {
      lines.push('\u25A0 \uD30C\uC774\uD504\uB77C\uC778 \uD604\uD669');
      lines.push(`  DAG: ${r.pipeline.total_dags}\uAC1C (Active ${r.pipeline.active}, Running ${r.pipeline.recent_running}, Failed ${r.pipeline.recent_failed})`);
      lines.push('');
    }
    if (r.security != null) { lines.push(`\u25A0 \uBCF4\uC548 \uC900\uC218\uC728: ${r.security.toFixed(1)}%`); lines.push(''); }
    lines.push('\u25A0 \uB370\uC774\uD130 \uD488\uC9C8 \uC9C0\uC218');
    r.quality.forEach(q => lines.push(`  ${q.domain}: ${q.score}\uC810 (\uC774\uC288 ${q.issues}\uAC74)`));
    lines.push('');
    lines.push('\u25A0 \uAC80\uC0AC \uD65C\uB3D9 \uCD94\uC774');
    r.activity.forEach(d => lines.push(`  ${d.month}: ${d.count.toLocaleString()}\uAC74`));
    lines.push('');
    lines.push('\u25A0 \uC778\uD504\uB77C \uCEE8\uD14C\uC774\uB108');
    r.containers.forEach(c => lines.push(`  ${c.status === 'running' ? '\u25CF' : '\u25CB'} ${c.name} (${c.status})`));
    lines.push('');
    lines.push('\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550');
    lines.push('  Generated by Asan IDP Dashboard');
    lines.push('\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550');
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
    title={<><FilePdfOutlined /> {'\uB9AC\uD3EC\uD2B8 \uB0B4\uBCF4\uB0B4\uAE30'}</>}
    open={open}
    onCancel={onCancel}
    onOk={onOk}
    okText={'\uB2E4\uC6B4\uB85C\uB4DC'}
    cancelText={'\uCDE8\uC18C'}
  >
    <div style={{ padding: '12px 0' }}>
      <p style={{ color: '#666', marginBottom: 12 }}>{'\uD604\uC7AC \uB300\uC2DC\uBCF4\uB4DC \uD604\uD669\uC744 \uB9AC\uD3EC\uD2B8\uB85C \uB0B4\uBCF4\uB0C5\uB2C8\uB2E4'}.</p>
      <div style={{ marginBottom: 16 }}>
        <p style={{ fontWeight: 600, marginBottom: 8, fontSize: 13 }}>{'\uB0B4\uBCF4\uB0B4\uAE30 \uD615\uC2DD'}:</p>
        <Radio.Group value={reportFormat} onChange={e => onFormatChange(e.target.value)} style={{ width: '100%' }}>
          <div style={{ display: 'flex', gap: 8 }}>
            <Radio.Button value="csv" style={{ flex: 1, textAlign: 'center' }}>CSV (Excel {'\uD638\uD658'})</Radio.Button>
            <Radio.Button value="txt" style={{ flex: 1, textAlign: 'center' }}>TXT ({'\uD14D\uC2A4\uD2B8'})</Radio.Button>
          </div>
        </Radio.Group>
      </div>
      <div style={{ background: '#f6ffed', border: '1px solid #b7eb8f', borderRadius: 8, padding: '12px 16px' }}>
        <p style={{ fontWeight: 600, marginBottom: 8 }}>{'\uD3EC\uD568 \uD56D\uBAA9'}:</p>
        <ul style={{ margin: 0, paddingLeft: 20, color: '#555', fontSize: 13 }}>
          <li>{'\uC2DC\uC2A4\uD15C \uC0C1\uD0DC'} (CPU, {'\uBA54\uBAA8\uB9AC'})</li>
          <li>{'\uB370\uC774\uD130 \uD604\uD669'} ({'\uCD1D \uB808\uCF54\uB4DC \uC218'})</li>
          <li>{'\uD30C\uC774\uD504\uB77C\uC778 \uD604\uD669'} (Airflow DAG)</li>
          <li>{'\uBCF4\uC548 \uC900\uC218\uC728'}</li>
          <li>{'\uB3C4\uBA54\uC778\uBCC4 \uD488\uC9C8 \uC9C0\uC218'}</li>
          <li>{'\uAC80\uC0AC \uD65C\uB3D9 \uCD94\uC774'}</li>
          <li>{'\uC778\uD504\uB77C \uCEE8\uD14C\uC774\uB108 \uBAA9\uB85D'}</li>
        </ul>
      </div>
    </div>
  </Modal>
);
