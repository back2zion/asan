import React, { useEffect, useState } from 'react';
import { Card, Row, Col, Button, Modal, Typography } from 'antd';
import { CloudServerOutlined, DownloadOutlined, WifiOutlined } from '@ant-design/icons';
import { AreaChart, Area, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer, BarChart, Bar, Cell, LineChart, Line } from 'recharts';

const { Title, Paragraph } = Typography;

const ingestionData = [
  { time: '00:00', volume: 450, errorRate: 0.01 },
  { time: '04:00', volume: 320, errorRate: 0.02 },
  { time: '08:00', volume: 1200, errorRate: 0.05 },
  { time: '12:00', volume: 2400, errorRate: 0.08 },
  { time: '16:00', volume: 1800, errorRate: 0.04 },
  { time: '20:00', volume: 950, errorRate: 0.03 },
  { time: '23:59', volume: 500, errorRate: 0.01 },
];

const qualityData = [
  { domain: '임상', score: 98, issues: 12 },
  { domain: '유전체', score: 92, issues: 45 },
  { domain: '영상', score: 88, issues: 78 },
  { domain: '원무', score: 99, issues: 3 },
  { domain: 'IoT', score: 85, issues: 156 },
];

const generateIoTData = () => Array.from({ length: 20 }, (_, i) => ({
  time: i,
  heartRate: 60 + Math.random() * 40,
  spo2: 95 + Math.random() * 5,
}));

const COLORS = ['#006241', '#52c41a', '#ff6600', '#1890ff', '#722ed1'];

const DataFabric: React.FC = () => {
  const [open, setOpen] = useState(false);
  const [iotData, setIotData] = useState(generateIoTData());

  const getScoreColor = (score: number) => {
    if (score > 90) return '#52c41a';
    if (score > 80) return '#ff6600';
    return '#ff4d4f';
  };

  useEffect(() => {
    const t = setInterval(() => {
      setIotData(prev => [
        ...prev.slice(1),
        { time: prev[prev.length - 1].time + 1, heartRate: 60 + Math.random() * 40, spo2: 95 + Math.random() * 5 }
      ]);
    }, 1000);
    return () => clearInterval(t);
  }, []);

  const openJupyterSimulation = () => setOpen(true);
  const closeModal = () => setOpen(false);

  const jupyterHtml = `
  <html><head><meta charset="utf-8" /><title>JupyterLab 시뮬레이션</title>
  <style>body{font-family:Inter,Arial,Helvetica,sans-serif;margin:0}</style></head>
  <body>
    <div style="padding:16px;background:#006241;color:#fff;display:flex;align-items:center;gap:12px;">
      <div style="font-weight:700;font-size:18px">JupyterLab 시뮬레이션</div>
      <div style="opacity:0.9">(데모용 콘텐츠)</div>
    </div>
    <div style="padding:24px;">
      <h3>가상 Jupyter 환경</h3>
      <p>이 창은 실 환경 연동을 시연하기 위한 임시 UI입니다. 실제 JupyterLab과 연결하려면 백엔드에서 JupyterHub 또는 프록시를 구성하세요.</p>
      <pre style="background:#f5f5f5;padding:12px;border-radius:6px;">In [1]: import numpy as np\nIn [2]: print('Hello Asan')\nHello Asan</pre>
    </div>
  </body></html>`;

  return (
    <div style={{ padding: 24 }}>
      <Title level={2}><CloudServerOutlined /> 데이터 페브릭</Title>

      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} md={16}>
          <Card title="데이터 수집량 (Ingestion)" style={{ height: 360 }}>
            <ResponsiveContainer width="99%" height="100%">
              <AreaChart data={ingestionData}>
                <defs>
                  <linearGradient id="colorVol" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#006241" stopOpacity={0.2}/>
                    <stop offset="95%" stopColor="#006241" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="time" />
                <YAxis />
                <Tooltip />
                <Area type="monotone" dataKey="volume" stroke="#006241" fill="url(#colorVol)" />
              </AreaChart>
            </ResponsiveContainer>
          </Card>
        </Col>

        <Col xs={24} md={8}>
          <Card title="데이터 품질 지수" style={{ height: 360 }}>
            <ResponsiveContainer width="99%" height="100%">
              <BarChart data={qualityData} layout="vertical">
                <XAxis type="number" domain={[0, 100]} hide />
                <YAxis dataKey="domain" type="category" width={100} />
                <Tooltip />
                <Bar dataKey="score" barSize={20}>
                  {qualityData.map((entry) => {
                    const fill = getScoreColor(entry.score);
                    return <Cell key={entry.domain} fill={fill} />;
                  })}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} lg={16}>
          <Card title="인프라 & GPU 상태">
            <div style={{ display: 'flex', gap: 12, alignItems: 'stretch' }}>
              <div style={{ flex: 1, background: '#F5F0E8', padding: 12, borderRadius: 8 }}>
                <div style={{ fontSize: 12, color: '#A8A8A8', marginBottom: 8 }}>KubeVirt Nodes</div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontWeight: 700 }}>
                  <div>vCPU Usage</div>
                  <div style={{ fontFamily: 'monospace' }}>824 / 1,024</div>
                </div>
                <div style={{ marginTop: 12, height: 8, background: '#e6e6e6', borderRadius: 4 }}>
                  <div style={{ width: '80%', height: '100%', background: '#006241', borderRadius: 4 }} />
                </div>
              </div>

              <div style={{ flex: 1, background: '#F5F0E8', padding: 12, borderRadius: 8 }}>
                <div style={{ fontSize: 12, color: '#A8A8A8', marginBottom: 8 }}>NVIDIA H100 (4 Units)</div>
                <div style={{ display: 'flex', gap: 8 }}>
                  {[1,2,3,4].map((g) => (
                    <div key={g} style={{ flex: 1, background: '#fff', padding: 8, borderRadius: 6, border: '1px solid #eee' }}>
                      <div style={{ fontSize: 10, color: '#A8A8A8', textAlign: 'center' }}>H100 #{g}</div>
                      <div style={{ height: 48, marginTop: 8, background: '#f3f3f3', borderRadius: 4, overflow: 'hidden', position: 'relative' }}>
                        <div style={{ position: 'absolute', bottom: 0, left: 0, right: 0, height: g===2 ? '40%' : '85%', background: g===2 ? '#52c41a' : '#ff6600' }} />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </Card>
        </Col>

        <Col xs={24} lg={8}>
          <Card title={<span><WifiOutlined /> IoT 활력징후 (실시간)</span>} style={{ height: '100%' }}>
            <div style={{ height: 220 }}>
              <ResponsiveContainer width="99%" height="100%">
                <LineChart data={iotData}>
                  <Line type="monotone" dataKey="heartRate" stroke="#ff6600" dot={false} isAnimationActive={false} />
                  <Line type="monotone" dataKey="spo2" stroke="#52c41a" dot={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 12, fontFamily: 'monospace' }}>
              <div style={{ color: '#ff6600' }}>HR: {Math.round(iotData[iotData.length-1].heartRate)} bpm</div>
              <div style={{ color: '#52c41a' }}>SpO2: {Math.round(iotData[iotData.length-1].spo2)}%</div>
            </div>
          </Card>
        </Col>
      </Row>

      <Row style={{ marginTop: 16 }}>
        <Col>
          <Button type="primary" icon={<CloudServerOutlined />} onClick={openJupyterSimulation}>JupyterLab 접속 (시뮬레이션)</Button>
          <Button style={{ marginLeft: 8 }} icon={<DownloadOutlined />}>리포트 내보내기</Button>
        </Col>
      </Row>

      <Modal title="JupyterLab 시뮬레이션" open={open} onOk={closeModal} onCancel={closeModal} width="90%" bodyStyle={{ height: '75vh', padding: 0 }}>
        <iframe title="jupyter-sim" srcDoc={jupyterHtml} style={{ width: '100%', height: '100%', border: 0 }} />
      </Modal>
    </div>
  );
};

export default DataFabric;
