/**
 * 데이터 페브릭 — /api/v1/portal-ops/fabric-stats에서 실제 데이터 로딩
 */
import React, { useEffect, useState } from 'react';
import { Card, Row, Col, Button, Modal, Typography, Spin, Empty } from 'antd';
import { CloudServerOutlined, DownloadOutlined, WifiOutlined } from '@ant-design/icons';
import { AreaChart, Area, CartesianGrid, XAxis, YAxis, Tooltip, ResponsiveContainer, BarChart, Bar, Cell, LineChart, Line } from 'recharts';

const { Title } = Typography;

interface FabricData {
  ingestion_data: { time: string; volume: number; errorRate: number }[];
  quality_data: { domain: string; score: number; issues: number }[];
  source_count: number;
}

const generateIoTData = () => Array.from({ length: 20 }, (_, i) => ({
  time: i,
  heartRate: 60 + Math.random() * 40,
  spo2: 95 + Math.random() * 5,
}));

const DataFabric: React.FC = () => {
  const [open, setOpen] = useState(false);
  const [iotData, setIotData] = useState(generateIoTData());
  const [fabricData, setFabricData] = useState<FabricData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch('/api/v1/portal-ops/fabric-stats')
      .then(res => res.json())
      .then(d => setFabricData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

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

  const ingestionData = fabricData?.ingestion_data ?? [];
  const qualityData = fabricData?.quality_data ?? [];
  const sourceCount = fabricData?.source_count ?? 0;

  return (
    <div style={{ padding: 24 }}>
      <Title level={2}><CloudServerOutlined /> 데이터 페브릭</Title>

      {loading ? (
        <div style={{ textAlign: 'center', padding: 80 }}>
          <Spin size="large" tip="데이터 로딩 중..." />
        </div>
      ) : (
        <>
          <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
            <Col xs={24} md={16}>
              <Card title="데이터 수집량 (Ingestion)" style={{ height: 360 }}>
                {ingestionData.length > 0 ? (
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
                ) : (
                  <Empty description="수집 데이터 없음" />
                )}
              </Card>
            </Col>

            <Col xs={24} md={8}>
              <Card title="데이터 품질 지수" style={{ height: 360 }}>
                {qualityData.length > 0 ? (
                  <ResponsiveContainer width="99%" height="100%">
                    <BarChart data={qualityData} layout="vertical">
                      <XAxis type="number" domain={[0, 100]} hide />
                      <YAxis dataKey="domain" type="category" width={100} />
                      <Tooltip />
                      <Bar dataKey="score" barSize={20}>
                        {qualityData.map((entry) => (
                          <Cell key={entry.domain} fill={getScoreColor(entry.score)} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <Empty description="품질 데이터 없음" />
                )}
              </Card>
            </Col>
          </Row>

          <Row gutter={[16, 16]}>
            <Col xs={24} lg={16}>
              <Card title="인프라 & 데이터 현황">
                <div style={{ display: 'flex', gap: 12, alignItems: 'stretch' }}>
                  <div style={{ flex: 1, background: '#F5F0E8', padding: 12, borderRadius: 8 }}>
                    <div style={{ fontSize: 12, color: '#A8A8A8', marginBottom: 8 }}>데이터베이스</div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontWeight: 700 }}>
                      <div>테이블 수</div>
                      <div style={{ fontFamily: 'monospace' }}>{sourceCount}</div>
                    </div>
                    <div style={{ marginTop: 12, height: 8, background: '#e6e6e6', borderRadius: 4 }}>
                      <div style={{ width: `${Math.min(sourceCount / 50 * 100, 100)}%`, height: '100%', background: '#006241', borderRadius: 4 }} />
                    </div>
                  </div>

                  <div style={{ flex: 1, background: '#F5F0E8', padding: 12, borderRadius: 8 }}>
                    <div style={{ fontSize: 12, color: '#A8A8A8', marginBottom: 8 }}>ETL 파이프라인</div>
                    <div style={{ display: 'flex', gap: 8 }}>
                      {qualityData.slice(0, 4).map((q) => (
                        <div key={q.domain} style={{ flex: 1, background: '#fff', padding: 8, borderRadius: 6, border: '1px solid #eee' }}>
                          <div style={{ fontSize: 10, color: '#A8A8A8', textAlign: 'center' }}>{q.domain}</div>
                          <div style={{ height: 48, marginTop: 8, background: '#f3f3f3', borderRadius: 4, overflow: 'hidden', position: 'relative' }}>
                            <div style={{ position: 'absolute', bottom: 0, left: 0, right: 0, height: `${q.score}%`, background: getScoreColor(q.score) }} />
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
              <Button type="primary" icon={<CloudServerOutlined />} onClick={() => window.open('http://localhost:18888/lab', '_blank')}>
                JupyterLab 접속
              </Button>
              <Button style={{ marginLeft: 8 }} icon={<DownloadOutlined />}>리포트 내보내기</Button>
            </Col>
          </Row>
        </>
      )}
    </div>
  );
};

export default DataFabric;
