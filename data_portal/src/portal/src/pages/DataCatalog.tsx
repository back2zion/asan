/**
 * 데이터 카탈로그 페이지
 * DGR-003: 데이터 카탈로그 - 표준 메타 + Biz 메타 집약, 오너쉽 관리
 * DPR-002: 지능형 데이터 카탈로그 및 탐색 - Lineage, 연관 추천, Semantic Layer
 */

import React, { useState, useEffect, useCallback, useRef, lazy, Suspense } from 'react';
import { Row, Col, Form, App, Tabs, Spin } from 'antd';
import {
  SearchOutlined, MergeCellsOutlined, BarChartOutlined, ExperimentOutlined,
} from '@ant-design/icons';
import { useSearchParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { semanticApi, chatApi, sanitizeText } from '../services/api';
import { catalogAnalyticsApi } from '../services/catalogAnalyticsApi';
import type { TableInfo } from '../services/api';

import CatalogHeader from '../components/catalog/CatalogHeader';
import CatalogSearchBar from '../components/catalog/CatalogSearchBar';
import AiSummaryCard from '../components/catalog/AiSummaryCard';
import FilterPanel from '../components/catalog/FilterPanel';
import type { Snapshot } from '../components/catalog/FilterPanel';
import CatalogResultsPanel from '../components/catalog/CatalogResultsPanel';
import SnapshotModal from '../components/catalog/SnapshotModal';
import RegisterDataModal from '../components/catalog/RegisterDataModal';
import TableDetailModal from '../components/catalog/TableDetailModal';
import LineageModal from '../components/catalog/LineageModal';
import RecommendationPanel from '../components/catalog/RecommendationPanel';

const QueryAnalyticsTab = lazy(() => import('../components/catalog/QueryAnalyticsTab'));
const MasterModelsTab = lazy(() => import('../components/catalog/MasterModelsTab'));
const DataComposePanel = lazy(() => import('../components/catalog/DataComposePanel'));

const DataCatalog: React.FC = () => {
  const { message } = App.useApp();
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchQuery, setSearchQuery] = useState(searchParams.get('q') || '');
  const [searchContext, setSearchContext] = useState(searchParams.get('context') || '');
  const [selectedDomains, setSelectedDomains] = useState<string[]>([]);
  const [selectedTags, setSelectedTags] = useState<string[]>([]);
  const [selectedSensitivity, setSelectedSensitivity] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [activeDetailTab, setActiveDetailTab] = useState('columns');
  const [lineageModalVisible, setLineageModalVisible] = useState(false);
  const [lineageTable, setLineageTable] = useState<TableInfo | null>(null);
  const [registerModalVisible, setRegisterModalVisible] = useState(false);
  const [registerStep, setRegisterStep] = useState(0);
  const [registerForm] = Form.useForm();
  const [viewMode, setViewMode] = useState<string>('table');
  const [treeExpandedKeys, setTreeExpandedKeys] = useState<React.Key[]>([]);
  const [aiSummary, setAiSummary] = useState<string | null>(null);
  const [aiSummaryLoading, setAiSummaryLoading] = useState(false);
  const aiSummaryAbort = useRef<AbortController | null>(null);
  const [snapshots, setSnapshots] = useState<Snapshot[]>(() => {
    try { return JSON.parse(localStorage.getItem('catalog_snapshots') || '[]'); } catch { return []; }
  });
  const [snapshotModalVisible, setSnapshotModalVisible] = useState(false);
  const [snapshotName, setSnapshotName] = useState('');
  const [activeMainTab, setActiveMainTab] = useState('catalog');

  // --- Data queries ---
  const { data: searchResult, isLoading: isSearching } = useQuery({
    queryKey: ['catalog-search', searchQuery, selectedDomains, selectedTags, selectedSensitivity],
    queryFn: () => semanticApi.facetedSearch({
      q: searchQuery || undefined,
      domains: selectedDomains.length > 0 ? selectedDomains : undefined,
      tags: selectedTags.length > 0 ? selectedTags : undefined,
      sensitivity: selectedSensitivity.length > 0 ? selectedSensitivity : undefined,
      limit: 50,
    }),
    enabled: true,
  });
  const { data: domainsData } = useQuery({ queryKey: ['domains'], queryFn: () => semanticApi.getDomains() });
  const { data: tagsData } = useQuery({ queryKey: ['tags'], queryFn: () => semanticApi.getTags() });
  const { data: popularData } = useQuery({ queryKey: ['popular-data'], queryFn: () => semanticApi.getPopularData(10) });

  const domains = domainsData?.data?.domains || [];
  const tags = tagsData?.data?.tags || [];
  const tables = searchResult?.data?.tables || [];
  const popular = popularData?.data?.items || [];
  const expansion = searchResult?.data?.expansion || null;

  // --- URL sync ---
  useEffect(() => {
    const q = searchParams.get('q');
    const ctx = searchParams.get('context');
    if (q) setSearchQuery(q);
    if (ctx) setSearchContext(ctx);
    else setSearchContext('');
  }, [searchParams]);

  const handleSearch = useCallback((value: string) => {
    const sanitized = sanitizeText(value);
    setSearchQuery(sanitized);
    setSearchParams(sanitized ? { q: sanitized } : {});
    // 쿼리 로깅 (fire & forget)
    if (sanitized) {
      catalogAnalyticsApi.logQuery({ query_text: sanitized, query_type: 'search' }).catch(() => {});
    }
  }, [setSearchParams]);

  // --- AI summary ---
  useEffect(() => {
    if (!searchQuery || searchQuery.length < 2) { setAiSummary(null); return; }
    aiSummaryAbort.current?.abort();
    const controller = new AbortController();
    aiSummaryAbort.current = controller;
    const timer = setTimeout(async () => {
      setAiSummaryLoading(true);
      try {
        const resp = await chatApi.sendMessage({
          message: `카탈로그 검색: "${searchQuery}" - 이 키워드와 관련된 OMOP CDM 테이블과 컬럼을 2~3문장으로 간단히 요약해줘.`,
          user_id: 'catalog_search',
        });
        if (!controller.signal.aborted) setAiSummary(resp.assistant_message);
      } catch {
        if (!controller.signal.aborted) setAiSummary(null);
      } finally {
        if (!controller.signal.aborted) setAiSummaryLoading(false);
      }
    }, 500);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [searchQuery]);

  // --- Snapshot handlers ---
  const handleSaveSnapshot = () => {
    if (!snapshotName.trim()) return;
    const snap: Snapshot = {
      id: `snap_${Date.now()}`, name: snapshotName.trim(), query: searchQuery,
      domains: selectedDomains, tags: selectedTags, sensitivity: selectedSensitivity,
      createdAt: new Date().toISOString(),
    };
    const updated = [snap, ...snapshots];
    setSnapshots(updated);
    localStorage.setItem('catalog_snapshots', JSON.stringify(updated));
    setSnapshotName('');
    setSnapshotModalVisible(false);
    message.success('스냅샷이 저장되었습니다.');
  };

  const handleRestoreSnapshot = (snap: Snapshot) => {
    setSearchQuery(snap.query);
    setSelectedDomains(snap.domains);
    setSelectedTags(snap.tags);
    setSelectedSensitivity(snap.sensitivity);
    setSearchParams(snap.query ? { q: snap.query } : {});
    message.success(`"${snap.name}" 스냅샷이 복원되었습니다.`);
  };

  const handleDeleteSnapshot = (id: string) => {
    const updated = snapshots.filter((s) => s.id !== id);
    setSnapshots(updated);
    localStorage.setItem('catalog_snapshots', JSON.stringify(updated));
  };

  // --- Detail / lineage handlers ---
  const handleViewDetail = async (table: TableInfo) => {
    setSelectedTable(table);
    setDetailModalVisible(true);
    try { await semanticApi.recordUsage('current_user', 'view', 'table', table.physical_name); }
    catch (error) { /* error silenced */ }
  };

  const handleCopyTableName = (name: string) => {
    navigator.clipboard.writeText(name);
    message.success('테이블명이 복사되었습니다.');
  };

  const handleCopyText = (text: string, label: string) => {
    navigator.clipboard.writeText(text);
    message.success(`${label}이(가) 복사되었습니다.`);
  };

  const handleRegisterClick = () => {
    setRegisterModalVisible(true);
    setRegisterStep(0);
    registerForm.resetFields();
  };

  const handleRegisterSubmit = () => {
    registerForm.validateFields().then(() => {
      message.success('데이터 등록 요청이 접수되었습니다. 관리자 승인 후 카탈로그에 반영됩니다.');
      setRegisterModalVisible(false);
    });
  };

  const hasActiveFilters = !!searchQuery || selectedDomains.length > 0 || selectedTags.length > 0 || selectedSensitivity.length > 0;

  const lazyFallback = <div style={{ textAlign: 'center', padding: 60 }}><Spin /></div>;

  return (
    <div>
      <CatalogHeader onRegisterClick={handleRegisterClick} />

      <Tabs
        activeKey={activeMainTab}
        onChange={setActiveMainTab}
        style={{ marginBottom: 16 }}
        items={[
          {
            key: 'catalog',
            label: <><SearchOutlined /> 카탈로그</>,
            children: (
              <>
                <CatalogSearchBar
                  searchQuery={searchQuery}
                  viewMode={viewMode}
                  hasActiveFilters={hasActiveFilters}
                  onSearchChange={setSearchQuery}
                  onSearch={handleSearch}
                  onViewModeChange={setViewMode}
                  onSaveSnapshotClick={() => { setSnapshotName(''); setSnapshotModalVisible(true); }}
                />

                <AiSummaryCard
                  searchQuery={searchQuery}
                  aiSummary={aiSummary}
                  aiSummaryLoading={aiSummaryLoading}
                  expansion={expansion}
                  onSearch={handleSearch}
                />

                <Row gutter={24}>
                  <Col xs={24} lg={6}>
                    <FilterPanel
                      domains={domains}
                      tags={tags}
                      selectedDomains={selectedDomains}
                      selectedTags={selectedTags}
                      selectedSensitivity={selectedSensitivity}
                      onDomainsChange={setSelectedDomains}
                      onTagsChange={setSelectedTags}
                      onSensitivityChange={setSelectedSensitivity}
                      onClearFilters={() => { setSelectedDomains([]); setSelectedTags([]); setSelectedSensitivity([]); }}
                      popular={popular}
                      onSearch={handleSearch}
                      snapshots={snapshots}
                      onRestoreSnapshot={handleRestoreSnapshot}
                      onDeleteSnapshot={handleDeleteSnapshot}
                    />
                    <RecommendationPanel onSearch={handleSearch} />
                  </Col>
                  <Col xs={24} lg={18}>
                    <CatalogResultsPanel
                      tables={tables}
                      viewMode={viewMode}
                      isSearching={isSearching}
                      searchQuery={searchQuery}
                      treeExpandedKeys={treeExpandedKeys}
                      onTreeExpandedKeysChange={setTreeExpandedKeys}
                      onViewDetail={handleViewDetail}
                      onCopyTableName={handleCopyTableName}
                      onShowLineage={(table) => { setLineageTable(table); setLineageModalVisible(true); }}
                      onTreeSelect={(table, tab) => { setSelectedTable(table); setActiveDetailTab(tab); setDetailModalVisible(true); }}
                    />
                  </Col>
                </Row>
              </>
            ),
          },
          {
            key: 'compose',
            label: <><MergeCellsOutlined /> 데이터 조합</>,
            children: <Suspense fallback={lazyFallback}><DataComposePanel /></Suspense>,
          },
          {
            key: 'analytics',
            label: <><BarChartOutlined /> 쿼리 분석</>,
            children: <Suspense fallback={lazyFallback}><QueryAnalyticsTab /></Suspense>,
          },
          {
            key: 'models',
            label: <><ExperimentOutlined /> 마스터 모델</>,
            children: <Suspense fallback={lazyFallback}><MasterModelsTab /></Suspense>,
          },
        ]}
      />

      <TableDetailModal
        table={selectedTable}
        visible={detailModalVisible}
        activeTab={activeDetailTab}
        onTabChange={setActiveDetailTab}
        onClose={() => setDetailModalVisible(false)}
        onCopyTableName={handleCopyTableName}
        onCopyText={handleCopyText}
        searchContext={searchContext}
      />

      <LineageModal
        table={lineageTable}
        visible={lineageModalVisible}
        onClose={() => { setLineageModalVisible(false); setLineageTable(null); }}
      />

      <SnapshotModal
        visible={snapshotModalVisible}
        snapshotName={snapshotName}
        searchQuery={searchQuery}
        selectedDomains={selectedDomains}
        selectedTags={selectedTags}
        selectedSensitivity={selectedSensitivity}
        onSnapshotNameChange={setSnapshotName}
        onSave={handleSaveSnapshot}
        onCancel={() => setSnapshotModalVisible(false)}
      />

      <RegisterDataModal
        visible={registerModalVisible}
        step={registerStep}
        form={registerForm}
        domains={domains}
        tags={tags}
        onStepChange={setRegisterStep}
        onSubmit={handleRegisterSubmit}
        onCancel={() => setRegisterModalVisible(false)}
      />
    </div>
  );
};

export default DataCatalog;
