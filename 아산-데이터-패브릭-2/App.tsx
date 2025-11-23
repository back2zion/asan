
import React, { useState } from 'react';
import { LayoutDashboard, Activity, GitBranch, Shield, Settings as SettingsIcon, Menu, X, BrainCircuit, Search } from 'lucide-react';
import { Dashboard } from './components/Dashboard';
import { DataCatalog } from './components/DataCatalog';
import { ETLPipeline } from './components/ETLPipeline';
import { Governance } from './components/Governance';
import { AIAnalytics } from './components/AIAnalytics';
import { Settings } from './components/Settings';
import { AppView } from './types';

function App() {
  const [currentView, setCurrentView] = useState<AppView>(AppView.DASHBOARD);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const renderView = () => {
    switch (currentView) {
      case AppView.CATALOG:
        return <DataCatalog />;
      case AppView.PIPELINE:
        return <ETLPipeline />;
      case AppView.GOVERNANCE:
        return <Governance />;
      case AppView.ANALYTICS:
        return <AIAnalytics />;
      case AppView.SETTINGS:
        return <Settings />;
      case AppView.DASHBOARD:
      default:
        return <Dashboard />;
    }
  };

  const NavItem = ({ view, icon, label }: { view: AppView; icon: React.ReactNode; label: string }) => (
    <button
      onClick={() => {
        setCurrentView(view);
        setMobileMenuOpen(false);
      }}
      className={`flex items-center gap-3 px-4 py-3 rounded-lg transition-all w-full text-left group ${
        currentView === view
          ? 'bg-[#006241] text-white shadow-lg shadow-[#006241]/20' /* ASAN GREEN */
          : 'text-[#53565A] hover:bg-[#F5F0E8] hover:text-[#006241]' /* ASAN GRAY -> HOVER */
      }`}
    >
      <div className={`transition-transform duration-300 ${currentView === view ? 'scale-110' : 'group-hover:scale-110'}`}>
        {icon}
      </div>
      <span className="font-medium text-sm">{label}</span>
    </button>
  );

  return (
    <div className="flex h-screen bg-[#F5F0E8] overflow-hidden font-inter text-[#53565A]">
      {/* Sidebar Navigation (Desktop) */}
      <aside className="hidden md:flex flex-col w-64 bg-white border-r border-[#A8A8A8]/20 p-4 shadow-xl z-20">
        <div className="flex items-center gap-3 px-2 py-4 mb-6 border-b border-[#A8A8A8]/10 pb-6">
          <div className="bg-[#006241] p-2 rounded-lg">
            <Activity className="text-white" size={24} />
          </div>
          <div className="leading-tight">
            <h1 className="text-lg font-bold text-[#006241] tracking-tight">아산 데이터 패브릭</h1>
            <span className="text-[10px] text-[#A8A8A8] font-mono">v2.0 Enterprise</span>
          </div>
        </div>

        <nav className="space-y-1 flex-grow overflow-y-auto custom-scrollbar">
          <div className="px-4 text-[10px] font-bold text-[#A8A8A8] uppercase tracking-widest mb-2 mt-2">플랫폼</div>
          <NavItem view={AppView.DASHBOARD} icon={<LayoutDashboard size={18} />} label="대시보드" />
          <NavItem view={AppView.CATALOG} icon={<Search size={18} />} label="데이터 카탈로그" />
          <NavItem view={AppView.PIPELINE} icon={<GitBranch size={18} />} label="ETL 파이프라인" />
          <NavItem view={AppView.GOVERNANCE} icon={<Shield size={18} />} label="거버넌스" />
          
          <div className="px-4 text-[10px] font-bold text-[#A8A8A8] uppercase tracking-widest mb-2 mt-6">인텔리전스</div>
          <NavItem view={AppView.ANALYTICS} icon={<BrainCircuit size={18} />} label="AI 데이터 분석가" />
          
          <div className="px-4 text-[10px] font-bold text-[#A8A8A8] uppercase tracking-widest mb-2 mt-6">관리</div>
          <NavItem view={AppView.SETTINGS} icon={<SettingsIcon size={18} />} label="설정" />
        </nav>

        <div className="mt-auto p-4 bg-[#F5F0E8] rounded-lg border border-[#A8A8A8]/20">
          <div className="flex items-center gap-2 mb-2">
             <div className="w-2 h-2 rounded-full bg-[#52A67D] animate-pulse"></div>
             <span className="text-xs text-[#53565A] font-medium">시스템 정상 (Online)</span>
          </div>
          <p className="text-[10px] text-[#A8A8A8]">K8s v1.29 • Trino • Neo4j v5</p>
        </div>
      </aside>

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col h-full overflow-hidden bg-[#F5F0E8]">
        
        {/* Mobile Header */}
        <header className="md:hidden flex items-center justify-between p-4 bg-white border-b border-[#A8A8A8]/20 z-30">
          <div className="flex items-center gap-2">
            <Activity className="text-[#006241]" size={20} />
            <h1 className="text-lg font-bold text-[#53565A]">아산 데이터 패브릭</h1>
          </div>
          <button 
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            className="text-[#53565A] p-2 hover:bg-[#F5F0E8] rounded-lg"
          >
            {mobileMenuOpen ? <X size={24} /> : <Menu size={24} />}
          </button>
        </header>

        {/* Mobile Menu Overlay */}
        {mobileMenuOpen && (
          <div className="md:hidden absolute top-16 left-0 w-full bg-white border-b border-[#A8A8A8]/20 z-50 p-4 space-y-2 shadow-2xl">
             <NavItem view={AppView.DASHBOARD} icon={<LayoutDashboard size={18} />} label="대시보드" />
             <NavItem view={AppView.CATALOG} icon={<Search size={18} />} label="데이터 카탈로그" />
             <NavItem view={AppView.PIPELINE} icon={<GitBranch size={18} />} label="ETL 파이프라인" />
             <NavItem view={AppView.GOVERNANCE} icon={<Shield size={18} />} label="거버넌스" />
             <NavItem view={AppView.ANALYTICS} icon={<BrainCircuit size={18} />} label="AI 데이터 분석가" />
             <NavItem view={AppView.SETTINGS} icon={<SettingsIcon size={18} />} label="설정" />
          </div>
        )}

        {/* View Container */}
        <main className="flex-1 overflow-y-auto relative z-10 scroll-smooth">
          {renderView()}
        </main>
      </div>
    </div>
  );
}

export default App;
