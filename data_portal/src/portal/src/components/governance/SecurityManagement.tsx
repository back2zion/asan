/**
 * DGR-005: 데이터 보안 관리 구축
 * 보안 정책 | 용어 기반 보안 | Biz 메타 보안 | 재식별 신청 | 사용자 속성 | 동적 정책 | 마스킹 규칙 | 접근 로그
 *
 * Orchestrator component that delegates to sub-components:
 *  - SecurityPoliciesTab: policies, term-based security, biz metadata security
 *  - SecurityAccessTab: reidentification requests, user attributes, dynamic policies
 *  - SecurityMaskingLogTab: masking rules, access audit log
 */
import React, { useState } from 'react';
import { Segmented } from 'antd';

import { SecurityPoliciesView, TermSecurityView, BizSecurityView } from './SecurityPoliciesTab';
import { ReidRequestsView, UserAttributesView, DynamicPoliciesView } from './SecurityAccessTab';
import { MaskingRulesView, AccessLogsView } from './SecurityMaskingLogTab';

const SecurityManagement: React.FC = () => {
  const [activeView, setActiveView] = useState('policies');

  const views: Record<string, React.ReactNode> = {
    policies: <SecurityPoliciesView />,
    terms: <TermSecurityView />,
    biz: <BizSecurityView />,
    reid: <ReidRequestsView />,
    users: <UserAttributesView />,
    dynamic: <DynamicPoliciesView />,
    masking: <MaskingRulesView />,
    logs: <AccessLogsView />,
  };

  return (
    <div>
      <Segmented
        value={activeView}
        onChange={(v) => setActiveView(v as string)}
        options={[
          { value: 'policies', label: '보안 정책' },
          { value: 'terms', label: '용어 기반 보안' },
          { value: 'biz', label: 'Biz 메타 보안' },
          { value: 'reid', label: '재식별 신청' },
          { value: 'users', label: '사용자 속성' },
          { value: 'dynamic', label: '동적 정책' },
          { value: 'masking', label: '마스킹 규칙' },
          { value: 'logs', label: '접근 로그' },
        ]}
        block
        style={{ marginBottom: 16 }}
      />
      {views[activeView]}
    </div>
  );
};

export default SecurityManagement;
