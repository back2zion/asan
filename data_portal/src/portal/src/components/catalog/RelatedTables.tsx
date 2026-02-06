/**
 * 연관 테이블 컴포넌트 (API 연동)
 */

import React from 'react';
import { Tag, Space, Typography, Spin } from 'antd';
import { LinkOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { semanticApi } from '../../services/api';

const { Text } = Typography;

const RelatedTables: React.FC<{ tableName: string }> = ({ tableName }) => {
  const { data, isLoading } = useQuery({
    queryKey: ['lineage', tableName],
    queryFn: () => semanticApi.getLineage(tableName),
    enabled: !!tableName,
  });

  if (isLoading) return <Spin size="small" />;

  const relatedTables = data?.lineage?.related_tables || [];

  return (
    <Space wrap>
      {relatedTables.length > 0 ? (
        relatedTables.map((t: any) => (
          <Tag key={t.name} icon={<LinkOutlined />} color="cyan">
            {t.name} ({t.business_name})
          </Tag>
        ))
      ) : (
        <Text type="secondary">연관 테이블 없음</Text>
      )}
    </Space>
  );
};

export default RelatedTables;
