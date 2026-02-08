/**
 * RegisterDataModal - 데이터 자산 등록 모달 (3단계 위자드)
 */

import React from 'react';
import {
  Modal, Form, Steps, Input, Select, Card, Space, Typography, Button,
} from 'antd';
import type { FormInstance } from 'antd';

const { Text } = Typography;

interface RegisterDataModalProps {
  visible: boolean;
  step: number;
  form: FormInstance;
  domains: string[];
  tags: string[];
  onStepChange: (step: number) => void;
  onSubmit: () => void;
  onCancel: () => void;
}

const RegisterDataModal: React.FC<RegisterDataModalProps> = ({
  visible,
  step,
  form,
  domains,
  tags,
  onStepChange,
  onSubmit,
  onCancel,
}) => {
  return (
    <Modal
      title="데이터 자산 등록"
      open={visible}
      onCancel={onCancel}
      width={720}
      footer={[
        <Button key="cancel" onClick={onCancel}>취소</Button>,
        step > 0 && (
          <Button key="prev" onClick={() => onStepChange(step - 1)}>이전</Button>
        ),
        step < 2 ? (
          <Button
            key="next"
            type="primary"
            onClick={() => onStepChange(step + 1)}
            style={{ background: '#006241' }}
          >
            다음
          </Button>
        ) : (
          <Button
            key="submit"
            type="primary"
            style={{ background: '#006241' }}
            onClick={onSubmit}
          >
            등록 요청
          </Button>
        ),
      ].filter(Boolean)}
    >
      <Steps
        current={step}
        size="small"
        style={{ marginBottom: 24 }}
        items={[
          { title: '기본 정보' },
          { title: '스키마 정의' },
          { title: '분류 및 태그' },
        ]}
      />
      <Form form={form} layout="vertical">
        {step === 0 && (
          <>
            <Form.Item
              label="테이블 물리명"
              name="physical_name"
              rules={[{ required: true, message: '물리명을 입력하세요' }]}
            >
              <Input placeholder="예: new_data_table" />
            </Form.Item>
            <Form.Item
              label="테이블 비즈니스명"
              name="business_name"
              rules={[{ required: true, message: '비즈니스명을 입력하세요' }]}
            >
              <Input placeholder="예: 신규 데이터 테이블" />
            </Form.Item>
            <Form.Item
              label="설명"
              name="description"
              rules={[{ required: true, message: '설명을 입력하세요' }]}
            >
              <Input.TextArea rows={3} placeholder="이 테이블의 목적과 내용을 설명하세요" />
            </Form.Item>
            <Form.Item label="데이터 소스" name="source">
              <Select
                placeholder="데이터 소스 선택"
                options={[
                  { label: 'OMOP CDM (PostgreSQL)', value: 'omop_cdm' },
                  { label: '외부 파일 (CSV/Excel)', value: 'file_upload' },
                  { label: 'API 연동', value: 'api' },
                  { label: '직접 입력', value: 'manual' },
                ]}
              />
            </Form.Item>
          </>
        )}
        {step === 1 && (
          <>
            <Form.Item label="스키마 정의 방식" name="schema_method">
              <Select
                placeholder="스키마 정의 방식 선택"
                defaultValue="auto"
                options={[
                  { label: '자동 감지 (AI 기반)', value: 'auto' },
                  { label: '수동 입력', value: 'manual' },
                  { label: 'DDL 파일 업로드', value: 'ddl' },
                ]}
              />
            </Form.Item>
            <Card size="small" style={{ background: '#f5f5f5' }}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <Text type="secondary">AI 기반 자동 스키마 감지 시:</Text>
                <Text>- 데이터 소스에서 자동으로 컬럼명, 타입, 제약조건 추출</Text>
                <Text>- PII (개인식별정보) 자동 탐지 및 민감도 분류</Text>
                <Text>- OMOP CDM 표준 매핑 제안</Text>
              </Space>
            </Card>
          </>
        )}
        {step === 2 && (
          <>
            <Form.Item
              label="도메인"
              name="domain"
              rules={[{ required: true, message: '도메인을 선택하세요' }]}
            >
              <Select
                placeholder="도메인 선택"
                options={domains.map((d: string) => ({ label: d, value: d }))}
              />
            </Form.Item>
            <Form.Item label="태그" name="tags">
              <Select
                mode="multiple"
                placeholder="태그 선택 (복수 가능)"
                options={tags.map((t: string) => ({ label: t, value: t }))}
              />
            </Form.Item>
            <Form.Item label="민감도 등급" name="sensitivity">
              <Select
                placeholder="민감도 등급 선택"
                options={[
                  { label: '일반 (Normal)', value: 'Normal' },
                  { label: '개인건강정보 (PHI)', value: 'PHI' },
                  { label: '제한 (Restricted)', value: 'Restricted' },
                ]}
              />
            </Form.Item>
            <Form.Item label="데이터 오너" name="owner">
              <Input placeholder="담당자 이름 또는 팀명" />
            </Form.Item>
          </>
        )}
      </Form>
    </Modal>
  );
};

export default RegisterDataModal;
