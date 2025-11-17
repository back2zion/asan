# 아산재단 코퍼리트 컬러(Corporate Color) 가이드

## 메인 컬러 (Main Colors)

### ASAN GREEN
- **Pantone**: 3155C
- **Process Color**: C100, M15, Y45, K40
- **HEX**: #006241
- **Usage**: 주 브랜드 컬러, 버튼, 아이콘, 강조 요소

### ASAN Orange  
- **Pantone**: 138C
- **Process Color**: C0, M55, Y100, K0
- **HEX**: #FF6F00
- **Usage**: 경고, 알림, 강조 포인트

### ASAN Light Green
- **Pantone**: 5483C / 3155c 60%
- **Process Color**: C60, M5, Y30, K20
- **HEX**: #52A67D
- **Usage**: 성공 상태, 보조 브랜드 컬러

## 서브 컬러 (Sub Colors)

### ASAN GRAY
- **Pantone**: Cool Gray 11C
- **HEX**: #53565A
- **Usage**: 텍스트, 보조 UI 요소

### ASAN GOLD
- **Pantone**: 872C
- **HEX**: #C9B037
- **Usage**: 프리미엄 요소, 특별 강조

### ASAN SILVER
- **Pantone**: 877C
- **HEX**: #A8A8A8
- **Usage**: 비활성화 상태, 보조 텍스트

### BEIGE
- **Pantone**: 475C
- **Process Color**: C0, M11, Y18, K0
- **HEX**: #F5F0E8
- **Usage**: 배경색, 부드러운 영역

### SIGN LIGHT GREEN
- **Pantone**: 7458C
- **HEX**: #7FB069
- **Usage**: 사인, 안내 요소

### SIGN LIGHT YELLOW
- **Pantone**: 100C
- **HEX**: #F2E74B
- **Usage**: 주의, 하이라이트

## UI 적용 가이드

### 색상 계층 구조
1. **Primary**: ASAN GREEN (#006241) - 주요 액션, 버튼
2. **Success**: ASAN Light Green (#52A67D) - 성공 상태
3. **Warning**: ASAN Orange (#FF6F00) - 경고, 알림
4. **Error**: #dc3545 - 오류 상태 (표준 유지)
5. **Info**: ASAN GREEN (#006241) - 정보성 메시지

### 배경 및 레이아웃
- **Main Background**: BEIGE (#F5F0E8)
- **Card Background**: #ffffff
- **Sidebar Background**: #ffffff
- **Header Background**: #ffffff

### 그림자 및 투명도
- **Primary Shadow**: rgba(0, 98, 65, 0.06) - ASAN GREEN 기반
- **Card Shadow**: 0 2px 8px rgba(0, 98, 65, 0.06)
- **Button Shadow**: 0 2px 4px rgba(0, 98, 65, 0.2)

### 호버 효과
- **Menu Item Selected**: rgba(0, 98, 65, 0.08)
- **Menu Item Hover**: rgba(0, 98, 65, 0.05)

## 접근성 고려사항

### 대비율
- ASAN GREEN (#006241)과 흰색 텍스트: 높은 대비율 확보
- BEIGE (#F5F0E8) 배경에 검은 텍스트: 충분한 가독성
- ASAN Orange (#FF6F00): 경고용으로만 사용, 텍스트 색상 주의

### 색맹 고려
- 색상만으로 정보 전달 금지
- 아이콘과 텍스트 병용
- 충분한 명도 대비 유지

## 구현 예시

```css
/* CSS Variables */
:root {
  --asan-green: #006241;
  --asan-orange: #FF6F00;
  --asan-light-green: #52A67D;
  --asan-gray: #53565A;
  --asan-beige: #F5F0E8;
}

/* Primary Button */
.btn-primary {
  background-color: var(--asan-green);
  border: 1px solid var(--asan-green);
  box-shadow: 0 2px 4px rgba(0, 98, 65, 0.2);
}

/* Success State */
.success {
  color: var(--asan-light-green);
}

/* Warning State */
.warning {
  color: var(--asan-orange);
}
```

## 적용 완료 상태

✅ **Ant Design Theme**: 모든 메인/서브 컬러 적용 완료
✅ **Layout Colors**: 배경, 사이드바, 헤더 색상 적용
✅ **Component Colors**: 버튼, 카드, 아이콘 색상 통일
✅ **Interactive Elements**: 호버, 선택 상태 색상 적용
✅ **Typography**: 텍스트 색상 계층 구조 적용

---

**업데이트**: 2025-11-17
**프로젝트**: 서울아산병원 IDP POC v2.0
**적용 범위**: 전체 UI/UX 시스템