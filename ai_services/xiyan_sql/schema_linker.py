"""
Schema Linker — XiYan-SQL 파이프라인의 동적 스키마 선별 모듈

ITMetaService(키워드→테이블 선별)와 BizMetaService(의료용어→ICD 코드 해석) 로직을
XiYanSQLService용으로 래핑합니다.

data_portal 모듈과 직접 import가 불가능하므로, schema.py에 정의된 공유 데이터
(SAMPLE_TABLES, TABLE_RELATIONSHIPS, KEYWORD_TABLE_MAP, ICD_CODE_MAP)를 참조합니다.
"""

from dataclasses import dataclass, field
from typing import List, Optional

from ai_services.xiyan_sql.schema import (
    SAMPLE_TABLES,
    TABLE_RELATIONSHIPS,
    KEYWORD_TABLE_MAP,
    ICD_CODE_MAP,
    SYNONYM_MAP,
    MEDICAL_EVIDENCE,
    build_m_schema_for_tables,
)


@dataclass
class SchemaLinkResult:
    """Schema Linking 결과.

    Attributes:
        m_schema: 선별된 테이블만 포함한 M-Schema 문자열
        evidence: BizMeta 기반 의료 용어 + ICD 코드 참조 정보
        selected_tables: 선별된 테이블명 리스트
        term_resolutions: 해석된 의료 용어 리스트 (디버깅/로깅용)
    """

    m_schema: str
    evidence: str
    selected_tables: List[str]
    term_resolutions: List[dict] = field(default_factory=list)


class SchemaLinker:
    """XiYan-SQL 파이프라인의 Schema Linking 단계.

    DBcopilot의 역할을 키워드 기반 + 의료용어 해석으로 구현합니다.
    사용자 질의에서 관련 테이블/컬럼을 선별하고 evidence를 생성합니다.
    """

    def __init__(self):
        self.tables = SAMPLE_TABLES
        self.relationships = TABLE_RELATIONSHIPS
        self.keyword_map = KEYWORD_TABLE_MAP
        self.icd_map = ICD_CODE_MAP
        self.synonym_map = SYNONYM_MAP
        self._table_index = {t["physical_name"]: t for t in self.tables}

    def link(
        self,
        query: str,
        previous_tables: Optional[List[str]] = None,
    ) -> SchemaLinkResult:
        """질의에서 관련 테이블/컬럼을 선별하고 evidence를 생성합니다.

        Args:
            query: 사용자 자연어 질의
            previous_tables: 이전 턴에서 사용된 테이블명 리스트

        Returns:
            SchemaLinkResult
        """
        # ① 의료 용어 해석 → evidence 생성
        evidence, term_resolutions = self._resolve_medical_terms(query)

        # ② 키워드 추출 → 테이블 선별
        keywords = self._extract_keywords(query)
        selected = self._select_tables(keywords, previous_tables or [])

        # ③ 동적 M-Schema 구성
        selected_table_dicts = [
            self._table_index[name]
            for name in selected
            if name in self._table_index
        ]
        relevant_rels = [
            rel for rel in self.relationships
            if rel["from_table"] in selected and rel["to_table"] in selected
        ]
        m_schema = build_m_schema_for_tables(
            selected_table_dicts, relevant_rels,
        )

        return SchemaLinkResult(
            m_schema=m_schema,
            evidence=evidence,
            selected_tables=selected,
            term_resolutions=term_resolutions,
        )

    def _extract_keywords(self, query: str) -> List[str]:
        """질의에서 스키마 선별용 키워드를 추출합니다.

        키워드 맵의 키, ICD 코드 맵의 키, MEDICAL_EVIDENCE 키를 매칭합니다.
        """
        keywords: List[str] = []
        seen: set = set()

        # 긴 키워드부터 먼저 매칭 (e.g. "입원 환자" > "입원", "환자")
        all_keys = sorted(
            set(self.keyword_map.keys())
            | set(self.icd_map.keys())
            | set(MEDICAL_EVIDENCE.keys()),
            key=len,
            reverse=True,
        )
        for key in all_keys:
            if key in query and key not in seen:
                keywords.append(key)
                seen.add(key)

        return keywords

    def _select_tables(
        self,
        keywords: List[str],
        previous_tables: List[str],
    ) -> List[str]:
        """키워드 + 이전 컨텍스트에서 관련 테이블을 선별합니다.

        선별 로직:
        1. 키워드 맵에서 매칭된 테이블 수집 (정확 매칭)
        2. 이전 턴 테이블 병합
        3. FK로 연결된 테이블이 있으면 PT_BSNF 자동 포함
        4. 아무 테이블도 선별되지 않으면 PT_BSNF 폴백
        """
        table_names: set = set()

        # 1. 키워드 맵에서 정확 매칭 검색
        for kw in keywords:
            if kw in self.keyword_map:
                table_names.update(self.keyword_map[kw])

        # 2. 이전 턴 테이블 병합
        for t in previous_tables:
            if t in self._table_index:
                table_names.add(t)

        # 3. 다른 테이블이 선별되었으면 PT_BSNF 자동 포함 (FK 허브)
        if table_names and "PT_BSNF" not in table_names:
            for rel in self.relationships:
                if rel["from_table"] in table_names:
                    table_names.add(rel["to_table"])
                    break

        # 4. 폴백: 아무것도 없으면 PT_BSNF
        if not table_names:
            table_names.add("PT_BSNF")

        # 정렬: SAMPLE_TABLES 정의 순서 유지
        order = [t["physical_name"] for t in self.tables]
        return sorted(table_names, key=lambda x: order.index(x) if x in order else 999)

    def _resolve_medical_terms(self, query: str) -> tuple:
        """의료 용어를 해석하여 ICD 코드 매핑 + evidence를 생성합니다.

        Returns:
            (evidence_str, term_resolutions_list)
        """
        evidence_parts: List[str] = []
        term_resolutions: List[dict] = []

        # ICD 코드 매핑
        for term, (icd_code, standard_name) in self.icd_map.items():
            if term in query:
                evidence_parts.append(
                    f"{standard_name}({term})의 ICD-10 코드는 {icd_code}입니다. "
                    f"DIAG_INFO.ICD_CD LIKE '{icd_code}%' 조건을 사용합니다."
                )
                term_resolutions.append({
                    "original_term": term,
                    "resolved_term": f"{standard_name} (ICD: {icd_code})",
                    "term_type": "icd_code",
                    "icd_code": icd_code,
                })

        # MEDICAL_EVIDENCE 기반 추가 evidence (ICD 이외: 성별, 입원/퇴원 등)
        for keyword, ev in MEDICAL_EVIDENCE.items():
            if keyword in query:
                # ICD 코드 관련 evidence는 위에서 이미 처리했으므로 중복 방지
                already_covered = any(
                    r["original_term"] == keyword for r in term_resolutions
                )
                if not already_covered:
                    evidence_parts.append(ev)

        evidence = "\n".join(evidence_parts)
        return evidence, term_resolutions

    def _build_dynamic_m_schema(
        self,
        tables: List[dict],
        relationships: List[dict],
    ) -> str:
        """선별된 테이블/FK만으로 M-Schema를 구성합니다.

        build_m_schema_for_tables()에 위임합니다.
        """
        return build_m_schema_for_tables(tables, relationships)
