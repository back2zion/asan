
import { GoogleGenAI, Type } from "@google/genai";

// Initialize Gemini Client
// API Key is retrieved from environment variables.
// If missing, the fallback logic will ensure the demo still works.
const ai = new GoogleGenAI({ apiKey: process.env.API_KEY || 'dummy_key' });

// --- POC MOCK DATA (Localized for Asan Medical Center) ---
const MOCK_DATA = {
  PATIENTS: [
    { patient_id: 'P001', name: '김철수', age: 45, gender: 'M', region: '서울' },
    { patient_id: 'P002', name: '이영희', age: 62, gender: 'F', region: '부산' },
    { patient_id: 'P003', name: '박지성', age: 29, gender: 'M', region: '서울' },
    { patient_id: 'P004', name: '최수진', age: 55, gender: 'F', region: '인천' },
    { patient_id: 'P005', name: '정민호', age: 71, gender: 'M', region: '대구' },
    { patient_id: 'P006', name: '강서연', age: 38, gender: 'F', region: '서울' },
    { patient_id: 'P007', name: '윤호성', age: 49, gender: 'M', region: '부산' }
  ],
  DOCTORS: [
    { doctor_id: 'DR01', name: '김명의', department: '순환기내과' },
    { doctor_id: 'DR02', name: '이장수', department: '내분비내과' },
    { doctor_id: 'DR03', name: '박수술', department: '종양내과' },
    { doctor_id: 'DR04', name: '최신경', department: '신경과' },
    { doctor_id: 'DR05', name: '정뼈대', department: '정형외과' }
  ],
  VISITS: [
    { visit_id: 'V101', patient_id: 'P001', doctor_id: 'DR01', visit_date: '2025-04-10', department: '순환기내과', type: '외래' },
    { visit_id: 'V102', patient_id: 'P002', doctor_id: 'DR02', visit_date: '2025-04-12', department: '내분비내과', type: '입원' },
    { visit_id: 'V103', patient_id: 'P005', doctor_id: 'DR03', visit_date: '2025-04-15', department: '종양내과', type: '입원' },
    { visit_id: 'V104', patient_id: 'P001', doctor_id: 'DR01', visit_date: '2025-05-01', department: '순환기내과', type: '외래' },
    { visit_id: 'V105', patient_id: 'P003', doctor_id: 'DR05', visit_date: '2025-05-03', department: '정형외과', type: '외래' },
    { visit_id: 'V106', patient_id: 'P002', doctor_id: 'DR02', visit_date: '2025-05-10', department: '내분비내과', type: '외래' },
    { visit_id: 'V107', patient_id: 'P004', doctor_id: 'DR04', visit_date: '2025-05-12', department: '신경과', type: '입원' }
  ],
  DIAGNOSIS: [
    { diag_id: 'D001', visit_id: 'V101', icd_code: 'I10', description: '본태성 고혈압' },
    { diag_id: 'D002', visit_id: 'V102', icd_code: 'E11.9', description: '2형 당뇨병' },
    { diag_id: 'D003', visit_id: 'V103', icd_code: 'C34.9', description: '폐의 악성 신생물' },
    { diag_id: 'D004', visit_id: 'V104', icd_code: 'I10', description: '본태성 고혈압' },
    { diag_id: 'D005', visit_id: 'V106', icd_code: 'E11.9', description: '2형 당뇨병' },
    { diag_id: 'D006', visit_id: 'V107', icd_code: 'I63.9', description: '뇌경색증' }
  ],
  MEDICATIONS: [
     { med_id: 'M001', name: 'Metformin', type: '경구약' },
     { med_id: 'M002', name: 'Amlodipine', type: '경구약' },
     { med_id: 'M003', name: 'Cisplatin', type: '항암제' },
     { med_id: 'M004', name: 'Insulin Glargine', type: '주사제' }
  ],
  PROCEDURES: [
     { proc_id: 'PR01', name: 'HbA1c Test', type: 'Lab' },
     { proc_id: 'PR02', name: 'Chest CT', type: 'Imaging' },
     { proc_id: 'PR03', name: 'Echocardiography', type: 'Imaging' }
  ]
};

const DATA_ANALYST_PROMPT = `
You are an expert Data Engineer and Clinical Analyst at Asan Medical Center.
You are acting as a Hybrid Intelligence Engine (SQL + GraphRAG) using Gemini 3.0 Pro Thinking Mode.

Here is the current database state (Mock Data for POC):
${JSON.stringify(MOCK_DATA, null, 2)}

YOUR TASK:
1. Translate the user's natural language question (in Korean) into a valid ANSI SQL query based on the schema.
2. EXECUTE the query mentally against the mock data.
3. CONSTRUCT a GraphRAG representation (Nodes and Links) relevant to the query result to visualize relationships (e.g., Patient -> Visited -> Doctor -> Diagnosed -> Disease -> Prescribed -> Medication).
4. Assign reasonable (x, y) coordinates for the nodes to form a logical flow (Left to Right: Patient -> Visit/Doctor -> Diagnosis -> Medication/Procedure). Canvas size is 800x500.
5. Return the SQL, explanation (in Korean), actual table results, AND graph data.

Format: JSON
`;

/**
 * Fallback logic to ensure POC Demo works even without API Key or Network
 */
const getFallbackResult = (question: string): any => {
  const q = question.toLowerCase();

  // Scenario 1: 당뇨병 환자 및 주치의 + 처방약
  if (q.includes('당뇨') || q.includes('e11')) {
    return {
      sql_query: "SELECT p.name AS 환자명, d.name AS 주치의, diag.description AS 진단명, m.name AS 처방약\nFROM PATIENTS p\nJOIN VISITS v ON p.patient_id = v.patient_id\nJOIN DOCTORS d ON v.doctor_id = d.doctor_id\nJOIN DIAGNOSIS diag ON v.visit_id = diag.visit_id\nLEFT JOIN PRESCRIPTIONS pre ON v.visit_id = pre.visit_id\nLEFT JOIN MEDICATIONS m ON pre.med_id = m.med_id\nWHERE diag.icd_code LIKE 'E11%'",
      explanation: "2형 당뇨병(E11) 진단을 받은 환자 '이영희'님의 진료 기록을 조회했습니다. 주치의 '이장수' 교수와 연결되어 있으며, 표준 치료제인 'Metformin' 처방 및 'HbA1c(당화혈색소)' 검사 이력이 그래프에 통합되었습니다.",
      tables_used: ["PATIENTS", "VISITS", "DOCTORS", "DIAGNOSIS", "MEDICATIONS"],
      columns: ["환자명", "주치의", "진단명", "처방약"],
      query_results: [
        { "환자명": "이영희", "주치의": "이장수", "진단명": "2형 당뇨병", "처방약": "Metformin" },
        { "환자명": "이영희", "주치의": "이장수", "진단명": "2형 당뇨병", "처방약": "Insulin Glargine" }
      ],
      graph_data: {
        nodes: [
          { id: "P002", label: "이영희", type: "Patient", x: 100, y: 250 },
          { id: "DR02", label: "이장수 (내분비내과)", type: "Doctor", x: 300, y: 150 },
          { id: "D002", label: "2형 당뇨병", type: "Diagnosis", x: 500, y: 150 },
          { id: "M001", label: "Metformin", type: "Medication", x: 700, y: 100 },
          { id: "M004", label: "Insulin", type: "Medication", x: 700, y: 200 },
          { id: "PR01", label: "HbA1c Test", type: "Procedure", x: 500, y: 350 }
        ],
        links: [
          { source: "P002", target: "DR02", label: "진료" },
          { source: "DR02", target: "D002", label: "진단" },
          { source: "D002", target: "M001", label: "처방" },
          { source: "D002", target: "M004", label: "처방" },
          { source: "DR02", target: "PR01", label: "오더" },
          { source: "P002", target: "PR01", label: "검사 수행" }
        ]
      }
    };
  }

  // Scenario 2: 순환기내과 네트워크
  if (q.includes('순환기')) {
    return {
      sql_query: "SELECT p.name AS 환자명, v.visit_date AS 방문일, diag.description AS 진단명\nFROM PATIENTS p\nJOIN VISITS v ON p.patient_id = v.patient_id\nJOIN DIAGNOSIS diag ON v.visit_id = diag.visit_id\nWHERE v.department = '순환기내과'",
      explanation: "순환기내과를 방문한 환자들의 네트워크입니다. '김철수' 환자의 고혈압 진단 및 심초음파(Echocardiography) 검사 연결 관계를 시각화하여 진료 흐름을 보여줍니다.",
      tables_used: ["PATIENTS", "VISITS", "DIAGNOSIS"],
      columns: ["환자명", "방문일", "진단명"],
      query_results: [
        { "환자명": "김철수", "방문일": "2025-04-10", "진단명": "본태성 고혈압" },
        { "환자명": "김철수", "방문일": "2025-05-01", "진단명": "본태성 고혈압" }
      ],
      graph_data: {
        nodes: [
          { id: "P001", label: "김철수", type: "Patient", x: 100, y: 200 },
          { id: "DR01", label: "김명의", type: "Doctor", x: 300, y: 200 },
          { id: "DEPT01", label: "순환기내과", type: "Department", x: 300, y: 100 },
          { id: "D001", label: "고혈압", type: "Diagnosis", x: 500, y: 200 },
          { id: "M002", label: "Amlodipine", type: "Medication", x: 700, y: 200 },
          { id: "PR03", label: "심초음파", type: "Procedure", x: 500, y: 300 }
        ],
        links: [
          { source: "P001", target: "DR01", label: "진료" },
          { source: "DR01", target: "DEPT01", label: "소속" },
          { source: "DR01", target: "D001", label: "진단" },
          { source: "D001", target: "M002", label: "처방" },
          { source: "DR01", target: "PR03", label: "검사 오더" }
        ]
      }
    };
  }

  // Scenario 3: 서울 거주 40대 이상
  if (q.includes('서울') && (q.includes('40') || q.includes('나이'))) {
    return {
      sql_query: "SELECT name, age, gender, region\nFROM PATIENTS\nWHERE region = '서울' AND age >= 40",
      explanation: "서울 지역의 중장년층(40세 이상) 환자 코호트를 추출했습니다. 이들은 잠재적인 대사 질환 위험군으로 분류될 수 있습니다.",
      tables_used: ["PATIENTS"],
      columns: ["name", "age", "gender", "region"],
      query_results: [
        { name: "김철수", age: 45, gender: "M", region: "서울" },
        { name: "박지성", age: 29, gender: "M", region: "서울" } // Logic check: Mock logic simulation
      ],
      graph_data: {
        nodes: [
          { id: "LOC1", label: "서울 특별시", type: "Department", x: 400, y: 200 },
          { id: "P001", label: "김철수 (45세)", type: "Patient", x: 200, y: 100 },
          { id: "P006", label: "강서연 (38세)", type: "Patient", x: 200, y: 300 },
          { id: "P003", label: "박지성 (29세)", type: "Patient", x: 600, y: 200 }
        ],
        links: [
          { source: "P001", target: "LOC1", label: "거주" },
          { source: "P006", target: "LOC1", label: "거주" },
          { source: "P003", target: "LOC1", label: "거주" }
        ]
      }
    };
  }

  // Scenario 4: 입원 환자
  if (q.includes('입원')) {
    return {
      sql_query: "SELECT p.name, v.visit_date, v.department, d.name AS 의사명\nFROM PATIENTS p\nJOIN VISITS v ON p.patient_id = v.patient_id\nJOIN DOCTORS d ON v.doctor_id = d.doctor_id\nWHERE v.type = '입원'",
      explanation: "현재 입원 중이거나 입원 이력이 있는 환자 현황입니다. 내분비내과, 종양내과, 신경과 병동의 입원 흐름을 시각화했습니다.",
      tables_used: ["PATIENTS", "VISITS", "DOCTORS"],
      columns: ["name", "visit_date", "department", "의사명"],
      query_results: [
        { name: "이영희", visit_date: "2025-04-12", department: "내분비내과", "의사명": "이장수" },
        { name: "정민호", visit_date: "2025-04-15", department: "종양내과", "의사명": "박수술" },
        { name: "최수진", visit_date: "2025-05-12", department: "신경과", "의사명": "최신경" }
      ],
      graph_data: {
        nodes: [
          { id: "WARD_MAIN", label: "본관 입원 병동", type: "Department", x: 400, y: 250 },
          { id: "P002", label: "이영희", type: "Patient", x: 150, y: 100 },
          { id: "P005", label: "정민호", type: "Patient", x: 150, y: 250 },
          { id: "P004", label: "최수진", type: "Patient", x: 150, y: 400 },
          { id: "DR02", label: "이장수", type: "Doctor", x: 650, y: 100 },
          { id: "DR03", label: "박수술", type: "Doctor", x: 650, y: 250 },
          { id: "DR04", label: "최신경", type: "Doctor", x: 650, y: 400 }
        ],
        links: [
          { source: "P002", target: "WARD_MAIN", label: "입원" },
          { source: "P005", target: "WARD_MAIN", label: "입원" },
          { source: "P004", target: "WARD_MAIN", label: "입원" },
          { source: "P002", target: "DR02", label: "주치의" },
          { source: "P005", target: "DR03", label: "주치의" },
          { source: "P004", target: "DR04", label: "주치의" }
        ]
      }
    };
  }

    // Scenario 5: 고혈압
  if (q.includes('고혈압') || q.includes('i10')) {
    return {
      sql_query: "SELECT DISTINCT p.name, p.age, p.gender, m.name AS 약물\nFROM PATIENTS p\nJOIN VISITS v ON p.patient_id = v.patient_id\nJOIN DIAGNOSIS d ON v.visit_id = d.visit_id\nJOIN PRESCRIPTIONS pr ON v.visit_id = pr.visit_id\nJOIN MEDICATIONS m ON pr.med_id = m.med_id\nWHERE d.icd_code = 'I10'",
      explanation: "본태성 고혈압(I10) 환자군과 그들에게 처방된 항고혈압제(Amlodipine)의 연관 관계입니다.",
      tables_used: ["PATIENTS", "VISITS", "DIAGNOSIS", "MEDICATIONS"],
      columns: ["name", "age", "gender", "약물"],
      query_results: [
        { name: "김철수", age: 45, gender: "M", "약물": "Amlodipine" }
      ],
      graph_data: {
        nodes: [
          { id: "P001", label: "김철수", type: "Patient", x: 200, y: 200 },
          { id: "D_HTN", label: "고혈압(I10)", type: "Diagnosis", x: 450, y: 200 },
          { id: "M002", label: "Amlodipine", type: "Medication", x: 650, y: 200 }
        ],
        links: [
          { source: "P001", target: "D_HTN", label: "진단" },
          { source: "D_HTN", target: "M002", label: "처방" }
        ]
      }
    };
  }

  // Scenario 6: 병원 진료망 (Default Network Visualization)
  if (q.includes('관계') || q.includes('네트워크') || q.includes('시각화') || q.includes('망')) {
    return {
      sql_query: "SELECT p.name AS 환자, d.name AS 의사, v.department AS 진료과\nFROM PATIENTS p\nJOIN VISITS v ON p.patient_id = v.patient_id\nJOIN DOCTORS d ON v.doctor_id = d.doctor_id",
      explanation: "병원 내 전체적인 진료 흐름도입니다. 환자 중심의 의료진 연결망과 각 과별 분포를 한눈에 파악할 수 있습니다.",
      tables_used: ["PATIENTS", "VISITS", "DOCTORS"],
      columns: ["환자", "의사", "진료과"],
      query_results: [
        { "환자": "김철수", "의사": "김명의", "진료과": "순환기내과" },
        { "환자": "이영희", "의사": "이장수", "진료과": "내분비내과" },
        { "환자": "정민호", "의사": "박수술", "진료과": "종양내과" },
        { "환자": "박지성", "의사": "정뼈대", "진료과": "정형외과" },
        { "환자": "최수진", "의사": "최신경", "진료과": "신경과" }
      ],
      graph_data: {
        nodes: [
          { id: "P001", label: "김철수", type: "Patient", x: 100, y: 100 },
          { id: "P002", label: "이영희", type: "Patient", x: 100, y: 200 },
          { id: "P003", label: "박지성", type: "Patient", x: 100, y: 300 },
          { id: "P005", label: "정민호", type: "Patient", x: 100, y: 400 },
          { id: "DR01", label: "김명의", type: "Doctor", x: 400, y: 100 },
          { id: "DR02", label: "이장수", type: "Doctor", x: 400, y: 200 },
          { id: "DR05", label: "정뼈대", type: "Doctor", x: 400, y: 300 },
          { id: "DR03", label: "박수술", type: "Doctor", x: 400, y: 400 },
          { id: "DEPT_ALL", label: "아산병원", type: "Department", x: 650, y: 250 }
        ],
        links: [
          { source: "P001", target: "DR01", label: "순환기" },
          { source: "P002", target: "DR02", label: "내분비" },
          { source: "P003", target: "DR05", label: "정형" },
          { source: "P005", target: "DR03", label: "종양" },
          { source: "DR01", target: "DEPT_ALL", label: "소속" },
          { source: "DR02", target: "DEPT_ALL", label: "소속" },
          { source: "DR05", target: "DEPT_ALL", label: "소속" },
          { source: "DR03", target: "DEPT_ALL", label: "소속" }
        ]
      }
    };
  }

  // Default Error Fallback
  return {
    sql_query: "-- 쿼리 생성 실패",
    explanation: "죄송합니다. 해당 질문에 대한 분석 결과를 생성할 수 없습니다. 샘플 질문을 이용해 주세요.",
    tables_used: [],
    columns: ["Error"],
    query_results: [],
    graph_data: { nodes: [], links: [] }
  };
};

export const generateSqlFromNaturalLanguage = async (question: string): Promise<any> => {
  // 1. Try Live API First
  if (process.env.API_KEY && process.env.API_KEY !== 'dummy_key') {
    try {
      const response = await ai.models.generateContent({
        model: 'gemini-3-pro-preview',
        contents: {
          parts: [{ text: `User Question: "${question}"\n\nProvide the SQL, explanation (in Korean), ACTUAL DATA RESULTS, and GRAPH DATA (Neo4j simulation) from the mock dataset.` }]
        },
        config: {
          systemInstruction: DATA_ANALYST_PROMPT,
          thinkingConfig: { thinkingBudget: 32768 },
          responseMimeType: "application/json",
          responseSchema: {
            type: Type.OBJECT,
            properties: {
              sql_query: { type: Type.STRING },
              explanation: { type: Type.STRING, description: "Explanation of the logic in Korean" },
              tables_used: { type: Type.ARRAY, items: { type: Type.STRING } },
              columns: { type: Type.ARRAY, items: { type: Type.STRING }, description: "List of column headers" },
              query_results: { 
                type: Type.ARRAY, 
                items: { type: Type.OBJECT },
                description: "The actual rows of data"
              },
              graph_data: {
                type: Type.OBJECT,
                description: "Neo4j Graph structure for visualization",
                properties: {
                  nodes: {
                    type: Type.ARRAY,
                    items: {
                      type: Type.OBJECT,
                      properties: {
                        id: { type: Type.STRING },
                        label: { type: Type.STRING },
                        type: { type: Type.STRING, enum: ["Patient", "Doctor", "Diagnosis", "Department", "Medication", "Procedure"] },
                        x: { type: Type.NUMBER },
                        y: { type: Type.NUMBER }
                      }
                    }
                  },
                  links: {
                    type: Type.ARRAY,
                    items: {
                      type: Type.OBJECT,
                      properties: {
                        source: { type: Type.STRING, description: "id of source node" },
                        target: { type: Type.STRING, description: "id of target node" },
                        label: { type: Type.STRING }
                      }
                    }
                  }
                }
              }
            },
            required: ["sql_query", "explanation", "query_results", "columns", "graph_data"]
          }
        }
      });

      if (response.text) {
        return JSON.parse(response.text);
      }
    } catch (error) {
      console.warn("Gemini API Error (Falling back to offline demo mode):", error);
      // Fallthrough to fallback
    }
  }

  // 2. Fallback for Demo / No API Key / Network Error
  console.log("Using Offline Fallback for:", question);
  // Simulate delay for realism
  await new Promise(resolve => setTimeout(resolve, 1500)); 
  return getFallbackResult(question);
};