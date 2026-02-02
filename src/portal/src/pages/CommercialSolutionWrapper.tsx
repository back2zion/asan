import React from 'react';
import { Spin, Alert } from 'antd';

interface Props {
  solution: 'TeraONE' | 'BIMatrixBI' | 'BIMatrixOLAP' | 'Terastream';
}

const solutionEndpoints: Record<Props['solution'], string> = {
  TeraONE: 'https://datastreams.co.kr/products/enterprise-data-fabric-teraone/',
  BIMatrixBI: 'https://www.bimatrix.co.kr/product/i-canvas/',
  BIMatrixOLAP: 'http://www.bimatrix.co.kr/solution/i-stream',
  Terastream: 'https://datastreams.co.kr/products/data-integration-terastream/'
};

const CommercialSolutionWrapper: React.FC<Props> = ({ solution }) => {
  const url = solutionEndpoints[solution];

  return (
    <div style={{ width: '100%', height: 'calc(100vh - 160px)', position: 'relative' }}>
      <Alert
        message="상용 솔루션 연동 예시"
        description="이 화면은 실제 SSO 연동을 통해 지정된 상용 솔루션의 웹 인터페이스를 임베딩하여 보여줍니다. 현재는 각 솔루션의 소개 페이지로 연결됩니다."
        type="info"
        showIcon
        style={{ marginBottom: '16px' }}
      />
      <div style={{ border: '1px solid #e8e8e8', borderRadius: '8px', height: '100%', overflow: 'hidden' }}>
        <iframe
          src={url}
          title={solution}
          frameBorder="0"
          style={{ width: '100%', height: '100%' }}
        />
      </div>
    </div>
  );
};

export default CommercialSolutionWrapper;
