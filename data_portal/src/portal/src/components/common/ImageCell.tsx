/**
 * ImageCell 컴포넌트
 *
 * 테이블 셀 값이 이미지 URL 패턴인지 감지하여:
 * - 매치 시: 40x40 썸네일 + Antd Image preview (클릭 시 원본)
 * - 비매치 시: 기존 텍스트 표시
 */
import React from 'react';
import { Image } from 'antd';
import { FileImageOutlined } from '@ant-design/icons';

// 이미지 URL 패턴
const IMAGE_URL_PATTERNS = [
  /\/api\/v1\/imaging\//i,
  /\.png$/i,
  /\.jpe?g$/i,
  /\.dicom$/i,
  /^s3:\/\//i,
  /https?:\/\/.*\.s3\./i,
];

function isImageUrl(value: unknown): boolean {
  if (typeof value !== 'string') return false;
  const str = value.trim();
  if (!str) return false;
  return IMAGE_URL_PATTERNS.some((pattern) => pattern.test(str));
}

/** 파일명만 있으면 imaging API 경로를 붙여서 반환 */
function resolveImageUrl(raw: string): string {
  const str = raw.trim();
  if (str.startsWith('http') || str.startsWith('/api/') || str.startsWith('s3://')) return str;
  // 파일명만 있는 경우 (예: 00000032_012.png)
  return `/api/v1/imaging/images/${str}`;
}

interface ImageCellProps {
  value: unknown;
}

const ImageCell: React.FC<ImageCellProps> = ({ value }) => {
  if (!isImageUrl(value)) {
    return <>{String(value ?? '')}</>;
  }

  const url = resolveImageUrl(String(value));

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <Image
        src={url}
        width={40}
        height={40}
        style={{
          objectFit: 'cover',
          borderRadius: 4,
          border: '1px solid #d9d9d9',
          cursor: 'pointer',
        }}
        preview={{
          mask: <FileImageOutlined style={{ fontSize: 16, color: '#fff' }} />,
        }}
        fallback="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNDAiIGhlaWdodD0iNDAiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+PHJlY3Qgd2lkdGg9IjQwIiBoZWlnaHQ9IjQwIiBmaWxsPSIjZjBmMGYwIi8+PHRleHQgeD0iMjAiIHk9IjIwIiB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBkeT0iLjNlbSIgZm9udC1zaXplPSIxMCIgZmlsbD0iIzk5OSI+WC1yYXk8L3RleHQ+PC9zdmc+"
      />
      <span style={{ fontSize: 11, color: '#999', maxWidth: 80, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {url.split('/').pop()}
      </span>
    </div>
  );
};

export default ImageCell;
export { isImageUrl };
