#!/bin/bash
# Self-signed TLS 인증서 생성 (개발/시연용)
# 프로덕션에서는 Let's Encrypt 또는 공인 CA 인증서 사용
set -e

CERT_DIR="$(dirname "$0")/certs"
mkdir -p "$CERT_DIR"

if [ -f "$CERT_DIR/server.crt" ] && [ -f "$CERT_DIR/server.key" ]; then
    echo "인증서가 이미 존재합니다: $CERT_DIR/"
    echo "재생성하려면 기존 파일을 삭제하세요."
    exit 0
fi

openssl req -x509 -nodes -days 365 \
    -newkey rsa:2048 \
    -keyout "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.crt" \
    -subj "/C=KR/ST=Seoul/L=Songpa/O=AsanMedicalCenter/OU=IDP/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

chmod 600 "$CERT_DIR/server.key"
chmod 644 "$CERT_DIR/server.crt"

echo "TLS 인증서 생성 완료: $CERT_DIR/"
