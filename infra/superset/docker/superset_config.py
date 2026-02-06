# Superset Configuration for Asan IDP
# Reference: https://superset.apache.org/docs/installation/configuring-superset

import os

# Database Configuration
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{os.environ.get('DATABASE_USER', 'superset')}:"
    f"{os.environ.get('DATABASE_PASSWORD', 'superset')}@"
    f"{os.environ.get('DATABASE_HOST', 'superset-db')}:"
    f"{os.environ.get('DATABASE_PORT', '5432')}/"
    f"{os.environ.get('DATABASE_DB', 'superset')}"
)

# Redis Configuration for Celery
REDIS_HOST = os.environ.get('REDIS_HOST', 'superset-redis')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')

class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

CELERY_CONFIG = CeleryConfig

# Secret key
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'asan-idp-superset-secret-key-2026')

# Disable CSRF for embedded mode (be careful in production)
WTF_CSRF_ENABLED = False

# Disable Talisman entirely for development (this allows iframe embedding)
TALISMAN_ENABLED = False

# If Talisman is enabled, use these settings
# TALISMAN_CONFIG = {
#     'force_https': False,
#     'frame_options': False,
#     'content_security_policy': None,
# }

# Feature flags
FEATURE_FLAGS = {
    'EMBEDDED_SUPERSET': True,
    'ENABLE_TEMPLATE_PROCESSING': True,
}

# Allow embedding dashboards
GUEST_ROLE_NAME = 'Public'
PUBLIC_ROLE_LIKE = 'Gamma'

# Dashboard embedding configuration
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'origins': ['http://localhost:5173', 'http://localhost:3000', 'http://localhost:8000'],
}

# Disable examples loading
SUPERSET_LOAD_EXAMPLES = False

# HTTP Headers override (as a fallback)
HTTP_HEADERS = {}
