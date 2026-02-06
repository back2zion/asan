# Airflow Webserver Configuration for Asan IDP
# This file is mounted into the Airflow container

from flask import Flask
from flask_appbuilder.security.manager import AUTH_DB

# Security configuration
AUTH_TYPE = AUTH_DB

# Allow embedding in iframes
# Note: In production, use more specific settings
WTF_CSRF_ENABLED = False

# Custom response headers to allow iframe embedding
class AirflowWebServerConfig:
    # Override X-Frame-Options to allow embedding
    @staticmethod
    def init_app(app: Flask):
        @app.after_request
        def add_headers(response):
            response.headers['X-Frame-Options'] = 'ALLOWALL'
            response.headers['Content-Security-Policy'] = "frame-ancestors *"
            return response
