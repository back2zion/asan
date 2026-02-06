FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir duckdb fastapi uvicorn pandas pyarrow streamlit plotly

EXPOSE 8501

CMD ["streamlit", "run", "/app/olap/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
