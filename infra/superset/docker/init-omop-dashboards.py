#!/usr/bin/env python3
"""
Superset OMOP CDM Dashboard Initializer

Creates database connection, datasets, charts, and a dashboard
for OMOP CDM patient data overview using Superset REST API.

Designed to run inside the superset-init container after superset init completes.
"""

import json
import logging
import os
import sys
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://superset:8088")
ADMIN_USER = os.environ.get("ADMIN_USERNAME", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "admin")

OMOP_DB_HOST = os.environ.get("OMOP_DB_HOST", "omop-db")
OMOP_DB_PORT = os.environ.get("OMOP_DB_PORT", "5432")
OMOP_DB_NAME = os.environ.get("OMOP_DB_NAME", "omop_cdm")
OMOP_DB_USER = os.environ.get("OMOP_DB_USER", "omopuser")
OMOP_DB_PASS = os.environ.get("OMOP_DB_PASS", "omop")

SQLALCHEMY_URI = (
    f"postgresql://{OMOP_DB_USER}:{OMOP_DB_PASS}@{OMOP_DB_HOST}:{OMOP_DB_PORT}/{OMOP_DB_NAME}"
)

DATABASE_NAME = "OMOP CDM (omop_cdm)"
DASHBOARD_TITLE = "OMOP CDM 환자 데이터 개요"


class SupersetAPI:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.csrf_token = None
        self.access_token = None
        self.admin_id = 1

    def login(self, username: str, password: str) -> bool:
        """Login via session-based auth (form login) to avoid g.user issues with JWT."""
        # Step 1: Get login page to extract CSRF token from form
        login_url = f"{self.base_url}/login/"
        resp = self.session.get(login_url)
        if resp.status_code != 200:
            log.error("Cannot fetch login page: %s", resp.status_code)
            return False

        # Extract CSRF token from hidden input
        import re
        match = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', resp.text)
        if not match:
            match = re.search(r'id="csrf_token"[^>]*value="([^"]+)"', resp.text)
        form_csrf = match.group(1) if match else ""

        # Step 2: Submit login form
        resp = self.session.post(login_url, data={
            "username": username,
            "password": password,
            "csrf_token": form_csrf,
        }, allow_redirects=False)

        if resp.status_code in (200, 302):
            if "session" in self.session.cookies.get_dict():
                log.info("Session login successful")
                self.admin_id = self._get_admin_id()
                return True
        log.error("Login failed: %s", resp.status_code)
        return False

    def _get_admin_id(self) -> int:
        """Get the current user's ID."""
        data = self._get("/api/v1/me/")
        if data and "result" in data:
            return data["result"].get("id", 1)
        return 1

    def get_csrf_token(self) -> str:
        """Get CSRF token for API write operations."""
        url = f"{self.base_url}/api/v1/security/csrf_token/"
        resp = self.session.get(url)
        if resp.status_code == 200:
            self.csrf_token = resp.json()["result"]
            self.session.headers.update({"X-CSRFToken": self.csrf_token})
            self.session.headers.update({"Referer": self.base_url})
            return self.csrf_token
        log.warning("CSRF token fetch failed (may not be required): %s", resp.status_code)
        return ""

    def _post(self, endpoint: str, payload: dict) -> dict | None:
        url = f"{self.base_url}{endpoint}"
        resp = self.session.post(url, json=payload)
        if resp.status_code in (200, 201):
            return resp.json()
        log.error("POST %s failed [%d]: %s", endpoint, resp.status_code, resp.text[:500])
        return None

    def _get(self, endpoint: str, params: dict | None = None) -> dict | None:
        url = f"{self.base_url}{endpoint}"
        resp = self.session.get(url, params=params)
        if resp.status_code == 200:
            return resp.json()
        log.error("GET %s failed [%d]: %s", endpoint, resp.status_code, resp.text[:300])
        return None

    def _put(self, endpoint: str, payload: dict) -> dict | None:
        url = f"{self.base_url}{endpoint}"
        resp = self.session.put(url, json=payload)
        if resp.status_code == 200:
            return resp.json()
        log.error("PUT %s failed [%d]: %s", endpoint, resp.status_code, resp.text[:500])
        return None

    # ---- Database ----
    def find_database(self, name: str) -> int | None:
        data = self._get("/api/v1/database/", params={"q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": name}]})})
        if data and data.get("count", 0) > 0:
            return data["result"][0]["id"]
        return None

    def create_database(self, name: str, uri: str) -> int | None:
        existing = self.find_database(name)
        if existing:
            log.info("Database '%s' already exists (id=%d)", name, existing)
            return existing
        payload = {
            "database_name": name,
            "sqlalchemy_uri": uri,
            "expose_in_sqllab": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
            "allow_run_async": True,
        }
        result = self._post("/api/v1/database/", payload)
        if result:
            db_id = result.get("id")
            log.info("Created database '%s' (id=%d)", name, db_id)
            return db_id
        return None

    # ---- Dataset ----
    def find_dataset(self, table_name: str, database_id: int) -> int | None:
        data = self._get("/api/v1/dataset/", params={
            "q": json.dumps({"filters": [
                {"col": "table_name", "opr": "eq", "value": table_name},
                {"col": "database", "opr": "rel_o_m", "value": database_id},
            ]})
        })
        if data and data.get("count", 0) > 0:
            return data["result"][0]["id"]
        return None

    def create_dataset(self, table_name: str, database_id: int, schema: str = "public") -> int | None:
        existing = self.find_dataset(table_name, database_id)
        if existing:
            log.info("Dataset '%s' already exists (id=%d)", table_name, existing)
            return existing
        payload = {
            "database": database_id,
            "table_name": table_name,
            "schema": schema,
        }
        result = self._post("/api/v1/dataset/", payload)
        if result:
            ds_id = result.get("id")
            log.info("Created dataset '%s' (id=%d)", table_name, ds_id)
            return ds_id
        return None

    # ---- Chart ----
    def find_chart(self, name: str) -> int | None:
        data = self._get("/api/v1/chart/", params={
            "q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": name}]})
        })
        if data and data.get("count", 0) > 0:
            return data["result"][0]["id"]
        return None

    def create_chart(self, name: str, viz_type: str, datasource_id: int, params_dict: dict) -> int | None:
        existing = self.find_chart(name)
        if existing:
            log.info("Chart '%s' already exists (id=%d)", name, existing)
            return existing
        payload = {
            "slice_name": name,
            "viz_type": viz_type,
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "owners": [self.admin_id],
            "params": json.dumps(params_dict),
        }
        result = self._post("/api/v1/chart/", payload)
        if result:
            chart_id = result.get("id")
            log.info("Created chart '%s' (id=%d)", name, chart_id)
            return chart_id
        return None

    # ---- Dashboard ----
    def find_dashboard(self, title: str) -> int | None:
        data = self._get("/api/v1/dashboard/", params={
            "q": json.dumps({"filters": [{"col": "dashboard_title", "opr": "eq", "value": title}]})
        })
        if data and data.get("count", 0) > 0:
            return data["result"][0]["id"]
        return None

    def create_dashboard(self, title: str, chart_ids: list[int]) -> int | None:
        existing = self.find_dashboard(title)
        if existing:
            log.info("Dashboard '%s' already exists (id=%d)", title, existing)
            return existing
        # Build position_json for the dashboard layout
        position = build_dashboard_layout(chart_ids)
        json_metadata = json.dumps({
            "default_filters": "{}",
            "expanded_slices": {},
            "timed_refresh_immune_slices": [],
            "filter_scopes": {},
            "chart_configuration": {},
        })
        payload = {
            "dashboard_title": title,
            "published": True,
            "owners": [self.admin_id],
            "position_json": json.dumps(position),
            "json_metadata": json_metadata,
        }
        result = self._post("/api/v1/dashboard/", payload)
        if result:
            dash_id = result.get("id")
            log.info("Created dashboard '%s' (id=%d)", title, dash_id)
            # Link charts to dashboard via Superset metadata DB
            self._link_charts_to_dashboard(dash_id, chart_ids)
            return dash_id
        return None

    def _link_charts_to_dashboard(self, dashboard_id: int, chart_ids: list[int]):
        """Insert dashboard_slices records via Superset's metadata DB.

        The REST API does not auto-populate the dashboard-chart M2M
        relationship from position_json, so we insert directly.
        """
        try:
            import subprocess
            db_host = os.environ.get("SUPERSET_META_DB_HOST", os.environ.get("DATABASE_HOST", "superset-db"))
            db_user = os.environ.get("SUPERSET_META_DB_USER", os.environ.get("DATABASE_USER", "superset"))
            db_pass = os.environ.get("SUPERSET_META_DB_PASS", os.environ.get("DATABASE_PASSWORD", "superset"))
            db_name = os.environ.get("SUPERSET_META_DB_NAME", os.environ.get("DATABASE_DB", "superset"))

            values = ", ".join(f"({dashboard_id}, {cid})" for cid in chart_ids)
            sql = (
                f"DELETE FROM dashboard_slices WHERE dashboard_id = {dashboard_id}; "
                f"INSERT INTO dashboard_slices (dashboard_id, slice_id) VALUES {values};"
            )

            # Try psycopg2 first (available in Superset image)
            try:
                import psycopg2
                conn = psycopg2.connect(host=db_host, user=db_user, password=db_pass, dbname=db_name)
                conn.autocommit = True
                cur = conn.cursor()
                cur.execute(sql)
                conn.close()
                log.info("Linked %d charts to dashboard %d via psycopg2", len(chart_ids), dashboard_id)
                return
            except ImportError:
                pass

            # Fallback: use psql via docker exec (when running from host)
            import shutil
            if shutil.which("psql"):
                env = os.environ.copy()
                env["PGPASSWORD"] = db_pass
                subprocess.run(
                    ["psql", "-h", db_host, "-U", db_user, "-d", db_name, "-c", sql],
                    env=env, check=True, capture_output=True,
                )
                log.info("Linked %d charts to dashboard %d via psql", len(chart_ids), dashboard_id)
            else:
                log.warning("Cannot link charts: psycopg2 and psql not available")
        except Exception as e:
            log.warning("Failed to link charts to dashboard: %s", e)


def build_dashboard_layout(chart_ids: list[int]) -> dict:
    """Build a 2-column grid layout for the dashboard."""
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
    }
    rows = []
    for i in range(0, len(chart_ids), 2):
        row_id = f"ROW-{i // 2}"
        row = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for j, chart_id in enumerate(chart_ids[i : i + 2]):
            chart_component_id = f"CHART-{chart_id}"
            chart_component = {
                "type": "CHART",
                "id": chart_component_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "chartId": chart_id,
                    "width": 6,
                    "height": 50,
                    "sliceName": "",
                },
            }
            position[chart_component_id] = chart_component
            row["children"].append(chart_component_id)
        position[row_id] = row
        rows.append(row_id)
    position["GRID_ID"]["children"] = rows
    return position


def wait_for_superset(api: SupersetAPI, max_wait: int = 300):
    """Wait for Superset to become available."""
    log.info("Waiting for Superset at %s ...", api.base_url)
    start = time.time()
    while time.time() - start < max_wait:
        try:
            resp = requests.get(f"{api.base_url}/health", timeout=5)
            if resp.status_code == 200:
                log.info("Superset is ready!")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(5)
    log.error("Superset did not become available within %ds", max_wait)
    return False


def define_charts(datasets: dict[str, int]) -> list[dict]:
    """Define the 6 OMOP CDM charts."""
    return [
        {
            "name": "성별 환자 분포",
            "viz_type": "pie",
            "datasource_id": datasets["person"],
            "params": {
                "viz_type": "pie",
                "datasource": f"{datasets['person']}__table",
                "groupby": ["gender_concept_id"],
                "metric": {
                    "label": "환자 수",
                    "expressionType": "SIMPLE",
                    "aggregate": "COUNT",
                    "column": {"column_name": "person_id"},
                },
                "adhoc_filters": [],
                "row_limit": 100,
                "sort_by_metric": True,
                "show_labels": True,
                "show_legend": True,
                "label_type": "key_percent",
                "number_format": "SMART_NUMBER",
                "date_format": "smart_date",
            },
        },
        {
            "name": "출생연도별 환자 수",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": datasets["person"],
            "params": {
                "viz_type": "echarts_timeseries_bar",
                "datasource": f"{datasets['person']}__table",
                "x_axis": "year_of_birth",
                "metrics": [
                    {
                        "label": "환자 수",
                        "expressionType": "SIMPLE",
                        "aggregate": "COUNT",
                        "column": {"column_name": "person_id"},
                    }
                ],
                "groupby": [],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 100,
                "truncate_metric": True,
                "show_legend": False,
                "rich_tooltip": True,
                "x_axis_title": "출생연도",
                "y_axis_title": "환자 수",
            },
        },
        {
            "name": "월별 방문 추이",
            "viz_type": "echarts_timeseries_line",
            "datasource_id": datasets["visit_occurrence"],
            "params": {
                "viz_type": "echarts_timeseries_line",
                "datasource": f"{datasets['visit_occurrence']}__table",
                "x_axis": "visit_start_date",
                "time_grain_sqla": "P1M",
                "metrics": [
                    {
                        "label": "방문 건수",
                        "expressionType": "SIMPLE",
                        "aggregate": "COUNT",
                        "column": {"column_name": "visit_occurrence_id"},
                    }
                ],
                "groupby": [],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 10000,
                "rich_tooltip": True,
                "show_legend": False,
                "x_axis_title": "월",
                "y_axis_title": "방문 건수",
            },
        },
        {
            "name": "진단 Top 10",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": datasets["condition_occurrence"],
            "params": {
                "viz_type": "echarts_timeseries_bar",
                "datasource": f"{datasets['condition_occurrence']}__table",
                "x_axis": "condition_concept_id",
                "metrics": [
                    {
                        "label": "진단 건수",
                        "expressionType": "SIMPLE",
                        "aggregate": "COUNT",
                        "column": {"column_name": "condition_occurrence_id"},
                    }
                ],
                "groupby": [],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 10,
                "truncate_metric": True,
                "show_legend": False,
                "orientation": "horizontal",
                "rich_tooltip": True,
                "x_axis_title": "Condition Concept ID",
                "y_axis_title": "진단 건수",
            },
        },
        {
            "name": "약물 처방 Top 10",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": datasets["drug_exposure"],
            "params": {
                "viz_type": "echarts_timeseries_bar",
                "datasource": f"{datasets['drug_exposure']}__table",
                "x_axis": "drug_concept_id",
                "metrics": [
                    {
                        "label": "처방 건수",
                        "expressionType": "SIMPLE",
                        "aggregate": "COUNT",
                        "column": {"column_name": "drug_exposure_id"},
                    }
                ],
                "groupby": [],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 10,
                "truncate_metric": True,
                "show_legend": False,
                "orientation": "horizontal",
                "rich_tooltip": True,
                "x_axis_title": "Drug Concept ID",
                "y_axis_title": "처방 건수",
            },
        },
        {
            "name": "검사 건수 추이",
            "viz_type": "echarts_timeseries_line",
            "datasource_id": datasets["measurement"],
            "params": {
                "viz_type": "echarts_timeseries_line",
                "datasource": f"{datasets['measurement']}__table",
                "x_axis": "measurement_date",
                "time_grain_sqla": "P1M",
                "metrics": [
                    {
                        "label": "검사 건수",
                        "expressionType": "SIMPLE",
                        "aggregate": "COUNT",
                        "column": {"column_name": "measurement_id"},
                    }
                ],
                "groupby": [],
                "adhoc_filters": [],
                "order_desc": True,
                "row_limit": 10000,
                "rich_tooltip": True,
                "show_legend": False,
                "x_axis_title": "월",
                "y_axis_title": "검사 건수",
            },
        },
    ]


def main():
    api = SupersetAPI(SUPERSET_URL)

    # Wait for Superset to be ready
    if not wait_for_superset(api):
        log.error("Cannot proceed - Superset is not available")
        sys.exit(1)

    # Login
    if not api.login(ADMIN_USER, ADMIN_PASS):
        log.error("Cannot proceed - login failed")
        sys.exit(1)

    # Get CSRF token
    api.get_csrf_token()

    # Step 1: Create database connection
    log.info("=== Step 1: Create OMOP CDM database connection ===")
    db_id = api.create_database(DATABASE_NAME, SQLALCHEMY_URI)
    if not db_id:
        log.error("Failed to create database connection")
        sys.exit(1)

    # Step 2: Create datasets
    log.info("=== Step 2: Create datasets ===")
    table_names = [
        "person",
        "visit_occurrence",
        "condition_occurrence",
        "drug_exposure",
        "measurement",
    ]
    datasets = {}
    for table in table_names:
        ds_id = api.create_dataset(table, db_id)
        if ds_id:
            datasets[table] = ds_id
        else:
            log.warning("Failed to create dataset for '%s', skipping", table)

    if len(datasets) < len(table_names):
        log.warning("Some datasets could not be created. Continuing with available ones.")

    if not datasets:
        log.error("No datasets created - cannot create charts")
        sys.exit(1)

    # Step 3: Create charts
    log.info("=== Step 3: Create charts ===")
    chart_definitions = define_charts(datasets)
    chart_ids = []
    for chart_def in chart_definitions:
        # Skip if the required dataset is not available
        if chart_def["datasource_id"] is None:
            log.warning("Skipping chart '%s' - dataset not available", chart_def["name"])
            continue
        chart_id = api.create_chart(
            chart_def["name"],
            chart_def["viz_type"],
            chart_def["datasource_id"],
            chart_def["params"],
        )
        if chart_id:
            chart_ids.append(chart_id)

    if not chart_ids:
        log.error("No charts created - cannot create dashboard")
        sys.exit(1)

    log.info("Created %d charts", len(chart_ids))

    # Step 4: Create dashboard
    log.info("=== Step 4: Create dashboard ===")
    dash_id = api.create_dashboard(DASHBOARD_TITLE, chart_ids)
    if dash_id:
        log.info("=== Done! Dashboard '%s' (id=%d) created with %d charts ===",
                 DASHBOARD_TITLE, dash_id, len(chart_ids))
    else:
        log.error("Failed to create dashboard")
        sys.exit(1)


if __name__ == "__main__":
    main()
