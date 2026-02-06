#!/usr/bin/env python3
"""
imaging_study 테이블 생성 및 NIH Chest X-ray 전체 데이터 삽입

OMOP DB (Docker)에 imaging_study 테이블을 생성하고,
다운로드된 NIH Chest X-rays Data_Entry_2017.csv 전체를 로드합니다.

Usage:
    python scripts/setup_imaging.py
"""
import subprocess
import os
import csv

# OMOP DB 설정
OMOP_CONTAINER = os.getenv("OMOP_CONTAINER", "infra-omop-db-1")
OMOP_USER = os.getenv("OMOP_USER", "omopuser")
OMOP_DB = os.getenv("OMOP_DB", "omop_cdm")

# NIH 데이터셋 경로
DATASET_BASE = os.path.expanduser("~/.cache/kagglehub/datasets/nih-chest-xrays/data/versions/3")
CSV_PATH = os.path.join(DATASET_BASE, "Data_Entry_2017.csv")

BATCH_SIZE = 500


def run_psql(sql: str, quiet: bool = False) -> str:
    """Docker exec으로 psql 명령 실행"""
    cmd = [
        "docker", "exec", OMOP_CONTAINER,
        "psql", "-U", OMOP_USER, "-d", OMOP_DB,
        "-t", "-A", "-c", sql
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 and not quiet:
        print(f"  ERROR: {result.stderr.strip()}")
    return result.stdout.strip()


def create_table():
    """imaging_study 테이블 생성"""
    print("[1/3] imaging_study 테이블 생성...")

    sql = """
    CREATE TABLE IF NOT EXISTS imaging_study (
        imaging_study_id SERIAL PRIMARY KEY,
        person_id INTEGER,
        image_filename VARCHAR(200),
        finding_labels VARCHAR(500),
        view_position VARCHAR(10),
        patient_age INTEGER,
        patient_gender VARCHAR(2),
        image_url VARCHAR(500),
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    run_psql(sql)
    print("  -> 테이블 생성 완료")


def get_person_ids() -> list:
    """OMOP person 테이블에서 실제 person_id 목록 조회"""
    output = run_psql("SELECT person_id FROM person ORDER BY person_id;")
    if not output:
        print("  WARNING: person 테이블에서 person_id를 조회할 수 없습니다. 기본값 사용.")
        return list(range(1, 1001))
    return [int(pid) for pid in output.strip().split('\n') if pid.strip()]


def insert_full_data():
    """NIH Data_Entry_2017.csv 전체 데이터 삽입"""
    print(f"[2/3] CSV 데이터 로드: {CSV_PATH}")

    if not os.path.isfile(CSV_PATH):
        print(f"  ERROR: CSV 파일을 찾을 수 없습니다: {CSV_PATH}")
        return

    # 기존 데이터 삭제
    run_psql("TRUNCATE imaging_study RESTART IDENTITY;", quiet=True)

    # person_id 매핑
    person_ids = get_person_ids()
    print(f"  -> person 테이블에서 {len(person_ids)}개 person_id 조회")

    total = 0
    batch_values = []

    with open(CSV_PATH, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header
        # Columns: Image Index, Finding Labels, Follow-up #, Patient ID, Patient Age, Patient Gender, View Position, ...

        for row in reader:
            if len(row) < 7:
                continue

            filename = row[0].strip()
            findings = row[1].strip().replace("'", "''")
            patient_id = int(row[3]) if row[3].strip() else 0
            age = int(row[4]) if row[4].strip() else 0
            gender = row[5].strip()
            view = row[6].strip()

            person_id = person_ids[patient_id % len(person_ids)]
            image_url = f"/api/v1/imaging/images/{filename}"

            batch_values.append(
                f"({person_id}, '{filename}', '{findings}', '{view}', {age}, '{gender}', '{image_url}')"
            )

            if len(batch_values) >= BATCH_SIZE:
                sql = f"""
                INSERT INTO imaging_study (person_id, image_filename, finding_labels, view_position, patient_age, patient_gender, image_url)
                VALUES {','.join(batch_values)};
                """
                run_psql(sql, quiet=True)
                total += len(batch_values)
                if total % 10000 == 0:
                    print(f"  -> {total}건 삽입 완료...")
                batch_values = []

    # 잔여 배치 삽입
    if batch_values:
        sql = f"""
        INSERT INTO imaging_study (person_id, image_filename, finding_labels, view_position, patient_age, patient_gender, image_url)
        VALUES {','.join(batch_values)};
        """
        run_psql(sql, quiet=True)
        total += len(batch_values)

    count = run_psql("SELECT COUNT(*) FROM imaging_study;")
    print(f"  -> 총 {count}건 삽입 완료")


def verify_images():
    """다운로드된 이미지 디렉토리 확인"""
    print("[3/3] 이미지 디렉토리 확인...")

    total_images = 0
    for d in sorted(os.listdir(DATASET_BASE)):
        img_dir = os.path.join(DATASET_BASE, d, "images")
        if d.startswith("images_") and os.path.isdir(img_dir):
            count = len(os.listdir(img_dir))
            total_images += count
            print(f"  -> {d}: {count}개 이미지")

    print(f"  -> 총 {total_images}개 이미지 파일 확인")
    return total_images


if __name__ == "__main__":
    print("=" * 50)
    print("NIH Chest X-ray Imaging Study Setup (Full Dataset)")
    print("=" * 50)

    create_table()
    insert_full_data()
    img_count = verify_images()

    print("\n설정 완료!")
    print(f"  - 테이블: imaging_study")
    print(f"  - 이미지: {DATASET_BASE}/images_*/images/")
    print(f"  - 이미지 수: {img_count}개")
    print(f"  - API: GET /api/v1/imaging/images/{{filename}}")
