"""
Airflow DAG validator — used in CI and local development.
Imports all DAG files and checks for syntax errors and cycles.
Exits with code 1 on any failure so CI catches it.
"""
import sys
import os
import importlib.util
import traceback
from pathlib import Path

DAGS_DIR = Path(__file__).parent.parent / "airflow" / "dags"


def validate_dag_file(filepath: Path) -> tuple[bool, str]:
    """Import a DAG file and check it loads without errors."""
    spec = importlib.util.spec_from_file_location("dag_module", filepath)
    module = importlib.util.module_from_spec(spec)

    try:
        spec.loader.exec_module(module)
    except Exception as e:
        return False, f"Import error: {e}\n{traceback.format_exc()}"

    # Check the module exposes at least one DAG object
    from airflow import DAG
    dags_found = [
        name for name, obj in vars(module).items()
        if isinstance(obj, DAG)
    ]

    if not dags_found:
        return False, "No DAG objects found in file"

    # Check each DAG for cycles
    for dag_id in dags_found:
        dag = getattr(module, dag_id)
        try:
            dag.test_cycle()
        except Exception as e:
            return False, f"DAG '{dag_id}' has a cycle: {e}"

    return True, f"OK — found DAGs: {dags_found}"


def main():
    dag_files = list(DAGS_DIR.glob("*.py"))

    if not dag_files:
        print(f"No DAG files found in {DAGS_DIR}")
        sys.exit(1)

    failures = []

    for filepath in sorted(dag_files):
        print(f"Validating {filepath.name}...", end=" ")
        success, message = validate_dag_file(filepath)
        if success:
            print(f"[PASS] {message}")
        else:
            print(f"[FAIL] {message}")
            failures.append(filepath.name)

    print(f"\n{'='*50}")
    print(f"Validated {len(dag_files)} DAG file(s)")

    if failures:
        print(f"FAILED: {', '.join(failures)}")
        sys.exit(1)
    else:
        print("All DAGs validated successfully")
        sys.exit(0)


if __name__ == "__main__":
    # Add airflow dags directory to path so imports resolve
    sys.path.insert(0, str(DAGS_DIR))
    main()