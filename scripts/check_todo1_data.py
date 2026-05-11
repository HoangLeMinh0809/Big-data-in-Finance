#!/usr/bin/env python3
"""Validate repository against requirements listed in TODO_1.md

Usage:
  python scripts/check_todo1_data.py --root <repo_root>

Checks performed:
- Extracts backticked paths from TODO_1.md and verifies file/directory existence
- Verifies presence of key directories (data, crawler/maiac_data, ingest, spark_jobs, config)
- Verifies presence of important spark job files listed in TODO_1.md
- Scans requirements*.txt files for required packages (cdsapi, pyyaml)
- Reports counts of sample files in data folders and MAIAC HDFs
"""

from pathlib import Path
import re
import argparse
import json
import sys


def extract_backticks(text):
    return re.findall(r"`([^`]+)`", text)


def read_todo(todo_path: Path):
    text = todo_path.read_text(encoding="utf-8")
    items = extract_backticks(text)
    return text, items


def find_requirements_files(root: Path):
    return list(root.glob("**/requirements*.txt"))


def check_paths(root: Path, items):
    results = {"files": [], "tables": [], "missing": []}
    for it in items:
        # Normalize windows paths and remove surrounding spaces
        it_norm = it.strip()
        p = None
        # If contains slash or backslash -> file/dir path
        if "/" in it_norm or "\\" in it_norm or it_norm.endswith(".py") or it_norm.endswith(".yaml") or it_norm.endswith(".yml") or it_norm.endswith(".sh"):
            p = root.joinpath(*Path(it_norm).parts)
            exists = p.exists()
            results["files"].append({"path": it_norm, "exists": exists, "absolute": str(p)})
            if not exists:
                results["missing"].append({"path": it_norm, "reason": "not found"})
        else:
            # treat as table name or identifier
            results["tables"].append(it_norm)
    return results


def check_key_dirs(root: Path):
    dirs = ["data", "crawler/maiac_data", "ingest", "spark_jobs", "config", "scripts", "airflow/dags"]
    out = []
    for d in dirs:
        p = root.joinpath(*Path(d).parts)
        out.append({"path": d, "exists": p.exists(), "absolute": str(p)})
    return out


def count_sample_files(root: Path):
    counts = {}
    # data subfolders
    data_dir = root / "data"
    if data_dir.exists():
        for child in data_dir.iterdir():
            if child.is_dir():
                counts[f"data/{child.name}"] = sum(1 for _ in child.rglob("*.*"))
    # maiac hdf fallback
    maiac = root.joinpath("crawler", "maiac_data")
    counts["crawler/maiac_data"] = sum(1 for _ in maiac.rglob("*.hdf")) if maiac.exists() else 0
    return counts


def scan_requirements(req_paths):
    needed = ["cdsapi", "pyyaml"]
    found = {p.name: p.read_text(encoding="utf-8") for p in req_paths}
    summary = {pkg: [] for pkg in needed}
    for name, content in found.items():
        for pkg in needed:
            if re.search(r"^" + re.escape(pkg) + r"\b", content, re.I | re.M):
                summary[pkg].append(name)
    return summary, list(found.keys())


def check_spark_jobs(root: Path, required_jobs):
    out = []
    for job in required_jobs:
        p = root.joinpath("spark_jobs", job)
        out.append({"file": f"spark_jobs/{job}", "exists": p.exists(), "absolute": str(p)})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Repository root (default: current directory)")
    ap.add_argument("--todo", default="TODO_1.md", help="Path to TODO_1.md relative to root")
    ap.add_argument("--json", action="store_true", help="Emit JSON report")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    todo_path = (root / args.todo).resolve()
    if not todo_path.exists():
        print(f"ERROR: TODO file not found: {todo_path}")
        sys.exit(2)

    text, items = read_todo(todo_path)

    report = {}
    report["repo_root"] = str(root)
    report["todo_path"] = str(todo_path)

    # extracted backticked items
    report["extracted_items_count"] = len(items)
    report["extracted_items_sample"] = items[:20]

    # check paths
    path_checks = check_paths(root, items)
    report["path_checks"] = path_checks

    # key dirs
    report["key_dirs"] = check_key_dirs(root)

    # sample counts
    report["sample_file_counts"] = count_sample_files(root)

    # requirements
    reqs = find_requirements_files(root)
    report["requirements_files"] = [str(p.relative_to(root)) for p in reqs]
    req_summary, req_list = scan_requirements(reqs)
    report["requirements_found_packages"] = req_summary

    # spark_jobs required: gather from TODO by searching for 'Tao File: `spark_jobs/<name>`' or known list
    known_jobs = [
        "era5_files_streaming.py",
        "era5_surface_hanoi_silver.py",
        "hanoi_openaq_silver.py",
        "hanoi_weather_surface_proxy_silver.py",
        "sentinel5p_hanoi_silver.py",
        "maiac_hanoi_silver.py",
        "hanoi_pm25_master_features_gold.py",
        "hanoi_pm25_training_dataset_gold.py",
        "hanoi_config.py",
        "ensure_iceberg_tables.py",
    ]
    report["spark_job_checks"] = check_spark_jobs(root, known_jobs)

    # summary missing
    missing = []
    for f in path_checks.get("files", []):
        if not f["exists"]:
            missing.append(f["path"])
    for d in report["key_dirs"]:
        if not d["exists"]:
            missing.append(d["path"])
    for sj in report["spark_job_checks"]:
        if not sj["exists"]:
            missing.append(sj["file"])
    report["missing_items"] = missing

    # Print human-friendly
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return

    print("Repository validation report\n===========================")
    print(f"Repo root: {report['repo_root']}")
    print(f"TODO file: {report['todo_path']}")
    print(f"Extracted backticked items: {report['extracted_items_count']}")
    print("\n-- Key directories --")
    for d in report["key_dirs"]:
        print(f"- {d['path']}: {'OK' if d['exists'] else 'MISSING'}")

    print("\n-- Sample file counts --")
    for k, v in report["sample_file_counts"].items():
        print(f"- {k}: {v}")

    print("\n-- Requirements files found --")
    for r in report["requirements_files"]:
        print(f"- {r}")
    print("Packages requested by TODO (cdsapi, pyyaml):")
    for pkg, where in report["requirements_found_packages"].items():
        print(f"- {pkg}: found in {where if where else 'NONE'}")

    print("\n-- Specific files from TODO_1.md --")
    for f in path_checks.get("files", []):
        print(f"- {f['path']}: {'OK' if f['exists'] else 'MISSING'}")

    print("\n-- Spark job checks --")
    for s in report["spark_job_checks"]:
        print(f"- {s['file']}: {'OK' if s['exists'] else 'MISSING'}")

    if report["missing_items"]:
        print("\nMISSING ITEMS SUMMARY:")
        for m in report["missing_items"]:
            print(f"- {m}")
    else:
        print("\nNo missing items detected from the basic checklist.")


if __name__ == "__main__":
    main()
