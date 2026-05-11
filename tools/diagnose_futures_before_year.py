import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.diagnose_futures import build_payload, fetch_raw_history, update_metadata_cache


def select_contracts_before_year(raw_history, before_year=2020, limit=30):
    # Выбираем несколько старых контрактов автоматически, чтобы руками не искать SECID
    # в длинном raw_history. Отбор идет по expdate, а не по startdate.
    cutoff = datetime(before_year, 1, 1).date()
    candidates = []
    seen = set()

    for item in raw_history:
        if len(item) < 7 or item[0] in seen or item[3] is None:
            continue

        try:
            expdate = datetime.strptime(item[3], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue

        if expdate < cutoff:
            candidates.append((expdate, item[0]))
            seen.add(item[0])

    candidates.sort(reverse=True)
    return [sec_id for _, sec_id in candidates[:limit]]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Select and diagnose older MOEX futures contracts before a year."
    )
    parser.add_argument("asset", help="Futures asset code, for example Eu, Si, RI.")
    parser.add_argument(
        "--before-year",
        type=int,
        default=2020,
        help="Only contracts with expdate before January 1 of this year are selected.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="Maximum number of contracts to diagnose.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path. Defaults to files_from_moex/futures_diagnostics_<asset>_before_<year>.json.",
    )
    parser.add_argument(
        "--update-cache",
        action="store_true",
        help="Also update files_from_moex/futures_metadata.json with valid raw history.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output = args.output or os.path.join(
        "files_from_moex",
        f"futures_diagnostics_{args.asset}_before_{args.before_year}.json",
    )
    output_dir = os.path.dirname(output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Сначала получаем полную историю, затем выбираем из нее старые контракты
    # и передаем их в общий диагностический payload.
    raw_history = asyncio.run(fetch_raw_history(args.asset))
    contracts = select_contracts_before_year(
        raw_history,
        before_year=args.before_year,
        limit=args.limit,
    )
    payload = build_payload(args.asset, raw_history, contracts=contracts)
    payload["selection"] = {
        "before_year": args.before_year,
        "limit": args.limit,
        "contracts": contracts,
    }

    with open(output, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    print(f"Wrote diagnostics to {output}")
    if args.update_cache:
        cache_path = update_metadata_cache(args.asset, payload["valid_history"])
        print(f"Updated futures metadata cache at {cache_path}")

    print(json.dumps(payload["counts"], ensure_ascii=False, indent=2))
    print(json.dumps(payload["selection"], ensure_ascii=False, indent=2))
    print(json.dumps(payload["contracts"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
