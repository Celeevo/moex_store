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

from moex_store import MoexStore


def _valid_history_rows(raw_history):
    # MOEX возвращает в series не только полноценные фьючерсные контракты.
    # Для цепочки экспираций нам нужны записи, где есть startdate и expdate.
    return [
        item for item in raw_history
        if len(item) >= 7 and item[2] is not None and item[3] is not None
    ]


def _duplicate_groups(history_rows):
    # Дубли группируем по базовому активу и дате экспирации.
    # Это помогает увидеть пары вроде EuH8 / EuH8_2018.
    groups = {}
    for item in history_rows:
        key = f"{item[4]}|{item[3]}"
        groups.setdefault(key, []).append(item)

    return {
        key: value
        for key, value in groups.items()
        if len(value) > 1
    }


def _format_contract(contract):
    return {
        "secid": contract[0],
        "shortname": contract[1],
        "startdate": contract[2],
        "expdate": contract[3],
        "assetcode": contract[4],
        "underlyingasset": contract[5],
        "is_traded": contract[6],
    }


def _contract_lookup(valid_history, normalized_history, contracts):
    # Для каждого указанного SECID показываем две вещи:
    # 1. raw-контракт, который реально пришел от MOEX;
    # 2. предыдущий контракт из нормализованной цепочки.
    result = {}
    by_secid = {item[0]: item for item in valid_history}

    for sec_id in contracts:
        contract = by_secid.get(sec_id)
        if contract is None:
            result[sec_id] = {
                "found": False,
                "normalized_found": False,
                "contract": None,
                "previous_contract": None,
            }
            continue

        normalized_index = next(
            (
                index for index, item in enumerate(normalized_history)
                if item[4] == contract[4] and item[3] == contract[3]
            ),
            None,
        )
        previous_contract = None
        if normalized_index is not None and normalized_index + 1 < len(normalized_history):
            previous_contract = normalized_history[normalized_index + 1]

        result[sec_id] = {
            "found": True,
            "normalized_found": normalized_index is not None,
            "contract": _format_contract(contract),
            "previous_contract": None if previous_contract is None else _format_contract(previous_contract),
        }

    return result


def build_payload(asset, raw_history, contracts=None):
    contracts = contracts or []
    store = MoexStore(read_from_file=False)
    valid_history = _valid_history_rows(raw_history)

    # Нормализация убирает календарные спреды и выбирает один контракт
    # на каждую дату экспирации, чтобы prevexpdate не зависел от порядка MOEX.
    normalized_history = store.futures._normalize_history_stat(valid_history)

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "asset": asset,
        "counts": {
            "raw": len(raw_history),
            "valid": len(valid_history),
            "normalized": len(normalized_history),
        },
        "duplicates_by_asset_expdate": _duplicate_groups(valid_history),
        "contracts": _contract_lookup(valid_history, normalized_history, contracts),
        "raw_history": raw_history,
        "valid_history": valid_history,
        "normalized_history": normalized_history,
    }


def update_metadata_cache(asset, history_stat):
    # Команда диагностики может сразу обновить futures_metadata.json.
    # В кэш пишется valid raw history, а не normalized history: так мы не теряем aliases.
    cache_path = os.path.join("files_from_moex", "futures_metadata.json")
    cache_dir = os.path.dirname(cache_path)
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)

    if os.path.isfile(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as file:
                cache = json.load(file)
        except (OSError, json.JSONDecodeError):
            cache = {}
    else:
        cache = {}

    cache.setdefault("history_stat", {})[asset] = history_stat

    with open(cache_path, "w", encoding="utf-8") as file:
        json.dump(cache, file, ensure_ascii=False, indent=2)

    return cache_path


async def fetch_raw_history(asset):
    # Берем именно сырой ответ MOEX. Retry/DNS fallback уже живет внутри MoexStore/Futures.
    stat = await MoexStore(read_from_file=False).futures._get_futures_stat(asset)
    return stat.get("series", {}).get("data", [])


def parse_args():
    parser = argparse.ArgumentParser(
        description="Dump raw and normalized MOEX futures history for debugging."
    )
    parser.add_argument("asset", help="Futures asset code, for example Eu, Si, RI.")
    parser.add_argument(
        "--contracts",
        nargs="*",
        default=[],
        help="Optional SECID list to resolve against normalized history.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path. Defaults to files_from_moex/futures_diagnostics_<asset>.json.",
    )
    parser.add_argument(
        "--update-cache",
        action="store_true",
        help="Also update files_from_moex/futures_metadata.json with normalized history.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output = args.output or os.path.join("files_from_moex", f"futures_diagnostics_{args.asset}.json")
    output_dir = os.path.dirname(output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    raw_history = asyncio.run(fetch_raw_history(args.asset))
    payload = build_payload(args.asset, raw_history, args.contracts)

    with open(output, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    print(f"Wrote diagnostics to {output}")
    if args.update_cache:
        cache_path = update_metadata_cache(args.asset, payload["valid_history"])
        print(f"Updated futures metadata cache at {cache_path}")

    print(json.dumps(payload["counts"], ensure_ascii=False, indent=2))
    if args.contracts:
        print(json.dumps(payload["contracts"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
