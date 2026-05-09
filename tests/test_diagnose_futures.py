import unittest
import os
import json
import tempfile
from pathlib import Path

from tools.diagnose_futures import build_payload, update_metadata_cache


class DiagnoseFuturesTests(unittest.TestCase):
    def test_payload_reports_previous_real_contracts_from_normalized_history(self):
        raw_history = [
            ["EuH8_2018", "Eu-3.18", "2016-11-22", "2018-03-15", "Eu", "EUR_RUB__TOM", 0],
            ["EuM8", "Eu-6.18", "2018-03-15", "2018-06-21", "Eu", "EUR_RUB__TOM", 0],
            ["EuH8", "EuH8", "2016-11-22", "2018-03-15", "Eu", "EUR_RUB__TOM", 0],
            ["EuZ7", "Eu-12.17", "2017-09-21", "2017-12-21", "Eu", "EUR_RUB__TOM", 0],
        ]

        payload = build_payload("Eu", raw_history, contracts=["EuM8", "EuH8_2018", "EuH8"])

        self.assertEqual(payload["counts"]["raw"], 4)
        self.assertEqual(payload["counts"]["normalized"], 3)
        self.assertEqual(payload["contracts"]["EuM8"]["previous_contract"]["secid"], "EuH8")
        self.assertTrue(payload["contracts"]["EuH8_2018"]["found"])
        self.assertTrue(payload["contracts"]["EuH8_2018"]["normalized_found"])
        self.assertEqual(payload["contracts"]["EuH8_2018"]["previous_contract"]["secid"], "EuZ7")
        self.assertEqual(payload["contracts"]["EuH8"]["previous_contract"]["secid"], "EuZ7")

    def test_payload_matches_real_eu_diagnostics_sample(self):
        raw_history = [
            ["EuH0_2000", "EuH0_2000", "2009-06-03", "2010-03-15", "Eu", "EUR_RUB__TOM", 0],
            ["EuH0_2010", "Eu-3.10", "2010-01-11", "2010-03-15", "Eu", "EUR_RUB__TOM", 0],
            ["EuZ9_2009", "Eu-12.09", "2009-03-06", "2009-12-15", "Eu", "EUR_RUB__TOM", 0],
            ["EuZ8_2018", "Eu-12.18", "2017-06-09", "2018-12-20", "Eu", "EUR_RUB__TOM", 0],
            ["EuH9", "Eu-3.19", "2017-09-15", "2019-03-21", "Eu", "EUR_RUB__TOM", 0],
            ["EuH2", "Eu-3.22", "2020-09-10", "2022-03-17", "Eu", "EUR_RUB__TOM", 0],
            ["EuM2", "Eu-6.22", "2020-12-11", "2022-06-16", "Eu", "EUR_RUB__TOM", 0],
            ["EuU5", "Eu-9.25", "2024-03-15", "2025-09-18", "Eu", "EUR_RUB__TOM", 0],
            ["EuZ5", "Eu-12.25", "2024-06-14", "2025-12-18", "Eu", "EUR_RUB__TOM", 0],
            ["EuH6", "Eu-3.26", "2024-09-13", "2026-03-19", "Eu", "EUR_RUB__TOM", 0],
        ]

        payload = build_payload(
            "Eu",
            raw_history,
            contracts=["EuH6", "EuM2", "EuU8", "EuZ5", "EuH9", "EuH0_2010", "EuH0_2000"],
        )

        self.assertEqual(payload["contracts"]["EuH6"]["previous_contract"]["secid"], "EuZ5")
        self.assertEqual(payload["contracts"]["EuM2"]["previous_contract"]["secid"], "EuH2")
        self.assertFalse(payload["contracts"]["EuU8"]["found"])
        self.assertEqual(payload["contracts"]["EuZ5"]["previous_contract"]["secid"], "EuU5")
        self.assertEqual(payload["contracts"]["EuH9"]["previous_contract"]["secid"], "EuZ8_2018")
        self.assertTrue(payload["contracts"]["EuH0_2010"]["found"])
        self.assertEqual(payload["contracts"]["EuH0_2010"]["contract"]["expdate"], "2010-03-15")
        self.assertEqual(payload["contracts"]["EuH0_2010"]["previous_contract"]["secid"], "EuZ9_2009")
        self.assertEqual(payload["contracts"]["EuH0_2000"]["previous_contract"]["secid"], "EuZ9_2009")

    def test_update_metadata_cache_writes_normalized_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            previous_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                normalized_history = [
                    ["EuM8", "Eu-6.18", "2018-03-15", "2018-06-21", "Eu", "EUR_RUB__TOM", 0],
                    ["EuH8", "EuH8", "2016-11-22", "2018-03-15", "Eu", "EUR_RUB__TOM", 0],
                ]

                cache_path = update_metadata_cache("Eu", normalized_history)

                self.assertEqual(cache_path, os.path.join("files_from_moex", "futures_metadata.json"))
                cache = json.loads(Path(cache_path).read_text(encoding="utf-8"))
                self.assertEqual(cache["history_stat"]["Eu"], normalized_history)
            finally:
                os.chdir(previous_cwd)


if __name__ == "__main__":
    unittest.main()
