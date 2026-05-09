import unittest

from tools.diagnose_futures_before_year import select_contracts_before_year


class DiagnoseFuturesBeforeYearTests(unittest.TestCase):
    def test_select_contracts_before_year_uses_expdate_and_limit(self):
        raw_history = [
            ["EuH0", "Eu-3.20", "2018-09-20", "2020-03-19", "Eu", "EUR_RUB__TOM", 0],
            ["EuZ9", "Eu-12.19", "2018-06-21", "2019-12-19", "Eu", "EUR_RUB__TOM", 0],
            ["EuU9", "Eu-9.19", "2018-03-15", "2019-09-19", "Eu", "EUR_RUB__TOM", 0],
            ["EuM9", "Eu-6.19", "2017-12-21", "2019-06-20", "Eu", "EUR_RUB__TOM", 0],
            ["BROKEN", "Broken", None, None, "Eu", "EUR_RUB__TOM", 0],
        ]

        result = select_contracts_before_year(raw_history, before_year=2020, limit=2)

        self.assertEqual(result, ["EuZ9", "EuU9"])


if __name__ == "__main__":
    unittest.main()
