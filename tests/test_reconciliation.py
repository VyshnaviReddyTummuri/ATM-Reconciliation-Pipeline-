import unittest

def reconciliation_logic(atm_amount, settlement_amount):
    if atm_amount is not None and settlement_amount is not None:
        if atm_amount == settlement_amount:
            return "MATCHED"
        else:
            return "AMOUNT_MISMATCH"
    elif atm_amount is not None:
        return "MISSING_IN_SETTLEMENT"
    elif settlement_amount is not None:
        return "MISSING_IN_ATM"
    else:
        return "UNKNOWN"


class TestReconciliation(unittest.TestCase):

    def test_matched(self):
        self.assertEqual(reconciliation_logic(100, 100), "MATCHED")

    def test_mismatch(self):
        self.assertEqual(reconciliation_logic(100, 200), "AMOUNT_MISMATCH")

    def test_missing_settlement(self):
        self.assertEqual(reconciliation_logic(100, None), "MISSING_IN_SETTLEMENT")

    def test_missing_atm(self):
        self.assertEqual(reconciliation_logic(None, 200), "MISSING_IN_ATM")


if __name__ == "__main__":
    unittest.main()