import unittest

from scholaridreconciler.models.scholar import Scholar


class TestScholar(unittest.TestCase):
    """
    test the scholar data model
    """


    def test_load_from_record(self):
        """
        test loading scholar from record
        """
        record = {"first_name": "John", "family_name": "Doe"}
        scholar = Scholar.model_validate(record)
        self.assertEqual(scholar.first_name, "John")
        self.assertEqual(scholar.family_name, "Doe")


if __name__ == '__main__':
    unittest.main()
