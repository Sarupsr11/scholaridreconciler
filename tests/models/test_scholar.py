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
        record = {"first_name": "John", "family_name": "Doe",
                  "name":"John Doe","affiliation_raw":"RWTH Aachen University, Germany",
                  "afffiliation":{"name":"RWTH Aachen University","location":"Aachen","country":"Germany"},
                  }
        scholar = Scholar.model_validate(record)
        self.assertEqual(scholar.first_name, "John")
        self.assertEqual(scholar.family_name, "Doe")
        self.assertEqual(scholar.name, "John Doe")
        self.assertEqual(scholar.affiliation_raw ,"RWTH Aachen University, Germany")


if __name__ == '__main__':
    unittest.main()
