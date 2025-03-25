import unittest
import numpy as np
from unittest.mock import patch, MagicMock
import pandas as pd
from scholaridreconciler.models.scholar import Scholar
from scholaridreconciler.services.scholar_affiliation import convert_to_dataframe
from scholaridreconciler.services.scholar_scoring import naive_scoring  # Replace with actual import path


class TestNaiveScoring(unittest.TestCase):

    @patch('scholaridreconciler.services.scholar_affiliation.convert_to_dataframe')
    def test_naive_scoring_full_data(self, mock_convert_to_dataframe):
        # Mock the dataframe returned by convert_to_dataframe
        mock_df = pd.DataFrame({
            'first_name': ['michael'],
            'last_name': ['schaub'],
            'Label': ['michael t. schaub'],
            'affiliation_score': [90]
        })
        mock_convert_to_dataframe.return_value = mock_df

        # Create a Scholar object with full data
        scholar = Scholar(name="Michael T. Schaub",
                          first_name="Michael",
                          family_name="Schaub",
                          affiliation_raw="Rwth Aachen University")

        # Call the naive_scoring function
        score_with_wikidata_id = naive_scoring(scholar)

        # Assert the score is calculated correctly
        self.assertIsInstance(score_with_wikidata_id, (list, np.ndarray))
        self.assertEqual(len(score_with_wikidata_id), 2)  # Ensure result has two elements
        self.assertGreaterEqual(score_with_wikidata_id[1], 80)
       
    @patch('scholaridreconciler.services.scholar_affiliation.convert_to_dataframe')
    def test_naive_scoring_partial_data(self, mock_convert_to_dataframe):
        # Mock the dataframe returned by convert_to_dataframe
        mock_df = pd.DataFrame({
            'first_name': ['michael'],
            'last_name': [''],
            'Label': ['michael t. schaub'],
            'affiliation_score': [80]
        })
        mock_convert_to_dataframe.return_value = mock_df

        # Create a Scholar object with partial data
        scholar = Scholar(name="Michael T. Schaub",
                          first_name="Michael",
                          family_name=None,
                          affiliation_raw="Rwth Aachen University")

       # Call the naive_scoring function
        score_with_wikidata_id = naive_scoring(scholar)

        # Assert the score is calculated correctly
        self.assertIsInstance(score_with_wikidata_id, (list, np.ndarray))
        self.assertEqual(len(score_with_wikidata_id), 2)  # Ensure result has two elements
        self.assertGreaterEqual(score_with_wikidata_id[1], 80)

    def test_naive_scoring_no_affiliation(self):
        # Create a Scholar object without affiliation
        scholar = Scholar(name="Michael T. Schaub",
                          first_name="Michael",
                          family_name="Schaub",
                          affiliation_raw=None)

        # Assert that a ValueError is raised
        with self.assertRaises(ValueError):
            naive_scoring(scholar)


if __name__ == '__main__':
    unittest.main()
