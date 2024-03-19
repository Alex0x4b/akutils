import pytest
import pandas as pd
import numpy as np
import pandas.api.types as ptypes
import akutils as ak


class TestColumnsToFloat():

    def test_columns_to_float(self):
        """
        Simple cases with homogeneous type with cols like:
        columns like 'int', 'float', '%', 'space thousand decimal'
        """
        columns = ["c1", "c2", "c3", "c4"]
        data = [
            ["1", "2.1", "2.01%", "1 000.23"],
            ["3", "2.22", "3%", "21 002.2"],
            ["2", "2.2992", "4.2%", "100.2"]
        ]
        df_input = pd.DataFrame(data=data, columns=columns)
        # Assert all df_input column are object type
        for column in columns:
            assert ptypes.is_object_dtype(df_input[column])
        # Expected
        columns_expected = columns + [f"{col}_flt" for col in columns]
        data = [
            ["1", "2.1", "2.01%", "1 000.23", 1.0, 2.1000, 0.0201, 1000.23],
            ["3", "2.22", "3%", "21 002.2", 3.0, 2.2200, 0.0300, 21002.20],
            ["2", "2.2992", "4.2%", "100.2", 2.0, 2.2992, 0.0420, 100.20]
        ]
        df_expected = pd.DataFrame(data=data, columns=columns_expected)
        # Convert to float & assert
        ak.columns_to_float(df_input, columns, keep_source=True)
        pd.testing.assert_frame_equal(df_input, df_expected)

    def test_columns_to_float_mixed(self):
        """
        Cases with mixed float like type with cols
        + test missing cols
        """
        columns = ["c1", "c2", "c3", "c4"]
        data = [
            ["1", "2,1", "2.01%", "1 000,23"],
            ["3.01", "2.22", "3,0%", "21.2"],
            ["2.1", "2", "4.21", "2 000 001"]
        ]
        df_input = pd.DataFrame(data=data, columns=columns)
        # Assert all df_input column are object type
        for column in columns:
            assert ptypes.is_object_dtype(df_input[column])
        # Expected
        columns_expected = columns + [f"{col}_flt" for col in columns]
        data = [
            ["1", "2,1", "2.01%", "1 000,23", 1.00, 2.10, 0.0201, 1000.23],
            ["3.01", "2.22", "3,0%", "21.2", 3.01, 2.22, 0.0300, 21.20],
            ["2.1", "2", "4.21", "2 000 001", 2.10, 2.00, 4.2100, 2000001.00]
        ]
        df_expected = pd.DataFrame(data=data, columns=columns_expected)
        # Convert to float & assert
        ak.columns_to_float(df_input, columns, keep_source=True)
        pd.testing.assert_frame_equal(df_input, df_expected)

    def test_columns_to_float_already_float(self):
        """
        Cases with col already in float
        """
        columns = ["c1", "c2"]
        data = [
            [1, 2.1],
            [3, 2.22],
            [2, 2.2992]
        ]
        df_input = pd.DataFrame(data=data, columns=columns)
        # Assert df_input column are float or int type
        for column in columns:
            try:
                print(column)
                assert df_input[column].dtype == np.float64
            except AssertionError:
                assert df_input[column].dtype == np.int64
        # Expected
        columns_expected = columns + [f"{col}_flt" for col in columns]
        data = [
            [1, 2.1, 1.0, 2.1000],
            [3, 2.22, 3.0, 2.2200],
            [2, 2.2992, 2.0, 2.2992]
        ]
        df_expected = pd.DataFrame(data=data, columns=columns_expected)
        # Convert to float & assert
        columns.append("random col")  # test not key_error
        ak.columns_to_float(df_input, columns, keep_source=True)
        pd.testing.assert_frame_equal(df_input, df_expected)

    def test_columns_to_float_bad_data(self):
        """
        Cases with cols containing bad data not covertible to float
        """
        columns = ["c1", "c2", "c3", "c4"]
        data = [
            ["1", "2,1", "2.01%", "1 000"],
            ["A", "2.22", 3, "21.2"],
            ["2.1", np.nan, "test", ""]
        ]
        df_input = pd.DataFrame(data=data, columns=columns)
        columns_expected = columns + [f"{col}_flt" for col in columns]
        data = [
            ["1", "2,1", "2.01%", "1 000", 1.0, 2.10, 0.0201, 1000.0],
            ["A", "2.22", 3, "21.2", 0.0, 2.22, 3.0000, 21.2],
            ["2.1", np.nan, "test", "", 2.1, 0.00, 0.0000, 0.0]
        ]
        df_expected = pd.DataFrame(data=data, columns=columns_expected)
        # Convert to float & assert
        ak.columns_to_float(df_input, columns, keep_source=True)
        pd.testing.assert_frame_equal(df_input, df_expected)


if __name__ == "__main__":
    pytest.main([__file__])
