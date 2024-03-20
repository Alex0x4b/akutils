import pytest
import pandas as pd
import numpy as np
from datetime import datetime
import akutils as ak


class TestStripColumns():

    def test_strip_columns_no_filter(self):
        # input
        columns = ["c1", "c2", "c3", "c4", "c5"]
        data = [
            ["  1  ", 2.1, "  2.01 ss  ", "  a   ", "1"],
            ["  3", np.nan, "   3 s", np.nan, "2"],
            ["2  ", 2.2, "4. 2  ", "  b", "3"]
        ]
        df_input = pd.DataFrame(data=data, columns=columns)
        # Expected
        data_expected = [
            ["1", 2.1, "2.01 ss", "a", "1"],
            ["3", np.nan, "3 s", np.nan, "2"],
            ["2", 2.2, "4. 2", "b", "3"]
        ]
        df_expected = pd.DataFrame(data=data_expected, columns=columns)
        df = ak.strip_columns(df_input)
        pd.testing.assert_frame_equal(df, df_expected)

    def test_strip_columns_with_filter(self):
        # input
        columns = ["c1", "c2", "c3", "c4"]
        data = [
            ["  1  ", 2.1, "  2.01 ss  ", datetime(2000, 1, 1)],
            ["  3", 2.2, "   3 s", datetime(2000, 1, 1)],
            ["2  ", 2.2, "4. 2  ", datetime(2000, 1, 1)]
        ]
        df_input = pd.DataFrame(data=data, columns=columns)
        # Expected
        data_expected = [
            ["1", 2.1, "  2.01 ss  ", datetime(2000, 1, 1)],
            ["3", 2.2, "   3 s", datetime(2000, 1, 1)],
            ["2", 2.2, "4. 2  ", datetime(2000, 1, 1)]
        ]
        df_expected = pd.DataFrame(data=data_expected, columns=columns)
        df = ak.strip_columns(df_input, cols=["c1", "c2", "c4"])
        pd.testing.assert_frame_equal(df, df_expected)


if __name__ == "__main__":
    pytest.main([__file__])
