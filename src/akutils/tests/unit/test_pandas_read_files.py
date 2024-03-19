import pytest
import pandas as pd
import akutils as ak
from akutils import PATH_TO_AKUTILS_PKG


class TestReadCsvInChunks():

    file_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales.csv"

    def test_read_csv_in_chunk(self):
        """
        Simple cases
        """
        df_expected = pd.read_csv(self.file_path, sep=";", dtype="string")
        df = ak.read_csv_in_chunks(self.file_path, sep=";")
        pd.testing.assert_frame_equal(df, df_expected)


if __name__ == "__main__":
    pytest.main([__file__])
