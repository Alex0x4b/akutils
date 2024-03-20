import pytest
import pandas as pd
import akutils as ak
from akutils import PATH_TO_AKUTILS_PKG


class TestReadCsvInChunks():

    def test_read_csv_in_chunk(self):
        """
        Simple cases, no chunk function
        """
        file_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales.csv"
        df_expected = pd.read_csv(file_path, sep=";", dtype="string")
        df = ak.read_csv_in_chunks(file_path, sep=";", chunksize=5)
        pd.testing.assert_frame_equal(df, df_expected)

    def test_read_csv_in_chunk_with_filtered_function(self):
        """
        Read with filter function apply on each chunk function
        """
        df_expected = pd.DataFrame(
            data=[
                [8, 8, "Spain"],
                [18, 18, "Spain"],
                [30, 30, "Spain"],
            ],
            columns=["col1", "col2", "country"]
        )

        def filter_chunk(df_chunk: pd.DataFrame, countries: list) -> pd.DataFrame:
            return df_chunk[df_chunk["country"].isin(countries)]

        file_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales.csv"
        df = ak.read_csv_in_chunks(
            file_path,
            chunk_func=filter_chunk,
            chunksize=5,
            countries=["Spain"],
            sep=";",
            dtype=None
        )
        pd.testing.assert_frame_equal(df, df_expected)


if __name__ == "__main__":
    pytest.main([__file__])
