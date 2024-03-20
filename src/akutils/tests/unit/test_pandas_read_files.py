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

    def test_read_csv_in_chunk_with_add_cols_function(self):
        """
        Read with new col function apply on each chunk function
        """
        file_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales.csv"
        df_expected = pd.read_csv(file_path, sep=";")
        df_expected["new_col"] = (df_expected["col1"] + df_expected["col2"]) * 2.4 + 151

        def add_new_col_to_chunk(
            df_chunk: pd.DataFrame,
            factor: float,
            constant: int
        ) -> pd.DataFrame:
            df_chunk["new_col"] = (
                (df_chunk["col1"] + df_chunk["col2"]) * factor + constant
            )
            return df_chunk

        file_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales.csv"
        df = ak.read_csv_in_chunks(
            file_path,
            chunk_func=add_new_col_to_chunk,
            factor=2.4,
            constant=151,
            chunksize=5,
            sep=";",
            dtype=None
        )
        pd.testing.assert_frame_equal(df, df_expected)


class TestReadMultipleCsvFromDir():

    df_expected = pd.DataFrame(
        data=[
            [1, 1, "Italy"],
            [1, 2, "Germany"],
            [1, 3, "France"],
            [2, 4, "Italy"],
            [3, 5, "Germany"],
            [3, 6, "France"]
        ],
        columns=["month", "nb_sales", "country"]
    )

    def test_read_multiple_csv_from_dir(self):
        """
        Simple cases (the dir containing also not text file extension)
        """
        df_expected = self.df_expected.copy()
        dir_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales_per_month"
        df = ak.read_multiple_csv_from_dir(dir_path, sep=";", chunksize=5, dtype=None)
        df = df.sort_values("nb_sales").reset_index(drop=True)
        pd.testing.assert_frame_equal(df, df_expected)

    def test_read_multiple_csv_from_dir_with_filter_files(self):
        """
        Case with file name filter (the dir containing also not text file extension)
        """
        df_expected = self.df_expected.copy()
        df_expected = df_expected[df_expected["month"] == 1]
        dir_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales_per_month"
        df = ak.read_multiple_csv_from_dir(
            dir_path,
            regex="^Sales_01",
            sep=";",
            dtype=None)
        df = df.sort_values("nb_sales").reset_index(drop=True)
        pd.testing.assert_frame_equal(df, df_expected)

    def test_read_multiple_csv_from_dir_with_filter_files_case_sensitive(self):
        """
        Case with file case sensitive name filter (the dir containing also not text
        file extension)
        """
        df_expected = self.df_expected.copy()
        df_expected = df_expected[df_expected["month"] == 3].reset_index(drop=True)
        dir_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales_per_month"
        df = ak.read_multiple_csv_from_dir(
            dir_path,
            regex="^Sales_",
            case_sensitive=True,
            sep=";",
            dtype=None)
        df = df.sort_values("nb_sales")
        pd.testing.assert_frame_equal(df, df_expected)

    def test_read_multiple_csv_from_dir_with_chunk_function(self):
        """
        Case with chunk function (the dir containing also not text file extension)
        """

        def filter_chunk(df_chunk: pd.DataFrame) -> pd.DataFrame:
            return df_chunk[df_chunk["nb_sales"] <= 4]

        df_expected = self.df_expected.copy()
        df_expected = df_expected[df_expected["nb_sales"] <= 4]
        dir_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales_per_month"
        df = ak.read_multiple_csv_from_dir(
            dir_path,
            chunk_func=filter_chunk,
            chunksize=2,
            sep=";",
            dtype=None)
        df = df.sort_values("nb_sales").reset_index(drop=True)
        pd.testing.assert_frame_equal(df, df_expected)


class TestReadMultipleXlsxFromDir():

    df_expected = pd.DataFrame(
        data=[
            [1, 1, "Italy"],
            [1, 2, "Germany"],
            [1, 3, "France"],
            [2, 4, "Italy"],
            [3, 5, "Germany"],
            [3, 6, "France"],
            [4, 7, "Italy"],
            [4, 8, "Germany"]
        ],
        columns=["month", "nb_sales", "country"]
    )

    def test_read_multiple_xlsx_from_dir(self):
        """
        Simple cases (the dir containing also not excel files)
        """
        df_expected = self.df_expected.copy()
        dir_path = PATH_TO_AKUTILS_PKG / "tests" / "_fixtures" / "sales_per_month"
        df = ak.read_multiple_xlsx_from_dir(dir_path)
        df = df.sort_values("nb_sales").reset_index(drop=True)
        pd.testing.assert_frame_equal(df, df_expected)


if __name__ == "__main__":
    pytest.main([__file__])
