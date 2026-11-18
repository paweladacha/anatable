class CardinalityDict(dict):
    def __init__(self, *args, total_rows_count=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.total_rows_count = total_rows_count

    def _sorted(self, descending=False)->list[tuple[str, int]]:
        """
        Sorts the cardinality dictionary, by default in ascending order of cardinality.

        Parameters:
        descending (bool): If True, sorts in descending order of cardinality.

        Returns:
        list[tuple[str, int]]: A list of tuples with column names as keys and their cardinality as values, sorted by cardinality.
        """
        return sorted(self.items(), key=lambda item: item[1], reverse=descending)

    def filter_bottom_n(self, n):
        """
        Filters the cardinality dictionary to return the bottom n columns with the lowest cardinality.

        Parameters:
        n (int): The number of bottom columns to return.

        Returns:
        CardinalityDict: A dictionary with the bottom n columns and their cardinality.
        """
        return CardinalityDict(self._sorted()[:n])

    def filter_below_threshold(self, threshold):
        """
        Filters the cardinality dictionary to return columns with cardinality below a certain threshold.

        Parameters:
        threshold (int): The maximum cardinality allowed for a column to be included.

        Returns:
        CardinalityDict: A dictionary with columns that have cardinality below the threshold.
        """
        return CardinalityDict({col: card for col, card in self.items() if card < threshold})

    def filter_below_cumulative_threshold(self, threshold):
        """
        Filters the cardinality dictionary to return columns until the cumulative cardinality exceeds a certain threshold.

        Parameters:
        threshold (int): The cumulative cardinality threshold.

        Returns:
        CardinalityDict: A dictionary with columns until the cumulative cardinality exceeds the threshold.
        """
        sorted_columns = self._sorted()
        cumulative_cardinality = 0
        filtered_columns = {}

        for col, card in sorted_columns:
            if cumulative_cardinality + card > threshold:
                break
            filtered_columns[col] = card
            cumulative_cardinality += card

        return CardinalityDict(filtered_columns)

    def filter_below_ratio(self, ratio):
        """
        Filters the cardinality dictionary to return columns with cardinality below a certain ratio of total rows.

        Parameters:
        ratio (float): The maximum ratio of cardinality to total rows allowed for a column to be included.
        total_rows (int): The total number of rows in the DataFrame.

        Returns:
        CardinalityDict: A dictionary with columns that have cardinality below the specified ratio of total rows.
        """
        threshold = ratio * self.total_rows_count
        return self.filter_below_threshold(threshold)

    def filter_below_cumulative_ratio(self, ratio):
        """
        Filters the cardinality dictionary to return columns until the cumulative cardinality exceeds a certain ratio of total rows.

        Parameters:
        ratio (float): The cumulative ratio of cardinality to total rows.
        total_rows (int): The total number of rows in the DataFrame.

        Returns:
        CardinalityDict: A dictionary with columns until the cumulative cardinality exceeds the specified ratio of total rows.
        """
        threshold = ratio * self.total_rows_count
        return self.filter_below_cumulative_threshold(threshold)

class PandasCardinality:
    @staticmethod
    def cols_cardinality(df, ignored_columns=None, only_cols=None, dropna=False) -> CardinalityDict:
        """
        Returns the cardinality of multiple columns in a DataFrame.

        Parameters:
        df (pandas.DataFrame): The DataFrame containing the columns.
        cols (list): A list of column names.

        Returns:
        CardinalityDict: A dictionary with column names as keys and their cardinality as values.
        """
        if only_cols is not None:
            columns_to_process = only_cols
        elif ignored_columns is None:
            columns_to_process = df.columns
        else:
            columns_to_process = [col for col in df.columns if col not in ignored_columns]

        return CardinalityDict({col: df[col].nunique(dropna=dropna) for col in columns_to_process}, total_rows_count=len(df))
        
    @staticmethod
    def col_cardinality(df, col):
        """
        Returns the cardinality of a column in a DataFrame.

        Parameters:
        df (pandas.DataFrame): The DataFrame containing the column.
        col (str): The name of the column.

        Returns:
        int: The number of unique values in the column.
        """
        return df[col].nunique()

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F


    from pyspark.sql import DataFrame, functions as F

    class PySparkCardinality:
        @staticmethod
        def cols_cardinality(
            df: DataFrame,
            ignored_columns: list[str] | None = None,
            only_cols: list[str] | None = None,
            exact: bool = False,
            rsd: float = 0.05
        ) -> dict[str, int]:
            """
            Oblicza kardynalność kolumn w jednym przebiegu (1 Spark Job).
            Domyślnie używa przybliżenia (HyperLogLog) dla maksymalnej wydajności.
            """
            df_cols_set = set(df.columns)

            if only_cols is not None:
                columns_to_process = [c for c in only_cols if c in df_cols_set]
            elif ignored_columns is None:
                columns_to_process = df.columns
            else:
                ignored_set = set(ignored_columns)
                columns_to_process = [c for c in df.columns if c not in ignored_set]

            if not columns_to_process:
                return {}

            # Wybór funkcji agregującej: dokładna vs przybliżona (HyperLogLog)
            count_fn = (
                (lambda c: F.count_distinct(F.col(c)))
                if exact
                else (lambda c: F.approx_count_distinct(F.col(c), rsd=rsd))
            )

            # Unikalny alias dla total_rows, żeby nie kolidował z żadną kolumną w df
            total_col_alias = "__total_rows_count__"
            agg_exprs = [F.count("*").alias(total_col_alias)] + [
                count_fn(col).alias(col) for col in columns_to_process
            ]

            # Jeden Spark Job na wszystko
            result_row = df.agg(*agg_exprs).first()
            
            if not result_row:
                return {}

            result_dict = result_row.asDict()
            total_rows = result_dict.pop(total_col_alias)

            return CardinalityDict(result_dict, total_rows_count=total_rows)
    
        @staticmethod
        def col_cardinality(df: DataFrame, col: str) -> int:
            """
            Returns the cardinality of a single column in a PySpark DataFrame.
            """
            result = df.agg(F.count_distinct(col)).first()
            return result[0] if result else 0
        
except ImportError:
    import warnings

    warnings.warn("PySpark integration is not available.", ImportWarning)