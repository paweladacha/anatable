
import pandas as pd

from cardinality import CardinalityDict


# ---- obsolete and ideas ----
# def sets_of_unique_values_dict(df, cols=None):
#     if cols is None:
#         cols = df.columns
#     return {col:set(df[col].unique()) for col in cols}

# def _get_largest_cardinality_column(uvd):
#     this_cardinality_dict = CardinalityDict({col_name: len(unique_values) for col_name, unique_values in uvd.items()})
#     col_name, cardinality = this_cardinality_dict._sorted(descending=True)[0]
#     return col_name, cardinality


# selected = [x[0] for x in cardinality_dict.filter_below_cumulative_ratio(0.5, len(df))._sorted(descending=True)]
# uvd = sets_of_unique_values_dict(df, selected)

# while uvd:
#     col_name, cardinality = _get_largest_cardinality_column(uvd)
#     print(col_name, cardinality)
#     representative_rows = df.drop_duplicates(subset=[col_name], keep='first')

#     uvd.pop(col_name) # remove the column because we already have all representative rows for it

#     # Remove values of other columns we already have representative rows for
#     for col_name in uvd.keys():
#         uvd[col_name].difference_update(
#             set(representative_rows[col_name].unique()))

#     # Remove columns that have no more unique values left
#     for col_name, unique_values in uvd.items():
#         if not unique_values:
#             uvd.pop(col_name)


class Pandas:
    @staticmethod
    def single_cols(df, filtered_cardinality_dict: CardinalityDict) -> pd.DataFrame:
        all_indexes_set = set()
        for selected_col, _ in filtered_cardinality_dict:
            all_indexes_set.update(df.drop_duplicates(subset=[selected_col], keep='first').index)

        return df.loc[list(all_indexes_set)].sort_index()

try:
    from functools import reduce
    from operator import or_
    from pyspark.sql import DataFrame, Window
    from pyspark.sql import functions as F


    class PySpark:
        @staticmethod
        def single_cols(df: DataFrame, filtered_cardinality_dict: CardinalityDict) -> DataFrame:
            cols = [col_name for col_name, _ in filtered_cardinality_dict]
            
            if not cols:
                return df.limit(0)

            # 1. Assign a deterministic row ID to preserve original order ("keep='first'")
            # Note: monotonically_increasing_id() is fast and avoids a global shuffle, 
            # though if strict global ordering is required, use zipWithIndex or a natural sort key.
            df_indexed = df.withColumn("_row_id", F.monotonically_increasing_id())

            # 2. Dla każdej kolumny o niskiej kardynalności znajdujemy po jednym przykładowym _row_id dla każdej unikalnej wartości
            unique_row_ids = set()

            for col_name in low_card_spark.keys():
                # Pobieramy po jednym identyfikatorze wiersza dla każdej unikalnej wartości danej kolumny
                sampled_ids = df_indexed.groupBy(col_name).agg(F.first("_row_id").alias("id")).select("id").collect()
                unique_row_ids.update([r["id"] for r in sampled_ids])

            # 3. Filtrujemy oryginalny DataFrame pozostawiając tylko wiersze, które reprezentują te unikalne wartości
            return df_indexed.filter(F.col("_row_id").isin(list(unique_row_ids))).drop("_row_id")

        
except ImportError:
    import warnings

    warnings.warn("PySpark integration is not available.", ImportWarning)