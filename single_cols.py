
from pandas import pd

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

            # 2. Build a filter condition: keep row if it is the 1st occurrence for ANY target column
            keep_conditions = [
                F.row_number().over(Window.partitionBy(col).orderBy("_row_id")) == 1
                for col in cols
            ]

            # 3. Combine conditions with OR (equivalent to set union of indexes) and filter in one pass
            combined_condition = reduce(or_, keep_conditions)

            return (
                df_indexed
                .filter(combined_condition)
                .drop("_row_id")
            )
        
except ImportError:
    import warnings

    warnings.warn("PySpark integration is not available.", ImportWarning)