
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
        def single_cols(df: DataFrame,
                        filtered_cardinality_dict) -> DataFrame:
            cols = [col_name for col_name, _ in filtered_cardinality_dict]
            if not cols:
                return df.limit(0)

            # Nadajemy ID. Jeśli df jest wynikiem prostego odczytu (Parquet/Delta/ORC), 
            # kolejność partycji jest stała i nie trzeba cache'ować całego szerokiego DF!
            df_indexed = df.withColumn("_row_id", F.monotonically_increasing_id())

            # Rzutujemy tylko na wąski wycinek kolumn przed stackiem (oszczędność pamięci i CPU)
            narrow_df = df_indexed.select("_row_id", *cols)

            stack_args = ", ".join([f"'{c}', cast(`{c}` as string)" for c in cols])
            unpivoted = narrow_df.select(
                "_row_id",
                F.expr(f"stack({len(cols)}, {stack_args}) as (_col_name, _col_val)")
            )

            # HashAggregate (brak sortowania!) - redukuje dane lokalnie przed shuffle
            sampled_ids_df = (
                unpivoted
                .groupBy("_col_name", "_col_val")
                .agg(F.min("_row_id").alias("_row_id"))
                .select("_row_id")
                .distinct()
            )

            # Left Semi Join z wymuszonym broadcastem małej tabeli ID
            return (
                df_indexed
                .join(F.broadcast(sampled_ids_df), on="_row_id", how="left_semi")
                .drop("_row_id")
            )

        from pyspark.sql import functions as F

# # 1. Dodajemy unikalny identyfikator wiersza, aby łatwo zidentyfikować wybrane rekordy
# df_with_id = spark_df.withColumn("_row_id", F.monotonically_increasing_id())

# # 2. Dla każdej kolumny o niskiej kardynalności znajdujemy po jednym przykładowym _row_id dla każdej unikalnej wartości
# unique_row_ids = set()

# for col_name in low_card_spark.keys():
#     # Pobieramy po jednym identyfikatorze wiersza dla każdej unikalnej wartości danej kolumny
#     sampled_ids = df_with_id.groupBy(col_name).agg(F.first("_row_id").alias("id")).select("id").collect()
#     unique_row_ids.update([r["id"] for r in sampled_ids])

# # 3. Filtrujemy oryginalny DataFrame pozostawiając tylko wiersze, które reprezentują te unikalne wartości
# result_df = df_with_id.filter(F.col("_row_id").isin(list(unique_row_ids))).drop("_row_id")

# # Wyświetlamy wynik
# result_df.show(5)
        
except ImportError:
    import warnings

    warnings.warn("PySpark integration is not available.", ImportWarning)