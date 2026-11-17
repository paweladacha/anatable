
def sets_of_unique_values_dict(df, cols=None):
    if cols is None:
        cols = df.columns
    return {col:set(df[col].unique()) for col in cols}

def _get_largest_cardinality_column(uvd):
    this_cardinality_dict = CardinalityDict({col_name: len(unique_values) for col_name, unique_values in uvd.items()})
    col_name, cardinality = this_cardinality_dict._sorted(descending=True)[0]
    return col_name, cardinality


selected = [x[0] for x in cardinality_dict.filter_below_cumulative_ratio(0.5, len(df))._sorted(descending=True)]
uvd = sets_of_unique_values_dict(df, selected)

while uvd:
    col_name, cardinality = _get_largest_cardinality_column(uvd)
    print(col_name, cardinality)
    representative_rows = df.drop_duplicates(subset=[col_name], keep='first')

    uvd.pop(col_name) # remove the column because we already have all representative rows for it

    # Remove values of other columns we already have representative rows for
    for col_name in uvd.keys():
        uvd[col_name].difference_update(
            set(representative_rows[col_name].unique()))

    # Remove columns that have no more unique values left
    for col_name, unique_values in uvd.items():
        if not unique_values:
            uvd.pop(col_name)

all_indexes_set = set()
for selected_col,_ in cardinality_dict.filter_below_cumulative_ratio(0.5, len(df))._sorted(descending=True):
    all_indexes_set.update(df.drop_duplicates(subset=[selected_col], keep='first').index)

result_df = df.loc[list(all_indexes_set)].sort_index()