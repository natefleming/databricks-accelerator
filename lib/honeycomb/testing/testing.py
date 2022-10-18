def sort_data_frame(df, columns):
    return df.sort_values(columns).reset_index(drop=True)
