import os
import pandas as pd
import functools

NULL_CONSTANT = "@#*NULL@#*"

class ColumnStore:
    def __init__(self, dataframe):
        """
        Initialize the ColumnStore object with the provided dataframe and an empty cache

        Args:
        - dataframe: preprocessed csv containing the data to be stored and processed
        """

        self.dataframe = dataframe
        self.cache = {}

        # Define the data types for each column
        self.data_types  = {
            'year': int,
            'month': str,
            'month_num': int,
            'town': str,
            'block': str,
            'street': str,
            'flat_type': str,
            'flat_model': str,
            'storey_range': str,
            'floor_area': int,
            'lease_year': int,
            'resale_price': int
        }

    def convert_to_column_store(self, directory):
        """
        Coverts and stores each column of the dataframe into separate files in the ../ColumnStore directory

        Args:
        - directory: Path to directory where the files will be stored
        """
        if not os.path.exists(directory):
            os.makedirs(directory)

        for column_name in self.dataframe.columns:
            file_path = os.path.join(directory, f"{column_name}.columnStore")
            try:
                column_data = self.dataframe[column_name].fillna(NULL_CONSTANT)
                column_data.to_csv(file_path, index=False)
                if (column_data.isnull()).any():
                    print(f"Empty entries detected in column '{column_name}' and was replaced with @#*NULL@#*")

                expected_datatype = self.data_types.get(column_name)
                if expected_datatype:
                    actual_datatype = column_data.dtype
                    if expected_datatype != actual_datatype:
                        print(f"Data mismatch detected in column '{column_name}', expected '{expected_datatype}', received '{actual_datatype}'")

            except Exception as e:
                print(f"Error with saving column '{column_name}': {e}")
