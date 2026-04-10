import pandas as pd
from ColumnStore import ColumnStore
import time
from Constant import search_for_unique_constants
from Module import sort_data_by_year_month, zone_mapping, shared_scan_min_pairs_with_cache


# Convert the data stored in the CSV into column store
# and stored in the Column Store directory
def to_column_store(dataframe):
      start_time = time.time()
      column_store = ColumnStore(dataframe)
      column_store.convert_to_column_store("../ColumnStore")

      end_time = time.time()
      print("CSV data converted to column store in",
            round((end_time - start_time), 2),
            "seconds")


# Program Start
print("Program Starting")
print("Reading Data")

resale_data = pd.read_csv("../Data/cleaned_data.csv")

# Find out all unique constants in the columns Town, FlatType, FlatModel, StoreyRange and stored as array in Constant
# Comment out after initial run
# search_for_unique_constants(resale_data)


print("====================================================================")
print("Starting column store + zone mapping + shared scan + caching")
print("====================================================================")

# zone mapping with zone size 1024
start_time = time.time()
to_column_store(resale_data)
zonemaps = zone_mapping(1024)
shared_scan_min_pairs_with_cache(zonemaps, 1, 2017, 3, ["Ang Mo Kio", "Jurong West", "Bedok"])
end_time = time.time()
print("Column store + zone mapping + shared scan + caching completed in",
      round((end_time - start_time), 2),
      "seconds")

print("====================================================================")
print("Starting sorted column store + zone mapping + shared scan + caching")
print("====================================================================")
# sorted by year and month + zone mapping
start_time = time.time()
sorted_data = sort_data_by_year_month(resale_data)
to_column_store(resale_data)
zonemaps = zone_mapping(1024)
shared_scan_min_pairs_with_cache(zonemaps, 1, 2017, 3, ["Ang Mo Kio", "Jurong West", "Bedok"])
end_time = time.time()
print("Sorted column store + zone mapping + shared scan + caching completed in",
      round((end_time - start_time), 2),
      "seconds")