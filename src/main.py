import pandas as pd
from ColumnStore import ColumnStore
import time
from Module import (
    sort_data_by_year_month,
    zone_mapping,
    query_column_store,
    compress_column_store_files,
    compressed_zone_mapping
)


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


resale_data = pd.read_csv("../Data/cleaned_data.csv")

# Find out all unique constants in the columns Town, FlatType, FlatModel, StoreyRange and stored as array in Constant
# Comment out after initial run
# search_for_unique_constants(resale_data)

# Main logic for querying the column store
# User will be prompted to enter metric number, x and y
def run_query_loop(zonemaps, compression):
    while True:
        metric_number = input("Enter metric number: ")
        desired_x = int(input("Enter desired length of months from the commencing month for the query: "))
        desired_y = int(input("Enter desired minimum square meters of HDB resale flats for the query: "))

        start_time = time.time()
        query_column_store(zonemaps, metric_number, desired_x, desired_y, compression)
        end_time = time.time()

        print("Query completed in",
              round((end_time - start_time), 2),
              "seconds")

        cont = input("Run another query? (y/n): ")
        if cont.lower() != 'y':
            break


# Main logic for querying the column store with compression
# User will be prompted to enter metric number, x and y
# def run_compressed_query_loop(zonemaps):
#     while True:
#         metric_number = input("Enter metric number: ")
#         desired_x = int(input("Enter desired length of months from the commencing month for the query: "))
#         desired_y = int(input("Enter desired minimum square meters of HDB resale flats for the query: "))
#
#         start_time = time.time()
#         query_compressed_column_store(zonemaps, metric_number, desired_x, desired_y)
#         end_time = time.time()
#
#         print("Compressed query completed in",
#               round((end_time - start_time), 2),
#               "seconds")
#
#         cont = input("Run another compressed query? (y/n): ")
#         if cont.lower() != 'y':
#             break


# Main function loop of the program
def main():
    while True:
        print("\nProgram Starting")
        print("1. Unsorted Column Store")
        print("2. Sorted Column Store")
        print("3. Unsorted Column Store + Compression")
        print("4. Sorted Column Store + Compression")
        print("0. Quit")

        choice = input("Enter your choice: ")

        if choice == "1":
            start_time = time.time()

            to_column_store(resale_data)
            zonemaps = zone_mapping(1024)

            end_time = time.time()
            print("Unsorted column store + zone mapping completed in",
                  round((end_time - start_time), 2),
                  "seconds")

            run_query_loop(zonemaps, False)

        elif choice == "2":
            start_time = time.time()

            sorted_data = sort_data_by_year_month(resale_data)
            to_column_store(sorted_data)
            zonemaps = zone_mapping(1024)

            end_time = time.time()
            print("Sorted column store + zone mapping completed in",
                  round((end_time - start_time), 2),
                  "seconds")

            run_query_loop(zonemaps, False)

        elif choice == "3":
            start_time = time.time()

            to_column_store(resale_data)
            compress_column_store_files()
            zonemaps = compressed_zone_mapping(1024)

            end_time = time.time()
            print("Unsorted column store + compression + zone mapping completed in",
                  round((end_time - start_time), 2), "seconds")

            run_query_loop(zonemaps, True)

        elif choice == "4":
            start_time = time.time()

            sorted_data = sort_data_by_year_month(resale_data)
            to_column_store(sorted_data)
            compress_column_store_files()
            zonemaps = compressed_zone_mapping(1024)

            end_time = time.time()
            print("Sorted column store + compression + zone mapping completed in",
                  round((end_time - start_time), 2), "seconds")

            run_query_loop(zonemaps, True)

        elif choice == "0":
            print("Exiting app...")
            break

        else:
            print("Invalid input, try again.")


if __name__ == "__main__":
    main()