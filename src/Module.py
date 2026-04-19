import time
import ZoneMap
from Constant import COLUMN_STORE_FILES, COMPRESSED_COLUMN_STORE_FILES, ASSIGNMENT_TOWN_LIST
import re
import csv, os
import gzip

# Helper function to convert the zone min and max values into comparable format for the zone pruning step during shared scan
def _to_comparable(value):
    if isinstance(value, (int, float)):
        return value

    text = str(value).strip()
    if text == "":
        return text

    try:
        if "." in text:
            return float(text)
        return int(text)
    except ValueError:
        return text

# Sorted the data based on Year and Month
# resale_data has the lowest year and month first (15 Jan) and the highest year and month last (25 Dec)
def sort_data_by_year_month(resale_data):
    start_time = time.time()
    sorted_data = resale_data.sort_values(by=['Year', 'MonthNum'])
    end_time = time.time()
    print("CSV data is sorted in",
          round((end_time-start_time),2 ),
          "seconds")

    return sorted_data


# Not used in final implementation, number of zones created was too large (11987)
# Conduct zone mapping on the column store MonthNum file
# Map into {'zone': , 'start': , 'end': } format, every 12 zones is a year
# Size of each zone is not fixed
def dynamic_zone_mapping():
    start_time = time.time()
    result = ZoneMap.dynamic_zone_mapping_using_month("../ColumnStore/MonthNum.columnStore")

    end_time = time.time()
    print("Dynamic Zone Mapping finished in",
          round((end_time-start_time),2 ),
          "seconds")

    print(result)
    return result


# Conduct zone mapping on the column store file
# Map into {'zone': ,'start': ,'end': ,'min': ,'max': } format,
# with size of each zone = ZONE_SIZE
def zone_mapping(ZONE_SIZE):
    start_time = time.time()

    zone_map = {}
    columns = ['month_num', 'floor_area', 'resale_price', 'town', 'year']

    for col in columns:
        zone_map[col] = ZoneMap.zone_map(COLUMN_STORE_FILES[col], ZONE_SIZE)

    end_time = time.time()
    print("Zone Mapping finished in",
          round((end_time-start_time),2 ),
          "seconds")

    print(zone_map)
    return zone_map


# Read a block of data from the column store file based on the provided start and end indices
def read_zone(file_path, start, end):
    if start < 0 or end < start:
        raise ValueError("Invalid slice bounds: start must be >= 0 and end must be >= start")

    with open(file_path, 'r') as f:
        lines = f.read().split('\n')[1:]  # skip header

    values = [line for line in lines if line != ""]
    data_start = max(start - 1, 0)
    data_end = end - 1

    if data_start >= len(values) or data_end < data_start:
        return []

    safe_end = min(data_end, len(values) - 1)
    return values[data_start:safe_end + 1]


# Global cache to store the data read from the column store files for each zone,
# to minimize the number of reads needed when there are overlapping zones in the shared scan step
ZONE_CACHE = {}


# Helper function to get the column data for a specific zone, with caching to minimize reads for overlapping zones in the shared scan step
def get_zone_data(column, start, end):
    key = (column, start, end)

    if key in ZONE_CACHE:
        return ZONE_CACHE[key]

    data = read_zone(COLUMN_STORE_FILES[column], start, end)
    ZONE_CACHE[key] = data
    return data

def clear_zone_cache():
    ZONE_CACHE.clear()

# Helper function used to fetch the remaining columns data not used in shared scanning for the final output,
# based on indices and zone mapping, to minimize the number of reads needed to fetch the additional columns data for the final output
def fetch_additional_columns_by_zone(zonemaps, indices, columns):
    zone_groups = {}

    for idx in indices:
        for z, zone in enumerate(zonemaps['month_num']):
            if zone['start'] < idx <= zone['end']:
                zone_groups.setdefault(z, []).append(idx)
                break

    results = {}

    for z, idx_list in zone_groups.items():
        start = zonemaps['month_num'][z]['start']
        end = zonemaps['month_num'][z]['end']

        for col in columns:
            col_data = get_zone_data(col, start, end)

            for idx in idx_list:
                local_i = idx - start - 1
                results.setdefault(idx, {})[col] = col_data[local_i]

    return results



# Shared scan to find the minimum price per (x,y) pair with the assignment filter requirements
def shared_scan_min_pairs_with_cache(zonemaps, commencing_month, target_year, desired_x, desired_y, valid_towns):

    main_start_time = time.time()

    best_valid_pairs = {}
    month_range = int(desired_x)
    if month_range < 1 or month_range > 8:
        print("Invalid x:", month_range)
        return []

    month_year_window = []
    for offset in range(month_range):
        month_in_window = (commencing_month + offset - 1) % 12 + 1
        year_for_month = target_year + (commencing_month + offset - 1) // 12
        month_year_window.append((month_in_window, year_for_month))

    encoded_towns = {str(t).strip().upper() for t in valid_towns}
    max_month = commencing_month + month_range - 1
    num_zones = len(zonemaps['month_num'])

    # Zone pruning on multiple zonemaps: month_num, year, floor_area, town, resale_price
    # Only keeps the zones that are potentially valid based on the filter requirements across all columns,
    # to minimize the number of zones to scan in the shared scan step
    valid_zones = [] # Valid zones after pruning

    start_time = time.time()
    for z in range(num_zones):
        m = zonemaps['month_num'][z]
        y_zone = zonemaps['year'][z]
        a = zonemaps['floor_area'][z]
        t = zonemaps['town'][z]

        m_min, m_max = _to_comparable(m['min']), _to_comparable(m['max'])
        y_min, y_max = _to_comparable(y_zone['min']), _to_comparable(y_zone['max'])
        a_min, a_max = _to_comparable(a['min']), _to_comparable(a['max'])
        t_min = str(t['min']).strip().upper()
        t_max = str(t['max']).strip().upper()

        if m_max < commencing_month or m_min > max_month:
            continue
        if target_year < y_min or target_year > y_max:
            continue
        if a_max < 80:  # min threshold is 80
            continue
        if not any(t_min <= town <= t_max for town in encoded_towns):
            continue

        valid_zones.append(z)

    end_time = time.time()

    print("Zone pruning finished in",
          round((end_time-start_time),2 ), "seconds")
    print("Valid zones after pruning:", valid_zones)

    start_time = time.time()

    # Shared scan on the valid zones after pruning, with lazy loading and caching of the column data to minimize reads
    for z in valid_zones:
        start = zonemaps['month_num'][z]['start']
        end = zonemaps['month_num'][z]['end']

        # Store the pruned columns in cache to minimze number of reads
        year_zone = get_zone_data('year', start, end)
        month_zone = get_zone_data('month_num', start, end)
        town_zone = get_zone_data('town', start, end)
        area_zone = None
        price_zone = None

        row_count = min(len(year_zone), len(month_zone), len(town_zone))

        for i in range(row_count):
            year = int(year_zone[i])
            if year != target_year:
                continue
            month = int(month_zone[i])
            if month < commencing_month or month > max_month:
                continue
            town = str(town_zone[i]).strip().upper()
            if town not in encoded_towns:
                continue

            # To minimize the number of reads, we only read the area and price zones when it's necessary based on the previous filters (year, month, town)
            if area_zone is None:
                area_zone = get_zone_data('floor_area', start, end)
            if price_zone is None:
                price_zone = get_zone_data('resale_price', start, end)

            actual_area = float(area_zone[i])
            if actual_area < 80: # minimum y is 80, so all area values below 80 can be skipped
                continue
            price = float(price_zone[i])
            if (price / actual_area) >= 4725: # price per sqm must be less than 4725, so all pairs that exceed this can be skipped
                continue

            month_offset = month - commencing_month + 1

            # Find the indexes for each (x,y) pair, and only keep the minimum price for each pair
            max_y = min(int(actual_area), 150)
            price_per_area = price / actual_area
            global_index = start + i + 1

            for y in range(desired_y, max_y + 1):
                for x in range(month_offset, month_range + 1):
                    key = (x, y)

                    if key not in best_valid_pairs or price_per_area < best_valid_pairs[key]['price_per_area']:
                        best_valid_pairs[key] = {
                            'price_per_area': price_per_area,
                            'index': global_index,
                            'year': year,
                            'month': month,
                            'floor_area': actual_area,
                            'town': town,
                            'zone': z + 1
                        }

    end_time = time.time()
    print("Shared scan finished in",
          round((end_time-start_time),2 ), "seconds")

    start_time = time.time()
    # Sort the valid pairs to ensure y values ascends before the x values
    sorted_best_value_pair = sorted(best_valid_pairs.items(), key=lambda item: (item[0][1], item[0][0]))
    end_time = time.time()
    print("Sorting of best valid pairs finished in",
          round((end_time-start_time),2 ), "seconds")

    start_time = time.time()
    # Extract indices
    indices = [v['index'] for _, v in sorted_best_value_pair]

    # Fetch the data from all other columns that is needed for the assignment output
    extra_data = fetch_additional_columns_by_zone(
        zonemaps,
        indices,
        ['block', 'street', 'flat_type', 'flat_model', 'storey_range', 'lease_year']
    )

    # Merge the extra data with the best valid pairs based on the indices,
    # to form the final result that contains all the required parameters for the assignment output
    final_result = []
    for key, value in sorted_best_value_pair:
        idx = value['index']

        merged = {
            **value,
            **extra_data.get(idx, {})
        }

        final_result.append((key, merged))
    # print(final_result)

    rows = []

    # Format the final results into rows with the required parameters mentioned in the assignment.
    for key, value in final_result:
        row = [
            key,
            value['year'],
            value['month'],
            value['town'],
            value.get('block', ''),
            value.get('floor_area', ''),
            value.get('flat_model', ''),
            value.get('lease_year', ''),
            # round(value['price'] / value.get('floor_area')),
            round(value['price_per_area']),
            # value['price'] # used to check against the Excel file, param not used in assignment requirements
        ]
        rows.append(row)

    end_time = time.time()
    print("Formatting of final results finished in",
          round((end_time-start_time),2 ), "seconds")
    print(rows)

    main_end_time = time.time()
    print("Total execution time for shared scan with cache:", round((main_end_time-main_start_time),2 ), "seconds")
    clear_zone_cache() # Clear the cache after the function execution to free up memory
    return rows


# Main function to run the column store query without compression
# def query_column_store(zonemaps, matric_number, desired_x, desired_y):
#     digits = extract_digits_from_matric(matric_number)
#     year = get_target_year(digits)
#     month = get_month(digits)
#     town = get_towns(digits)
#
#
#     rows = shared_scan_min_pairs_with_cache(zonemaps, month, year, desired_x, desired_y, town)
#     to_csv(matric_number, rows)

# Helper function to extract the numbers from the metric number
def extract_digits_from_matric(matric_number):
    return [int(d) for d in re.findall(r'\d', matric_number)]


# Helper function to derive the target year from the last digit of the matric number
def get_target_year(digits):
    if digits[-1] < 5:
        return 2020 + digits[-1]
    return 2010 + digits[-1]


# Helper function to derive the month from the second last digit of the matric number, with 0 = october
def get_month(digits):
    second_last = digits[-2]

    if second_last == 0:
        return 10
    else:
        return second_last


# Helper function to dervie the list of valid towns based on all digits of the metric number
# ASSIGNMENT_TOWN_LIST is taken directly from the assignment description
def get_towns(digits):
    towns = set()
    for d in digits:
        towns.add(ASSIGNMENT_TOWN_LIST[d])
    return list(towns)


def to_csv(matric_number, rows):
    folder = os.path.join("..", "ScanResults")
    os.makedirs(folder, exist_ok=True)

    filename = os.path.join(folder, f"ScanResult_{matric_number}.csv")

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['(x, y)', 'Year', 'Month', 'Town', 'Block', 'Floor_Area', 'Flat_Model', 'Lease_Commence_Date', 'Price_Per_Square_Meter'])
        for row in rows:
            writer.writerow(row)

# Compression Start
COMPRESSED_FILE_CACHE = {}
COMPRESSED_ZONE_CACHE = {}

NUMERIC_ZONE_COLUMNS = {'month_num', 'floor_area', 'resale_price', 'year', 'lease_year'}

# Helper function to normalize the zone values based on the column type for comparison during zone mapping and shared scan
def normalize_zone_value(column, value):
    text = str(value).strip()
    if text == "":
        return None

    if column in NUMERIC_ZONE_COLUMNS:
        return _to_comparable(text)

    if column == 'town':
        return text.upper()

    return text


# Load and decompress the column store file
def load_compressed_lines(file_path):
    if file_path in COMPRESSED_FILE_CACHE:
        return COMPRESSED_FILE_CACHE[file_path]

    if not os.path.exists(file_path):
        return []

    with open(file_path, 'rb') as f:
        compressed_bytes = f.read()

    decompressed_text = gzip.decompress(compressed_bytes).decode('utf-8')
    lines = decompressed_text.splitlines()
    COMPRESSED_FILE_CACHE[file_path] = lines
    return lines


# Compress the column store files in ../ColumnStore and store the compressed version in ../CompressedColumnStore
def compress_column_store_files():
    source_directory = "../ColumnStore"
    target_directory = "../CompressedColumnStore"

    if not os.path.exists(target_directory):
        os.makedirs(target_directory)

    compressed_files = {}

    for column_name, source_path in COLUMN_STORE_FILES.items():
        source_file = os.path.join(source_directory, os.path.basename(source_path))
        target_file = os.path.join(target_directory, f"{os.path.basename(source_path)}.gz")

        if not os.path.exists(source_file):
            print(f"Skipping compression for '{column_name}' because '{source_file}' does not exist")
            continue

        with open(source_file, 'rb') as f:
            raw_bytes = f.read()

        with open(target_file, 'wb') as f:
            f.write(gzip.compress(raw_bytes))

        compressed_files[column_name] = target_file

    COMPRESSED_FILE_CACHE.clear()
    COMPRESSED_ZONE_CACHE.clear()
    return compressed_files

# Read a block of data from the compressed column store file based on the provided start and end indices
def read_compressed_zone(file_path, start, end):
    if start < 0 or end < start:
        raise ValueError("Invalid slice bounds: start must be >= 0 and end must be >= start")

    lines = load_compressed_lines(file_path)[1:]  # skip header
    values = [line for line in lines if line != ""]
    data_start = max(start - 1, 0)
    data_end = end - 1

    if data_start >= len(values) or data_end < data_start:
        return []

    safe_end = min(data_end, len(values) - 1)
    return values[data_start:safe_end + 1]

# Helper function to get the column data for a specific zone from the compressed column store files,
# with caching to minimize reads for overlapping zones in the shared scan step
def get_compressed_zone_data(column, start, end):
    key = (column, start, end)

    if key in COMPRESSED_ZONE_CACHE:
        return COMPRESSED_ZONE_CACHE[key]

    data = read_compressed_zone(COMPRESSED_COLUMN_STORE_FILES[column], start, end)
    COMPRESSED_ZONE_CACHE[key] = data
    return data


def clear_compressed_zone_cache():
    COMPRESSED_ZONE_CACHE.clear()

# Zone mapping for the compressed column store files
def zone_map_compressed(file_path, zone_size, column_name):
    if not os.path.exists(file_path):
        print("File does not exist")
        return []

    zones = []
    lines = load_compressed_lines(file_path)[1:]  # skip header
    lines = [line for line in lines if line != '']

    total_values = len(lines)

    for zone_number, start in enumerate(range(0, total_values, zone_size)):
        end = min(start + zone_size, total_values)
        zone_values = lines[start:end]

        normalized_values = [
            normalize_zone_value(column_name, value)
            for value in zone_values
        ]
        normalized_values = [value for value in normalized_values if value is not None]

        if not normalized_values:
            continue

        zones.append({
            'zone': zone_number + 1,
            'start': start,
            'end': end - 1,
            'min': min(normalized_values),
            'max': max(normalized_values)
        })

    return zones

# Zone mapping for the compressed column store files
def compressed_zone_mapping(ZONE_SIZE):
    start_time = time.time()

    zone_map = {}
    columns = ['month_num', 'floor_area', 'resale_price', 'town', 'year']

    for col in columns:
        zone_map[col] = zone_map_compressed(COMPRESSED_COLUMN_STORE_FILES[col], ZONE_SIZE, col)

    end_time = time.time()
    print("Compressed Zone Mapping finished in",
          round((end_time-start_time),2 ),
          "seconds")

    print(zone_map)
    return zone_map

# Retrieving the additional columns data for the final output
def fetch_compressed_additional_columns_by_zone(zonemaps, indices, columns):
    zone_groups = {}

    for idx in indices:
        for z, zone in enumerate(zonemaps['month_num']):
            if zone['start'] < idx <= zone['end']:
                zone_groups.setdefault(z, []).append(idx)
                break

    results = {}

    for z, idx_list in zone_groups.items():
        start = zonemaps['month_num'][z]['start']
        end = zonemaps['month_num'][z]['end']

        for col in columns:
            col_data = get_compressed_zone_data(col, start, end)

            for idx in idx_list:
                local_i = idx - start - 1
                results.setdefault(idx, {})[col] = col_data[local_i]

    return results

# Shared scan to find the minimum price per (x,y) pair with the assignment filter requirements for the compressed column store files,
def shared_scan_min_pairs_with_cache_for_compressed_column_store(zonemaps, commencing_month, target_year, desired_x, desired_y, valid_towns):
    main_start_time = time.time()

    best_valid_pairs = {}
    month_range = int(desired_x)
    if month_range < 1 or month_range > 8:
        print("Invalid x:", month_range)
        return []

    month_year_window = []
    for offset in range(month_range):
        month_in_window = (commencing_month + offset - 1) % 12 + 1
        year_for_month = target_year + (commencing_month + offset - 1) // 12
        month_year_window.append((month_in_window, year_for_month))

    encoded_towns = {str(t).strip().upper() for t in valid_towns}
    max_month = commencing_month + month_range - 1
    num_zones = len(zonemaps['month_num'])

    valid_zones = []

    start_time = time.time()
    for z in range(num_zones):
        m = zonemaps['month_num'][z]
        y_zone = zonemaps['year'][z]
        a = zonemaps['floor_area'][z]
        t = zonemaps['town'][z]

        m_min, m_max = _to_comparable(m['min']), _to_comparable(m['max'])
        y_min, y_max = _to_comparable(y_zone['min']), _to_comparable(y_zone['max'])
        a_min, a_max = _to_comparable(a['min']), _to_comparable(a['max'])
        t_min = str(t['min']).strip().upper()
        t_max = str(t['max']).strip().upper()

        if m_max < commencing_month or m_min > max_month:
            continue
        if target_year < y_min or target_year > y_max:
            continue
        if a_max < 80:
            continue
        if not any(t_min <= town <= t_max for town in encoded_towns):
            continue

        valid_zones.append(z)

    end_time = time.time()

    print(f"Zone pruning finished in",
          round((end_time-start_time),2 ), "seconds")
    print(f"Valid zones after pruning:", valid_zones)

    start_time = time.time()

    for z in valid_zones:
        start = zonemaps['month_num'][z]['start']
        end = zonemaps['month_num'][z]['end']

        year_zone = get_compressed_zone_data('year', start, end)
        month_zone = get_compressed_zone_data('month_num', start, end)
        town_zone = get_compressed_zone_data('town', start, end)
        area_zone = None
        price_zone = None

        row_count = min(len(year_zone), len(month_zone), len(town_zone))

        for i in range(row_count):
            year = int(year_zone[i])
            if year != target_year:
                continue

            month = int(month_zone[i])
            if month < commencing_month or month > max_month:
                continue

            town = str(town_zone[i]).strip().upper()
            if town not in encoded_towns:
                continue

            if area_zone is None:
                area_zone = get_compressed_zone_data('floor_area', start, end)
            if price_zone is None:
                price_zone = get_compressed_zone_data('resale_price', start, end)

            actual_area = float(area_zone[i])
            if actual_area < 80:
                continue

            price = float(price_zone[i])
            if (price / actual_area) >= 4725:
                continue

            month_offset = month - commencing_month + 1
            max_y = min(int(actual_area), 150)
            price_per_area = price / actual_area
            global_index = start + i + 1

            for y in range(desired_y, max_y + 1):
                for x in range(month_offset, month_range + 1):
                    key = (x, y)

                    if key not in best_valid_pairs or price_per_area < best_valid_pairs[key]['price_per_area']:
                        best_valid_pairs[key] = {
                            'price_per_area': price_per_area,
                            'index': global_index,
                            'year': year,
                            'month': month,
                            'floor_area': actual_area,
                            'town': town,
                            'zone': z + 1
                        }

    end_time = time.time()
    print(f"Shared scan finished in",
          round((end_time-start_time),2 ), "seconds")

    start_time = time.time()
    sorted_best_value_pair = sorted(best_valid_pairs.items(), key=lambda item: (item[0][1], item[0][0]))
    end_time = time.time()
    print(f"Sorting of best valid pairs finished in",
          round((end_time-start_time),2 ), "seconds")

    start_time = time.time()
    indices = [v['index'] for _, v in sorted_best_value_pair]

    extra_data = fetch_compressed_additional_columns_by_zone(
        zonemaps,
        indices,
        ['block', 'street', 'flat_type', 'flat_model', 'storey_range', 'lease_year']
    )

    final_result = []
    for key, value in sorted_best_value_pair:
        idx = value['index']
        merged = {
            **value,
            **extra_data.get(idx, {})
        }
        final_result.append((key, merged))

    rows = []
    for key, value in final_result:
        row = [
            key,
            value['year'],
            value['month'],
            value['town'],
            value.get('block', ''),
            value.get('floor_area', ''),
            value.get('flat_model', ''),
            value.get('lease_year', ''),
            round(value['price_per_area']),
        ]
        rows.append(row)

    end_time = time.time()
    print(f"Formatting of final results finished in",
          round((end_time-start_time),2 ), "seconds")
    print(rows)

    main_end_time = time.time()
    print(f"Total execution time for:", round((main_end_time-main_start_time),2 ), "seconds")
    clear_compressed_zone_cache()
    return rows


# Main function to run the column store query
def query_column_store(zonemaps, matric_number, desired_x, desired_y, compression):
    digits = extract_digits_from_matric(matric_number)
    year = get_target_year(digits)
    month = get_month(digits)
    town = get_towns(digits)

    if compression:
        rows = shared_scan_min_pairs_with_cache_for_compressed_column_store(zonemaps, month, year, desired_x, desired_y, town)
    else:
        rows = shared_scan_min_pairs_with_cache(zonemaps, month, year, desired_x, desired_y, town)
    to_csv(matric_number, rows)
