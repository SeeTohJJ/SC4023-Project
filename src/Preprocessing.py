
# DATA STORAGE & INPUT


import csv

def load_and_clean_data(input_file, output_file):

    
    # Initializing column storage (COLUMN-ORIENTED)
    
    years = []
    months = []
    month_nums = []

    towns = []
    blocks = []
    street_names = []

    flat_types = []
    flat_models = []

    storey_ranges = []
    floor_areas = []

    lease_years = []
    resale_prices = []

    
    # Opening and reading CSV file
    
    with open(input_file, "r", encoding="utf-8") as file:
        reader = csv.reader(file)

        header = next(reader)  # skip header
        print("Header:", header)

        total_rows = 0

        
        # Processing each row
        
        for parts in reader:
            total_rows += 1

            if len(parts) < 10:
                continue

            try:

                print(parts)  # DEBUG

                month = parts[0]
                town = parts[1]
                flat_type = parts[2]
                block = parts[3]
                street = parts[4]
                storey = parts[5]
                area = float(parts[6])          
                flat_model = parts[7]
                lease = int(parts[8])
                price = float(parts[9])

                # Extracting year and month number
                
                month_str = parts[0]

                month_map = {
                    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
                    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
                    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

                m, y = month_str.split("-")
                month_num = month_map[m]
                year = 2000 + int(y)
                

                # Storing in column format
                years.append(year)
                months.append(month)
                month_nums.append(month_num)

                towns.append(town)
                blocks.append(block)
                street_names.append(street)

                flat_types.append(flat_type)
                flat_models.append(flat_model)

                storey_ranges.append(storey)
                floor_areas.append(area)

                lease_years.append(lease)
                resale_prices.append(price)

            except Exception as e:
                print("ERROR:", e)
                print("ROW:", parts)
                break
                
                continue

    print("Total rows read:", total_rows)
    print("Total valid rows processed:", len(years))

    
    # Writing cleaned data to new CSV
    
    with open(output_file, "w", encoding="utf-8", newline="") as f:

        writer = csv.writer(f)

        # Writing header
        writer.writerow([
            "Year", "Month", "MonthNum", "Town", "Block",
            "Street", "FlatType", "FlatModel", "StoreyRange",
            "FloorArea", "LeaseYear", "ResalePrice"
        ])

        # Writing rows
        for i in range(len(years)):
            writer.writerow([
                years[i], months[i], month_nums[i],
                towns[i], blocks[i], street_names[i],
                flat_types[i], flat_models[i], storey_ranges[i],
                floor_areas[i], lease_years[i], resale_prices[i]
            ])

    print("Cleaned data written to:", output_file)



# Running the program

if __name__ == "__main__":
    input_file = "ResalePricesSingapore.csv"
    output_file = "cleaned_data.csv"

    load_and_clean_data(input_file, output_file)