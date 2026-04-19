columns_to_find_unique_constants = ["Town", "FlatType", "FlatModel", "StoreyRange"]

# Helper function to find out all unique constants in the columns Town, FlatType, FlatModel, StoreyRange
def search_for_unique_constants(dataframe):

    for col in columns_to_find_unique_constants:
        unique_values = dataframe[col].unique().tolist()
        print(unique_values)


# File paths for the column stored files
COLUMN_STORE_FILES = {
    'month': '../ColumnStore/month.columnStore',
    'month_num': '../ColumnStore/monthnum.columnStore',
    'town': '../ColumnStore/town.columnStore',
    'block': '../ColumnStore/block.columnStore',
    'street': '../ColumnStore/street.columnStore',
    'flat_type': '../ColumnStore/flattype.columnStore',
    'flat_model': '../ColumnStore/flatmodel.columnStore',
    'storey_range': '../ColumnStore/storeyrange.columnStore',
    'floor_area': '../ColumnStore/floorarea.columnStore',
    'lease_year': '../ColumnStore/leaseyear.columnStore',
    'resale_price': '../ColumnStore/resaleprice.columnStore',
    'year': '../ColumnStore/year.columnStore'
}


# File paths for the compressed column stored files
COMPRESSED_COLUMN_STORE_FILES = {
    'month': '../CompressedColumnStore/month.columnStore.gz',
    'month_num': '../CompressedColumnStore/monthnum.columnStore.gz',
    'town': '../CompressedColumnStore/town.columnStore.gz',
    'block': '../CompressedColumnStore/block.columnStore.gz',
    'street': '../CompressedColumnStore/street.columnStore.gz',
    'flat_type': '../CompressedColumnStore/flattype.columnStore.gz',
    'flat_model': '../CompressedColumnStore/flatmodel.columnStore.gz',
    'storey_range': '../CompressedColumnStore/storeyrange.columnStore.gz',
    'floor_area': '../CompressedColumnStore/floorarea.columnStore.gz',
    'lease_year': '../CompressedColumnStore/leaseyear.columnStore.gz',
    'resale_price': '../CompressedColumnStore/resaleprice.columnStore.gz',
    'year': '../CompressedColumnStore/year.columnStore.gz'
}


# Taken from the Assignment Description Table 1
ASSIGNMENT_TOWN_LIST = {
    0: "BEDOK",
    1: "BUKIT PANJANG",
    2: "CLEMENTI",
    3: "CHOA CHU KANG",
    4: "HOUGANG",
    5: "JURONG WEST",
    6: "PASIR RIS",
    7: "TAMPINES",
    8: "WOODLANDS",
    9: "YISHUN"
}

TOWN_LIST = [
    'ANG MO KIO',
    'BEDOK',
    'BISHAN',
    'BUKIT BATOK',
    'BUKIT MERAH',
    'BUKIT PANJANG',
    'BUKIT TIMAH',
    'CENTRAL AREA',
    'CHOA CHU KANG',
    'CLEMENTI',
    'GEYLANG',
    'HOUGANG',
    'JURONG EAST',
    'JURONG WEST',
    'KALLANG/WHAMPOA',
    'MARINE PARADE',
    'PASIR RIS',
    'PUNGGOL',
    'QUEENSTOWN',
    'SEMBAWANG',
    'SENGKANG',
    'SERANGOON',
    'TAMPINES',
    'TOA PAYOH',
    'WOODLANDS',
    'YISHUN'
 ]

FLAT_TYPE_LIST = [
    '1 ROOM',
    '2 ROOM',
    '3 ROOM',
    '4 ROOM',
    '5 ROOM',
    'EXECUTIVE',
    'MULTI-GENERATION'
]

FLAT_MODEL_LIST = [
    'Improved',
    'New Generation',
    'Model A',
    'Standard',
    'Simplified',
    'Premium Apartment',
    'Maisonette',
    'Apartment',
    'Model A2',
    'Type S1',
    'Type S2',
    'Adjoined flat',
    'Terrace',
    'DBSS',
    'Model A-Maisonette',
    'Premium Maisonette',
    'Multi Generation',
    'Premium Apartment Loft',
    'Improved-Maisonette',
    '2-room',
    '3Gen'
]

STOREY_RANGE_LIST = [
    '01 TO 03',
    '04 TO 06',
    '07 TO 09',
    '10 TO 12',
    '13 TO 15',
    '16 TO 18',
    '19 TO 21',
    '22 TO 24',
    '25 TO 27',
    '28 TO 30',
    '31 TO 33',
    '34 TO 36',
    '37 TO 39',
    '40 TO 42',
    '43 TO 45',
    '46 TO 48',
    '49 TO 51'
]