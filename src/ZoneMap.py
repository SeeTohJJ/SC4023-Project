import os

"""
Instead of zone mapping using a fixed zone size,
we have map the start and end index of each zone based on month

eg: 2015 Jan to Dec will be zone 1 to zone 12
    2016 Jan to Dec will be zone 13 to zone 24
    
Not used in the final implementation
"""
def dynamic_zone_mapping_using_month(file_path):
    if not os.path.exists(file_path):
        print("File does not exist")
        return []

    zones = []

    with open(file_path, 'r') as f:
        lines = [line.strip() for line in f][1:]  # skip header

    lines = [line for line in lines if line != '']
    values = [int(v) for v in lines]

    current_start = 0
    zone_number = 1

    for i in range(1, len(values)):
        # when value changes → new zone
        if values[i] != values[i - 1]:
            zone_values = values[current_start:i]

            zones.append({
                'zone': zone_number,
                'start': current_start,
                'end': i - 1,
                'min': min(zone_values),
                'max': max(zone_values)
            })

            zone_number += 1
            current_start = i

    # last zone
    zone_values = values[current_start:]

    zones.append({
        'zone': zone_number,
        'start': current_start,
        'end': len(values) - 1,
        'min': min(zone_values),
        'max': max(zone_values)
    })

    return zones


"""
Zone mapping using a fixed zone size, where each zone will contain ZONE_SIZE number of values
Zone number starts from 1 

Zone map will be in the format of {'zone': ,'start': ,'end': ,'min': ,'max': }
where start and end are the index of the first and last value in the zone, 
and min and max are the minimum and maximum values in the zone
"""
def zone_map(file_path, ZONE_SIZE):
    if not os.path.exists(file_path):
        print("File does not exist")
        return []

    zones = []

    with open(file_path, 'r') as f:
        lines = [line.strip() for line in f][1:]  # skip header

    lines = [line for line in lines if line != '']
    total_values = len(lines)

    for zone_number, start in enumerate(range(0, total_values, ZONE_SIZE)):

        end = min(start + ZONE_SIZE, total_values)
        zone_values = lines[start:end]

        values = [v for v in zone_values]
        values = [v for v in values if v is not None]

        if not values:
            continue

        zones.append({
            'zone': zone_number + 1,  # make the zone index start from 1
            'start': start,
            'end': end - 1,
            'min': min(values),
            'max': max(values)
        })

    return zones