from scripts.Database.Database import Database
import requests


def extract_state_county_data():
    url = 'https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt'
    response = requests.get(url)
    data = response.text

    # Find the index of the "county-level place" header
    start_header = " ------------    --------------"
    start_index = data.find(start_header)

    # Adjust the start index to the line after the header
    start_index = data.find('\n', start_index) + 1

    # Remove all text before the header
    data = data[start_index:]

    # Extract state and county data
    state_data = []
    county_data = []

    suffixes_to_remove = [' Borough', ' Census Area', ' County', ' Parish', ' city']

    lines = data.splitlines()
    for line in lines:
        line = line.strip()
        parts = line.split('        ')
        state_code = int(parts[0][:2])
        county_code = int(parts[0][2:])
        name = parts[1]

        if county_code == 0:
            state_data.append((state_code, name))
        else:
            for suffix in suffixes_to_remove:
                if name.endswith(suffix):
                    name = name[:-len(suffix)].strip()
                    break

            county_data.append((state_code, county_code, name))

    county_data.append((12, 86, "Miami-Dade"))
    county_data.append((23, 901, "Farmville"))

    return state_data, county_data


def create_tables():
    db = Database()
    db.connect()

    db.create_table("states", {
            "state_code": "VARCHAR(2)",
            "state_name": "VARCHAR(255)"
        }, primary_keys=["state_code"])

    db.create_table("counties", {
            "state_code": "VARCHAR(2)",
            "county_code": "VARCHAR(3)",
            "county_name": "VARCHAR(255)"
        }, primary_keys=["state_code", "county_code"], foreign_keys=[
            ("fk_state_code", "states", ["state_code"], ["state_code"])
        ])

    db.disconnect()
    print("Table 'states' and 'counties' created successfully")


def fill_tables():
    db = Database()
    db.connect()

    states_data, counties_data = extract_state_county_data()

    db.bulk_insert("states", ["state_code", "state_name"], states_data, ["state_code"])

    db.bulk_insert("counties", ["state_code", "county_code", "county_name"], counties_data,
                   ["state_code", "county_code"])

    print("Table 'states' and 'counties' filled")


if __name__ == '__main__':
    create_tables()
    fill_tables()
