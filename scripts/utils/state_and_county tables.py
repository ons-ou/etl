import psycopg2

# This only needs to be run once to initialize tables
from scripts.utils.Database import Database


def create_tables(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
            DROP TABLE IF EXISTS counties CASCADE;
            DROP TABLE IF EXISTS states CASCADE;
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS states (
                state_code VARCHAR(2) PRIMARY KEY,
                state_name VARCHAR(20) NOT NULL
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS counties (
                state_code VARCHAR(2),
                county_code VARCHAR(3),
                county_name VARCHAR(255) NOT NULL,
                PRIMARY KEY (state_code, county_code),
                CONSTRAINT fk_state_code FOREIGN KEY (state_code) REFERENCES states(state_code)
            )
        """)

        connection.commit()
        print("Table 'states' and 'counties' created successfully")
    except psycopg2.Error as e:
        print("Error creating table:", e)
        connection.rollback()


def fill_tables(connection):
    try:
        cursor = connection.cursor()
        states_data = [
            (1, "Alabama"), (2, "Alaska"), (4, "Arizona"), (5, "Arkansas"), (6, "California"),
            (8, "Colorado"), (9, "Connecticut"), (10, "Delaware"), (11, "District Of Columbia"), (12, "Florida"),
            (13, "Georgia"), (15, "Hawaii"), (16, "Idaho"), (17, "Illinois"), (18, "Indiana"),
            (19, "Iowa"), (20, "Kansas"), (21, "Kentucky"), (22, "Louisiana"), (23, "Maine"),
            (24, "Maryland"), (25, "Massachusetts"), (26, "Michigan"), (27, "Minnesota"), (28, "Mississippi"),
            (29, "Missouri"), (30, "Montana"), (31, "Nebraska"), (32, "Nevada"), (33, "New Hampshire"),
            (34, "New Jersey"), (35, "New Mexico"), (36, "New York"), (37, "North Carolina"), (38, "North Dakota"),
            (39, "Ohio"), (40, "Oklahoma"), (41, "Oregon"), (42, "Pennsylvania"), (44, "Rhode Island"),
            (45, "South Carolina"), (46, "South Dakota"), (47, "Tennessee"), (48, "Texas"), (49, "Utah"),
            (50, "Vermont"), (51, "Virginia"), (53, "Washington"), (54, "West Virginia"), (55, "Wisconsin"),
            (56, "Wyoming")
        ]
        cursor.executemany("INSERT INTO states (state_code, state_name) VALUES (%s, %s)", states_data)

        county_data = [(6, 1, 'Alameda'), (35, 1, 'Bernalillo'), (34, 1, 'Atlantic'), (42, 1, 'Adams'), (40, 1, 'Adair'), (39, 1, 'Adams'), (16, 1, 'Ada'), (36, 1, 'Albany'), (23, 1, 'Androscoggin'), (24, 1, 'Allegany'), (8, 1, 'Adams'), (9, 1, 'Fairfield'), (11, 1, 'District of Columbia'), (32, 3, 'Clark'), (27, 3, 'Anoka'), (15, 3, 'Honolulu'), (34, 3, 'Bergen'), (4, 3, 'Cochise'), (9, 3, 'Hartford'), (23, 3, 'Aroostook'), (10, 3, 'New Castle'), (18, 3, 'Allen'), (42, 3, 'Allegheny'), (9, 5, 'Litchfield'), (36, 5, 'Bronx'), (33, 5, 'Cheshire'), (8, 5, 'Arapahoe'), (41, 5, 'Clackamas'), (23, 5, 'Cumberland'), (29, 5, 'Atchison'), (24, 5, 'Baltimore'), (4, 5, 'Coconino'), (32, 5, 'Douglas'), (49, 5, 'Cache'), (6, 5, 'Amador'), (34, 5, 'Burlington'),  (23, 7, 'Franklin'), (36, 7, 'Broome'), (50, 7, 'Chittenden'), (33, 7, 'Coos'), (44, 7, 'Providence'), (42, 7, 'Beaver'), (15, 7, 'Kauai'), (34, 7, 'Camden'), (6, 7, 'Butte'), (23, 9, 'Hancock'), (54, 9, 'Brooke'), (56, 9, 'Converse'), (25, 9, 'Essex'), (9, 9, 'New Haven'), (6, 9, 'Calaveras'), (39, 9, 'Athens'), (53, 9, 'Clallam'), (47, 9, 'Blount'), (53, 11, 'Clark'), (34, 11, 'Cumberland'), (12, 11, 'Broward'), (33, 11, 'Hillsborough'), (49, 11, 'Davis'), (42, 11, 'Berks'), (4, 12, 'La Paz'), (8, 13, 'Boulder'), (56, 13, 'Fremont'), (39, 13, 'Belmont'), (42, 13, 'Blair'), (35, 13, 'Dona Ana'), (4, 13, 'Maricopa'), (34, 13, 'Essex'), (25, 13, 'Hampden'), (49, 13, 'Duchesne'), (21, 13, 'Bell'), (51, 13, 'Arlington'), (6, 13, 'Contra Costa'), (30, 13, 'Cascade'), (38, 15, 'Burleigh'), (36, 15, 'Chemung'), (33, 15, 'Rockingham'), (34, 15, 'Gloucester'), (25, 17, 'Middlesex'), (38, 17, 'Cass'), (34, 17, 'Hudson'), (27, 17, 'Carlton'), (6, 17, 'El Dorado'), (39, 17, 'Butler'), (42, 17, 'Bucks'), (41, 17, 'Deschutes'), (24, 19, 'Dorchester'), (21, 19, 'Boyd'), (4, 19, 'Pima'), (49, 19, 'Grand'), (6, 19, 'Fresno'), (28, 19, 'Choctaw'), (23, 19, 'Penobscot'), (17, 19, 'Champaign'), (45, 19, 'Charleston'), (2, 20, 'Anchorage '), (25, 21, 'Norfolk'), (40, 21, 'Cherokee'), (16, 21, 'Boundary'), (50, 21, 'Rutland'), (56, 21, 'Laramie'), (34, 21, 'Mercer'), (42, 21, 'Cambria'), (24, 23, 'Garrett'), (6, 23, 'Humboldt'), (34, 23, 'Middlesex'), (55, 25, 'Dane'), (25, 25, 'Suffolk'), (4, 25, 'Yavapai'), (6, 25, 'Imperial'), (34, 25, 'Monmouth'), (54, 25, 'Greenbrier'), (16, 27, 'Canyon'), (55, 27, 'Dodge'), (4, 27, 'Yuma'), (34, 27, 'Morris'), (40, 27, 'Cleveland'), (6, 27, 'Inyo'), (25, 27, 'Worcester'), (36, 27, 'Dutchess'), (24, 27, 'Howard'), (54, 29, 'Hancock'), (6, 29, 'Kern'), (41, 29, 'Jackson'), (30, 29, 'Flathead'), (36, 29, 'Erie'), (34, 29, 'Ocean'), (48, 29, 'Bexar'), (8, 31, 'Denver'), (34, 31, 'Passaic'), (40, 31, 'Comanche'), (17, 31, 'Cook'), (24, 31, 'Montgomery'), (32, 31, 'Washoe'), (30, 31, 'Gallatin'), (12, 31, 'Duval'), (41, 33, 'Josephine'), (18, 33, 'DeKalb'), (24, 33, "Prince George's"), (53, 33, 'King'), (34, 33, 'Salem'), (22, 33, 'East Baton Rouge'), (37, 33, 'Caswell'), (12, 33, 'Escambia'), (6, 33, 'Lake'), (34, 35, 'Somerset'), (49, 35, 'Salt Lake'), (41, 35, 'Klamath'), (39, 35, 'Cuyahoga'), (53, 35, 'Kitsap'), (38, 35, 'Grand Forks'), (47, 37, 'Davidson'), (56, 37, 'Sweetwater'), (6, 37, 'Los Angeles'), (27, 37, 'Dakota'), (8, 37, 'Eagle'), (37, 37, 'Chatham'), (56, 39, 'Teton'), (41, 39, 'Lane'), (6, 39, 'Madera'), (34, 39, 'Union'), (54, 39, 'Kanawha'), (6, 41, 'Marin'), (34, 41, 'Warren'), (31, 41, 'Custer'), (8, 41, 'El Paso'), (56, 41, 'Uinta'), (6, 43, 'Mariposa'), (35, 43, 'Sandoval'), (24, 43, 'Washington'), (17, 43, 'DuPage'), (31, 43, 'Dakota'), (42, 43, 'Dauphin'), (6, 45, 'Mendocino'), (45, 45, 'Greenville'), (42, 45, 'Delaware'), (35, 45, 'San Juan'), (36, 47, 'Kings'), (6, 47, 'Merced'), (49, 47, 'Uintah'), (41, 47, 'Marion'), (29, 47, 'Clay'), (42, 49, 'Erie'), (26, 49, 'Genesee'), (49, 49, 'Utah'), (28, 49, 'Hinds'), (39, 49, 'Franklin'), (30, 49, 'Lewis and Clark'), (35, 49, 'Santa Fe'), (13, 51, 'Chatham'), (6, 51, 'Mono'), (37, 51, 'Cumberland'), (22, 51, 'Jefferson'), (41, 51, 'Multnomah'), (6, 53, 'Monterey'), (27, 53, 'Hennepin'), (53, 53, 'Pierce'), (49, 53, 'Washington'), (36, 55, 'Monroe'), (31, 55, 'Douglas'), (6, 55, 'Napa'), (39, 57, 'Greene'), (26, 57, 'Gratiot'), (12, 57, 'Hillsborough'), (20, 57, 'Ford'), (8, 57, 'Jackson'), (6, 57, 'Nevada'), (49, 57, 'Weber'), (42, 59, 'Greene'), (51, 59, 'Fairfax'), (55, 59, 'Kenosha'), (36, 59, 'Nassau'), (21, 59, 'Daviess'), (8, 59, 'Jefferson'), (6, 59, 'Orange'), (48, 61, 'Cameron'), (21, 61, 'Edmonson'), (6, 61, 'Placer'), (36, 61, 'New York'), (19, 61, 'Dubuque'), (53, 61, 'Snohomish'), (39, 61, 'Hamilton'), (26, 63, 'Huron'), (18, 63, 'Hendricks'), (37, 63, 'Durham'), (53, 63, 'Spokane'), (45, 63, 'Lexington'), (30, 63, 'Missoula'), (6, 63, 'Plumas'), (36, 63, 'Niagara'), (36, 65, 'Oneida'), (6, 65, 'Riverside'), (53, 67, 'Thurston'), (8, 67, 'La Plata'), (36, 67, 'Onondaga'), (39, 67, 'Harrison'), (41, 67, 'Washington'), (6, 67, 'Sacramento'), (37, 67, 'Forsyth'), (21, 67, 'Fayette'), (16, 69, 'Nez Perce'), (8, 69, 'Larimer'), (37, 69, 'Franklin'), (42, 69, 'Lackawanna'), (54, 69, 'Ohio'), (42, 71, 'Lancaster'), (6, 71, 'San Bernardino'), (37, 71, 'Gaston'), (40, 71, 'Kay'), (22, 71, 'Orleans'), (53, 73, 'Whatcom'), (45, 73, 'Oconee'), (6, 73, 'San Diego'), (42, 73, 'Lawrence'), (1, 73, 'Jefferson'), (6, 75, 'San Francisco'), (47, 75, 'Haywood'), (53, 77, 'Yakima'), (42, 77, 'Lehigh'), (6, 77, 'San Joaquin'), (37, 77, 'Granville'), (26, 77, 'Kalamazoo'), (8, 77, 'Mesa'), (29, 77, 'Greene'), (42, 79, 'Luzerne'), (45, 79, 'Richland'), (6, 79, 'San Luis Obispo'), (55, 79, 'Milwaukee'), (26, 81, 'Kent'), (37, 81, 'Guilford'), (6, 81, 'San Mateo'), (36, 81, 'Queens'), (39, 81, 'Jefferson'), (6, 83, 'Santa Barbara'), (36, 83, 'Rensselaer'), (39, 85, 'Lake'), (6, 85, 'Santa Clara'), (12, 86, 'Miami-Dade'), (6, 87, 'Santa Cruz'), (51, 87, 'Henrico'), (1, 89, 'Madison'), (6, 89, 'Shasta'), (18, 89, 'Lake'), (42, 89, 'Monroe'), (13, 89, 'DeKalb'), (2, 90, 'Fairbanks North Star '), (20, 91, 'Johnson'), (45, 91, 'York'), (42, 91, 'Montgomery'), (21, 91, 'Hancock'), (5, 93, 'Mississippi'), (6, 93, 'Siskiyou'), (30, 93, 'Silver Bow'), (36, 93, 'Schenectady'), (47, 93, 'Knox'), (12, 95, 'Orange'), (29, 95, 'Jackson'), (6, 95, 'Solano'), (55, 95, 'Polk'), (39, 95, 'Lucas'), (42, 95, 'Northampton'), (1, 97, 'Mobile'), (18, 97, 'Marion'), (6, 97, 'Sonoma'), (6, 99, 'Stanislaus'), (46, 99, 'Minnehaha'), (12, 99, 'Palm Beach'), (26, 99, 'Macomb'), (39, 99, 'Mahoning'), (54, 99, 'Wayne'), (42, 101, 'Philadelphia'), (55, 101, 'Racine'), (1, 101, 'Montgomery'), (21, 101, 'Henderson'), (36, 101, 'Steuben'), (6, 101, 'Sutter'), (8, 101, 'Pueblo'), (36, 103, 'Suffolk'), (39, 103, 'Medina'), (12, 103, 'Pinellas'), (6, 105, 'Trinity'), (47, 105, 'Loudon'), (20, 107, 'Linn'), (6, 107, 'Tulare'), (18, 107, 'Montgomery'), (42, 107, 'Schuylkill'), (27, 109, 'Olmsted'), (6, 109, 'Tuolumne'), (37, 109, 'Lincoln'), (40, 109, 'Oklahoma'), (31, 109, 'Lancaster'), (55, 109, 'St. Croix'), (18, 109, 'Morgan'), (30, 111, 'Yellowstone'), (6, 111, 'Ventura'), (21, 111, 'Jefferson'), (6, 113, 'Yolo'), (39, 113, 'Montgomery'), (48, 113, 'Dallas'), (26, 113, 'Missaukee'), (19, 113, 'Linn'), (40, 115, 'Ottawa'), (12, 115, 'Sarasota'), (27, 115, 'Pine'), (17, 117, 'Macoupin'), (21, 117, 'Kenton'), (37, 119, 'Mecklenburg'), (17, 119, 'Madison'), (36, 119, 'Westchester'), (5, 119, 'Pulaski'), (26, 121, 'Muskegon'), (47, 121, 'Meigs'), (13, 121, 'Fulton'), (8, 123, 'Weld'), (27, 123, 'Ramsey'), (42, 125, 'Washington'), (47, 125, 'Montgomery'), (26, 125, 'Oakland'), (20, 125, 'Montgomery'), (46, 127, 'Union'), (37, 129, 'New Hanover'), (42, 129, 'Westmoreland'), (12, 129, 'Wakulla'), (18, 129, 'Posey'), (55, 133, 'Waukesha'), (20, 133, 'Neosho'), (26, 133, 'Osceola'), (42, 133, 'York'), (37, 135, 'Orange'), (26, 135, 'Oscoda'), (39, 135, 'Preble'), (48, 135, 'Ector'), (27, 137, 'Saint Louis'), (55, 139, 'Winnebago'), (18, 141, 'St. Joseph'), (27, 141, 'Sherburne'), (55, 141, 'Wood'), (48, 141, 'El Paso'), (40, 143, 'Tulsa'), (17, 143, 'Peoria'), (27, 145, 'Stearns'), (39, 145, 'Scioto'), (21, 145, 'McCracken'), (20, 145, 'Pawnee'), (26, 145, 'Saginaw'), (26, 147, 'St. Clair'), (39, 151, 'Stark'), (19, 153, 'Polk'), (39, 153, 'Summit'), (26, 153, 'Schoolcraft'), (18, 155, 'Switzerland'), (18, 157, 'Tippecanoe'), (47, 157, 'Shelby'), (37, 157, 'Rockingham'), (37, 159, 'Rowan'), (51, 161, 'Roanoke'), (17, 161, 'Rock Island'), (18, 163, 'Vanderburgh'), (26, 163, 'Wayne'), (19, 163, 'Scott'), (17, 163, 'Saint Clair'), (47, 163, 'Sullivan'), (47, 165, 'Sumner'), (29, 165, 'Platte'), (48, 167, 'Galveston'), (18, 167, 'Vigo'), (17, 167, 'Sangamon'), (20, 173, 'Sedgwick'), (20, 181, 'Sherman'), (37, 183, 'Wake'), (2, 185, 'North Slope '), (29, 189, 'Saint Louis'), (20, 191, 'Sumner'), (19, 193, 'Woodbury'), (17, 197, 'Will'), (17, 201, 'Winnebago'), (48, 201, 'Harris'), (20, 209, 'Wyandotte'), (29, 213, 'Taney'), (13, 223, 'Paulding'), (48, 245, 'Jefferson'), (48, 309, 'McLennan'), (48, 355, 'Nueces'), (48, 439, 'Tarrant'), (48, 453, 'Travis'), (48, 479, 'Webb'), (51, 510, 'Alexandria City'), (24, 510, 'Baltimore (City)'), (29, 510, 'St. Louis City'), (32, 510, 'Carson City'), (51, 600, 'Fairfax City'), (51, 650, 'Hampton City'), (51, 700, 'Newport News City'), (51, 710, 'Norfolk City'), (51, 760, 'Richmond City'), (51, 770, 'Roanoke City'), (51, 775, 'Salem City'), (51, 810, 'Virginia Beach City')]
        cursor.executemany("INSERT INTO counties (state_code, county_code, county_name) VALUES (%s, %s, %s)", county_data)
        connection.commit()
        print("Data inserted into 'states' and 'counties' tables successfully")
    except psycopg2.Error as e:
        print("Error inserting data:", e)
        connection.rollback()


if __name__ == '__main__':
    db = Database()
    print(db.dbname)
    db.connect()
    create_tables(db.connection)
    fill_tables(db.connection)