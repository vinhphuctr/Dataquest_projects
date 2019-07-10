#------------------------------# 
#   EXPLORING THE DATA TYPES   #
#------------------------------#
import pandas as pd
data = pd.read_csv('storm_data.csv')
print(data.head())  #printing the first 5 rows, we can see that there are 14 columns in the data.

# exploring the data-types
print(data.FID.max()) #=> INTEGER because the ID run from 2001 to 59228

# for the YEAR, MONTH, DAY and AD_TIME => transform and merge into one column as type TIMESTAMP
# (which includes both date and time values). Looking closer at the AD_TIME column, each value is a record
# of the time in UTC (Coordinated Universal Time), which contains the hour (first 2 digits) and the minute
# (last 2 digits) of the recorded hurricane's time.

# exploring the BTID and NAME columns
print(data.BTID.max()) #=> SMALLINT because the ID maximum number is 1410
print(max([len(name) for name in data.NAME])) #=> max length = 9 so VARCHAR(9)

# for the LAT and LONG columns, thanks to geographic knowledge, we know that:
# -----the valid range of latitude is -90° and +90°. => NUMERIC(3,1) for the LAT column
# -----the valid range of longitude is -180° and +180° => NUMERIC(4,1) for the LONG column

# exploring the WIND_KTS and PRESSURE columns
print(data.WIND_KTS.unique()) #=> SMALLINT because there are values running from 20 to 165
print(data.PRESSURE.unique()) #=> SMALLINT because there are values running from 0 to 1024

# exploring the CAT and BASIN columns
print(max[len(cat) for cat in data.CAT]) #=> max length = 2 so VARCHAR(2)
print(max[len(basin) for basin in data.BASIN]) #=> max length = 15 so VARCHAR(15)

# The last column, Shape_Leng, seems to keep the same precision of 1 digit before the decimal and 6 digits after.
# Using DECIMAL, we can specify a max scale and precision that will ensure the data stays within the ranges.


#--------------------------------------------------------------#
#       CREATING THE IHW DATABASE, THE USERS AND A TABLE       #
#--------------------------------------------------------------#
import psycopg2
conn = psycopg2.connect("dbname=postgres user=postgres")
cur = conn.cursor()

# creating the new database, called "ihw"
cur.execute("CREATE DATABASE ihw")

# creating the users
cur.execute("CREATE USER prod_1 WITH PASSWORD 'meproductor123'")
cur.execute("CREATE USER analy_1 WITH PASSWORD 'meanalyst123'")

# reconnecting to iwh database
conn = psycopg2.connect("dbname=iwh user=postgres")
cur = conn.cursor()

# creating a new table, called "hurricane"
query = """
        CREATE TABLE IF NOT EXISTS hurricane(
        fid INTEGER PRIMARY KEY,
        date_time TIMESTAMP,
        btid SMALLINT,
        name VARCHAR(9),
        lat NUMERIC(3,1),
        long NUMERIC(4,1),
        wind_kts SMALLINT,
        pressure SMALLINT,
        categ VARCHAR(2),
        basin VARCHAR(15),
        shape_leng DECIMAL
        );
        """
cur.execute(query)
conn.commit()

# managing privileges
cur.execute("""
            REVOKE ALL ON hurricane FROM prod_1;
            GRANT SELECT, INSERT, UPDATE ON hurricane TO prod_1;
            """;
            )

cur.execute("""
            REVOKE ALL ON hurricane FROM analy_1;
            GRANT SELECT ON hurricane TO analy_1;
            """;
            )

conn.commit()
conn.close()


#----------------------------------#
#  FINAL STEP: INSERTING THE DATA  #
#----------------------------------#
# reconnect with production user
conn = psycopg2.connect("dbname=ihw user=prod_1 password=meproductor123")
cur = conn.cursor()
conn.autocommit = True

import csv
from datetime import datetime

with open("storm_data.csv", "w+") as f:
    reader = csv.reader(f)
    next(reader)  #skip the header
    rows = list()
    for line in reader:
        time_recorded = datetime(int(line[1]), int(line[2]), int(line[3]), hour=int(line[4][:2]), minute=int(line[4][2:-1]))
        update_line = [line[0], time_recorded] + line[5:]
        rows.append(
            cur.mogrify(
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", update_line).decode('utf-8')
        )
cur.execute("INSERT INTO hurricane VALUES" + ",".join(rows))
conn.close