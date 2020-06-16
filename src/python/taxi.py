"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 This example is based on the blog post of Uwe Korn https://uwekorn.com/2019/10/19/taking-duckdb-for-a-spin.html

"""
import monetdbe
import time
import sys

conn = monetdbe.connect(':memory:')
if not conn:
    print('Could not access the memory')
    exit -1

def createdb():
    clk = time.time()
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE yellow_tripdata_2016_01 (   
        VendorID bigint,
        tpep_pickup_datetime timestamp,
        tpep_dropoff_datetime timestamp,
        passenger_count bigint,
        trip_distance double,
        pickup_longitude double,
        pickup_latitude double,
        RatecodeID bigint,
        store_and_fwd_flag string,
        dropoff_longitude double,
        dropoff_latitude double,
        payment_type bigint,
        fare_amount double,
        extra double,
        mta_tax double,
        tip_amount double,
        tolls_amount double,
        improvement_surcharge double,
        total_amount double
    ) 
    """)
    cursor.close()
    print("Create database %6.3f milliseconds"  %(time.time() - clk))

def loaddb(dir):
    clk = time.time()
    cursor = conn.cursor()
    cursor.execute("""
    COPY OFFSET 2 INTO yellow_tripdata_2016_01 FROM '%s/data/yellow_tripdata_2016-01.csv' delimiters ',','\n'  best effort
    """ % dir)
    cursor.execute("SELECT count(*) FROM yellow_tripdata_2016_01")
    print(cursor.fetchone())
    cursor.close()
    print("Load database %6.3f milliseconds"  %(time.time() - clk))

def distinct():
    clk = time.time()
    cursor = conn.cursor()
    cursor.execute("""
    SELECT
        COUNT(DISTINCT VendorID),
        COUNT(DISTINCT passenger_count),
        COUNT(DISTINCT trip_distance),
        COUNT(DISTINCT RatecodeID),
        COUNT(DISTINCT store_and_fwd_flag),
        COUNT(DISTINCT payment_type),
        COUNT(DISTINCT fare_amount),
        COUNT(DISTINCT extra),
        COUNT(DISTINCT mta_tax),
        COUNT(DISTINCT tip_amount),
        COUNT(DISTINCT tolls_amount),
        COUNT(DISTINCT improvement_surcharge),
        COUNT(DISTINCT total_amount)
    FROM yellow_tripdata_2016_01
    """)
    # columns discard
    # COUNT(DISTINCT tpep_pickup_datetime),
    # COUNT(DISTINCT tpep_dropoff_datetime),
    # COUNT(DISTINCT pickup_longitude),
    # COUNT(DISTINCT pickup_latitude),
    # COUNT(DISTINCT dropoff_longitude),
    # COUNT(DISTINCT dropoff_latitude),
    
    print(cursor.fetchdf())
    cursor.close()
    print("Distinct %6.3f milliseconds"  %(time.time() - clk))

def frequency():
    clk = time.time()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            MIN(cnt),
            AVG(cnt),
            -- MEDIAN(cnt),
            MAX(cnt)
        FROM
        (
            SELECT 
                COUNT(*) as cnt
            FROM yellow_tripdata_2016_01
            GROUP BY  
                EXTRACT(DOY FROM tpep_pickup_datetime),
                EXTRACT(HOUR FROM tpep_pickup_datetime)
        ) stats
    """)
    print(cursor.fetchdf())
    cursor.close()
    print("Frequency %6.3f milliseconds"  %(time.time() - clk))
    
def regression():
    clk = time.time()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            (SUM(trip_distance * fare_amount) - SUM(trip_distance) * SUM(fare_amount) / COUNT(*)) /
            (SUM(trip_distance * trip_distance) - SUM(trip_distance) * SUM(trip_distance) / COUNT(*)) AS beta,
            AVG(fare_amount) AS avg_fare_amount,
            AVG(trip_distance) AS avg_trip_distance
        FROM 
            yellow_tripdata_2016_01,
            (
                SELECT 
                    AVG(fare_amount) + 3 * STDDEV_SAMP(fare_amount) as max_fare,
                    AVG(trip_distance) + 3 * STDDEV_SAMP(trip_distance) as max_distance
                FROM yellow_tripdata_2016_01
            ) AS sub
        WHERE 
            fare_amount > 0 AND
            fare_amount < sub.max_fare AND 
            trip_distance > 0 AND
            trip_distance < sub.max_distance
        """)
    beta, avg_fare_amount, avg_trip_distance = cursor.fetchone()
    alpha = avg_fare_amount - beta * avg_trip_distance
    print(alpha)
    cursor.close()
    print("Regression %6.3f milliseconds"  %(time.time() - clk))

if __name__ == "__main__":
    print(sys.argv[1])
    createdb()
    loaddb(sys.argv[1])
    distinct()
    frequency()
    regression()
    conn.close()
