"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

This program implements https://github.com/crottyan/mgbench
"""
import monetdbe
import os, sys
import pathlib
import time

dataprefix = "/ufs/mk/repository/monetdbe-examples/src/python/"

ddl = [
    {'bench':1,  'ddl':"""
        CREATE TABLE logs (
            log_time      TIMESTAMP NOT NULL,
            machine_name  VARCHAR(25) NOT NULL,
            machine_group VARCHAR(15) NOT NULL,
            cpu_idle      FLOAT,
            cpu_nice      FLOAT,
            cpu_system    FLOAT,
            cpu_user      FLOAT,
            cpu_wio       FLOAT,
            disk_free     FLOAT,
            disk_total    FLOAT,
            part_max_used FLOAT,
            load_fifteen  FLOAT,
            load_five     FLOAT,
            load_one      FLOAT,
            mem_buffers   FLOAT,
            mem_cached    FLOAT,
            mem_free      FLOAT,
            mem_shared    FLOAT,
            swap_free     FLOAT,
            bytes_in      FLOAT,
            bytes_out     FLOAT
            );
        """,
        'copy': """COPY 1000000 OFFSET 2 RECORDS INTO logs FROM '%s' USING DELIMITERS ',' NULL AS ''"""
    },
    {'bench': 2, 'ddl': """
CREATE TABLE logs (
  log_time    TIMESTAMP NOT NULL,
  client_ip   VARCHAR(15) NOT NULL,
  request     VARCHAR(1000) NOT NULL,
  status_code SMALLINT NOT NULL,
  object_size BIGINT NOT NULL
);
    """,
    'copy': """COPY 75748119 OFFSET 2 RECORDS INTO logs FROM '%s' USING DELIMITERS ',' NULL AS ''"""
    },
    {'bench': 3, 'ddl': """
CREATE TABLE logs (
  log_time     TIMESTAMP NOT NULL,
  device_id    CHAR(15) NOT NULL,
  device_name  VARCHAR(25) NOT NULL,
  device_type  VARCHAR(15) NOT NULL,
  device_floor SMALLINT NOT NULL,
  event_type   VARCHAR(15) NOT NULL,
  event_unit   CHAR(1),
  event_value  FLOAT
);
    """,
    'copy': """COPY 108957040 OFFSET 2 RECORDS INTO logs FROM '%s' USING DELIMITERS ',' NULL AS ''"""
    },
    ]
tasks = [
    {'bench': 1, 'query': 1, 
    'sql': """
        SELECT machine_name,
        MIN(cpu) AS cpu_min,
        MAX(cpu) AS cpu_max,
        AVG(cpu) AS cpu_avg,
        MIN(net_in) AS net_in_min,
        MAX(net_in) AS net_in_max,
        AVG(net_in) AS net_in_avg,
        MIN(net_out) AS net_out_min,
        MAX(net_out) AS net_out_max,
        AVG(net_out) AS net_out_avg
        FROM (
        SELECT machine_name,
                COALESCE(cpu_user, 0.0) AS cpu,
                COALESCE(bytes_in, 0.0) AS net_in,
                COALESCE(bytes_out, 0.0) AS net_out
        FROM logs
        WHERE machine_name IN ('anansi','aragog','urd')
            AND log_time >= TIMESTAMP '2017-01-11 00:00:00'
        ) AS r
        GROUP BY machine_name;
    """
    },
    {'bench': 1, 'query': 2, 
    'sql': """
SELECT machine_name,
       log_time
FROM logs
WHERE (machine_name LIKE 'cslab%' OR
       machine_name LIKE 'mslab%')
  AND load_one IS NULL
  AND log_time >= TIMESTAMP '2017-01-10 00:00:00'
ORDER BY machine_name,
         log_time;
    """},
    {'bench': 1, 'query': 3, 
    'sql': """
SELECT dt,
       hr,
       AVG(load_fifteen) AS load_fifteen_avg,
       AVG(load_five) AS load_five_avg,
       AVG(load_one) AS load_one_avg,
       AVG(mem_free) AS mem_free_avg,
       AVG(swap_free) AS swap_free_avg
FROM (
  SELECT CAST(log_time AS DATE) AS dt,
         EXTRACT(HOUR FROM log_time) AS hr,
         load_fifteen,
         load_five,
         load_one,
         mem_free,
         swap_free
  FROM logs
  WHERE machine_name = 'babbage'
    AND load_fifteen IS NOT NULL
    AND load_five IS NOT NULL
    AND load_one IS NOT NULL
    AND mem_free IS NOT NULL
    AND swap_free IS NOT NULL
    AND log_time >= TIMESTAMP '2017-01-01 00:00:00'
) AS r
GROUP BY dt,
         hr
ORDER BY dt,
         hr;
    """},
    {'bench': 1, 'query': 4, 
    'sql': """
SELECT machine_name,
       COUNT(*) AS spikes
FROM logs
WHERE machine_group = 'Servers'
  AND cpu_wio > 0.99
  AND log_time >= TIMESTAMP '2016-12-01 00:00:00'
  AND log_time < TIMESTAMP '2017-01-01 00:00:00'
GROUP BY machine_name
ORDER BY spikes DESC
LIMIT 10;
    """},
    {'bench': 1, 'query': 5, 
    'sql': """
SELECT machine_name,
       dt,
       MIN(mem_free) AS mem_free_min
FROM (
  SELECT machine_name,
         CAST(log_time AS DATE) AS dt,
         mem_free
  FROM logs
  WHERE machine_group = 'DMZ'
    AND mem_free IS NOT NULL
) AS r
GROUP BY machine_name,
         dt
HAVING MIN(mem_free) < 10000
ORDER BY machine_name,
         dt;
    """},
    {'bench': 1, 'query': 6, 
    'sql': """
SELECT dt,
       hr,
       SUM(net_in) AS net_in_sum,
       SUM(net_out) AS net_out_sum,
       SUM(net_in) + SUM(net_out) AS both_sum
FROM (
  SELECT CAST(log_time AS DATE) AS dt,
         EXTRACT(HOUR FROM log_time) AS hr,
         COALESCE(bytes_in, 0.0) / 1000000000.0 AS net_in,
         COALESCE(bytes_out, 0.0) / 1000000000.0 AS net_out
  FROM logs
  WHERE machine_name IN ('allsorts','andes','bigred','blackjack','bonbon',
      'cadbury','chiclets','cotton','crows','dove','fireball','hearts','huey',
      'lindt','milkduds','milkyway','mnm','necco','nerds','orbit','peeps',
      'poprocks','razzles','runts','smarties','smuggler','spree','stride',
      'tootsie','trident','wrigley','york')
) AS r
GROUP BY dt,
         hr
ORDER BY both_sum DESC
LIMIT 10;
    """},


    {'bench': 2, 'query': 1, 
    'sql': """
SELECT *
FROM logs
WHERE status_code >= 500
  AND log_time >= TIMESTAMP '2012-12-18 00:00:00'
ORDER BY log_time;
    """},
    {'bench': 2, 'query': 2, 
    'sql': """
SELECT *
FROM logs
WHERE status_code >= 200
  AND status_code < 300
  AND request LIKE '%/etc/passwd%'
  AND log_time >= TIMESTAMP '2012-05-06 00:00:00'
  AND log_time < TIMESTAMP '2012-05-20 00:00:00';
    """},
    {'bench': 2, 'query': 3, 
    'sql': """
SELECT top_level,
       AVG(LENGTH(request) - LENGTH(REPLACE(request, '/', ''))) AS depth
FROM (
  SELECT SUBSTRING(request FROM 1 FOR len) AS top_level,
         request
  FROM (
    SELECT POSITION('/' IN SUBSTRING(request FROM 2)) AS len,
           request
    FROM logs
    WHERE status_code >= 200
      AND status_code < 300
      AND log_time >= TIMESTAMP '2012-12-01 00:00:00'
  ) AS r
  WHERE len > 0
) AS s
WHERE top_level IN ('/about','/courses','/degrees','/events',
                    '/grad','/industry','/news','/people',
                    '/publications','/research','/teaching','/ugrad')
GROUP BY top_level
ORDER BY top_level;
    """},
    {'bench': 2, 'query': 4, 
    'sql': """
SELECT client_ip,
       COUNT(*) AS num_requests
FROM logs
WHERE log_time >= TIMESTAMP '2012-10-01 00:00:00'
GROUP BY client_ip
HAVING COUNT(*) >= 100000
ORDER BY num_requests DESC;
    """},
    {'bench': 2, 'query': 5, 
    'sql': """
SELECT mo,
       COUNT(DISTINCT client_ip)
FROM (
  SELECT EXTRACT(MONTH FROM log_time) AS mo,
         client_ip
  FROM logs
) AS r
GROUP BY mo
ORDER BY mo
    """},
    {'bench': 2, 'query': 6, 
    'sql': """
SELECT AVG(bandwidth) / 1000000000.0 AS avg_bandwidth,
       MAX(bandwidth) / 1000000000.0 AS peak_bandwidth
FROM (
  SELECT log_time,
         SUM(object_size) AS bandwidth
  FROM logs
  GROUP BY log_time
) AS r;
    """},


    {'bench': 3, 'query': 1, 
    'sql': """
SELECT *
FROM logs
WHERE event_type = 'temperature'
  AND event_value <= 32.0
  AND log_time >= TIMESTAMP '2019-11-29 17:00:00';
    """},
    {'bench': 3, 'query': 2, 
    'sql': """
WITH power_hourly AS (
  SELECT EXTRACT(HOUR FROM log_time) AS hr,
         device_id,
         device_name,
         CASE WHEN device_name LIKE 'coffee%' THEN 'coffee'
              WHEN device_name LIKE 'printer%' THEN 'printer'
              WHEN device_name LIKE 'projector%' THEN 'projector'
              WHEN device_name LIKE 'vending%' THEN 'vending'
              ELSE 'other'
         END AS device_category,
         device_floor,
         event_value
  FROM logs
  WHERE event_type = 'power'
    AND log_time >= TIMESTAMP '2019-11-01 00:00:00'
)
SELECT hr,
       device_id,
       device_name,
       device_category,
       device_floor,
       power_avg,
       category_power_avg
FROM (
  SELECT hr,
         device_id,
         device_name,
         device_category,
         device_floor,
         AVG(event_value) AS power_avg,
         (SELECT AVG(event_value)
          FROM power_hourly
          WHERE device_id <> r.device_id
            AND device_category = r.device_category
            AND hr = r.hr) AS category_power_avg
  FROM power_hourly AS r
  GROUP BY hr,
           device_id,
           device_name,
           device_category,
           device_floor
) AS s
WHERE power_avg >= category_power_avg * 2.0;
    """},
    {'bench': 3, 'query': 3, 
    'sql': """
WITH room_use AS (
  SELECT dow,
         hr,
         device_name,
         AVG(motions) AS in_use
  FROM (      
    SELECT dt,
           dow,
           hr,
           device_name,
           COUNT(*) AS motions
    FROM (
      SELECT CAST(log_time AS DATE) AS dt,
             EXTRACT(DOW FROM log_time) AS dow,
             EXTRACT(HOUR FROM log_time) AS hr,
             device_name
      FROM logs
      WHERE device_name LIKE 'room%'
        AND event_type = 'motion_start'
        AND log_time >= TIMESTAMP '2019-09-01 00:00:00'
    ) AS r
    WHERE dow IN (1,2,3,4,5)
      AND hr BETWEEN 9 AND 16
    GROUP BY dt,
             dow,
             hr,
             device_name
  ) AS s 
  GROUP BY dow,
           hr,
           device_name
)         
SELECT device_name,
       dow, 
       hr,  
       in_use
FROM room_use AS r
WHERE in_use = (
  SELECT MIN(in_use)
  FROM room_use
  WHERE device_name = r.device_name
) 
ORDER BY device_name;
    """},
    {'bench': 3, 'query': 4, 
    'sql': """
SELECT device_name,
       device_floor,
       COUNT(*) AS ct
FROM logs
WHERE event_type = 'door_open'
  AND log_time >= TIMESTAMP '2019-06-01 00:00:00'
GROUP BY device_name,
         device_floor
ORDER BY ct DESC;
    """},
    {'bench': 3, 'query': 5, 
    'sql': """
WITH temperature AS (
  SELECT dt,
         device_name,
         device_type,
         device_floor
  FROM (
    SELECT dt,
           hr,
           device_name,
           device_type,
           device_floor,
           AVG(event_value) AS temperature_hourly_avg
    FROM (
      SELECT CAST(log_time AS DATE) AS dt,
             EXTRACT(HOUR FROM log_time) AS hr,
             device_name,
             device_type,
             device_floor,
             event_value
      FROM logs
      WHERE event_type = 'temperature'
    ) AS r
    GROUP BY dt,
             hr,
             device_name,
             device_type,
             device_floor
  ) AS s
  GROUP BY dt,
           device_name,
           device_type,
           device_floor
  HAVING MAX(temperature_hourly_avg) - MIN(temperature_hourly_avg) >= 25.0
)
SELECT DISTINCT device_name,
       device_type,
       device_floor,
       'WINTER'
FROM temperature
WHERE dt >= DATE '2018-12-01'
  AND dt < DATE '2019-03-01'
UNION
SELECT DISTINCT device_name,
       device_type,
       device_floor,
       'SUMMER'
FROM temperature
WHERE dt >= DATE '2019-06-01'
  AND dt < DATE '2019-09-01';
    """},
    {'bench': 3, 'query': 6, 
    'sql': """
SELECT yr,
       mo,
       SUM(coffee_hourly_avg) AS coffee_monthly_sum,
       AVG(coffee_hourly_avg) AS coffee_monthly_avg,
       SUM(printer_hourly_avg) AS printer_monthly_sum,
       AVG(printer_hourly_avg) AS printer_monthly_avg,
       SUM(projector_hourly_avg) AS projector_monthly_sum,
       AVG(projector_hourly_avg) AS projector_monthly_avg,
       SUM(vending_hourly_avg) AS vending_monthly_sum,
       AVG(vending_hourly_avg) AS vending_monthly_avg
FROM (     
  SELECT dt,
         yr,
         mo, 
         hr, 
         AVG(coffee) AS coffee_hourly_avg,
         AVG(printer) AS printer_hourly_avg,
         AVG(projector) AS projector_hourly_avg,
         AVG(vending) AS vending_hourly_avg
  FROM (
    SELECT CAST(log_time AS DATE) AS dt,
           EXTRACT(YEAR FROM log_time) AS yr,
           EXTRACT(MONTH FROM log_time) AS mo,
           EXTRACT(HOUR FROM log_time) AS hr,
           CASE WHEN device_name LIKE 'coffee%' THEN event_value ELSE 0 END AS coffee,
           CASE WHEN device_name LIKE 'printer%' THEN event_value ELSE 0 END AS printer,
           CASE WHEN device_name LIKE 'projector%' THEN event_value ELSE 0 END AS projector,
           CASE WHEN device_name LIKE 'vending%' THEN event_value ELSE 0 END AS vending
    FROM logs
    WHERE device_type = 'meter'
  ) AS r   
  GROUP BY dt,
           yr,
           mo,
           hr
) AS s 
GROUP BY yr,
         mo
ORDER BY yr,
         mo;
    """}
]

def loaddata(conn, idx):
    clk = time.time()
    n = ddl[idx]['bench']
    csv = dataprefix + "../../data/bench%s.csv" % n
    try:
        assert os.path.exists(csv)
    except:
       raise SystemExit(f"Error: bench{ddl[idx]['bench']}.csv missing!")
    conn.execute('drop table if exists logs');
    conn.execute(ddl[idx]['ddl'])
    conn.execute(ddl[idx]['copy'] % csv)
    result = conn.execute(f"SELECT count(*) FROM logs ")
    sec = "%6.3f" % (time.time() - clk)
    print(f"Load {tasks[idx]['bench']} {sec} sec tuples {result.fetchall()}")

def run_query(conn, idx, kind):
    if not tasks[idx]['query']:
        return
    clk = time.time()
    curr = conn.cursor()
    result = curr.execute(tasks[idx]['sql'])
    if not result:
        print(f"Error encountered:{err}")

    res = result.fetchall()
    sec = "%6.3f" % (time.time() - clk)
    print(f"QUERY bench{tasks[idx]['bench']}.q{tasks[idx]['query']} {sec} sec {kind}")

if __name__ == "__main__":
    conn=None
    try:
        conn = monetdbe.connect(':memory:')
        print('Start mgbench')
        for idx in range(len(ddl)):
            loaddata(conn, idx)
            for qry in [ v for v in range(len(tasks)) if tasks[v]['bench'] == idx + 1]:
                run_query(conn, qry, 'cold')
            for qry in [ v for v in range(len(tasks)) if tasks[v]['bench'] == idx + 1]:
                run_query(conn, qry, 'hot')
    except Exception as e:
        raise SystemExit(e)
    finally:
        if conn:
            conn.close()
