"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 This trivial program can be used to check if all the basic ingredients
 for using MonetDBe has been available and accessible.
"""
import monetdbe
import os, sys
import pathlib

def load(conn):
    ddl="""
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
        """
    root_dir = pathlib.Path(__file__).parent.parent.parent.absolute()
    csv = os.path.join(root_dir, 'data', 'bench1.csv')
    try:
        assert os.path.exists(csv)
    except:
       raise SystemExit('Error: bench1.csv missing! Run mgbench1.sh in {}/data'.format(root_dir))
    sql= """COPY 1000000 OFFSET 2 RECORDS INTO logs FROM '{}' USING DELIMITERS ',' NULL AS ''""".format(csv)
    conn.execute(ddl)
    conn.execute(sql)

def run_query(conn: monetdbe.Connection):
    sql = """
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
    curr = conn.cursor()
    curr.execute(sql)
    res = curr.fetchall()
    print(res)

def dump(conn):
    pass


if __name__ == "__main__":
    conn=None
    try:
        conn = monetdbe.connect(':memory:')
        load(conn)
        run_query(conn)
        print('Done')
    except Exception as e:
        raise SystemExit(e)
    finally:
        if conn:
            conn.close()
