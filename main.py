import multiprocessing

import psycopg2

from Cluster import Cluster

def mproc(db, cmd, return_dict):
    print(db)
    conn = psycopg2.connect(
        host='localhost',
        port=5433,
        database=db,
        user="postgres",
        password='fLXyFS0RpmIX9uxGII4N')
    cur = conn.cursor()
    cur.execute(cmd)
    record = cur.fetchall()
    cur.close()
    conn.close()
    return_dict[db] = [rec[0] for rec in record]

if __name__ == '__main__':
    sandbox = Cluster(host='localhost', port=5433, passw='fLXyFS0RpmIX9uxGII4N')
    cmd = "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE '%$hist'"

    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    jobs = []
    print('started')
    for db in sandbox.databases:
        p = multiprocessing.Process(target=mproc, args=(db, cmd, return_dict))
        jobs.append(p)
        p.start()

    for job in jobs:
        job.join()
    print(return_dict)