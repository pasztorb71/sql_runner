import multiprocessing

from Cluster import Cluster

def mproc(conn, cmd, return_dict):
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
    for db in sandbox.databases:
        conn = sandbox.get_conn(db)
        p = multiprocessing.Process(target=mproc, args=(conn, cmd, return_dict))
        jobs.append(p)
        p.start()

    for job in jobs:
        job.join()
    print(return_dict)