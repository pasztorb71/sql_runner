import psycopg2 as psycopg2


class Cluster:
    def __init__(self, host, port, passw):
        self.host = host
        self.port = port
        self.passw = passw
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database="postgres",
            user="postgres",
            password=passw)
        self.databases = self.get_databases()
        self.conn.close()

    def get_databases(self):
        cur = self.conn.cursor()
        cur.execute("SELECT datname from pg_database WHERE datistemplate IS FALSE AND datname NOT IN "
                    "('cloudsqladmin', 'postgres', 'sb-managed-db', 'sandbox') ORDER BY datname")
        record = cur.fetchall()
        cur.close()
        return [rec[0] for rec in record]

    def get_conn(self, db):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=db,
            user="postgres",
            password=self.passw)

    def db_command(self, db, cmd):
        conn = self.get_conn(db)
        cur = conn.cursor()
        cur.execute(cmd)
        record = cur.fetchall()
        cur.close()
        conn.close()
        return [rec[0] for rec in record]
    