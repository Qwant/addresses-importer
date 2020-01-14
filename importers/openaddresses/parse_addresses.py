import csv
import os
import sqlite3
import datetime
import sys


BUFFER_SIZE = 1000


class Address:
    def __init__(self, lon, lat, number, street, unit, city, district, region, postcode):
        self.lon = lon
        self.lat = lat
        self.number = number
        self.street = street or ""
        self.unit = unit
        self.city = city
        self.district = district
        self.region = region
        self.postcode = postcode


class SQLite:
    def __init__(self):
        self.conn = sqlite3.connect('addresses.db')
        self.cursor = self.conn.cursor()
        self.inserted = 0
        self.errors = 0
        self.not_committed = 0
        self.cursor.execute("DROP TABLE IF EXISTS addresses")
        self.cursor.execute("DROP TABLE IF EXISTS addresses_errors")
        self.cursor.execute("""
        CREATE TABLE addresses(
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            number TEXT,
            street TEXT NOT NULL,
            unit TEXT,
            city TEXT,
            district TEXT,
            region TEXT,
            postcode TEXT,
            PRIMARY KEY (lat, lon, number, street, city, place)
        )""")
        self.cursor.execute("""
        CREATE TABLE addresses_errors(
            lat REAL,
            lon REAL,
            number TEXT,
            street TEXT,
            unit TEXT,
            city TEXT,
            district TEXT,
            region TEXT,
            postcode TEXT,
            kind TEXT
        )""")
        self.conn.commit()

    def insert_address(self, addr):
        try:
            if len(addr.city) == 0 and len(addr.street) == 0:
                return
            self.cursor.execute(
                "INSERT INTO addresses VALUES (?,?,?,?,?,?,?,?,?)",
                (addr.lat, addr.lon, addr.number, addr.street, addr.unit, addr.city,
                 addr.district, addr.region, addr.postcode)
            )
            self.commit()
            self.inserted += 1
        except Exception as e:
            kind = 'unknown'
            s_e = str(e)
            if s_e.startswith('UNIQUE constraint failed'):
                kind = 'duplicate'
            elif s_e.startswith('NOT NULL constraint failed'):
                kind = 'missing_value: {}'.format(s_e.rsplit('.')[-1])
            else:
                import pdb;pdb.set_trace()
            self.cursor.execute(
                "INSERT INTO addresses_errors VALUES (?,?,?,?,?,?,?,?,?,?)",
                (addr.lat, addr.lon, addr.number, addr.street, addr.unit, addr.city,
                 addr.district, addr.region, addr.postcode, kind)
            )
            self.commit()
            self.errors += 1

    def commit(self, force=False):
        self.not_committed += 1
        if self.not_committed > BUFFER_SIZE or (force is True and self.not_committed > 0):
            self.conn.commit()
            self.not_committed = 0

    def get_nb_cities(self):
        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM addresses GROUP BY city")
        rows = c.fetchall()
        return rows[0][0]

    def get_nb_addresses(self):
        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM addresses")
        rows = c.fetchall()
        return rows[0][0]

    def get_nb_errors(self):
        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM addresses_errors")
        rows = c.fetchall()
        return rows[0][0]

    def get_nb_by_errors_kind(self):
        c = self.conn.cursor()
        c.execute('SELECT kind, COUNT(*) FROM addresses_errors GROUP BY kind')
        return [r for r in c.fetchall() if r[0] is not None]


def parse_csv(file_path, db):
    start_len = db.inserted
    start_err = db.errors
    start_time = datetime.datetime.now()

    city_name = os.path.splitext(os.path.basename(file_path))[0].replace('city_of_', '').replace('_', ' ')
    print("-> Reading {}".format(file_path))
    with open(file_path) as f:
        content = csv.DictReader(f, delimiter=',')
        for row in content:
            city = row.get('CITY') or row.get('city') or city_name
            db.insert_address(Address(
                row.get('LON') or row.get('lon'),
                row.get('LAT') or row.get('lat'),
                row.get('NUMBER') or row.get('number'),
                row.get('STREET') or row.get('street'),
                row.get('UNIT') or row.get('unit'),
                city,
                row.get('DISTRICT') or row.get('district'),
                row.get('REGION') or row.get('region'),
                row.get('POSTCODE') or row.get('postcode'),
            ))
    print('<- Done! ({} inserted lines and {} errors in {} seconds)'
        .format(db.inserted - start_len, db.errors - start_err,
            (datetime.datetime.now() - start_time).total_seconds()))


def get_addresses(argv):
    if len(argv) != 1:
        print("Expected folder to read as argument!")
        return 1
    db = SQLite()
    for dirpath, dirs, files in os.walk(argv[0]):
        for filename in files:
            if not filename.endswith('.csv'):
                continue
            parse_csv(os.path.join(dirpath, filename), db)
    db.commit(force=True)
    try:
        print('Got {} addresses in {} cities (and {} errors)'
            .format(db.get_nb_addresses(), db.get_nb_cities(), db.get_nb_errors()))
        print('Errors by categories:')
        rows = db.get_nb_by_errors_kind()
        for row in rows:
            print('  {} => {} occurences'.format(row[0], row[1]))
    except Exception as e:
        import pdb;pdb.set_trace()
        print(e)
    return 0


if __name__ == "__main__":
    sys.exit(get_addresses(sys.argv[1:]))
