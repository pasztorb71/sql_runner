import pprint


def print_dict(d):
    for db, tables in d.items():
        if tables:
            print(db)
            pp = pprint.PrettyPrinter(indent=2, width=1)
            pp.pprint(d[db])