import re
import csv


def templatize(x):
    # replace do_ids
    x = re.sub(r'/do_[0-9]+', '/<do_id>', x)
    x = re.sub(r'^do_[0-9]+$', '<do_id>', x)
    # replace uuids
    x = re.sub(r'/[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}', '/<uuid>', x)
    # replace taxonomy url special case
    x = re.sub(r'^app/taxonomy/([^/]+)(/?.*)', r'app/taxonomy/<slug>\2', x)
    # replace numbers between "//" in url
    x = re.sub(r'/[0-9]+/', '/<num>/', x)
    x = re.sub(r'/[0-9]+$', '/<num>', x)
    # mark everything after ; as <params> since it looks like a round about way of passing params
    x = re.sub(r';.*$', ';<params>', x)
    return x


def cleanup():
    f = open('query-7f0b288e-7a78-4456-ae1b-4a062e59247c.csv')
    r = csv.reader(f)
    rows = []
    for row in r:
        rows.append(row)
    h = rows[0]
    rows = rows[1:]
    rows = [(p, int(c)) for p, c in rows]
    n_rows = [(templatize(p), c) for p, c in rows]
    n_d = {}
    for p, c in n_rows:
        if p in n_d:
            n_d[p] += c
        else:
            n_d[p] = c
    res = sorted(n_d.items(), key=lambda x: x[0])
    with open('res.csv', 'w', newline='') as csv_file:
        wr = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        wr.writerows(res)
    res_count = sorted(n_d.items(), key=lambda x: x[1], reverse=True)
    with open('res-count-sorted.csv', 'w', newline='') as csv_file:
        wr = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        wr.writerows(res_count)
    return res
