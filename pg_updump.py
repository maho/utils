#!/home/maho/.virtualenvs/invpdf2/bin/python

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy.orm.exc

from psycopg2.extensions import adapt


from optparse import OptionParser

p = OptionParser()
p.add_option("-d", "--dburl", dest="dburl", default=None)
p.add_option("-t", "--table", dest="table", default=None)
p.add_option("-i", "--id", dest="idcolumn", default="id")
(options,args) = p.parse_args()

engine = create_engine(options.dburl,echo=False)
sess = sessionmaker(bind=engine)()

assert options.table

sql = "SELECT * FROM %s WHERE %s" % (options.table, " AND ".join(args + ["TRUE"]))

cursor = sess.execute(sql);

cols = []
for k in cursor.keys():
    if isinstance(k, int):
        continue
    cols.append(k)

for r in cursor.fetchall():
    data = {}
    for c in cols:
        data[c] = r[c]

    pk = data[options.idcolumn]
    del(data[options.idcolumn])

    setvals = ", ".join(["%s=%s"%(x, adapt(y)) for x,y in data.items()])

    print "UPDATE %s SET %s WHERE %s=%s" % (options.table, setvals, options.idcolumn, pk)





