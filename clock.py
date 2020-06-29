import os
import numpy as np
import yaml
from spatial import q3c, none, postgis
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from sqlalchemy.ext.declarative import declarative_base
from astropy.coordinates import SkyCoord
from astropy import units as u
import time

SEED = 8675309
np.random.seed(SEED)

basedir = os.path.dirname(__file__)
conf = yaml.load(f'{basedir}/config.yaml', Loader=yaml.FullLoader)
itype = conf['index_type'].lower()

# Get the requested backend
if itype == 'postgis':
    Spatial = postgis.Spatial
elif itype == 'q3c':
    Spatial = q3c.Spatial
else:
    Spatial = none.Spatial


DBSession = scoped_session(sessionmaker())
Base = declarative_base()


def init_db(user, database, password=None, host=None, port=None):
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password or '', host or '', port or '', database)

    conn = sa.create_engine(url, client_encoding='utf8', echo=True)

    DBSession.configure(bind=conn)
    Base.metadata.bind = conn

    return conn


class Object(Spatial, Base):
    id = sa.Column(sa.Integer, primary_key=True)


init_db(**conf['database'])
radius = 3600.  # arcsec

for nr in [10, 100, 1000, 10000, 100000, 1000000]:
    DBSession().create_all()
    ra = np.random.uniform(low=0, high=360, size=nr)
    dec = np.random.uniform(low=-90, high=90, size=nr)

    truth = SkyCoord(ra, dec, unit='deg')
    coord = truth[0]
    matches = truth[truth.separation(coord) < radius * u.arcsec]

    objs = [Object(ra=r, dec=d) for r, d in zip(ra, dec)]
    DBSession().add_all(objs)

    start = time.time()
    DBSession().flush()
    DBSession().commit()
    stop = time.time()
    print(f'{nr} rows: {stop - start:.2e} sec to load DB ({itype} index)')

    start = time.time()
    q = DBSession().query(Object).filter(Object.radially_within(objs[0], radius))
    res = q.all()
    stop = time.time()
    print(f'{nr} rows: {stop - start:.2e} sec to do rad query ({itype} index)')
    assert len(res) == len(matches)

    print('query successful')

    DBSession().drop_all()
