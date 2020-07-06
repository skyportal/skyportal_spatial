import os
import numpy as np
import yaml
from spatial import PostGISSpatialBackend, Q3CSpatialBackend, UnindexedSpatialBackend
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from sqlalchemy.ext.declarative import declarative_base
from astropy.coordinates import SkyCoord
from astropy import units as u
import time

SEED = 8675309
np.random.seed(SEED)

basedir = os.path.dirname(__file__)
confpath = os.path.join(basedir, 'config.yaml')
conf = yaml.load(open(confpath, 'r'), Loader=yaml.FullLoader)
itype = conf['index_type']
if itype is not None:
    itype = itype.lower()

# Get the requested backend
if itype == 'postgis':
    Spatial = PostGISSpatialBackend
elif itype == 'q3c':
    Spatial = Q3CSpatialBackend
else:
    Spatial = UnindexedSpatialBackend


DBSession = scoped_session(sessionmaker())
Base = declarative_base()


def init_db(user, database, password=None, host=None, port=None):
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password or '', host or '', port or '', database)

    conn = sa.create_engine(url, client_encoding='utf8')

    DBSession.configure(bind=conn)
    Base.metadata.bind = conn

    return conn


class Object(Spatial, Base):
    __tablename__ = 'objects'
    id = sa.Column(sa.Integer, primary_key=True)


init_db(**conf['database'])
radius = 3600.  # arcsec


def check_differences(res, jm1, jm2):
    resids = set((r[0].id - 1, r[1].id - 1) for r in res)
    jmids = set(zip(jm1, jm2))
    resmjm = resids - jmids
    jmmres = jmids - resids
    return {'resmjm': resmjm, 'jmmres': jmmres}


for nr in [10, 100, 1000, 10000, 100000, 1000000]:

    Base.metadata.create_all()

    ra = np.random.uniform(low=0, high=360, size=nr)
    dec = np.random.uniform(low=-90, high=90, size=nr)

    truth = SkyCoord(ra, dec, unit='deg')
    coord = truth[0]
    matches = truth[truth.separation(coord) <= radius * u.arcsec]
    jm, jm2, _, _ = truth.search_around_sky(truth, seplimit=radius * u.arcsec)
    objs = [Object(ra=r, dec=d) for r, d in zip(ra, dec)]

    DBSession().add_all(objs)

    start = time.time()
    DBSession().flush()
    DBSession().commit()
    stop = time.time()
    print(f'{nr} rows: {stop - start:.2e} sec to load DB ({itype} index)')

    start = time.time()
    q = DBSession().query(Object).filter(Object.radially_within(objs[0], radius))
    print(q.statement.compile(compile_kwargs={'literal_binds':True}))
    res = q.all()
    stop = time.time()
    print(f'{nr} rows: {stop - start:.2e} sec to do rad query ({itype} index)')
    assert len(res) == len(matches)

    if nr < 1e5:

        # do a self join
        o1 = sa.orm.aliased(Object)
        o2 = sa.orm.aliased(Object)
        start = time.time()
        q = DBSession().query(o1, o2).join(o2, o1.radially_within(o2, radius))
        print(q.statement.compile(compile_kwargs={'literal_binds':True}))
        res = q.all()
        stop = time.time()
        print(f'{nr} rows: {stop - start:.2e} sec to do rad join ({itype} index)')
        assert len(res) == len(jm)

        diffs = check_differences(res, jm, jm2)
        for k in diffs:
            assert len(diffs[k]) == 0

    DBSession().execute('TRUNCATE TABLE objects')
    DBSession().execute('DROP TABLE objects')
    DBSession().commit()
