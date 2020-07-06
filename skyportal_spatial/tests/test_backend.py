import os
import numpy as np
import yaml
from skyportal_spatial import (PostGISSpatialBackend, Q3CSpatialBackend,
                               UnindexedSpatialBackend)
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from sqlalchemy.ext.declarative import declarative_base
from astropy.coordinates import SkyCoord
from astropy import units as u
import time

import pytest

SEED = 8675309
basedir = os.path.dirname(__file__)
confpath = os.path.join(basedir, 'config.yaml')
conf = yaml.load(open(confpath, 'r'), Loader=yaml.FullLoader)

def check_differences(res, jm1, jm2):
    resids = set((r[0].id - 1, r[1].id - 1) for r in res)
    jmids = set(zip(jm1, jm2))
    resmjm = resids - jmids
    jmmres = jmids - resids
    return {'resmjm': resmjm, 'jmmres': jmmres}




class _TestBase(object):

    radius = 3600  # arcsec

    @pytest.fixture(scope='class')
    def rng(self):
        return np.random.RandomState(seed=SEED)

    @pytest.fixture(scope='class')
    def DBSession(self):

        c = conf['database']
        user = c.get('username')
        password = c.get('password')
        host = c.get('host', None)
        port = c.get('port', None)
        database = c.get('database', None)

        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(user, password or '', host or '', port or '', database)

        conn = sa.create_engine(url, client_encoding='utf8')

        sess = scoped_session(sessionmaker())
        sess.configure(bind=conn)
        self.Base.metadata.bind = conn

        return sess

    def points(self, nr, rng):
        ra = rng.uniform(low=0, high=360, size=nr)
        dec = rng.uniform(low=-90, high=90, size=nr)
        return ra, dec

    @pytest.mark.parametrize("nr", [10, 100, 1000, 10000])
    def test_distance_join_and_radial(self, nr, DBSession, rng):

        DBSession().execute(f'DROP TABLE IF EXISTS {self.Object.__tablename__}')
        DBSession().commit()
        self.Base.metadata.create_all()

        ra, dec = self.points(nr, rng)
        truth = SkyCoord(ra, dec, unit='deg')
        coord = truth[0]
        matches = truth[truth.separation(coord) <= self.radius * u.arcsec]
        jm, jm2, _, _ = truth.search_around_sky(truth,
                                                seplimit=self.radius * u.arcsec)
        objs = [self.Object(ra=r, dec=d) for r, d in zip(ra, dec)]

        DBSession().add_all(objs)

        start = time.time()
        DBSession().flush()
        DBSession().commit()
        stop = time.time()
        print(f'{nr} rows: {stop - start:.2e} sec to load DB ({self.itype} index)')

        # distance calculation
        start = time.time()
        q = DBSession().query(self.Object.distance(objs[0]))
        print(q.statement.compile(compile_kwargs={'literal_binds': True}))
        res = q.all()
        distances_db = np.asarray([r[0] for r in res])
        distances_true = truth.separation(coord).to('arcsec').value
        np.testing.assert_allclose(distances_db, distances_true, atol=1e-8,
                                   rtol=1e-5)
        stop = time.time()
        print(f'{nr} rows: {stop - start:.2e} sec to do distance calculation ({self.itype} index)')

        start = time.time()
        q = DBSession().query(self.Object).filter(self.Object.radially_within(objs[0], self.radius))
        print(q.statement.compile(compile_kwargs={'literal_binds': True}))
        res = q.all()
        stop = time.time()
        print(f'{nr} rows: {stop - start:.2e} sec to do rad query ({self.itype} index)')
        assert len(res) == len(matches)

        # do a self join
        o1 = sa.orm.aliased(self.Object)
        o2 = sa.orm.aliased(self.Object)
        start = time.time()
        q = DBSession().query(o1, o2).join(o2, o1.radially_within(o2, self.radius))
        print(q.statement.compile(compile_kwargs={'literal_binds': True}))
        res = q.all()
        stop = time.time()
        print(f'{nr} rows: {stop - start:.2e} sec to do rad join ({self.itype} index)')
        assert len(res) == len(jm)

        diffs = check_differences(res, jm, jm2)
        for k in diffs:
            assert len(diffs[k]) == 0

        DBSession().execute(f'DROP TABLE {self.Object.__tablename__}')
        DBSession().commit()


class TestPostGIS(_TestBase):

    itype = 'postgis'

    Base = declarative_base()

    class Object(PostGISSpatialBackend, Base):
        __tablename__ = 'postgis_objects'
        id = sa.Column(sa.Integer, primary_key=True)


class TestQ3C(_TestBase):

    itype = 'q3c'

    Base = declarative_base()

    class Object(Q3CSpatialBackend, Base):
        __tablename__ = 'q3c_objects'
        id = sa.Column(sa.Integer, primary_key=True)


class TestNone(_TestBase):

    itype = 'none'

    Base = declarative_base()

    class Object(UnindexedSpatialBackend, Base):
        __tablename__ = 'none_objects'
        id = sa.Column(sa.Integer, primary_key=True)
