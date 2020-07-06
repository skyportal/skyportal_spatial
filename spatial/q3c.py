import sqlalchemy as sa
from astropy.coordinates import SkyCoord
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.dialects import postgresql as psql


DEGREES_PER_ARCSEC = 1 / 3600.


class Q3CSpatialBackend(object):
    """A mixin indicating to the database that an object has sky coordinates.
    Classes that mix this class get a q3c spatial index on ra and dec.
    Columns:
        ra: the icrs right ascension of the object in degrees
        dec: the icrs declination of the object in degrees
    Indexes:
        q3c index on ra, dec
    Properties: skycoord: astropy.coordinates.SkyCoord representation of the
    object's coordinate
    """

    # database-mapped
    ra = sa.Column(psql.DOUBLE_PRECISION)
    dec = sa.Column(psql.DOUBLE_PRECISION)

    @property
    def skycoord(self):
        return SkyCoord(self.ra, self.dec, unit='deg')

    @declared_attr
    def __table_args__(cls):
        tn = cls.__tablename__
        return sa.Index(f'{tn}_q3c_ang2ipix_idx', sa.func.q3c_ang2ipix(
            cls.ra, cls.dec)),

    @hybrid_method
    def distance(self, other):
        """Return an SQLalchemy clause element that can be used to calculate
        the angular separation between `self` and `other` in arcsec.

        Parameters
        ----------

        other: subclass of Q3CSpatialBackend or instance of Q3CSpatialBackend
           The class or object to query against. If a class, will generate
           a clause element that can be used to join two tables, otherwise
           will generate a clause element that can be used to filter a
           single table.
        """

        return sa.func.q3c_dist(self.ra, self.dec, other.ra, other.dec) * 3600.

    @hybrid_method
    def radially_within(self, other, angular_sep_arcsec):
        """Return an SQLalchemy clause element that can be used as a join or
        filter condition for a radial query.

        Parameters
        ----------

        other: subclass of Q3CSpatialBackend or instance of Q3CSpatialBackend
           The class or object to query against. If a class, will generate
           a clause element that can be used to join two tables, otherwise
           will generate a clause element that can be used to filter a
           single table.

        angular_sep_arcsec:
           The radius, in arcseconds, to use for the radial query. The
           query will return true if two objects are within this angular
           distance of one another.
        """

        if isinstance(other, Q3CSpatialBackend):
            func = sa.func.q3c_radial_query
        elif issubclass(other, Q3CSpatialBackend):
            func = sa.func.q3c_join
        else:
            raise ValueError('Input to `raidally_within` must be an instance '
                             'of PostGISSpatialBackend or a subclass of '
                             'PostGISSpatialBackend.')

        return func(
            other.ra, other.dec, self.ra, self.dec,
            angular_sep_arcsec * DEGREES_PER_ARCSEC
        )
