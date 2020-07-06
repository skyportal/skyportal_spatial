import numpy as np
import sqlalchemy as sa
from astropy.coordinates import SkyCoord
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.dialects import postgresql as psql


DEG_TO_RAD = np.pi / 180.
RADIANS_PER_ARCSEC = DEG_TO_RAD / 3600.


class UnindexedSpatialBackend(object):
    """A mixin indicating to the database that an object has sky coordinates.
    Classes that mix this class get no index on RA and DEC. Instead, a direct
    great circle distance formula is used in postgres for radial queries.
    Columns:
        ra: the icrs right ascension of the object in degrees
        dec: the icrs declination of the object in degrees
    Indexes:
        none
    Properties: skycoord: astropy.coordinates.SkyCoord representation of the
    object's coordinate
    """

    # database-mapped
    ra = sa.Column(psql.DOUBLE_PRECISION)
    dec = sa.Column(psql.DOUBLE_PRECISION)

    @property
    def skycoord(self):
        return SkyCoord(self.ra, self.dec, unit='deg')

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
        ca1 = sa.func.cos((90 - self.dec) * DEG_TO_RAD)
        ca2 = sa.func.cos((90 - other.dec) * DEG_TO_RAD)
        sa1 = sa.func.sin((90 - self.dec) * DEG_TO_RAD)
        sa2 = sa.func.sin((90 - other.dec) * DEG_TO_RAD)
        cf = sa.func.cos((self.ra - other.ra) * DEG_TO_RAD)
        roundoff_safe = sa.func.greatest(ca1 * ca2 + sa1 * sa2 * cf, -1)
        roundoff_safe = sa.func.least(roundoff_safe, 1)
        return sa.func.acos(roundoff_safe) / RADIANS_PER_ARCSEC

    @hybrid_method
    def radially_within(self, other, angular_sep_arcsec):
        """Return an SQLalchemy clause element that can be used as a join or
        filter condition for a radial query.

        Parameters
        ----------

        other: subclass of UnindexedSpatialBackend or instance of UnindexedSpatialBackend
           The class or object to query against. If a class, will generate
           a clause element that can be used to join two tables, otherwise
           will generate a clause element that can be used to filter a
           single table.

        angular_sep_arcsec:
           The radius, in arcseconds, to use for the radial query. The
           query will return true if two objects are within this angular
           distance of one another.
        """

        return self.distance(other) <= angular_sep_arcsec
