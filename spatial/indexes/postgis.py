import sqlalchemy as sa
from astropy.coordinates import SkyCoord
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.dialects import postgresql as psql


RADIANS_PER_ARCSEC = 4.84814e-6


class Spatial(object):
    """A mixin indicating to the database that an object has sky coordinates.
    Classes that mix this class get a PostGIS spatial index on ra and dec.
    Columns:
        ra: the icrs right ascension of the object in degrees
        dec: the icrs declination of the object in degrees
    Indexes:
        PostGIS index on ra, dec
    Properties: skycoord: astropy.coordinates.SkyCoord representation of the
    object's coordinate
    """

    # standard spherical geometry WGS 84
    SRID = 4326
    RADIUS = 6378137.  # meters, for converting angles to distances

    # database-mapped
    ra = sa.Column(psql.DOUBLE_PRECISION)
    dec = sa.Column(psql.DOUBLE_PRECISION)

    @property
    def skycoord(self):
        return SkyCoord(self.ra, self.dec, unit='deg')

    @declared_attr
    def __table_args__(cls):
        tn = cls.__tablename__

        # create the postGIS geography object
        # subtract off 180 from RA to keep things within the
        # geo bounds (GIS convention: longitude goes from -180 to 180)

        point = sa.func.ST_Point(cls.ra - 180., cls.dec)

        # set its SRID
        setpoint = sa.func.ST_SetSRID(point, cls.SRID)

        return sa.Index(f'{tn}_postgis_radec_index', setpoint,
                        postgresql_using='spgist'),

    @hybrid_method
    def radially_within(self, other, angular_sep_arcsec):
        """Return an SQLalchemy clause element that can be used as a join or
        filter condition for a radial query.

        Parameters
        ----------

        other: subclass of Spatial or instance of Spatial
           The class or object to query against. If a class, will generate
           a clause element that can be used to join two tables, otherwise
           will generate a clause element that can be used to filter a
           single table.

        angular_sep_arcsec:
           The radius, in arcseconds, to use for the radial query. The
           query will return true if two objects are within this angular
           distance of one another.
        """

        # spatial information from the other class or object
        opoint = sa.func.ST_Point(other.ra - 180., other.dec)

        # set its SRID
        osetpoint = sa.func.ST_SetSRID(opoint, self.SRID)

        # equivalent angular distance in meters on the surface of the earth
        eqdist = self.RADIUS * angular_sep_arcsec * RADIANS_PER_ARCSEC

        # spatial information from this class
        mypoint = sa.func.ST_Point(self.ra - 180., self.dec)
        mysetpoint = sa.func.SetSRID(mypoint, self.SRID)

        # this is the filter / join clause
        return sa.func.ST_DWithin(mysetpoint, osetpoint, eqdist, False)
