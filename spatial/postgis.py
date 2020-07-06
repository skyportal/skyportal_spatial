import numpy as np
import sqlalchemy as sa
from astropy.coordinates import SkyCoord
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
import binascii
from sqlalchemy.sql import expression
from sqlalchemy.types import UserDefinedType
from sqlalchemy import func


RADIANS_PER_ARCSEC = np.pi / 180. / 3600.


# Python datatypes


class GisElement(object):
    """Represents a geometry value."""

    def __str__(self):
        return self.desc

    def __repr__(self):
        return "<%s at 0x%x; %r>" % (
            self.__class__.__name__,
            id(self),
            self.desc,
        )


class BinaryGisElement(GisElement, expression.Function):
    """Represents a Geography value expressed as binary."""

    def __init__(self, data):
        self.data = data
        expression.Function.__init__(
            self, "ST_GeogFromWKB", data, type_=Geography(coerce_="binary")
        )

    @property
    def desc(self):
        return self.as_hex

    @property
    def as_hex(self):
        return binascii.hexlify(self.data)


class TextualGisElement(GisElement, expression.Function):
    """Represents a Geography value expressed as text."""

    def __init__(self, desc):
        self.desc = desc
        expression.Function.__init__(
            self, "ST_GeogFromText", desc, type_=Geography
        )


# SQL datatypes.


class Geography(UserDefinedType):
    """Base PostGIS Geography column type."""

    name = "GEOGRAPHY"

    def __init__(self, dimension=None, coerce_="text"):
        self.dimension = dimension
        self.coerce = coerce_

    class comparator_factory(UserDefinedType.Comparator):
        """Define custom operations for geometry types."""

        # override the __eq__() operator
        def __eq__(self, other):
            return self.op("~=")(other)

        # add a custom operator
        def intersects(self, other):
            return self.op("&&")(other)

        # any number of GIS operators can be overridden/added here
        # using the techniques above.

    def _coerce_compared_value(self, op, value):
        return self

    def get_col_spec(self):
        return self.name

    def bind_expression(self, bindvalue):
        if self.coerce == "text":
            return TextualGisElement(bindvalue)
        elif self.coerce == "binary":
            return BinaryGisElement(bindvalue)
        else:
            assert False

    def column_expression(self, col):
        if self.coerce == "text":
            return func.ST_AsText(col, type_=self)
        elif self.coerce == "binary":
            return func.ST_AsBinary(col, type_=self)
        else:
            assert False

    def bind_processor(self, dialect):
        def process(value):
            if isinstance(value, GisElement):
                return value.desc
            else:
                return value

        return process

    def result_processor(self, dialect, coltype):
        if self.coerce == "text":
            fac = TextualGisElement
        elif self.coerce == "binary":
            fac = BinaryGisElement
        else:
            assert False

        def process(value):
            if value is not None:
                return fac(value)
            else:
                return value

        return process

    def adapt(self, impltype):
        return impltype(
            dimension=self.dimension, coerce_=self.coerce
        )


class PostGISSpatialBackend(object):
    """A mixin indicating to the database that an object has sky coordinates.
    Classes that mix this class get a PostGIS spatial index on ra and dec.

    NOTE: Due to the way PostGIS stores spatial data (one column for both RA
    and DEC), if the value of either of these columns is null then the value
    of the nonnull column will not be persisted to the DB. Both RA and Dec must
    be specified for the coordinate to be saved to postgres.

    Columns:
        ra: the icrs right ascension of the object in degrees
        dec: the icrs declination of the object in degrees
    Indexes:
        PostGIS index on ra, dec
    Properties: skycoord: astropy.coordinates.SkyCoord representation of the
    object's coordinate
    """

    # standard spherical geometry WGS 84
    RADIUS = 6370986. * 1.00000357  # meters, for converting angles to distances
    DEFAULT = 'POINT(NULL NULL)'

    # how RA/DEC is stored
    radec = sa.Column(Geography(2))

    def _splstr(self):
        return str(self.radec)[6:-1].split()

    def _complete(self):
        if self.radec is None:
            return False
        else:
            for key in self._splstr():
                if key == 'NULL':
                    return False
        return True

    @hybrid_property
    def ra(self):
        if not self._complete():
            return None
        else:
            return float(self._splstr()[0]) + 180

    @hybrid_property
    def dec(self):
        if not self._complete():
            return None
        else:
            return float(self._splstr()[1])

    @ra.expression
    def ra(self):
        return func.ST_X(self.radec) + 180.

    @dec.expression
    def dec(self):
        return func.ST_Y(self.radec)

    @ra.setter
    def ra(self, value):
        if self.radec is None:
            self.radec = self.DEFAULT
        self.radec = f'POINT({value - 180} {self.dec})'

    @dec.setter
    def dec(self, value):
        if self.radec is None:
            self.radec = self.DEFAULT
        self.radec = f'POINT({self.ra} {value})'

    @property
    def skycoord(self):
        return SkyCoord(self.ra, self.dec, unit='deg')

    @declared_attr
    def __table_args__(cls):
        tn = cls.__tablename__

        # create the postGIS geography object
        # subtract off 180 from RA to keep things within the
        # geo bounds (GIS convention: longitude goes from -180 to 180)

        return sa.Index(f'{tn}_postgis_radec_index', cls.radec,
                        postgresql_using='spgist'),

    @hybrid_method
    def distance(self, other):
        """Return an SQLalchemy clause element that can be used to calculate
        the angular separation between `self` and `other` in arcsec.

        Parameters
        ----------

        other: subclass of PostGISSpatialBackend or instance of PostGISSpatialBackend
           The class or object to query against. If a class, will generate
           a clause element that can be used to join two tables, otherwise
           will generate a clause element that can be used to filter a
           single table.
        """

        dist_m = sa.func.ST_Distance(self.radec, other.radec, False)
        return dist_m / self.RADIUS / RADIANS_PER_ARCSEC

    @hybrid_method
    def radially_within(self, other, angular_sep_arcsec):
        """Return an SQLalchemy clause element that can be used as a join or
        filter condition for a radial query.

        Parameters
        ----------

        other: subclass of PostGISSpatialBackend or instance of PostGISSpatialBackend
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
        # equivalent angular distance in meters on the surface of the earth
        eqdist = self.RADIUS * angular_sep_arcsec * RADIANS_PER_ARCSEC

        # spatial information from this class
        # this is the filter / join clause
        return sa.func.ST_DWithin(self.radec, other.radec, eqdist, False)
