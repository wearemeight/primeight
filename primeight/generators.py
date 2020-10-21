from datetime import datetime, timedelta

import pytz
import h3.api.basic_str as h3


class Generators:

    @property
    def names(self):
        return self._names

    def __init__(self):
        self._names = []
        for x, y in Generators.__dict__.items():
            if isinstance(y, staticmethod):
                self.names.append(x)

    @staticmethod
    def minute(ts):
        """Generate new timestamp for that minute at second zero, in UTC.

        :param ts: timestamp
        :type ts: int
        :return: datetime object for that minute at second
        """
        ts = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC) \
            .replace(second=0, microsecond=0, tzinfo=pytz.UTC) \
            .timestamp()

        return int(ts * 1000)

    @staticmethod
    def hour(ts):
        """Generate new timestamp for that hour at minute zero, in UTC.

        :param ts: timestamp
        :type ts: int
        :return: datetime object for that hour at minute
        """
        ts = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC) \
            .replace(minute=0, second=0, microsecond=0, tzinfo=pytz.UTC) \
            .timestamp()

        return int(ts * 1000)

    @staticmethod
    def day(ts):
        """Generate new timestamp for that day at midnight, in UTC.

        :param ts: timestamp
        :type ts: int
        :return: datetime object for that day at midnight
        """
        ts = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC) \
            .replace(hour=0, minute=0, second=0, microsecond=0,
                     tzinfo=pytz.UTC).timestamp()

        return int(ts * 1000)

    @staticmethod
    def week(ts):
        """Generate new timestamp for that week on monday at midnight, in UTC.

        :param ts: timestamp
        :type ts: int
        :return: datetime object for that week on monday at midnight
        """
        d = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC)
        last_monday = d - timedelta(days=d.weekday())

        ts = last_monday.replace(hour=0, minute=0, second=0, microsecond=0,
                                 tzinfo=pytz.UTC).timestamp()

        return int(ts * 1000)

    @staticmethod
    def month(ts):
        """Generate new timestamp for that month on the first day at midnight, in UTC.

        :param ts: timestamp
        :type ts: int
        :return: datetime object for that month on the first day at midnight
        """
        ts = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC) \
            .replace(day=1, hour=0, minute=0, second=0, microsecond=0,
                     tzinfo=pytz.UTC).timestamp()

        return int(ts * 1000)

    @staticmethod
    def year(ts):
        """Generate new timestamp for that year on the first day at midnight, in UTC.

        :param ts: timestamp
        :type ts: int
        :return: datetime object for that year on the first day at midnight
        """
        ts = datetime.fromtimestamp(ts / 1000, tz=pytz.UTC) \
            .replace(month=1, day=1, hour=0, minute=0,
                     second=0, microsecond=0, tzinfo=pytz.UTC) \
            .timestamp()

        return int(ts * 1000)

    @staticmethod
    def h3(lat, lon):
        """Returns h3 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 3)

    @staticmethod
    def h3_begin(lat, lon):
        """Returns h3 identifier for the trip start point.
        Calls :func:`~generators.Generators.h3`"""
        return Generators.h3(lat, lon)

    @staticmethod
    def h3_end(lat, lon):
        """Returns h3 identifier for the trip end point.
        Calls :func:`~generators.Generators.h3`"""
        return Generators.h3(lat, lon)

    @staticmethod
    def h4(lat, lon):
        """Returns h4 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 4)

    @staticmethod
    def h4_begin(lat, lon):
        """Returns h4 identifier for the trip start point.
        Calls :func:`~generators.Generators.h4`"""
        return Generators.h4(lat, lon)

    @staticmethod
    def h4_end(lat, lon):
        """Returns h4 identifier for the trip end point.
        Calls :func:`~generators.Generators.h4`"""
        return Generators.h4(lat, lon)

    @staticmethod
    def h5(lat, lon):
        """Returns h5 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 5)

    @staticmethod
    def h5_begin(lat, lon):
        """Returns h5 identifier for the trip start point.
        Calls :func:`~generators.Generators.h5`"""
        return Generators.h5(lat, lon)

    @staticmethod
    def h5_end(lat, lon):
        """Returns h5 identifier for the trip end point.
        Calls :func:`~generators.Generators.h5`"""
        return Generators.h5(lat, lon)

    @staticmethod
    def h6(lat, lon):
        """Returns h6 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 6)

    @staticmethod
    def h6_begin(lat, lon):
        """Returns h6 identifier for the trip start point.
        Calls :func:`~generators.Generators.h6`"""
        return Generators.h6(lat, lon)

    @staticmethod
    def h6_end(lat, lon):
        """Returns h6 identifier for the trip end point.
        Calls :func:`~generators.Generators.h6`"""
        return Generators.h6(lat, lon)

    @staticmethod
    def h7(lat, lon):
        """Returns h7 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 7)

    @staticmethod
    def h7_begin(lat, lon):
        """Returns h7 identifier for the trip start point.
        Calls :func:`~generators.Generators.h7`"""
        return Generators.h7(lat, lon)

    @staticmethod
    def h7_end(lat, lon):
        """Returns h7 identifier for the trip end point.
        Calls :func:`~generators.Generators.h7`"""
        return Generators.h7(lat, lon)

    @staticmethod
    def h8(lat, lon):
        """Returns h8 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 8)

    @staticmethod
    def h8_begin(lat, lon):
        """Returns h8 identifier for the trip start point.
        Calls :func:`~generators.Generators.h8`"""
        return Generators.h8(lat, lon)

    @staticmethod
    def h8_end(lat, lon):
        """Returns h8 identifier for the trip end point.
        Calls :func:`~generators.Generators.h8`"""
        return Generators.h8(lat, lon)

    @staticmethod
    def h9(lat, lon):
        """Returns h9 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 9)

    @staticmethod
    def h9_begin(lat, lon):
        """Returns h9 identifier for the trip start point.
        Calls :func:`~generators.Generators.h9`"""
        return Generators.h9(lat, lon)

    @staticmethod
    def h9_end(lat, lon):
        """Returns h9 identifier for the trip end point.
        Calls :func:`~generators.Generators.h9`"""
        return Generators.h9(lat, lon)

    @staticmethod
    def h10(lat, lon):
        """Returns h10 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 10)

    @staticmethod
    def h10_begin(lat, lon):
        """Returns h10 identifier for the trip start point.
        Calls :func:`~generators.Generators.h10`"""
        return Generators.h10(lat, lon)

    @staticmethod
    def h10_end(lat, lon):
        """Returns h10 identifier for the trip end point.
        Calls :func:`~generators.Generators.h10`"""
        return Generators.h10(lat, lon)

    @staticmethod
    def h11(lat, lon):
        """Returns h11 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 11)

    @staticmethod
    def h11_begin(lat, lon):
        """Returns h11 identifier for the trip start point.
        Calls :func:`~generators.Generators.h11`"""
        return Generators.h11(lat, lon)

    @staticmethod
    def h11_end(lat, lon):
        """Returns h11 identifier for the trip end point.
        Calls :func:`~generators.Generators.h11`"""
        return Generators.h11(lat, lon)

    @staticmethod
    def h12(lat, lon):
        """Returns h12 identifier corresponding to the given identifier.

        :param lat: latitude
        :type lat: float
        :param lon: longitude
        :type lon: float
        :return: datetime object for that year on the first day at midnight
        """
        return h3.geo_to_h3(lat, lon, 12)

    @staticmethod
    def h12_begin(lat, lon):
        """Returns h12 identifier for the trip start point.
        Calls :func:`~generators.Generators.h12`"""
        return Generators.h12(lat, lon)

    @staticmethod
    def h12_end(lat, lon):
        """Returns h12 identifier for the trip end point.
        Calls :func:`~generators.Generators.h12`"""
        return Generators.h12(lat, lon)
