
class NoConnectionError(Exception):
    """Exception thrown when the Cassandra Cluster cannot be reached."""

    def __init__(self, seed):
        super(NoConnectionError, self) \
            .__init__(f"The Cassandra cluster '{seed}' can not be reached.")


class DateNotDefinedError(Exception):
    """Exception thrown when date is required but not defined."""

    def __init__(self, message):
        super(DateNotDefinedError, self).__init__(message)


class MissingColumnError(Exception):
    """Exception thrown when a column is required but not defined."""

    def __init__(self, message):
        super(MissingColumnError, self).__init__(message)


class QueryNotFound(Exception):
    """Exception thrown when the specified query name is not defined."""

    def __init__(self, name):
        super(QueryNotFound, self).__init__(f"Query '{name}' not found.")


class NotARequiredColumnError(Exception):
    """Exception thrown when a query specifies a required column that is not defined in the yaml."""

    def __init__(self, name, query):
        super(NotARequiredColumnError, self) \
            .__init__(f"'{name}' is not a required column in query '{query}'.")
