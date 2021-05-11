"""
Primeight custom exceptions module

    Classes:
        NoConnectionError: exception thrown when the Cassandra Cluster cannot be reached
        DateNotDefinedError: exception thrown when date is required but not defined
        MissingColumnError: exception thrown when a column is required but not defined
        QueryNotFoundError: exception thrown when the query is not defined
        NotARequiredColumnError: exception thrown when a query
            specifies a required column that is not defined

"""


class NoConnectionError(Exception):
    """Exception thrown when the Cassandra Cluster cannot be reached."""

    def __init__(self, cassandra_seed: str):
        super().__init__(f"The Cassandra cluster '{cassandra_seed}' can not be reached.")


class DateNotDefinedError(Exception):
    """Exception thrown when date is required but not defined."""

    def __init__(self):
        super().__init__("When a table has a split, you need to specify a time frame.")


class MissingColumnError(Exception):
    """Exception thrown when a column is required but not defined."""

    def __init__(self, column_name: str):
        super().__init__(f"Column '{column_name}' not in table columns")


class QueryNotFound(Exception):
    """Exception thrown when the specified query name
    is not defined in the table configuration.

    """

    def __init__(self, query_name: str):
        super().__init__(f"Query '{query_name}' not found.")


class NotARequiredColumnError(Exception):
    """Exception thrown when a query specifies a required column
    that is not defined in the table configuration.

    """

    def __init__(self, column_name: str, query_name: str):
        super().__init__(f"'{column_name}' is not a required column in query '{query_name}'.")
