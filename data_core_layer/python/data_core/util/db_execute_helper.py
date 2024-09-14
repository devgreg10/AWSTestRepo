from __future__ import annotations
from enum import Enum
from typing import Any, List
import boto3
import psycopg2
import json
from attrs import define, fields
from data_core.util.db_model import DbModel
from psycopg2 import sql
from psycopg2.extensions import connection
from psycopg2.errorcodes import FOREIGN_KEY_VIOLATION, UNIQUE_VIOLATION
from data_core.util.db_exceptions import DbException, DbErrorCode
from datetime import datetime

psycopg2_internal_exception_types = (psycopg2.OperationalError, psycopg2.ProgrammingError,
                                     psycopg2.InternalError, psycopg2.NotSupportedError)

psycopg2_data_integrity_exception_types = (
    psycopg2.IntegrityError, psycopg2.DataError)


class CompareType(str, Enum):
    """CompareType provides a mapping to supported SQL comparison operators"""
    LESS_THAN = '<'
    LESS_THAN_EQUAL = '<='
    GREATER_THAN = '>'
    GREATER_THAN_EQUAL = '>='
    EQUAL = '='
    NOT_EQUAL = '<>'
    ILIKE = 'ILIKE'
    IS_NULL = 'IS NULL'
    IS_NOT_NULL = 'IS NOT NULL'


class MatchType(str, Enum):
    """MatchType provides a mapping to logical operators"""
    ANY = 'OR'
    ALL = 'AND'


class SortType(str, Enum):
    """SortType provides a mapping to sort directions"""
    ASCENDING = 'ASC'
    DESCENDING = 'DESC'


@define(kw_only=True)
class SortOption:
    """SortOption combines a column that is to be sorted and how it should be sorted"""
    col_name: str
    sort_type: SortType


@define(kw_only=True)
class SortOptions:
    """A list of all the columns that should be sorted and how they should be sorted"""
    # Apply sorts following list order
    sort_options: list[SortOption]


@define(kw_only=True)
class FilterOption:
    """FilterOption represents a singular comparison to be contained within a Where clause"""
    col_name: str
    col_value: str
    compare_type: CompareType

    # add id at the end to ensure uniqueness
    def generate_filter_statement(self, id: int, alias: str = ""):
        values = {}
        query = None
        # If a column name is used in multiple filters accross a single query
        # we append an id to the end to ensure proper value matching
        column_name_placeholder = f'{self.col_name}_{str(id)}'
        if self.compare_type in [CompareType.IS_NULL, CompareType.IS_NOT_NULL]:
            query = sql.SQL(' ').join([sql.SQL(alias) + sql.Identifier(self.col_name), sql.SQL(
                self.compare_type.value)])
        else:
            query = (sql.SQL(' ').join([sql.SQL(alias) + sql.Identifier(self.col_name), sql.SQL(
                self.compare_type.value), sql.Placeholder(column_name_placeholder)]))
            col_value = str(self.col_value) + \
                        ('%' if self.compare_type ==
                                CompareType.ILIKE else '')
            values.update({column_name_placeholder: col_value})

        return query, values


@define(kw_only=True)
class FilterOptions:
    """
    FilterOptions provides a way to connect one to many different filter statements. Each FilterOptions object can combine a number of filter statements (FilterOption) with a single MatchType. To use multiple match types utilize the nested filter options parameter to combine several FilterOptions classes into one. 
    """
    filter_options: list[FilterOption]
    match_type: MatchType
    nested_filter_options: list[FilterOptions] = None

    def generate_query_statement(self, filter_id: int = 0, alias: str = ""):
        values = {}
        query_segments = []
        if self.filter_options:
            for filter in self.filter_options:
                filter_statement, filter_values = filter.generate_filter_statement(
                    filter_id, alias)
                filter_id += 1
                query_segments.append(filter_statement)
                values.update(filter_values)
        if self.nested_filter_options:
            for nested_filter_options in self.nested_filter_options:
                nested_filter_statement, nested_values = nested_filter_options.generate_query_statement(alias=alias)
                values.update(nested_values)
                query_segments.append(
                    sql.SQL('(') + nested_filter_statement + sql.SQL(')'))
        return sql.SQL(f' {self.match_type.value} ').join(query_segments), values


# TODO Update currently auto numbered placeholders to be named placeholders sql.Placeholder(name) and provide a map of values

@define(kw_only=True)
class Parameter:
    """Parameter represents a singular named arguement to be passed into a query"""
    col_name: str
    col_value: str

    # add id at the end to ensure uniqueness
    def generate_param_placeholder_statement(self, id: int, alias: str = ""):
        values = {}
        query = None
        # If a column name is used in multiple parameters accross a single query
        # we append an id to the end to ensure proper value matching
        column_name_placeholder = f'{self.col_name}_{str(id)}'
        query = (sql.SQL(' ').join([sql.SQL(alias), sql.Placeholder(column_name_placeholder)]))
        col_value = self.col_value
        values.update({column_name_placeholder: col_value})

        return query, values


@define(kw_only=True)
class ParameterGroup:
    """
    ParameterGroup provides a way to connect one to many different parameter placeholders. Each Parameter Group object can combine a number of parameter placeholder statements (Parameter) with a LIST MatchType.
    """
    param_group: list[Parameter]

    def generate_param_query_statement(self, param_id: int = 0, alias: str = ""):
        values = {}
        query_segments = []
        if self.param_group:
            for param in self.param_group:
                param_statement, param_values = param.generate_param_placeholder_statement(
                    param_id, alias)
                param_id += 1
                query_segments.append(param_statement)
                values.update(param_values)
        return sql.SQL(', ').join(query_segments), values


class DbExecutorHelper:
    """

    **DbExecutorHelper**

    This is a class composed of static methods that perform CRUD operations for every entity

    **Methods**

    - `execute_insert`: This method creates a row for a schema and table and returns a view.
    - `execute_select_with_options`: This method can read a single or multiple rows. This incorporates sort, filter, pagination and search
    - `execute_update`: This method updates a row within the table and returns a view.
    - `execute_soft_delete_by_id` : This method updates a row within a table to have is_active = False and returns an id 
    - `execute_delete_by_id`: This is a hard delete of a row.
    - `execute_function`: This method allows for a function to be run in the database and returns a dictionary with the applicable output from the function.
    """

    @staticmethod
    def get_db_connection_by_secret_arn(
        secret_arn: str,
        region: str
    ):
    

        session = boto3.session.Session()

        secret_client = session.client(
            service_name='secretsmanager',
            region_name=region
        )
        secret_response = secret_client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(secret_response['SecretString'])

        db_connection = DbExecutorHelper.get_db_connection(
            host=secret['host'],
            database=secret['dbname'],
            user=secret['username'],
            password=secret['password']
        )

        return db_connection
        
    @staticmethod
    def get_db_connection(
        host: str,
        database: str,
        user: str,
        password: str
    ):
        db_connection: connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )

        return db_connection

    @staticmethod
    def get_db_proxy_connection(
            database_proxy_endpoint: str,
            database_proxy_user: str,
            client: boto3.client,
            region: str
    ):
        """
        **get_db_proxy_connection**

        **Parameters**

        - `database_proxy_endpoint`: The Database Proxy Endpoint to use for RDS access
        - `database_proxy_user`: The Database user that calls / has access to RDS proxy
        - `client`: The Boto3 rds client
        - `region`: The region associated with Boto3 and RDS proxy call

        **Returns**

        connection - The database connection that can be passed to Data Core functions

        """
        database_token = client.generate_db_auth_token(
            DBHostname=database_proxy_endpoint,
            Port=5432,
            DBUsername=database_proxy_user,
            Region=region
        )

        db_connection: connection = psycopg2.connect(
            f"""host={database_proxy_endpoint} dbname=wdge_db user={database_proxy_user} password={database_token}""")

        return db_connection

    @staticmethod
    def execute_insert(*, db_connection: connection, db_schema: str, db_table: str,
                       db_model: DbModel, commit_changes: bool = True,
                       close_db_conn: bool = True) -> dict:
        """
        **execute_insert**

        Base data core method for inserting into a database table

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database. 
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        dict - This is a dictionary of the data for the entity that was inserted

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()
            query_string = '''INSERT INTO {db_schema}.{db_table} ({columns}) VALUES ({values_place_holder}) RETURNING *'''
            insert_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                columns=sql.SQL(', ').join(
                    map(sql.Identifier, db_model.get_column_names())),
                values_place_holder=sql.SQL(', ').join(
                    sql.Placeholder() * len(db_model.get_column_values())))
            print(insert_query)
            print(db_model.get_column_values_as_tuple())
            # Execute the command passing in placeholder values
            db_cursor.execute(
                insert_query, db_model.get_column_values_as_tuple())

            # First item in description is name, get a list of the column names
            column_names = [columns[0] for columns in db_cursor.description]

            # Fetch all gets an array of tuples with the values for each row
            # Zip together a dictionary of columns to values
            response_dict = dict(zip(column_names, db_cursor.fetchone()))

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            return response_dict

        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except psycopg2_data_integrity_exception_types as error:
            # Caller Error
            if error.pgcode == UNIQUE_VIOLATION:
                raise DbException(error_code=DbErrorCode.NATURAL_KEY_VIOLATION,
                                  error_message=error.pgerror) from error
            if error.pgcode == FOREIGN_KEY_VIOLATION:
                raise DbException(error_code=DbErrorCode.FOREIGN_KEY_VIOLATION,
                                  error_message=error.pgerror) from error
            raise DbException(error_code=DbErrorCode.DATA_INTEGRITY_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by called
            # connection.closed: 0 if the connection is open, nonzero if it is closed or broken.
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    # TODO: Write code to validate missing parameter pairs within this
    @staticmethod
    def execute_select_with_options(*, db_connection: connection, db_schema: str, db_table: str,
                                    search_col_names: list[str], search_text: str, filter: dict,
                                    limit: int, offset: int, sort_col_name: str,
                                    sort_type: SortType,
                                    close_db_conn: bool = True) -> List[dict]:
        """
        **execute_select_with_options**

        This function allows for a read on the current set of players in the database. This incorporates the ability to sort, filter, paginate and search.

        **Parameters**

        - `db_connection`: pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity
        - `search_col_names`: list that contains all of the columns being searched on.
        - `search_text`: string that is being searched.
        - `filter`: dictionary that holds columns that are being filtered and the values that are being used to be filtered.
        - `limit`: an integer that determines the amount of values returned. 
        - `offset`: an integer shifts the values by n rows. (EX) if you want to start on row 4 set offset = 4.
        - `sort_col_name`: string value that represents the column name that is used to sort the response 
        - `sort_type`: of SortType type. This parameter is passed to choose the which column and the order the values are arranged
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.


        **Returns**

        list[dict] - This is a list of dict. Each dict represents an entity

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()

            query_string = sql.SQL('Select * from {db_schema}.{db_table}').format(
                db_schema=sql.Identifier(db_schema), db_table=sql.Identifier(db_table))

            values = []
            # TODO: FIXME ADD conditional/specific field searching
            if search_text:
                # TODO: FIXME Check if casing of search text is captured/required
                query_string += sql.SQL(' Where {} ').format(sql.SQL(' OR ').join([sql.Identifier(
                    col_names) + sql.SQL(' ILIKE ') + sql.Placeholder() for col_names in
                                                                                   search_col_names]))
                for col_names in search_col_names:
                    # TODO: CHECK if this '%' syntax is required for
                    values.append(search_text + "%")

            if filter and search_text:
                # Format all the potential filters
                query_string += sql.SQL(' AND ').join([
                    sql.SQL('(') + sql.SQL(' OR ').join([
                        sql.Identifier(col) + sql.SQL(' = ') + sql.Placeholder() for val in values
                    ]) + sql.SQL(')') if isinstance(values, list) else
                    sql.Identifier(col) + sql.SQL(' = ') + sql.Placeholder()
                    for col, values in filter.items()
                ])
                for value in filter.values():
                    if isinstance(value, list):
                        values += value
                    else:
                        values.append(value)
            elif filter:
                query_string += sql.SQL(' WHERE {} ').format(
                    sql.SQL(' AND ').join([
                        sql.SQL('(') + sql.SQL(' OR ').join([
                            sql.Identifier(col) + sql.SQL(' = ') + sql.Placeholder() for val in
                            values
                        ]) + sql.SQL(')') if isinstance(values, list) else
                        sql.Identifier(col) + sql.SQL(' = ') +
                        sql.Placeholder()
                        for col, values in filter.items()
                    ]))
                for value in filter.values():
                    if isinstance(value, list):
                        values += value
                    else:
                        values.append(value)

            if sort_type and sort_col_name:
                query_string += sql.SQL(" Order By {} {} ").format(
                    sql.Identifier(sort_col_name), sql.SQL(sort_type.value))

            if limit:
                print(f"Limit {limit}")
                query_string += sql.SQL(" Limit {}").format(sql.Placeholder())
                values.append(limit)

            if offset:
                print(f"Offset {offset}")
                query_string += sql.SQL(" Offset {}").format(sql.Placeholder())
                values.append(offset)

            print(query_string)
            print(values)

            # Execute the command passing in placeholder values
            db_cursor.execute(query_string, values)

            # First item in description is name, get a list of the column names
            column_names = [columns[0] for columns in db_cursor.description]

            print("completed")

            response_array = [dict(zip(column_names, row_values))
                              for row_values in db_cursor.fetchall()]
            print(len(response_array))
            return response_array

        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_select_with_filter_options(*, db_connection: connection, db_schema: str,
                                           db_table: str, filter_options: FilterOptions, limit: int,
                                           offset: int, sort_options=SortOptions,
                                           db_model: DbModel = None,
                                           close_db_conn: bool = True) -> List[dict]:
        """
        **execute_select_with_options**

        This function allows for a read on the current set of players in the database. This incorporates the ability to sort, filter, paginate and search.

        **Parameters**

        - `db_connection`: pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model type to input to know which fields to read
        - `filter_options`: an object that can be used to represent the filter conditions of a query
        - `limit`: an integer that determines the amount of values returned. 
        - `offset`: an integer shifts the values by n rows. (EX) if you want to start on row 4 set offset = 4.
        - `sort_options`: sort options object that determins how to sort the response 
        - `sort_type`: of SortType type. This parameter is passed to choose the which column and the order the values are arranged
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.


        **Returns**

        list[dict] - This is a list of dict. Each dict represents an entity

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()

            if db_model:
                column_list = sql.SQL(",").join(
                    [sql.Identifier(attribute.name) for attribute in fields(db_model)])
            else:
                column_list = sql.SQL("*")
            query_string = sql.SQL('Select {column_list} from {db_schema}.{db_table}').format(
                db_schema=sql.Identifier(db_schema), db_table=sql.Identifier(db_table),
                column_list=column_list)
            values = {}

            if filter_options and (
                    (filter_options.filter_options and len(filter_options.filter_options) > 0) or
                    (filter_options.nested_filter_options and len(
                        filter_options.nested_filter_options) > 0)
            ):
                filter_statement, filter_values = filter_options.generate_query_statement()
                query_string += sql.SQL('Where {}').format(filter_statement)
                values.update(filter_values)

            if sort_options and len(sort_options.sort_options) > 0:
                sort_statements = []
                for sort_option in sort_options.sort_options:
                    sort_statements.append(sql.Identifier(
                        sort_option.col_name) + sql.SQL(' ') + sql.SQL(sort_option.sort_type.value))
                query_string += sql.SQL(" Order By {}").format(
                    sql.SQL(', ').join(sort_statements))

            if limit:
                query_string += sql.SQL(" Limit {}").format(
                    sql.Placeholder('limit'))
                values.update({'limit': limit})

            if offset:
                query_string += sql.SQL(" Offset {}").format(
                    sql.Placeholder('offset'))
                values.update({'offset': offset})

            print(query_string)
            # Execute the command passing in placeholder values
            db_cursor.execute(query_string, values)

            # First item in description is name, get a list of the column names
            column_names = [columns[0] for columns in db_cursor.description]

            print("completed")

            response_array = [dict(zip(column_names, row_values))
                              for row_values in db_cursor.fetchall()]
            print(len(response_array))
            return response_array

        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_update(*, db_connection: connection, db_schema: str, db_table: str, filter: dict,
                       db_model: DbModel,
                       commit_changes: bool = True, close_db_conn: bool = True) -> dict:
        # TODO Add filter logic like in select with options
        """
        **execute_update**

        This function accepts a table id and updates that id with the model provided
        One important thing to note is that this is a put so there are no partial updates. All of the values that are not included are added as puts.

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be updated into
        - `db_model`: Model for an entity
        - `filter`: Constraint key value pairs. Usually denotes a column and a value to filter by
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.


        **Returns**

        dict - This is a dictionary of the data for the entity that was inserted

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()

            # TODO Only update if changes are made (new record is distinct from existing
            # build update query
            query_string = '''
            UPDATE {db_schema}.{db_table} dest 
            SET {set_clause}, update_ts = now() 
            WHERE {filter_clause} RETURNING *
            '''

            update_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                set_clause=sql.SQL(', ').join(sql.Identifier(
                    key) + sql.SQL(" = ") + sql.Placeholder() for key in
                                              db_model.get_column_names()),
                filter_clause=sql.SQL(' AND ').join(
                    [sql.SQL('dest.') + sql.Identifier(col) + sql.SQL(" = ") + sql.Placeholder() for
                     col in filter.keys()])
            )

            print(update_query)

            # Execute the command passing in placeholder values
            db_cursor.execute(
                update_query, db_model.get_column_values_as_tuple() + tuple(list(filter.values()), )
            )

            # First item in description is name, get a list of the column names
            column_names = [columns[0] for columns in db_cursor.description]
            print(column_names)

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            return filter
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except psycopg2_data_integrity_exception_types as error:
            # Caller Error
            raise DbException(error_code=DbErrorCode.DATA_INTEGRITY_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_update_with_filter_options(*, db_connection: connection, db_schema: str,
                                           db_table: str, filter_options: FilterOptions,
                                           db_model: DbModel,
                                           commit_changes: bool = True,
                                           close_db_conn: bool = True) -> None:
        """
        **execute_update**

        This function accepts a table id and updates that id with the model provided
        One important thing to note is that this is a put so there are no partial updates. All of the values that are not included are added as puts.

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be updated into
        - `db_model`: Model for an entity
        - `filter_options`: Object to represent the filter to be used to determine what row(s) to update
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.


        **Returns**

        Nothing

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()

            # TODO Only update if changes are made (new record is distinct from existing
            # build update query
            query_string = '''
            UPDATE {db_schema}.{db_table} dest 
            SET {set_clause}, update_ts = now() 
            WHERE {filter_clause} RETURNING *
            '''

            filter_clause, filter_values = filter_options.generate_query_statement()

            update_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                set_clause=sql.SQL(', ').join(sql.Identifier(
                    key) + sql.SQL(" = ") + sql.Placeholder(key) for key in
                                              db_model.get_column_names()),
                filter_clause=filter_clause
            )

            filter_values.update(db_model.as_dict())

            print(update_query)

            # Execute the command passing in placeholder values
            db_cursor.execute(
                update_query, filter_values)

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            return None
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except psycopg2_data_integrity_exception_types as error:
            # Caller Error
            raise DbException(error_code=DbErrorCode.DATA_INTEGRITY_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_soft_delete_by_id(*, db_connection: connection, db_schema: str, db_table: str,
                                  where_col_name: str, where_col_value: str,
                                  commit_changes: bool = True, close_db_conn: bool = True) -> str:
        """
        **execute_soft_delete_by_id**

        This is a soft delete. The soft delete This returns the where column value that the values were soft deleted on.

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity
        - `where_col_name`: Column that will be deleted on 
        - `where_col_value`: Value that will be deleted on 
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        where_col_value

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()
            query_string = '''UPDATE {db_schema}.{db_table} SET is_active = False, update_ts = now() WHERE ({where_col_name}) = ({where_col_value}) RETURNING *'''
            update_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                where_col_name=sql.Identifier(where_col_name),
                where_col_value=sql.Placeholder())

            print(update_query)
            # Execute the command passing in placeholder values
            db_cursor.execute(update_query, (where_col_value,))

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            # Return the matched value that was "deleted"
            return where_col_value
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_soft_delete_with_filter_options(*, db_connection: connection, db_schema: str,
                                                db_table: str, filter_options: FilterOptions,
                                                commit_changes: bool = True,
                                                close_db_conn: bool = True) -> None:
        """
        **execute_soft_delete_with_filter_options**

        This is a soft delete. The soft delete This returns the where column value that the values were soft deleted on.

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity
        - `filter_options`: Object to represent the filters that determine what record should be soft deleted
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        Nothing

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()
            where_clause, where_values = filter_options.generate_query_statement()
            # TODO Does this need to set updated by?
            query_string = '''UPDATE {db_schema}.{db_table} SET is_active = False, update_ts = now() WHERE {where_clause} RETURNING *'''
            update_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                where_clause=where_clause)

            print(update_query)
            # Execute the command passing in placeholder values
            db_cursor.execute(update_query, where_values)

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            return None
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_delete_by_id(*, db_connection: connection, db_schema: str, db_table: str,
                             where_col_name: str, where_col_value: str, commit_changes: bool = True,
                             close_db_conn: bool = True) -> Any:
        """
        **execute_delete_by_id**

        This is a hard delete. Their is nothing returned in the function

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity
        - `where_col_name`: Column that will be deleted on 
        - `where_col_value`: Value that will be deleted on 
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        None - This function returns nothing

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()
            query_string = '''DELETE FROM {db_schema}.{db_table} WHERE ({where_col_name}) = ({where_col_value}) RETURNING *'''
            delete_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                where_col_name=sql.Identifier(where_col_name),
                where_col_value=sql.Placeholder())

            # Execute the command passing in placeholder values
            db_cursor.execute(delete_query, (where_col_value,))

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            # Return the matched value that was deleted
            return where_col_value
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_hard_delete_with_filter_options(*, db_connection: connection, db_schema: str,
                                                db_table: str, filter_options: FilterOptions,
                                                commit_changes: bool = True,
                                                close_db_conn: bool = True) -> None:
        """
        **execute_hard_delete_with_filter_options**

        This is a hard delete. The soft delete This returns what was deleted.

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity
        - `filter_options`: Object to represent the filters that determine what record should be soft deleted
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        Nothing

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()
            where_clause, where_values = filter_options.generate_query_statement()
            # TODO Does this need to set updated by?
            query_string = '''DELETE FROM {db_schema}.{db_table} WHERE {where_clause} RETURNING *'''
            update_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                where_clause=where_clause)

            print(update_query)
            # Execute the command passing in placeholder values
            db_cursor.execute(update_query, where_values)

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            return None
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_get_table_size(*, db_connection: connection, db_schema: str, db_table: str,
                               close_db_conn: bool = True) -> int:
        """
        **execute_get_table_size**

        Get the size of a table

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity.

        **Returns**

        int - Table size

        """
        # TODO allow for filters and searches to match select query
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()
            query_string = '''SELECT COUNT(*) FROM {db_schema}.{db_table}'''
            count_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table))

            # Execute the command passing in placeholder values
            db_cursor.execute(count_query)

            result = db_cursor.fetchone()
            # Return the matched value that was deleted
            return result[0]
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Commit and close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_get_table_size_with_filter_options(*, db_connection: connection, db_schema: str,
                                                   db_table: str, filter_options: FilterOptions,
                                                   close_db_conn: bool = True) -> int:
        """
        **execute_get_table_size_with_filter_options**

        Get the size of a table

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity.

        **Returns**

        int - Table size

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()
            query_string = '''SELECT COUNT(*) FROM {db_schema}.{db_table}'''
            where_values = {}
            count_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table))

            if filter_options and (
                    (filter_options.filter_options and len(filter_options.filter_options) > 0) or
                    (filter_options.nested_filter_options and len(
                        filter_options.nested_filter_options) > 0)
            ):
                where_clause, where_values = filter_options.generate_query_statement()
                count_query += sql.SQL(' Where {}').format(where_clause)

            # Execute the command passing in placeholder values
            db_cursor.execute(count_query, where_values)

            result = db_cursor.fetchone()
            # Return the matched value that was deleted
            return result[0]
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Commit and close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_upsert(*, db_connection: connection, db_schema: str, db_table: str,
                       filter: dict = None, db_model: DbModel,
                       nk_model: DbModel, commit_changes: bool = True,
                       close_db_conn: bool = True) -> dict:
        """
        **execute_upsert**

        This function will perform an upsert based on the provided entity id

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity
        - `filter`: Constraint key value pairs. Usually denotes a column and a value to filter by
        - `nk_model`: Represents the natural key of a table used to determine wether to perform an insert or update
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.


        **Returns**

        dict - This is a dictionary of the data for the entity that was inserted

        """
        # TODO Add filter logic like in select with options
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()

            # build upsert query
            query_string = '''
            INSERT INTO {db_schema}.{db_table} ({columns})
            SELECT src.*
            FROM (
                VALUES ({column_values})
            ) src ({columns})
            LEFT OUTER JOIN {db_schema}.{db_table} dest ON ({joined_nk_columns})
            WHERE ({filter_clause}) OR
                ({dest_columns})
                IS DISTINCT FROM
                ({src_columns})
            ON CONFLICT ({nk_columns})
            DO UPDATE
            SET update_ts = now(), {ex_columns}
            RETURNING *
            '''

            upsert_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                columns=sql.SQL(', ').join(
                    map(sql.Identifier, db_model.get_column_names())),
                column_values=sql.SQL(', ').join(
                    sql.Placeholder() * len(db_model.get_column_values())),
                joined_nk_columns=sql.SQL(' AND ').join(
                    [sql.SQL('src.') + sql.Identifier(col) + sql.SQL(
                        ' = dest.') + sql.Identifier(col) for col in nk_model.get_column_names()]),
                filter_clause=sql.SQL(' AND ').join(
                    [sql.SQL('dest.') + sql.Identifier(col) + sql.SQL(
                        " = ") + sql.Placeholder() for col in
                     filter.keys()]) if filter else sql.SQL('1=1'),
                dest_columns=sql.SQL(', ').join(
                    [sql.SQL('dest.') + sql.Identifier(col) for col in
                     db_model.get_column_names()]),
                src_columns=sql.SQL(', ').join(
                    [sql.SQL('src.') + sql.Identifier(col) for col in db_model.get_column_names()]),
                nk_columns=sql.SQL(', ').join(
                    map(sql.Identifier, nk_model.get_column_names())),
                ex_columns=sql.SQL(', ').join([sql.Identifier(
                    col) + sql.SQL(' = EXCLUDED.') + sql.Identifier(col) for col in
                                               db_model.get_column_names()])
            )

            print(upsert_query)

            # Execute the command passing in placeholder values
            if filter:
                db_cursor.execute(
                    upsert_query,
                    db_model.get_column_values_as_tuple() + tuple(list(filter.values()), ))
            else:
                db_cursor.execute(
                    upsert_query, db_model.get_column_values_as_tuple())

            # First item in description is name, get a list of the column names
            column_names = [columns[0] for columns in db_cursor.description]
            print(column_names)

            # Fetch all gets an array of tuples with the values for each row
            # Zip together a dictionary of columns to values
            response_row = db_cursor.fetchone()
            if response_row:
                response_dict = dict(zip(column_names, response_row))
            else:
                response_dict = {}
                print("No Changes made on Upsert")

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            return response_dict

        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except psycopg2_data_integrity_exception_types as error:
            # Caller Error
            if error.pgcode == UNIQUE_VIOLATION:
                raise DbException(error_code=DbErrorCode.NATURAL_KEY_VIOLATION,
                                  error_message=error.pgerror) from error
            if error.pgcode == FOREIGN_KEY_VIOLATION:
                raise DbException(error_code=DbErrorCode.FOREIGN_KEY_VIOLATION,
                                  error_message=error.pgerror) from error
            raise DbException(error_code=DbErrorCode.DATA_INTEGRITY_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_upsert_with_filter_options(*, db_connection: connection, db_schema: str,
                                           db_table: str, filter_options: FilterOptions = None,
                                           db_model: DbModel,
                                           nk_model: DbModel, commit_changes: bool = True,
                                           close_db_conn: bool = True) -> dict:
        """
        **execute_upsert**

        This function will perform an upsert based on the provided entity id

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_table`: Database table name to be inserted into
        - `db_model`: Model for an entity
        - `filter`: Constraint key value pairs. Usually denotes a column and a value to filter by
        - `nk_model`: Represents the natural key of a table used to determine wether to perform an insert or update
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.


        **Returns**

        dict - This is a dictionary of the data for the entity that was inserted

        """
        # TODO Add filter logic like in select with options
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()

            # build upsert query
            query_string = '''
            INSERT INTO {db_schema}.{db_table} ({columns})
            SELECT src.*
            FROM (
                VALUES ({column_values})
            ) src ({columns})
            LEFT OUTER JOIN {db_schema}.{db_table} dest ON ({joined_nk_columns})
            WHERE ({filter_clause}) OR
                ({dest_columns})
                IS DISTINCT FROM
                ({src_columns})
            ON CONFLICT ({nk_columns})
            DO UPDATE
            SET update_ts = now(), {ex_columns}
            RETURNING *
            '''

            filter_clause, values = None, {}
            if filter_options:
                filter_clause, values = filter_options.generate_query_statement(alias=' dest.')

            upsert_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                columns=sql.SQL(', ').join(
                    map(sql.Identifier, db_model.get_column_names())),
                column_values=sql.SQL(', ').join(
                    sql.Placeholder(key) for key in db_model.get_column_names()),
                joined_nk_columns=sql.SQL(' AND ').join(
                    [sql.SQL('src.') + sql.Identifier(col) + sql.SQL(
                        ' = dest.') + sql.Identifier(col) for col in nk_model.get_column_names()]),
                filter_clause=filter_clause if filter_clause else sql.SQL(
                    '1=1'),
                dest_columns=sql.SQL(', ').join(
                    [sql.SQL('dest.') + sql.Identifier(col) for col in
                     db_model.get_column_names()]),
                src_columns=sql.SQL(', ').join(
                    [sql.SQL('src.') + sql.Identifier(col) for col in db_model.get_column_names()]),
                nk_columns=sql.SQL(', ').join(
                    map(sql.Identifier, nk_model.get_column_names())),
                ex_columns=sql.SQL(', ').join([sql.Identifier(
                    col) + sql.SQL(' = EXCLUDED.') + sql.Identifier(col) for col in
                                               db_model.get_column_names()])
            )

            print(upsert_query)

            values.update(db_model.as_dict())

            db_cursor.execute(
                upsert_query, values)

            # First item in description is name, get a list of the column names
            column_names = [columns[0] for columns in db_cursor.description]
            print(column_names)

            # Fetch all gets an array of tuples with the values for each row
            # Zip together a dictionary of columns to values
            response_row = db_cursor.fetchone()
            if response_row:
                response_dict = dict(zip(column_names, response_row))
            else:
                response_dict = {}
                print("No Changes made on Upsert")

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            return response_dict

        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except psycopg2_data_integrity_exception_types as error:
            # Caller Error
            if error.pgcode == UNIQUE_VIOLATION:
                raise DbException(error_code=DbErrorCode.NATURAL_KEY_VIOLATION,
                                  error_message=error.pgerror) from error
            if error.pgcode == FOREIGN_KEY_VIOLATION:
                raise DbException(error_code=DbErrorCode.FOREIGN_KEY_VIOLATION,
                                  error_message=error.pgerror) from error
            raise DbException(error_code=DbErrorCode.DATA_INTEGRITY_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_delete_by_filter(*, db_connection: connection, db_schema: str, db_table: str,
                                 filter: dict, commit_changes: bool = True,
                                 close_db_conn: bool = True) -> Any:
        """
        **execute_delete_by_filter**

        This is a hard delete. Their is nothing returned in the function

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being deleted from.
        - `filter`: Constraint key value pairs. Usually denotes a column and a value to filter by.
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        Any - This function returns the filter key value pair passed into the function

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()
            query_string = '''DELETE FROM {db_schema}.{db_table} WHERE {filter_clause} RETURNING *'''
            delete_query = sql.SQL(query_string).format(
                db_schema=sql.Identifier(db_schema),
                db_table=sql.Identifier(db_table),
                filter_clause=sql.SQL(' AND ').join(
                    [sql.Identifier(col) + sql.SQL(" = ") + sql.Placeholder() for col in
                     filter.keys()]))

            # Execute the command passing in placeholder values
            db_cursor.execute(delete_query, tuple(list(filter.values()), ))

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            # Return the matched value that was deleted
            return filter
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_function(*, db_connection: connection, db_schema: str, db_function: str,
                         input_parameters: dict = None,
                         commit_changes: bool = True, close_db_conn: bool = True) -> list[dict]:
        """
        **execute_function**

        Data core method for executing a function

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database. 
        - `db_schema`: Database schema name containing the function being executed.
        - `db_function`: Database function name being executed. 
        - `input_parameters`: A dictionary of the parameters required to run the identified function. 
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        dict - This is a dictionary of the data from the function executed. 

        """
        db_cursor = None
        try:
            db_cursor = db_connection.cursor()

            function_name_string = db_schema + "." + db_function

            # Execute the command passing in the function name and input parameters
            db_cursor.callproc(function_name_string, input_parameters)

            # Format output to send back to caller
            column_names = [columns[0] for columns in db_cursor.description]
            function_response = [dict(zip(column_names, row_values)) for row_values in
                                 db_cursor.fetchall()]

            # Commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            return function_response
        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except psycopg2_data_integrity_exception_types as error:
            # Caller Error
            raise DbException(error_code=DbErrorCode.DATA_INTEGRITY_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_function_read_by_ids(*, db_connection: connection, db_schema: str,
                                     db_table_name: str, input_parameters: list[str],
                                     parent_entity_id_name: str, parent_ids: tuple[int],
                                     function_name: str, state_as_of_ts: datetime,
                                     filter_options: FilterOptions = None,
                                     sort_options: SortOptions = None, db_model: DbModel = None,
                                     limit: int = None, offset: int = None,
                                     close_db_conn: bool = True):
        """
        **execute_function_read_by_ids**

        Data core method to call the comp.f_* functions and join in the corresponding lgad/scor data to get the corresponding

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database. 
        - `db_schema`: Database schema name of the table where data will be joined from
        - `db_table_name`: Database table where data will be joined from
        - `db_model`: Model type to input to know which fields to read
        - `db_function`: Database function name being executed. 
        - `input_parameters`: A dictionary of the parameters required to run the identified function. 
        - `parent_id_entity_name`: This is the unique identifier for the id that all of the child tables follow under
        - `parent_ids`: This is the list of the parent ids are being requested that the children calls will be filtered on
        - `state_as_of_ts`: this is the timestamp of where to get the most recent data before or at the designated time
        - `filter_options: Object to represent the filters that determine what record should be soft deleted
        - `sort_options`: sort options object that determins how to sort the response 
        - `limit`: an integer that determines the amount of values returned. 
        - `offset`: an integer shifts the values by n rows. (EX) if you want to start on row 4 set offset = 4.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        dict - This is a dictionary of the data from the function executed. 

        """

        db_cursor = None
        try:
            db_cursor = db_connection.cursor()
            from_statement = sql.SQL(
                """
                FROM(SELECT * FROM {db_schema}.{table} WHERE {parent_entity_id_name} in {parent_ids}) a
                """
            ).format(
                db_schema=sql.Identifier(db_schema),
                table=sql.Identifier(db_table_name),
                parent_entity_id_name=sql.Identifier(parent_entity_id_name),
                parent_ids=sql.Placeholder('parent_ids')

            )
            values = {}
            # Build column list to query specific fields
            if db_model:
                column_list = sql.SQL(",").join(
                    [sql.SQL("l.") + sql.Identifier(attribute.name) for attribute in
                     fields(db_model)])
            else:
                column_list = sql.SQL("l.*")

            select_query = sql.SQL("""
                SELECT {column_list}
                {from_statement}
                INNER JOIN comp.{function}({natural_keys}, {state_as_of_ts}) l ON 1=1 """).format(
                from_statement=from_statement,
                natural_keys=sql.SQL(', ').join(
                    map(lambda x: sql.SQL("a.") + sql.Identifier(x), input_parameters)),
                function=sql.Identifier(function_name),
                column_list=column_list,
                state_as_of_ts=sql.Placeholder("state_as_of_ts")
            )
            values.update({'parent_ids': parent_ids})
            values.update({'state_as_of_ts': state_as_of_ts})

            if filter_options and (
                    (filter_options.filter_options and len(filter_options.filter_options) > 0) or
                    (filter_options.nested_filter_options and len(
                        filter_options.nested_filter_options) > 0)
            ):
                filter_statement, filter_values = filter_options.generate_query_statement(alias='l.')
                select_query += sql.SQL(' Where {}').format(filter_statement)
                values.update(filter_values)

            if sort_options and len(sort_options.sort_options) > 0:
                sort_statements = []
                for sort_option in sort_options.sort_options:
                    sort_statements.append(sql.Identifier(
                        sort_option.col_name) + sql.SQL(' ') + sql.SQL(sort_option.sort_type.value))
                select_query += sql.SQL(" Order By {}").format(
                    sql.SQL(', ').join(sort_statements))

            if limit:
                select_query += sql.SQL(" Limit {}").format(
                    sql.Placeholder('limit'))
                values.update({'limit': limit})

            if offset:
                select_query += sql.SQL(" Offset {}").format(
                    sql.Placeholder('offset'))
                values.update({'offset': offset})

            print(values.values())
            db_cursor.execute(select_query, values)
            column_names = [columns[0] for columns in db_cursor.description]
            print(select_query)
            print("completed")

            response_array = [dict(zip(column_names, row_values))
                              for row_values in db_cursor.fetchall()]
            print(len(response_array))
            return response_array

        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_function_with_filter_options(*, db_connection: connection, db_schema: str, db_function: str,
                                             limit: int = None, offset: int = None, sort_options: SortOptions = None,
                                             filter_options: FilterOptions = None, db_model: DbModel = None,
                                             input_parameters: dict = None, commit_changes: bool = True,
                                             close_db_conn: bool = True) -> list[dict]:
        """
        **execute_function_with_filter_options**

        Data core method for executing a function with filter options. PLEASE NOTE this method only works when the database function returns values. If the database function you are looking to call doesn't return a value use execute_function helper.

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database. 
        - `db_schema`: Database schema name containing the function being executed.
        - `db_function`: Database function name being executed. 
        - `filter_options`: an object that can be used to represent the filter conditions of a query
        - `limit`: an integer that determines the amount of values returned. 
        - `offset`: an integer shifts the values by n rows. (EX) if you want to start on row 4 set offset = 4.
        - `sort_options`: sort options object that determins how to sort the response
        - `db_model`: Model type to input to know which fields to read
        - `input_parameters`: A dictionary of the parameters required to run the identified function. 
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        dict - This is a dictionary of the data from the function executed. 

        """
        # TODO update this helper to utilize ParameterGroup class as seen in execute_stored_procedure
        db_cursor = None
        try:
            # open cursor
            db_cursor = db_connection.cursor()
            values = {}

            # build column list to query specific fields
            if db_model:
                column_list = sql.SQL(",").join(
                    [sql.SQL("f.") + sql.Identifier(attribute.name) for attribute in
                     fields(db_model)])
            else:
                column_list = sql.SQL("f.*")

            # build parameters list for function
            state_as_of_ts = None
            input_list = []

            for k, v in input_parameters.items():
                if v and k != 'input_state_as_of_ts':
                    input_list.append("{}=>'{}'".format(k, str(v)))
                elif not v:
                    input_list.append(k + '=>NULL')
                elif k == 'input_state_as_of_ts':
                    state_as_of_ts = v

            # format query string
            query_string = sql.SQL(
                """SELECT {column_list} FROM {db_schema}.{db_function}({function_params}, {state_ts}) f"""
            ).format(
                column_list=column_list,
                db_schema=sql.Identifier(db_schema),
                db_function=sql.Identifier(db_function),
                function_params=sql.SQL(', '.join(input_list)),
                state_ts=sql.SQL('input_state_as_of_ts=>') + sql.Placeholder("input_state_as_of_ts")
            )
            values.update({'input_state_as_of_ts': state_as_of_ts})

            if filter_options and (
                    (filter_options.filter_options and len(filter_options.filter_options) > 0) or
                    (filter_options.nested_filter_options and len(
                        filter_options.nested_filter_options) > 0)
            ):
                filter_statement, filter_values = filter_options.generate_query_statement()
                query_string += sql.SQL(' WHERE {}').format(filter_statement)
                values.update(filter_values)

            # add sort options to query string if provided

            if sort_options and len(sort_options.sort_options) > 0:
                sort_statements = []
                for sort_option in sort_options.sort_options:
                    sort_statements.append(sql.Identifier(
                        sort_option.col_name) + sql.SQL(' ') + sql.SQL(sort_option.sort_type.value))
                query_string += sql.SQL(" ORDER BY {}").format(
                    sql.SQL(', ').join(sort_statements))

            # add limit to query string if provided
            if limit:
                query_string += sql.SQL(" LIMIT {}").format(
                    sql.Placeholder('limit'))
                values.update({'limit': limit})

            # add offset to query string if provided
            if offset:
                query_string += sql.SQL(" OFFSET {}").format(
                    sql.Placeholder('offset'))
                values.update({'offset': offset})

            # execute query string
            print(values.values())
            db_cursor.execute(query_string, values)
            column_names = [columns[0] for columns in db_cursor.description]
            print(query_string)
            print("completed")

            # format response
            response_array = [dict(zip(column_names, row_values))
                              for row_values in db_cursor.fetchall()]
            print(len(response_array))

            # commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

            return response_array

        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_get_table_function_size_with_filter_options(*, db_connection: connection, db_schema: str,
                                                            db_function: str, input_parameters: dict = None,
                                                            filter_options: FilterOptions = None,
                                                            close_db_conn: bool = True) -> int:

        """
        **execute_get_table_function_size_with_filter_options**

        Get the size of a table function response

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database.
        - `db_schema`:  Database schema name containing the table being inserted into
        - `db_function`: Database function name being executed. 
        - `input_parameters`: A dictionary of the parameters required to run the identified function. 
        - `filter_options`: an object that can be used to represent the filter conditions of a query
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        **Returns**

        int - Table size

        """
        # TODO update this helper to utilize ParameterGroup class as seen in execute_stored_procedure
        db_cursor = None
        try:
            # open cursor
            db_cursor = db_connection.cursor()
            values = {}

            # build parameters list for function
            state_as_of_ts = None
            input_list = []

            for k, v in input_parameters.items():
                if v and k != 'input_state_as_of_ts':
                    input_list.append("{}=>'{}'".format(k, str(v)))
                elif not v:
                    input_list.append(k + '=>NULL')
                elif k == 'input_state_as_of_ts':
                    state_as_of_ts = v

            # format query string
            query_string = sql.SQL(
                """SELECT COUNT(*) FROM {db_schema}.{db_function}({function_params}, {state_ts}) f"""
            ).format(
                db_schema=sql.Identifier(db_schema),
                db_function=sql.Identifier(db_function),
                function_params=sql.SQL(', '.join(input_list)),
                state_ts=sql.SQL('input_state_as_of_ts=>') + sql.Placeholder("input_state_as_of_ts")
            )
            values.update({'input_state_as_of_ts': state_as_of_ts})

            if filter_options and (
                    (filter_options.filter_options and len(filter_options.filter_options) > 0) or
                    (filter_options.nested_filter_options and len(
                        filter_options.nested_filter_options) > 0)
            ):
                filter_statement, filter_values = filter_options.generate_query_statement()
                query_string += sql.SQL(' WHERE {}').format(filter_statement)
                values.update(filter_values)

            # execute query string passing in placeholder values
            db_cursor.execute(query_string, values)
            result = db_cursor.fetchone()

            # return the result (number of records returned with the given function parameters)
            return result[0]

        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Commit and close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()

    @staticmethod
    def execute_stored_procedure(*, db_connection: connection, db_schema: str, db_stored_procedure: str,
                                 input_parameters: dict = None, commit_changes: bool = True,
                                 close_db_conn: bool = True):
        """
        **execute_stored_procedure**

        Data core method for executing a stored procedure

        **Parameters**

        - `db_connection`: This parameter is a pyscopg2.connection to the database. 
        - `db_schema`: Database schema name containing the function being executed.
        - `db_stored_procedure`: Database stored procedure name being executed. 
        - `input_parameters`: A dictionary of the parameters required to run the identified function. 
        - `commit_changes`: This parameter is a boolean that allows you to decide whether to commit your changes to the Database. Setting this to false enables the ability to rollback changes.
        - `close_db_conn`: This parameter allows you to keep the connection open to be able to pass it to different helpers.

        """
        db_cursor = None
        try:
            # open cursor
            db_cursor = db_connection.cursor()

            # format initial query string
            query_string = sql.SQL('CALL {db_schema}.{db_stored_procedure} ').format(
                db_schema=sql.Identifier(db_schema), db_stored_procedure=sql.Identifier(db_stored_procedure))

            # build parameters list for stored procedure
            parameters_list = []
            values = {}

            for key, value in input_parameters.items():
                parameters_list.append(Parameter(col_name=key, col_value=value))

            if parameters_list and len(parameters_list) > 0:
                params = ParameterGroup(param_group=parameters_list)
                if params and params.param_group and len(params.param_group) > 0:
                    param_statement, param_values = params.generate_param_query_statement()
                    query_string += sql.SQL('({})').format(param_statement)
                    values.update(param_values)

            # execute query string
            print(query_string.as_string(db_connection))
            db_cursor.execute(query_string, values)
            print("completed")

            # commit the changes unless overridden by caller
            if commit_changes:
                db_connection.commit()

        except psycopg2_internal_exception_types as error:
            # Internal Error
            raise DbException(error_code=DbErrorCode.INTERNAL_DATA_EXCEPTION,
                              error_message=error.pgerror) from error
        except Exception as error:
            # Unexpected Error
            raise DbException(error_code=DbErrorCode.UNHANDLED_ERROR,
                              error_message=str(error)) from error
        finally:
            # Always close the cursor
            if db_cursor:
                db_cursor.close()
            # Close the DB connection unless overridden by caller
            if db_connection and close_db_conn and db_connection.closed == 0:
                db_connection.close()
