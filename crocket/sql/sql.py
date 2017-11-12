from contextlib import closing
from itertools import chain

from MySQLdb import connect, OperationalError, ProgrammingError


class Database:
    """
    The database object.
    """

    def __init__(self,
                 hostname,
                 username,
                 password,
                 database_name,
                 logger=None):

        self.database_name = database_name
        self.logger = logger

        try:
            self.connection = connect(host=hostname,
                                      user=username,
                                      passwd=password,
                                      db=database_name)
        except OperationalError:
            self.connection = None

    def insert_query(self, table, tuples):
        """
        Execute an insert query.
        :param table:
        :param tuples:
        :return:
        """
        columns, data = zip(*tuples)

        formatted_columns = ','.join(columns)

        data_format = ','.join(['%s'] * len(columns))

        query = 'INSERT INTO `{}` ({}) VALUES ({})'.format(table, formatted_columns, data_format)

        with closing(self.connection.cursor()) as cursor:

            try:

                cursor.execute(query, data)
                self.connection.commit()

            except (OperationalError, ProgrammingError) as e:

                if self.logger:
                    self.logger.debug(e)

    def insert_transaction_query(self, entries):
        """
        Execute multiple insert queries in a single transaction
        :param entries: tuple(market, columns, values)
        :return:
        """
        print(entries)
        query = ['INSERT INTO `{}` ({}) VALUES ({})'.format(entry[0], ','.join(entry[1]), ','.join(map(lambda x: "'{}'".format(str(x)), entry[2])))
                 for entry in entries]
        print(query)
        query = 'START TRANSACTION;{};COMMIT;'.format(';'.join(query))
        print(query)        

        with closing(self.connection.cursor()) as cursor:

            try:

                cursor.execute(query)
                self.connection.commit()

            except (OperationalError, ProgrammingError) as e:

                if self.logger:
                    self.logger.debug(e)

    def select_query(self, table, columns):
        """
        Execute a select query.
        :param table:
        :param columns:
        :return:
        """
        if isinstance(columns, list):

            formatted_columns = ','.join(columns)

        elif isinstance(columns, str) and columns == '*':

            formatted_columns = columns

        else:

            print('Columns must be list of column names or "*"')
            return

        query = 'SELECT {} from `{}`'.format(formatted_columns, table)

        with closing(self.connection.cursor()) as cursor:

            try:

                cursor.execute(query)
                entries = list(cursor)
                return entries

            except OperationalError as e:

                if self.logger:
                    self.logger.debug(e)

    def create_price_table(self, table_name):
        """
        Execute a create table query with sic columns: (DATETIME, PRICE, WPRICE, BASEVOLUME, BUYORDER, SELLORDER).
        :param table_name: Table name
        :return:
        """
        query = 'CREATE TABLE IF NOT EXISTS `{}` (' \
                'time DATETIME NOT NULL PRIMARY KEY, ' \
                'price DECIMAL(9,8) UNSIGNED NOT NULL,' \
                'wprice DECIMAL(9,8) UNSIGNED NOT NULL,' \
                'basevolume DECIMAL(9,8) UNSIGNED NOT NULL,' \
                'buyorder MEDIUMINT UNSIGNED NOT NULL,' \
                'sellorder MEDIUMINT UNSIGNED NOT NULL)'.format(table_name)

        with closing(self.connection.cursor()) as cursor:

            try:
                cursor.execute(query)
            except OperationalError as e:
                if self.logger:
                    self.logger.debug(e)

    def create_analysis_table(self, table_name):
        """
        Execute a create table query with two columns: (DATETIME, PRICE).
        :param table_name: Table name
        :return:
        """
        query = 'CREATE TABLE `{}` (time DATETIME NOT NULL PRIMARY KEY, price DECIMAL(9,8) NOT NULL)'.format(table_name)

        with closing(self.connection.cursor()) as cursor:

            try:
                cursor.execute(query)
            except OperationalError as e:
                if self.logger:
                    self.logger.debug(e)

    def get_all_tables(self):
        """
        Get all tables in current database.
        :return: (list)
        """
        query = 'select table_name from information_schema.tables where table_schema="{}"'.format(self.database_name)

        with closing(self.connection.cursor()) as cursor:

            try:

                cursor.execute(query)
                tables = list(chain.from_iterable(cursor))

            except (OperationalError, ProgrammingError) as e:

                if self.logger:
                    self.logger.debug(e)

        return tables

    def close(self):
        """
        Close the database connection
        :return:
        """
        self.connection.close()
