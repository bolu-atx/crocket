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

    def execute_query(self, query):
        """
        Execute a query
        :param query:
        :return:
        """

        with closing(self.connection.cursor()) as cursor:

            try:
                cursor.execute(query)
            except (OperationalError, ProgrammingError) as e:

                self.connection.rollback()

                if self.logger:
                    self.logger.debug(e)

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

                self.connection.rollback()

                if self.logger:
                    self.logger.debug(e)

    def insert_transaction_query(self, entries):
        """
        Execute multiple insert queries in a single transaction
        :param entries: tuple(market, columns, values)
        :return:
        """

        query = ['INSERT INTO `{}` ({}) VALUES ({})'.format(entry[0], ','.join(entry[1]), ','.join(map(lambda x: "'{}'".format(str(x)), entry[2])))
                 for entry in entries]

        query = '{};COMMIT;'.format(';'.join(query))

        with closing(self.connection.cursor()) as cursor:

            try:
                cursor.execute(query)
            except (OperationalError, ProgrammingError) as e:

                self.connection.rollback()

                if self.logger:
                    self.logger.debug(e)

    def select_query(self, table, columns, condition=''):
        """
        Execute a select query.
        :param table:
        :param columns:
        :param condition:
        :return:
        """
        if isinstance(columns, list):

            formatted_columns = ','.join(columns)

        elif isinstance(columns, str) and columns == '*':

            formatted_columns = columns

        else:

            print('Columns must be list of column names or "*"')
            return

        query = 'SELECT {} from `{}` {}'.format(formatted_columns, table, condition)

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
                'price DECIMAL(14,8) UNSIGNED NOT NULL,' \
                'wprice DECIMAL(14,8) UNSIGNED NOT NULL,' \
                'base_volume DECIMAL(14,8) UNSIGNED NOT NULL,' \
                'buy_volume DECIMAL(14,8) UNSIGNED NOT NULL,' \
                'sell_volume DECIMAL(14,8) UNSIGNED NOT NULL,' \
                'buy_order MEDIUMINT UNSIGNED NOT NULL,' \
                'sell_order MEDIUMINT UNSIGNED NOT NULL)'.format(table_name)

        self.execute_query(query)

    def create_trade_table(self, table_name):
        """
        Execute a create table query with sic columns: (DATETIME, PRICE, WPRICE, BASEVOLUME, BUYORDER, SELLORDER).
        :param table_name: Table name
        :return:
        """
        query = 'CREATE TABLE IF NOT EXISTS `{}` (' \
                'id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, ' \
                'market CHAR(11) NOT NULL, ' \
                'buy_time DATETIME NOT NULL, ' \
                'buy_price DECIMAL(9,8) UNSIGNED NOT NULL, ' \
                'buy_total DECIMAL(9,8) UNSIGNED NOT NULL, ' \
                'sell_time DATETIME NOT NULL, ' \
                'sell_price DECIMAL(9,8) UNSIGNED NOT NULL, ' \
                'sell_total DECIMAL(9,8) UNSIGNED NOT NULL, ' \
                'profit DECIMAL(9,8) SIGNED NOT NULL, ' \
                'percent DECIMAL(7,4) SIGNED NOT NULL)'.format(table_name)

        self.execute_query(query)

    def create_database(self, database_name):
        """
        Execute a create database query.
        :param database_name: Database name
        :return:
        """
        query = 'CREATE DATABASE IF NOT EXISTS {}'.format(database_name)

        self.execute_query(query)

    def create_analysis_table(self, table_name):
        """
        Execute a create table query with two columns: (DATETIME, PRICE).
        :param table_name: Table name
        :return:
        """
        query = 'CREATE TABLE `{}` (time DATETIME NOT NULL PRIMARY KEY, price DECIMAL(9,8) NOT NULL)'.format(table_name)

        self.execute_query(query)

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
                return tables

            except (OperationalError, ProgrammingError) as e:

                if self.logger:
                    self.logger.debug(e)

    def close(self):
        """
        Close the database connection
        :return:
        """
        self.connection.close()
