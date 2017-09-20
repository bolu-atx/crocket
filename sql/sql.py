from MySQLdb import connect, OperationalError, ProgrammingError


class Database:

    def __init__(self,
                 hostname,
                 username,
                 password,
                 database_name):

        try:
            self.connection = connect(host=hostname,
                                      user=username,
                                      passwd=password,
                                      db=database_name)
        except OperationalError:
            self.connection = None

    def insert_query(self, table, tuples):

        columns, data = zip(*tuples)
        formatted_columns = ','.join(columns)
        data_format = ['%s'] * len(columns)

        query = 'INSERT INTO {} ({}) VALUES ({})'.format(table, formatted_columns, data_format)

        cursor = self.connection.cursor()

        try:
            cursor.execute(query, data)
            self.connection.commit()

        except (OperationalError, ProgrammingError) as e:

            cursor.close()
            print(e.error)
            return

        cursor.close()

    def select_query(self, table, columns):
        """
        Execute a select query
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

        query = 'SELECT {} from {}'.format(formatted_columns, table)

        cursor = self.connection.cursor()

        try:
            cursor.execute(query)
            result = cursor.fetchall()
        except (OperationalError, ProgrammingError) as e:

            cursor.close()
            print(e.error)
            return

        cursor.close()

        return result

    def close(self):
        """
        Close the database connection
        :return:
        """
        self.connection.close()
