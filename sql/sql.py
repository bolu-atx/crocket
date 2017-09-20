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

    def select_query(self, table, columns):

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

        self.connection.close()
