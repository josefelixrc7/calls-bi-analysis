
import mysql.connector

class Connection:
    def __init__(self, host, user, password, name, port=3306):
        self.host = host
        self.user = user
        self.password = password
        self.name = name
        self.port = port

    def Connect_(self):
        return mysql.connector.connect(
            host=self.host
            ,user=self.user
            ,passwd=self.password
            ,db=self.name
            ,port=self.port
        )