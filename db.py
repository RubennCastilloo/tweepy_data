import mysql.connector

database = mysql.connector.connect(
    host="localhost",
    port="8889",
    user="root",
    passwd="root",
    unix_socket='/Applications/MAMP/tmp/mysql/mysql.sock', # Para conectar a la base de datos en MacOS o Linux
    database="twitter"
)

class DataBase():

    def __init__(self):

        self.cursor = database.cursor(buffered=True)
        
    
    def insert_data(self, table, tweets):
        
        if table == "tweets":
            self.cursor.execute('SET NAMES utf8mb4;')
            self.cursor.execute('SET CHARACTER SET utf8mb4;')
            self.cursor.execute('SET character_set_connection=utf8mb4;')
            self.cursor.executemany("INSERT INTO tweets VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tweets)
            database.commit()

        if table == "retweets":
            self.cursor.execute('SET NAMES utf8mb4;')
            self.cursor.execute('SET CHARACTER SET utf8mb4;')
            self.cursor.execute('SET character_set_connection=utf8mb4;')
            self.cursor.execute("SELECT id, retweet_count FROM retweets WHERE tweet = %s", (tweets[0][0],))
            rows = self.cursor.fetchall()
            if rows:
                number_of_retweets = rows[0][1] + 1
                self.cursor.execute("UPDATE retweets SET retweet_count = %s WHERE id = %s", (number_of_retweets, rows[0][0]))
                database.commit()
            else:
                self.cursor.execute('SET NAMES utf8mb4;')
                self.cursor.execute('SET CHARACTER SET utf8mb4;')
                self.cursor.execute('SET character_set_connection=utf8mb4;')
                self.cursor.executemany("INSERT INTO retweets VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0)", tweets)
                database.commit()
