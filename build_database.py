import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="movieuser",
  password="password"
)
mycursor = mydb.cursor()
mycursor.execute("DROP DATABASE IF EXISTS moviedatabase")
mycursor.execute("CREATE DATABASE moviedatabase")
