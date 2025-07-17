import mysql.connector
from mysql.connector import errorcode

import dotenv
from dotenv import dotenv_values

secrets = dotenv_values(".env")

""" database config object """
config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True
}
try:

    db = mysql.connector.connect(**config)

    # output the connection status
    print("\n  Database user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"],
                                                                                       config["database"]))
    #Studio
    cursor = db.cursor()
    def show_films(cursor, title):
        cursor.execute("SELECT film_name AS Name, film_director AS Director, genre_name AS Genre, studio_name AS "
                       "'Studio Name' FROM film INNER JOIN genre ON film.genre_id = genre.genre_id INNER JOIN studio "
                       "ON film.studio_id = studio.studio_id")

        films = cursor.fetchall()

        print("\n  --  {}  --".format(title))
        for film in films:
            print("Film Name: {}\nDirector: {}\nGenre Name ID: {}\nStudio Name: {}\n"
                  .format(film[0], film[1], film[2], film[3]))

    #Displaying Films
    DF = "DISPLAYING FILMS"
    show_films(cursor, DF)

    #Insert
    cursor.execute("INSERT INTO film "
                   "(film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id) "
                   "VALUES ('Sinister', 2012, 109, 'Scott Derrickson', 2, 1)")

    DF = "DISPLAYING FILMS AFTER INSERT"
    show_films(cursor, DF)

    #Update

    cursor.execute("UPDATE film SET genre_id = 1 WHERE film_name = 'Alien'")
    DF = "DISPLAYING FILMS AFTER UPDATE - Changed Alien to Horror"
    show_films(cursor, DF)

    #Delete

    cursor.execute("DELETE FROM film WHERE film_name = 'Gladiator'")

    DF = "DISPLAYING FILMS AFTER DELETE"
    show_films(cursor, DF)

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")

    else:
        print(err)

finally:
    db.close()
    print("Database Closed Successfully")