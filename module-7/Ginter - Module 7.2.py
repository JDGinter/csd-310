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
    cursor.execute("SELECT * FROM studio")
    studios = cursor.fetchall()

    print("----Studios----")
    for studio in studios:
        print("Studio ID: {}\nStudio Name: {}\n".format(studio[0], studio[1]))



    #Genre
    cursor.execute("SELECT * FROM genre")
    genres = cursor.fetchall()

    print("----Genres----")
    for genre in genres:
        print("Genre ID: {}\nGenre Name: {}\n".format(genre[0], genre[1]))

    #Movies < 2 hours

    cursor.execute("SELECT film_name, film_runtime FROM film WHERE film_runtime < 120")
    films = cursor.fetchall()

    print("----Films < 2 Hours----")
    for film in films:
        print("Film: {}\nRuntime: {}\n".format(film[0], film[1]))

    #Films by Director
    cursor.execute("SELECT GROUP_CONCAT(film_name), film_director FROM film GROUP BY film_director, film_name")
    films = cursor.fetchall()


    print("----Films By Director----")
    for film in films:
        print("Film Name: {}\nFilm Director: {}\n".format(film[0], film[1]))

    input("\n\n  Press any key to continue...")
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