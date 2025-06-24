from decouple import config
from flask_mysqldb import MySQL
import pymysql
import traceback

# Logger
from src.utils.Logger import Logger


def get_connection():
    try:
        return pymysql.connect(
            host=config('MYSQL_HOST'),
            user=config('MYSQL_USER'),
            # password=config('MYSQL_PASSWORD'),
            db=config('MYSQL_DB'),
            #password=app.config["MYSQL_PASSWORD"],
            ssl={"ca": None}  # Deshabilita SSL en PyMySQL
        )
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())



