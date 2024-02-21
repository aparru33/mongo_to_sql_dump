# %%
import logging
import pymongo
import sqlalchemy

# .mongo_client import MongoClient
# from pymongo.server_api import ServerApi

# MongoDB connection settings
mongodb_host = "mongodb+srv://dev_read_and_write:host/rep?retryWrites=true&w=majority"
mongodb_db = "db"

# PostgresSQL connection settings
# use a more secure way in prod
username = "name"
password = "password!!"
host = "11.222.255.33"  # "localhost" for a local database
port = "5432"  # "5432" is the default port
dbname = "dbname"

logging.basicConfig(filename='dir/migration_db.log', encoding='utf-8', level=logging.DEBUG )
logger = logging.getLogger(__name__)
# %%
def connect_postgres():
    return sqlalchemy.create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
    )


def migrate_data(db_client, sql_conn):
    logger.info("migrating datas")

    get_conf_id_stm = (
        "SELECT id FROM app.table WHERE name = :name"
    )

    insert_table_stm = (
        "INSERT INTO app.table (name, id, created_at, modified_at) "
        + "VALUES (:name, :id,  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id"
    )
    doc_collection = db_client["docs"]
    for doc in doc_collection.find():
        try:
            logger.info("inserting name %s", doc_collection["name"])
            name = doc_collection["configuration"]
            id = sql_conn.execute(
                sqlalchemy.text(get_conf_id_stm), {"name": name}
            ).scalar()
            if not id:
                print(f"configuration {name} not found")
            else:
                sql_conn.execute(
                    sqlalchemy.text(insert_table_stm),
                    {
                        "name": doc["organization_name"],
                        "id": id,
                    },
                )
        except Exception as e:
            logger.error("error while inserting organization %s", doc["organization_name"])
            logger.error(e)

def trad_boolen(value):
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
    if isinstance(value, int):
        if value == 0:
            return False
        if value == 1:
            return True
    else:
        print("error boolean", value)
        return False


def migrate_users(db_client, sql_conn):
    logger.info("migrating users")
    insert_user_stm = (
        "INSERT INTO app.users (user_email ) "
        + "VALUES (:user_email ) RETURNING user_id"
    )
    user_collection = db_client["users"]
    for user in user_collection.find():
        logger.info("inserting user %s", user["user_email"])
        print("inserting user", user["user_email"])
        role_id = None
        try:
            sql_conn.execute(
                sqlalchemy.text(insert_user_stm),
                {
                    "user_email": user["user_email"]
                },
            )
        except Exception as e:
            logger.error("error while inserting user %s", user["user_email"])
            logger.error(e)


def get_user_id(sql_conn, user_email):
    logger.info("getting user_id")
    try:
        get_user_id_stm = "SELECT user_id FROM app.users WHERE user_email = :user_email"
        user_id = sql_conn.execute(
            sqlalchemy.text(get_user_id_stm), {"user_email": user_email}
        ).scalar()
        if not user_id:
            logger.info("user %s not found", user_email)
            print(f"user {user_email} not found")
        else:
            logger.info("user_id found %s", str(user_id))
    except Exception as e:
        logger.error("error while getting user_id for %s", user_email)
        logger.error(e)
        return None
    return user_id


def migrate_log(db_client, sql_conn):
    logger.info("migrating logs")
    insert_log_stm = (
        "INSERT INTO app.logs (file_id, date_hour, user_id,file_name) "
        + "VALUES (:file_id, :date_hour, :user_id, :file_name) RETURNING log_id"
    )
    log_collection = db_client["logs"]
    for log in log_collection.find():
        try:
            user_id = get_user_id(sql_conn, log["user_email"])
            if user_id:
                sql_conn.execute(
                    sqlalchemy.text(insert_log_stm),
                    {
                        "file_id": log.get("file_id"),
                        "user_id": user_id,
                        "file_name": log.get("file_name"),
                    },
                )
        except Exception as e:
            logger.error("error while inserting log %s", log["file_id"])
            logger.error(e)


# %%
try:
    logger.info("connecting to MongoDB")
    # connect to mongo
    mongo_client = pymongo.MongoClient(
        mongodb_host, server_api=pymongo.server_api.ServerApi("1")
    )
    mongo_db = mongo_client[mongodb_db]
    logger.info("connected to MongoDB")
    # sql connection
    # Create a connection pool
    logger.info("connecting to PostgreSQL")
    db_pool = connect_postgres()

    # Obtain a connection from the pool
    sql_conn = db_pool.connect()
    logger.info("connected to PostgreSQL")
    print("Connected to PostgreSQL!")
    for fct in [migrate_users, migrate_log]:
        fct(mongo_db, sql_conn)
        sql_conn.commit()
    migrate_users(mongo_db, sql_conn)
    migrate_log(mongo_db, sql_conn)
    sql_conn.commit()
    logger.info("commit done")
    logger.info("Data migration successful!")
    print("Data migration successful!")

except Exception as e:
    logger.error("error while migrating data")
    logger.error(e)

finally:
    logger.info("closing connection")
    mongo_client.close()
    sql_conn.close()
    logger.info("connection closed")
    logger.info("Migration done !")

# %%
