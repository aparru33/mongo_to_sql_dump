import logging
from contextlib import contextmanager
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.session import Session
from dotenv import load_dotenv
from orms.base import Base
# # Import all modules in the models package
# for _, module_name, _ in pkgutil.iter_modules(models.__path__):
#     module = importlib.import_module(f'models.{module_name}')

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCHEMA = "app"
class DatabaseConnector:
    def __init__(self, db_url=None):
        if db_url is None:

            ###### MODIFY THIS PART TO GET VALUES FROM ENVIRONMENT VARIABLES ######
            db_name = "admin"
            db_user = "postgres"
            db_pass = "password!!"
            db_host = "11.222.255.33"  # "localhost" for a local database
            db_port = "5432"  # "5432" is the default port
            ####### It should be somethink like this : ############################

            # # Use environment variables to get database credentials
            # db_name = os.environ.get('DB_NAME')
            # db_user = os.environ.get('DB_USER')
            # db_pass = os.environ.get('DB_PASS')
            # db_host = os.environ.get('DB_HOST')
            # db_port = os.environ.get('DB_PORT')


            # if None in (db_name, db_user, db_pass, db_host, db_port):
            #     raise ValueError("Missing necessary database credentials")

            db_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        #create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}')
        self.engine = create_engine(db_url, echo=False, pool_pre_ping=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)


    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations. 
        handles the commit and rollback of transactions
        It's mandatory to use this method to handle the session and the transactions.
        always use it with the with statement and use the methods in this 
        class inside the with block
        """
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    ######## CRUD METHODS ########
    def execute_query(self,session, query: str, params=None):
        """
        Execute a raw SQL query.
        """
        try:
            logger.info("Executing query: %s with params: %s", query , params)
            result = session.execute(text(query), params)
            return result
        except Exception as e:
            logger.error("Error executing query: %s", e)
            return None

    # def get_id_from_table_by_name(self,session, table_name: str, id_col_name: str, data: dict):
    #     conditions = ' AND '.join([f"{col} = :{col}" for col in data.keys()])
    #     query = f"SELECT {id_col_name} FROM {SCHEMA}.{table_name} WHERE {conditions}"
    #     result = self.execute_query(session,query, data)
    #     id_value = result.scalar() if result else None
    #     print(f"ID value from {table_name}: {id_value}")
    #     return id_value
    def get_id_from_table_by_name(
            self, session: Session, table_name: str, id_col_name: str, data: dict
            ) -> Optional[int]:
        try:
            table = Base.metadata.tables.get(f"{SCHEMA}.{table_name}")
            if table is None:
                raise ValueError(f"Table {SCHEMA}.{table_name} does not exist")

            conditions = [getattr(table.c, col) == val for col, val in data.items()]
            query = session.query(table.c[id_col_name]).filter(*conditions)
            print(query)
            id_value = query.scalar()

            logger.info("ID value from %s : %s", {table_name}, {id_value})
            return id_value

        except SQLAlchemyError as e:
            logger.error("Database error: %s",e)
            return None
        except Exception as e:
            logger.error("An error occurred: %s",e)
            return None


    def insert_into_table_by_name(self,session, table_name: str, id_col_name: str, data: dict):
        _id = self.get_id_from_table_by_name(session, table_name, id_col_name, data)
        if _id:
            logger.info("Record already exists with ID: %s", _id)
            return {"status": "exists", "id": _id}

        cols = ', '.join(data.keys())
        placeholders = ', '.join([f":{col}" for col in data.keys()])
        query = f"INSERT INTO {SCHEMA}.{table_name} ({cols}) VALUES ({placeholders}) RETURNING {id_col_name}"
        result = self.execute_query(session, query, data)
        inserted_id = result.scalar() if result else None

        if inserted_id:
            logger.info("Record inserted with ID: %s", inserted_id)
            return {"status": "ok", "id": inserted_id}

        logger.error("Failed to insert record")
        return {"status": "error"}


    def delete_by_id(self,session,table_name:str, id_col_name:str,_id:int):
        try:
            self.execute_query(
                session,
                f"DELETE FROM {SCHEMA}.{table_name} WHERE {id_col_name} = :{id_col_name}_x",
                {f"{id_col_name}_x" : str(_id)}
            )
            return {"status": "ok"}
        except Exception as e:
            logger.error("Error deleting record: %s", e)
            return {"status": "error", "message": str(e)}

    def update_table_by_id(self, session,table_name: str, id_col_name: str, _id: int, data: dict):
        try:
            # Creating a string of assignments for the SQL query
            assignments = ', '.join([f"{col} = :{col}" for col in data.keys()])

            # Creating the full SQL query string
            sql_query = f"UPDATE {SCHEMA}.{table_name} SET {assignments} WHERE {id_col_name} = :id"

            # Executing the query
            self.execute_query(
                session,
                sql_query,
                id=_id,
                **data
            )
            return {"status": "ok"}
        except Exception as e:
            logger.error("Error updating record: %s", e)
            return {"status": "error", "message": str(e)}

    ######## ORM METHODS ########
    def add_object(self, session,obj):
        try:
            session.add(obj)
            return {"status": "ok"}
        except Exception as e:
            logger.error("Error adding object: %s", e)
            return {"status": "error", "message": str(e)}

    def get_object_by_id(self,session, model_class, col_name_id_val:dict):
        try:
            obj = session.query(model_class).filter_by(**col_name_id_val).one_or_none()
            return obj
        except Exception as e:
            logger.error("Error getting object: %s", e)
            return None

    def get_objects_by_column_value(self,session, model_class, column_name, value):
        try:
            filter_kwargs = {column_name: value}
            # This creates a dictionary like {'column_name': value},
            # allowing dynamic column filtering
            return session.query(model_class).filter_by(**filter_kwargs).all()
        except Exception as e:
            logger.error("Error getting objects: %s", e)
            return None

    def delete_object_by_id(self,session, model_class, object_id):
        obj = self.get_object_by_id(session,model_class, object_id)
        if obj:
            session.delete(obj)
            return {"status": "ok"}
        return {"status": "error", "message": "Object not found"}

    def update_object_by_id(self, session,model_class, _filter, update_data):
        obj = session.query(model_class).filter_by(**_filter).one_or_none()
        if obj:
            for key, value in update_data.items():
                setattr(obj, key, value)
            return {"status": "ok"}
        return {"status": "error", "message": "Object not found"}
