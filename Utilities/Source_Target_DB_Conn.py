from sqlalchemy import create_engine
import cx_Oracle
def MySQL_DB_Conn():
    mysql_host = "localhost"
    mysql_user = "root"
    mysql_pwd = "admin"
    mysql_source_db = "source"
    source_db_conn = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_pwd}@{mysql_host}/{mysql_source_db}")
    return source_db_conn

def Oracle_DB_Conn():
    oracle_host = "localhost"
    oracle_port = 1521
    oracle_servicename = "orcll"
    oracle_user = "target"
    oracle_password = "target"
    target_db_conn = create_engine(f"oracle+cx_oracle://{oracle_user}:{oracle_password}@{cx_Oracle.makedsn(oracle_host,oracle_port,service_name=oracle_servicename)}")
    return target_db_conn


