import pandas as pd
import json
from datetime import datetime
from Utilities.Source_Target_DB_Conn import MySQL_DB_Conn, Oracle_DB_Conn
from Utilities.logging import Logs

source_db_conn = MySQL_DB_Conn()
target_db_conn = Oracle_DB_Conn()

#Calling Log_gen function from Utilities.
dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logger=Logs.Log_Gen(f'C:\\NSR_Python_Projects\\Python_Project_Version2\\ETL_Testing_Framework(Config Driven Approach)\\Logs\\Cost_dim_{dt}.log')


with open(r'C:\NSR_Python_Projects\Python_Project_Version2\ETL_Testing_Framework(Config Driven Approach)\Config\SQL_Queries_config\Cost_dim.json','r') as Cost_data_SQL_fle:
    Cost_SQL_Queries = json.load(Cost_data_SQL_fle)

def Source_Target_Count_check(source_db_conn,target_db_conn,Cost_SQL_Queries,logger):
    source_result = pd.read_sql(Cost_SQL_Queries['count_comparison']['source_query'],source_db_conn)
    target_result = pd.read_sql(Cost_SQL_Queries['count_comparison']['target_query'],target_db_conn)

    #print(source_result)
    #print(target_result)

#source Count checking
    if source_result.empty: #Will result true or false
        source_count=0
    else:
        source_count = source_result.iloc[0,0]
    #print(source_count)

#Target Count checking
    if target_result.empty: #This will result True or False
        target_count=0
    else:
        target_count = target_result.iloc[0,0]

    print(target_result)

#Comparing Source & Target record counts:
    if source_count == target_count:
        Result_status = 'Source & Target counts matched'
        status='PASS'
    else:
        Result_status = 'Source & Target counts not matched'
        status='FAIL'

#Defining a format of output result to write into excel file
    Df_count_result = pd.DataFrame(
        {
        "Table":["Source Table","Target Table"],
        "Count":[source_count,target_count],
        "Result":[Result_status,Result_status],
        "Status":[status,status]
    }
    )

    logger.info(f"Source Cost table Count:{source_count}")
    logger.info(f"Target cost_dim table Count: {target_count}")
    logger.info(f"Source & Target Count validation status: {status}")
    logger.info("Source & Target Record Count Validation Successfully Completed!")
    return Df_count_result

#Checking Null values in Product_dim table.

def Null_Checks(target_db_conn,Product_SQL_Queries,logger):
    target_result = pd.read_sql(Product_SQL_Queries['null_check']['target_query'],target_db_conn)

    if target_result.empty:
        null_count = 0
    else:
        null_count = target_result.iloc[0,0]

    if null_count == 0:
        Result_status = "No Records found with Null values."
        status = "PASS"
    else:
        Result_status = "Records found with Null values."
        status = "FAIL"

    df_nulls = pd.DataFrame(
        {
        'Table':['Target table'],
        'Count':[null_count],
        'Result':[Result_status],
        'Status':[status]
    }
    )
    logger.info(f"Null records count:{null_count}")
    logger.info (f"Null check validation status:{status}")
    logger.info("Null check Validation Completed Successfully!.")
    return df_nulls

def Duplicate_Records_chk(target_db_conn,Product_SQL_Queries,logger):
    tgt_result = pd.read_sql(Product_SQL_Queries["duplicate_check"]["target_query"],target_db_conn)

    if tgt_result.empty:
        duplicate_count = 0
    else:
        duplicate_count = tgt_result.iloc[0,0]

    if duplicate_count == 0:
        Result_status = "Duplicate rows not found!"
        status = "PASS"
    else:
        Result_status = "Duplicate rows found!"
        status = "FAIL"

    df_Duplicate_records = pd.DataFrame(
        {
        "Table": ["Target table"],
        'Count': [duplicate_count],
        'Result': [Result_status],
        'Status': [status]
    }
    )

    logger.info(f"Duplicate Records count:{duplicate_count}")
    logger.info(f"Duplicate check validation status:{status}")
    logger.info(f"Duplicate check validation completed Successfully!")
    return df_Duplicate_records

def Column_mapping_Validation(source_db_conn,target_db_conn,Product_validations,logger):
    source_result = pd.read_sql(Cost_SQL_Queries["column_mapping"]["source_query"],source_db_conn)
    target_result = pd.read_sql(Cost_SQL_Queries["column_mapping"]["target_query"],target_db_conn)

    source_columns = source_result.columns.tolist()
    target_columns = target_result.columns.tolist()

    if set(source_columns) == set(target_columns):
        Result_status = "Source & Target tables columns mapping data matched!"
        status = "PASS"
    else:
        Result_status = "Source & Target table column mapping data not matched!"
        status = "FAIL"
    df_column_mapping_result = pd.DataFrame(
        {
            "Table": ["Source table", "Target table"],
            "Result": [Result_status, Result_status],
            "Status": [status,status]
        }
    )
    logger.info(f"Source & Target columns mapping status:{status}")
    logger.info("Source & Target columns mapping completed Successfully!")
    return df_column_mapping_result

#Calling all above functions

counts = Source_Target_Count_check(source_db_conn,target_db_conn,Cost_SQL_Queries,logger)
null_chk = Null_Checks(target_db_conn,Cost_SQL_Queries,logger)
duplicates = Duplicate_Records_chk(target_db_conn,Cost_SQL_Queries,logger)
columns_mapp = Column_mapping_Validation(source_db_conn,target_db_conn,Cost_SQL_Queries,logger)

df_Validations_result = {
    "Src_Tgt_count_comp":counts,
    "Null_checks":null_chk,
    "Duplicate_checks":duplicates,
    "Column_mapping":columns_mapp
}
dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_file_path = f"C:\\NSR_Python_Projects\\Python_Project_Version2\\ETL_Testing_Framework(Config Driven Approach)\\Output_Result\\Cost_dim_Result_{dt}.xlsx"
with pd.ExcelWriter(output_file_path) as product_output_file_path:
    for sheet, df in df_Validations_result.items():
        df.to_excel(product_output_file_path,sheet_name=sheet,index = False)