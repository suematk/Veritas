from utils.logging_config import logger
import pandas as pd
from sqlalchemy import text
import json

def dataframe_summary(conn, table_name: str, db):

    '''
    Generates a summary report for the table. 
    This code assumes multiple schemas with the same table name does not exist.

    Parameters:
    conn (connection object): The connection object to a database
    table_name (schema_name.table_name): The schema and table name to generate the report for
    db : The connection engine

    Returns:
    pd.DataFrame: A DataFrame containing a summary of the original DataFrame including shape, null counts,
                  non null counts, duplicate counts, data types, and unique value counts for every column.
                  Returns None if some error occurs.

    '''
    
    # Test connection
    logger.info(f"Checking if {table_name} is accessible...")
    test_query = f'SELECT * FROM {table_name} LIMIT 1;'
    try:
        test_df = conn.execute(text(test_query)).fetchall() 
        logger.info(f'{table_name} is accessible')
    except Exception as e:
        logger.error(f'Table is not accessible: {e}')
        return None
    
    # Fetching queries json, make additions to queries in this template file and add it in the code
    logger.info("Retrieving query template")
    with open(f'./utils/queries.json', 'r') as f:
        queries = json.load(f)
        
    logger.info("Retrieving column names")
    schema, table = table_name.split(".")
    col_names_query = queries['col_names_query'].format(schema=schema,table=table)
    col_names_list = list(pd.read_sql_query(col_names_query,conn)['column_name'])
    col_names_list.sort()
    logger.info(f'Column Names are {col_names_list}')

    num_rows = []
    datatype = []
    null = []
    non_null = []
    unique = []
    duplicates = []
    top = []

    for col_name in col_names_list:
        logger.info(f"Generating queries for {col_name}")
        try:
            num_rows_query = queries['num_rows_query'].format(table_name=table_name)
            datatype_query = queries['datatype_query'].format(table=table,col_name=col_name)
            num_null_query = queries['num_null_query'].format(table_name=table_name,col_name=col_name)
            num_non_null_query = queries['num_non_null_query'].format(table_name=table_name,col_name=col_name)
            num_unique_query = queries['num_unique_query'].format(table_name=table_name,col_name=col_name)
            top_query = queries['top_query'].format(table_name=table_name,col_name=col_name)
            logger.info("Queries generated successfully")
        except Exception as e:
            conn.close()
            db.dispose()
            logger.error(f"An error occured while generating the queries for {col_name}: {e}")
            raise e
        
        logger.info(f"Executing queries for {col_name}")
        try:
            logger.info(num_rows_query)
            row_count = pd.read_sql_query(num_rows_query,conn)['count'][0]
            num_rows.append(int(row_count))

            logger.info(datatype_query)
            datatype.append(pd.read_sql_query(datatype_query,conn)['data_type'][0])

            logger.info(num_null_query)
            null_count = pd.read_sql_query(num_null_query,conn)['count'][0]
            null.append(int(null_count))

            logger.info(num_non_null_query)
            non_null_count = pd.read_sql_query(num_non_null_query,conn)['count'][0]
            non_null.append(int(non_null_count))

            logger.info(num_unique_query)
            unique_count = pd.read_sql_query(num_unique_query,conn)['count'][0]
            unique.append(int(unique_count))

            logger.info(f'Number of duplicates = num of non null - number of unique values')
            duplicates.append(int(non_null_count)-int(unique_count))

            logger.info(top_query)
            top_vals = list(pd.read_sql_query(top_query,conn)['val'])
            if len(top_vals)>1:
                top_vals.sort()
            top.append(top_vals[0])

            logger.info(f"Done for {col_name}")
        except Exception as e:
            conn.close()
            db.dispose()
            logger.error(f"An error occured while generating the values for {col_name}: {e}")
            raise e

    conn.close()
    db.dispose()        
    
    logger.info("Generating report")
    report = pd.DataFrame({
        'Column':col_names_list,
        'Num_Of_Rows':num_rows,
        'Datatype':datatype,
        'Num_Of_Nulls':null,
        'Num_Of_Non_Nulls':non_null,
        'Num_Unique_Vals':unique,
        'Num_Of_Duplicates':duplicates,
        'Most_Occurring_Vals':top
    })
    logger.info(f"Report generated successfully for {table_name} with shape {report.shape}!")
    logger.info(report)
    '''
    make changes to testing report for ad-hocs tests here, comment out line 107 and 108.

    test_df = pd.DataFrame({'Column':col_names_list})
    test_df.to_sql(name='report_table', con=db, schema=schema, if_exists='replace', index=False)
    conn.close()
    db.dispose()
    '''
    return report