from utils.logging_config import logger
import pandas as pd

def get_owner_report(conn, db, table_name):
    '''
    Gets the connection details for the data owner's report and extracts it from the specified table.

    Returns:
    report_dataframe(pd.DataFrame): A Pandas DataFrame with the data owner's reports for the specified columns.

    '''
    schema,table = table_name.split(".")

    logger.info("Getting data owner's report")
    try:
        report_dataframe = pd.read_sql_table(table_name=table, con=conn, schema=schema)
        if report_dataframe.shape[0]>1 and report_dataframe.shape[1]>1:
            logger.info(f'{table_name} with shape {report_dataframe.shape} extracted succesfully!')
        else:
            logger.warning(f'{table_name} has insufficient rows or columns.')
        logger.info(report_dataframe)
        return report_dataframe
    except Exception as e:
        logger.error(f'Uh-oh! Could not fetch the table: {e}')
        raise e
    finally:
        conn.close()
        db.dispose()

