from utils.logging_config import logger
from sqlalchemy import create_engine
import getpass

def input_configuration(table_owner: str) -> tuple:

    '''
    Prompts the user for database connection details and returns a connection object.
    Parameters:
    table_owner(str) : 'user' if you want to connect to the database to generate reports,
                       'owner' if you want to extract data upstream data owner's report.

    Returns:
    conn: A SQLAlchemy connection object to the specified database if successful, or None if failed.
    '''

    db_user = input(f"Please enter the {table_owner}'s username: ").strip()
    passkey = getpass.getpass('Please enter the password: ').strip()

    table_owner = table_owner.lower().strip()
    if table_owner=='owner':
        db = input(f"Enter the name of the database which stores the expected values: ").strip()
        table = input(f"Enter the name of the table which stores the expected values (format-schema.table_name): ").strip()
    elif table_owner=='user':
        db = input('Enter the name of the database with the original table: ').strip()
        table = input('Enter the name of the table you want to generate the report for (format-schema.table_name): ').strip()
    else:
        logger.error("Invalid table_owner value. Use 'user' or 'owner'.")
        return None
    if '.' not in table:
        logger.error("Invalid format for table name. Aborting...")
        raise ValueError("Invalid format for table name")
    
    # Optional: Ask for host and port
    host = input(f"Enter the database host (default is 'localhost') for the {table_owner} or press Enter to use default: ").strip() or 'localhost'
    port = input(f"Enter the database port (default is 5432) for the {table_owner} or press Enter to use default: ").strip() or '5432'

    conn_string = f'postgresql+psycopg2://{db_user}:<PASSWORD HIDDEN>@{host}:{port}/{db}'
    logger.info(f'Connection string generated for {table_owner}')

    # creating connection
    logger.info(f'Trying to establish connection...')
    db = create_engine(f'postgresql+psycopg2://{db_user}:{passkey}@{host}:{port}/{db}')
    try:
        conn = db.connect()
        logger.info("Connection successful!")
        return conn, table, db
    except Exception as e:
        logger.error("Failed to establish connection!",e)
    return None
 