def exec_and_commit_query(sql_query,
                          engine):
    """
    Creates a connection with the database, then converts a string containing a SQL query to a SQLAlchemy text object.  The connection object executes the SQL query and commits the changes to the database.
    
    sql_query (string): a string containing a query in SQL syntax
    
    engine (sql alchemy engine object): Used to establish a connection to the db
    """
    
    from sqlalchemy import text
    
    conn = engine.connect()
    
    with conn as con:
        text_sql_query = text(sql_query)

        conn.execute(text_sql_query)
        conn.commit()
        
    print("Query executed and committed.")
    
    
def sql_query_to_pandas_df(sql_query, 
                           engine, 
                           index_column=None, 
                           dates_column=None):
    """
    Establishes a connection to a SQL database, then sends a SQL query
    to that database, returning the results as a Pandas DataFrame.  Closes
    connection to the database after results are returned.
    
    sql_query (string): a string containing a query in SQL syntax
    
    engine (sql alchemy engine object): Used to establish a connection to the db
    
    index_column (str or list of str): Specifies which column(s) should be set 
    as the index in the Pandas DataFrame that gets returned
    
    dates_column (str or list of str): Specifies which column(s) in the Pandas 
    DataFrame should be parsed as dates
    """
    
    import pandas as pd
    
    conn = engine.connect()
    
    with conn as con:
        df=pd.read_sql_query(sql=sql_query, 
                             con=conn, 
                             index_col=index_column,
                             parse_dates=dates_column)
    
    return df