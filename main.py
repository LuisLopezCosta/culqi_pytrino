from datalake_conn import DatalakeConn


def main():
    conn = DatalakeConn(catalog='dl_pfc')
    query = '''
        SELECT * 
        FROM silver.md_comercio 
        LIMIT 10
    '''
    df = conn.execute_query(query)
    print(df)


if __name__ == "__main__":
    main()
