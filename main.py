from datalake_conn import DatalakeConn
import pandas as pd

def get_data():
    conn = DatalakeConn(catalog='dl_sandbox')
    query = '''
        SELECT * 
        FROM silver.md_comercio 
        LIMIT 10
    '''
    df = conn.get_query(query)
    print(df)

def set_data():
    print('Generando conexión')
    conn = DatalakeConn(catalog='dl_sandbox')
    
    print('Leyendo excel')
    df = pd.read_excel('dummy_data.xlsx',sheet_name='BASE')
    df['fecha_creacion'] = pd.to_datetime(df['fecha_actualizacion'], errors='coerce')
    df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], errors='coerce').dt.date
    df['fecha_actualizacion'] = pd.to_datetime(df['fecha_actualizacion'], errors='coerce')
    
    # Ordenar tus columnas
    print('Iniciando importación')
    conn.import_data(df,'marimar','edm_pcom')
    
def main():
    get_data()
    set_data()


if __name__ == "__main__":
    main()
