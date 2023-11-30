import pandas as pd
import functions.work_with_duckdb as wwd
import configs.sql_sentences as sqls
import datetime as dt



if __name__=='__main__':
    file_path = '../data/database.db'
    # Create duckdb connection
    conn = wwd.create_duckdb_connection(file_path)
    
    # DROP Tables
    wwd.drop_duckdb_table(table_name='raw_data', conn=conn)
    wwd.drop_duckdb_table(table_name='nacional', conn=conn)
    wwd.drop_duckdb_table(table_name='local', conn=conn)
    # load CSV file in dataframe
    csv_file = pd.read_csv('../datos_base/48191.csv', delimiter=";")
    csv_file.columns = ['areas_movilidad','tipo_dato','periodo', 'porcentaje']
    csv_file['porcentaje'] = csv_file['porcentaje'].str.strip()
    csv_file['porcentaje'] = csv_file['porcentaje'].str.replace(',','.')
    csv_file['porcentaje'] = csv_file['porcentaje'].str.replace('..','0.00')
    csv_file['periodo'] = pd.to_datetime(csv_file['periodo'], format="%d/%m/%Y")
    csv_file['porcentaje'] = csv_file['porcentaje'].astype(float)
    csv_file = csv_file.drop_duplicates()

    
    wwd.load_csv_file_in_duckdb(dataframe=csv_file, table_name='raw_data', conn=conn)
    conn.sql("SELECT * FROM raw_data").show()
    
    # Create silver tables
    wwd.create_duckdb_table(create_sentence=sqls.create_total_nacional, conn=conn)
    wwd.create_duckdb_table(create_sentence=sqls.create_total_local, conn=conn)
    wwd.insert_duckbd_table(sentence=sqls.insert_data_in_nacional, conn=conn)
    wwd.insert_duckbd_table(sentence=sqls.insert_data_in_local, conn=conn)
    
    result = wwd.select_duckbd_table(sentence="SELECT * FROM nacional", conn=conn)    
    print(result)
    result = wwd.select_duckbd_table(sentence="SELECT * FROM local", conn=conn)    
    print(result)
