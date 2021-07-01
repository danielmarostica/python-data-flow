import sys
import time
import pandas as pd
import numpy as np
from datetime import datetime
def func5(gc, cursor):
    wb = gc.open_by_url('https://docs.google.com/spreadsheets/d/1mOa_ipZ8xyzvpDcd3QoyRsows')
    nexp = wb.worksheet('Downloads')
    print('\nConectado ao Google Sheets:Dados Adobe / Downloads.')
        
    print('Executando query...')
    sql = """
        SELECT
        dpd.data as Data,
        sum(dpd.downloads) as Downloads
        from func5.dbo.Programas_Downloads dpd
        LEFT JOIN func5.dbo.Programas dp ON dpd.codprog = dp.codprog
        WHERE dp.TemGerenciador = 1 AND dpd.data >= '2021-01-01'
        GROUP BY dpd.data
    """

    start_time = time.time()
    rows = cursor.execute(sql)
    elapsed_time = time.time() - start_time
    print(f'Tempo de consulta: {elapsed_time}s')

    df = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description]).fillna(0).sort_values('Data')
    df['Data'] = df['Data'].astype(str)
    nexp.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
    print('Dados atualizados: https://docs.google.com/spreadsheets/d/1mOa_ipZ8xyJd3QoyRsows')