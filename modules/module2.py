import sys
import time
import pandas as pd
import numpy as np
from datetime import datetime
def func2(gc, cursor):
    wb = gc.open_by_url('https://docs.google.com/spreadsheets/d/103Cd0OVVPUmqvkdFijdKilns')
    nexp = wb.worksheet('Principal')
    print('\nConectado ao Google Sheets func2.')
        
    print('Executando query...')
    sql = """
            SELECT
              nx.idProjeto as Projeto
            , nx.dataAprovado as [Data de aprovação]
            , nx.totalPalavrasEntregue as [Total de palavras entregues]
            , nx.precoPorPalavra as [Preço por palavra]
            , nx.preco as [Preço (banco)]
            , nxp.nome as Nome
            , ve.nome as Veículo
            , cc.identificador as Identificador
            , cc.nome as [Nome centro custo]
            ,'NZN - ' + ccs.nome + ' - ' + cca.nome + ' - ' + cc.nome as 'Centro Custo'
            FROM func2.dbo.ProjetoEtapa as nx
            INNER JOIN func2.dbo.CentroCustoAgrupador cca on cca.id = cc.idAgrupador
            INNER JOIN func2.dbo.CentroCustoSetor ccs on ccs.id = cca.idsetor
            WHERE
            nx.dataAprovado >= '2021-01-01'
         """

    start_time = time.time()
    rows = cursor.execute(sql)
    elapsed_time = time.time() - start_time
    print(f'Tempo de consulta: {elapsed_time}s')
    
    # casts as string
    def cast_for_gsheets(df):
        for column, dt in zip(df.columns, df.dtypes):
            df.loc[:, column] = df[column].astype(str)
        return df

    df = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description]).fillna(0)
    df = cast_for_gsheets(df)

    df['Preço por palavra'] = df['Preço por palavra'].str.replace('.',',')
    df['Preço (banco)'] = df['Preço (banco)'].str.replace('.',',')
    
    wb.values_clear('Principal!A:J')
    nexp.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
    print('Dados atualizados: https://docs.google.com/spreadsheets/d/103CJhfl6D0dkdFijdKilns')