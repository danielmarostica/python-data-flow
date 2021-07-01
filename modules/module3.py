import pandas as pd
import time
import pdb
from datetime import datetime
def func3(gc, cursor):
    wb = gc.open_by_url('https://docs.google.com/spreadsheets/d/1AaPoJgPvzG6jzjUIaR7zDk')
    func3 = wb.worksheet('func3')
    
    print('\nConectado ao Google Sheets.')
    
    print('Executando query...')
    sql = """
    SELECT 
    Data,
    codMateria,
    nomeAutor,
    experts,
    Tag,
    publieditorial,
    possuiLinkAfiliacao,
    ativo,
    save_as as URL,
    share_count as COMPARTILHAMENTOS,
    views as VISUALIZAÇÕES
    FROM [func3].[dbo].[materias]
    WHERE [Data] >= '2020-10-01'
    """
    
    start_time = time.time()
    rows = cursor.execute(sql)
    elapsed_time = time.time() - start_time
    print(f'Tempo de consulta: {elapsed_time}s')
    
    df = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description])

    df_1 = df.loc[df.ativo==1][['codMateria', 'Data', 'Tag', 'publieditorial', 'possuiLinkAfiliacao']].reset_index() # somente matérias ativas
    df_1 = df_1.groupby(['Data', 'Tag', 'publieditorial', 'possuiLinkAfiliacao'], as_index=False).agg({'codMateria':'nunique'}).rename(columns={'codMateria':'n_materias'})
    df_1 = df_1[['n_materias', 'Data', 'Tag', 'publieditorial', 'possuiLinkAfiliacao']].fillna(0)
    bad_materias = df.loc[(df['VISUALIZAÇÕES'] < 1000) & (df.Data >= '2021-01-01')]['codMateria'].nunique()
    total_materias = df.loc[df.Data >= '2021-01-01']['codMateria'].nunique()
    frac_1000 = bad_materias/total_materias
    
    print('Número de matérias: atualizando dados...')
    df_1['Data'] = df_1.Data.astype(str)
    func3.update([df_1.columns.values.tolist()] + df_1.values.tolist())
    func3.update('G2', str(frac_1000).replace('.', ','), value_input_option='USER_ENTERED')
    func3.update('K2', datetime.now().strftime('%d/%m/%Y'), value_input_option='USER_ENTERED')
    print('Dados atualizados: https://docs.google.com/spreadsheets/d/1AaPoG6jzjUIaR7zDk')
    
# ------------ Menos 1000 views
    wb3 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1y7DZPhZj5-jmwkIV_oPqEzig9I')
    func3_3 = wb3.worksheet('func3')
    print('\nMenos de 1000 views: func3')
    
    sql = \
    """
    WITH materias_table as (
    SELECT
    Data,
    codMateria,
    Tag,
    Titulo,
    views
    FROM func3.dbo.materias
    WHERE Data > '2020-12-31'
    ),

    views_materias as (
    SELECT
    views_artigo,
    views_data
    FROM func3.dbo.materias_views
    )

    SELECT 
        codMateria,
        min(views_data) as Data,
        views,
        Tag as Caderno,
        Titulo as Título
    FROM materias_table
    LEFT JOIN views_materias
    ON materias_table.codMateria = views_materias.views_artigo
    WHERE views > 0 AND views < 1000
    GROUP BY codMateria, Tag, Titulo, views
    HAVING min(views_data) > '2020-12-31'
    ORDER BY codMateria
    """
    
    start_time = time.time()
    rows = cursor.execute(sql)
    elapsed_time = time.time() - start_time
    print(f'Tempo de consulta: {elapsed_time}s')
    
    df_5 = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description])
    df_5['Data'] = df_5['Data'].astype(str)
    
    print('Matérias com menos de 1000 views MEGA: atualizando dados...')
    func3_3.clear()
    func3_3.update([df_5.columns.values.tolist()] + df_5.values.tolist())
    print('Dados atualizados: https://docs.google.com/spreadsheets/d/1y7DZPhZj5-wLn_oPqEzig9I')

# ------------ Pautas por tipo
    wb4 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KdjpvFzTVnQKKjkE5nbMQ')
    func3_4 = wb4.worksheet('Principal')
    lista_materias = func3_4.col_values(2)
    df_materias = pd.DataFrame({'codMateria':lista_materias[1:]})
    df_materias = df_materias.astype({'codMateria':int})

    df_pautas = pd.merge(df_materias, df, how='left').fillna('-')
    df_pautas['URL'] = df_pautas.URL.apply(lambda x: 'https://www.func3.com.br'+x)
    df_pautas = df_pautas[['COMPARTILHAMENTOS', 'VISUALIZAÇÕES', 'URL']]
    func3_4.update('E2:G', df_pautas.values.tolist(), value_input_option='USER_ENTERED')