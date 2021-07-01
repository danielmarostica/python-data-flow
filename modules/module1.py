import sys
import time
import pandas as pd
from datetime import datetime
def func1(gc, cursor):
    wb = gc.open_by_url('https://docs.google.com/spreadsheets/d/1AaPoJgBPvzG6jzjUIaR7zDk')
    func1 = wb.worksheet('func1')
    print('Conectado ao Google Sheets.')
        
    print('Executando query...')
    sql = """
    WITH materias_table as (
    SELECT
    codMateria,
    nomeAutor,
    experts,
    Tag,
    save_as,
    publieditorial,
    possuiLinkAfiliacao,
    ativo,
    Titulo,
    share_count,
    views,
    Data
    FROM func1.dbo.materias
    WHERE Data >= '2020-10-01'
    ),

    min_data as (SELECT
    views_artigo,
    cast(min(views_data) as date) AS data_pub
    FROM func1.dbo.materias_views
    GROUP BY views_artigo
    )

    SELECT 
        data_pub as Data,
        codMateria,
        nomeAutor,
        experts,
        Tag,
        Titulo,
        save_as,
        publieditorial,
        possuiLinkAfiliacao,
        ativo,
        share_count as COMPARTILHAMENTOS, 
        views as VISUALIZAÇÕES
    FROM materias_table
    INNER JOIN min_data
    ON materias_table.codMateria = min_data.views_artigo
    ORDER BY data_pub
    """

    start_time = time.time()
    rows = cursor.execute(sql)
    elapsed_time = time.time() - start_time
    print(f'Tempo de consulta: {elapsed_time}s')

    df = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description])
    df['Data'] = pd.to_datetime(df['Data'])
    
# ---------------- Número de matérias
    print('\nNúmero de matérias: lendo dados...')
    
    wb1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1AaPoJvzG6jzjUIaR7zDk')
    func1 = wb1.worksheet('func1')

    df_1 = df.loc[df.ativo==1][['codMateria', 'Data', 'Tag', 'publieditorial', 'possuiLinkAfiliacao']].reset_index() # somente matérias ativas
    df_1 = df_1.groupby(['Data', 'Tag', 'publieditorial', 'possuiLinkAfiliacao'], as_index=False).agg({'codMateria':'nunique'}).rename(columns={'codMateria':'n_materias'})
    df_1 = df_1[['n_materias', 'Data', 'Tag', 'publieditorial', 'possuiLinkAfiliacao']]
    df_1 = df_1.loc[df_1.Data >= '2020-10-01']
    
    bad_materias = df[(df['VISUALIZAÇÕES'] < 1000) & (df.Data >= '2021-01-01')]['codMateria'].nunique()
    total_materias = df[df.Data >= '2021-01-01']['codMateria'].nunique()
    frac_1000 = bad_materias/total_materias
    
    print('Número de matérias: atualizando dados...')
    df_1['Data'] = df_1.Data.astype(str)
    func1.update([df_1.columns.values.tolist()] + df_1.values.tolist(), value_input_option='USER_ENTERED')
    func1.update('G2', str(frac_1000).replace('.', ','), value_input_option='USER_ENTERED') #USER_ENTERED interpreta no sheets como float
    func1.update('K2', datetime.now().strftime('%d/%m/%Y'), value_input_option='USER_ENTERED')
    print('Dados atualizados: https://docs.google.com/spreadsheets/d/PvzG6jzjUIaR7zDk')
    
# ---------------- Compartilhamento de originais
    print('\nCompartilhamento de originais:  lendo dados...')
    wb2 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1YDfmuwhT4dE6mLY0Wzc9y8')
    originais = wb2.worksheet('Originais')

    lista_materias = originais.col_values(2)
    df_materias = pd.DataFrame({'codMateria':lista_materias[1:]})
    df_materias = df_materias.astype({'codMateria':int})

    df_2 = pd.merge(df_materias, df, how='left').dropna()
    df_2 = df_2[['COMPARTILHAMENTOS', 'VISUALIZAÇÕES']]
    print('Compartilhamento de originais:  atualizando dados...')
    originais.update('G1:H', [df_2.columns.values.tolist()] + df_2.values.tolist())
    
 # ---------------- Long Tails Q1'21
    print('\nLong Tails Q121: lendo dados...')
    long_tail = wb2.worksheet('Long Tail')

    lista_materias = long_tail.col_values(2)
    df_materias = pd.DataFrame({'codMateria':lista_materias[1:]})
    df_materias = df_materias.astype({'codMateria':int})

    df_3 = pd.merge(df_materias, df, how='left').fillna('0')
    df_3['URL'] = df_3.save_as.apply(lambda x: 'https://www.func1.com.br'+x)
    df_3 = df_3[['COMPARTILHAMENTOS', 'VISUALIZAÇÕES', 'URL']]

    print('Long Tails Q121: atualizando dados...')
    long_tail.update('E1:G', [df_3.columns.values.tolist()] + df_3.values.tolist(), value_input_option='USER_ENTERED')
    long_tail.update('H2', str(df.loc[df.Data >= '2021-01-01']['VISUALIZAÇÕES'].sum()), value_input_option='USER_ENTERED')

 # ---------------- Pautas Internas
    print('\nPautas Internas: lendo dados...')
    pautas_internas = wb2.worksheet('Pautas Internas')

    df_5 = df[(df.experts == 0) & (df.publieditorial == 0) & (df.Data >= '2021-01-01')].reset_index(drop=True).drop_duplicates(subset='codMateria')
    df_5 = df_5[['Data', 'codMateria', 'nomeAutor', 'VISUALIZAÇÕES']].fillna(0)
    df_5['Data'] = df_5['Data'].astype(str)
    print('Pautas Internas: atualizando dados...')
    pautas_internas.update([df_5.columns.values.tolist()] + df_5.values.tolist(), value_input_option='USER_ENTERED')
    print('Dados atualizados: https://docs.google.com/spreadsheets/d/1YDfmuwhT4gx8VEmLY0Wzc9y8')
    
 # ---------------- Long Tails Q4'20
    print('\nLong Tails Q420: lendo dados...')
    wb3 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1Ky7qTtE6tseAfQqgUDNFhY-k')
    long_tail_q4 = wb3.worksheet('Página1')
    
    SEO_LT = [205101, 205103, 205110, 205161, 205280, 205410, 205443, 205573, 205696,
              206076, 206073, 206074, 206021, 206022, 206049, 206082, 205958, 206075, 206077, 
              205977, 206736, 206602, 206703, 143848, 138347, 207077, 207127, 206985, 207076, 
              206995, 207640, 207701, 207818, 207819, 207821, 207804, 207811, 207761, 208079, 
              208204, 208207, 208205, 208538, 208466, 208633, 205954, 206399, 206611, 206737, 
              208007, 207331, 208444, 208156, 208592, 207206, 205052, 205105, 205160, 205375, 
              205358, 206020, 205816, 205695, 205849, 206050, 206024, 206100, 206607, 207814, 
              206950, 206708, 206750, 206327, 206706, 207128, 207805, 207143, 207615, 207567, 
              208150, 208631]

    df_4 = df.loc[df.codMateria.isin(SEO_LT)].reset_index()
    df_4['save_as'] = df_4.save_as.apply(lambda x: 'https://www.func1.com.br'+x)
    df_4['Data'] = df_4['Data'].astype(str)
    df_4 = df_4.groupby(['codMateria', 'Titulo', 'Data', 'save_as'], as_index=False).agg({'VISUALIZAÇÕES':'sum', 'COMPARTILHAMENTOS':'sum'}).fillna(0)
    print('Long Tails Q420: atualizando dados...')
    long_tail_q4.update([df_4.columns.values.tolist()] + df_4.values.tolist(), value_input_option='USER_ENTERED')
    print('Dados atualizados: https://docs.google.com/spreadsheets/d/1Ky7qT1kDCax34yHFlfQqgUDNFhY-k')

 # ---------------- Menos 1000 views Tec
    wb4 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1y7D7daB96pvwLn_oPqEzig9I')
    func1_2 = wb4.worksheet('Tec')
    print('\nMenos de 1000 views: Tec')
    
    df_6 = df.loc[(df.Data >= '2021-01-01') & (df.VISUALIZAÇÕES < 1000)]
    df_6 = df_6.rename(columns={'Tag':'Caderno', 'Titulo':'Título' , 'VISUALIZAÇÕES':'views'})
    df_6 = df_6[['codMateria', 'Data', 'views', 'Caderno', 'Título']].fillna(0)
    df_6['Data'] = df_6.Data.astype(str)
    print('Matérias com menos de 1000 views TEC: atualizando dados...')
    func1_2.clear()
    func1_2.update([df_6.columns.values.tolist()] + df_6.values.tolist())
    print('Dados atualizados: https://docs.google.com/spreadsheets/d/1y7DZPhZaB96pvwLn_oPqEzig9I')