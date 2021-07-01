import sys
import time
import pandas as pd
import numpy as np
import pdb
from datetime import datetime
from datetime import timedelta

def func4(gc, cursor):
    wb = gc.open_by_url('https://docs.google.com/spreadsheets/d/1HC4tsTBXcipUHn1ZGO6fvb7c')
    ws = wb.worksheet('Principal')
    print('\nConectado ao Google Sheets func4 Daily.')
    ws_materias1 = wb.worksheet('Matérias t-1')
    ws_materias2 = wb.worksheet('Matérias t-2')
    ws_cadernos = wb.worksheet('Cadernos')
    oculta = wb.worksheet('(ocultar)') # planilha para controlar a execução

    if datetime.strptime(oculta.acell('A1').value, '%Y-%m-%d').date() == datetime.now().date():
        print('A planilha func4 Daily já foi atualizada hoje.')

    elif datetime.strptime(oculta.acell('A1').value, '%Y-%m-%d').date() < datetime.now().date():
        print('Executando query...')

        """Total de visualizações do site ontem + hoje"""
        sql_totalviews = """
                        SELECT
                        sum(views_total) as [total_views]
                        FROM func4.dbo.materias_views
                        WHERE views_data >= dateadd(day, -2, getdate())
                        """
        total_views = cursor.execute(sql_totalviews)
        total_views = total_views.fetchall()[0][0]

        """Dados de matérias dos últimos 2 dias"""
        sql = """ WITH materias_table as (
                SELECT
                Data as Datetime, 
                codMateria,
                Tag,
                Titulo,
                views
                FROM func4.dbo.materias
                WHERE Data >= DATEADD(day, -2, CAST(GETDATE() AS DATE)) AND Data < CAST(GETDATE() AS DATE)
                AND ativo = 1
                ),

                views_materias as (
                SELECT
                views_artigo,
                views_data,
                views_total as views_dia
                FROM func4.dbo.materias_views
                WHERE views_data >= '2021-01-01'
                ),

                min_data as (
                SELECT
                views_artigo,
                min(views_data) as data_pub
                FROM func4.dbo.materias_views
                WHERE views_data >= '2021-01-01'
                GROUP BY views_artigo
                ),

                cod_tags as (
                    SELECT 
                    Idf_Artigo as codigo_materia,
                    Idf_Tag as codSubtag
                FROM func4.dbo.Tags_Artigo 
                ),

                name_tags as (
                SELECT
                Des_Tag as subtag,
                Idf_Tag as codSubtag
                FROM func4.dbo.Tags
                )

                SELECT
                    codMateria,
                    datetime,
                    data_pub,
                    views_data,
                    views as views_total,
                    views_dia,
                    Tag as caderno,
                    Titulo as título,
                    subtag
                FROM materias_table
                LEFT JOIN views_materias
                ON materias_table.codMateria = views_materias.views_artigo
                LEFT JOIN min_data
                ON materias_table.codMateria = min_data.views_artigo
                LEFT JOIN cod_tags
                ON materias_table.codMateria = cod_tags.codigo_materia
                LEFT JOIN name_tags
                ON cod_tags.codSubtag = name_tags.codSubtag
                GROUP BY codMateria, Tag, Titulo, views_dia, views, Datetime, data_pub, views_data, subtag
                HAVING min(views_data) >=  DATEADD(day, -2, CAST(GETDATE() AS DATE)) AND min(views_data) < CAST(GETDATE() AS DATE)
                ORDER BY datetime, codMateria
            """

        rows = cursor.execute(sql)
        df_ = pd.DataFrame.from_records(rows, columns=[col[0] for col in cursor.description])

        df_['datetime'] = pd.to_datetime(df_.datetime)
        df_['uptime'] = datetime.now() - df_['datetime']
        df_['views_hour'] = round(df_['views_total']/(df_.uptime.dt.total_seconds()/3600)).astype(int)
        df_['uptime'] = (df_.uptime.dt.total_seconds()/3600).astype(int)
        df_

        """Juntar subtags"""
        subtags = df_.groupby('codMateria').apply(lambda x: ', '.join(x.subtag))
        df = df_.groupby(['codMateria', 'datetime', 'data_pub', 'views_data', 'views_total', 'views_dia', 'caderno', 'título', 'uptime', 'views_hour'], as_index=False).agg({'subtag':'first'}).set_index('codMateria')
        df.subtag = subtags

        """Matérias de ontem e anteontem"""
        df_ontem = df.loc[df.data_pub==(datetime.today() - timedelta(days=1)).date()].reset_index()
        df_ontem.data_pub = df_ontem.data_pub.astype(str)
        df_ontem.views_data = df_ontem.views_data.astype(str)

        df_anteontem = df.loc[df.data_pub==(datetime.today() - timedelta(days=2)).date()].reset_index()
        df_anteontem.data_pub = df_anteontem.data_pub.astype(str)
        df_anteontem.views_data = df_anteontem.views_data.astype(str)
        df_anteontem = df_anteontem.loc[df_anteontem.views_data==df_anteontem.views_data.min()]

        """Aba principal"""
        df_append = df_ontem.groupby(['data_pub']).agg({'codMateria':'nunique', 'views_total':'sum', 'views_hour':'sum'}).reset_index()
        df_append = df_append.T[0].tolist()

        """Cadernos e Subtags"""
        df_['acima_média'] = np.where(df_.views_total>df_.views_total.mean(), 1, 0)

        """## Resumo por caderno"""
        df_caderno_ = df_.groupby(['caderno', 'data_pub']).agg(matérias=('codMateria', 'nunique'))
        df_caderno = df_.drop_duplicates(subset=['caderno', 'codMateria']).groupby('caderno').agg(matérias_acima_média=('acima_média', 'sum'), views=('views_total', 'sum'))
        df_caderno['% views'] = (100 * (df_caderno.views/df_caderno.views.sum()).round(2)).astype(int)
        df_caderno = df_caderno.join(df_caderno_)
        df_caderno = df_caderno.reset_index()
        df_caderno['views/matéria'] = ((df_caderno.views/df_caderno.matérias).round(0)).astype(int)
        df_caderno = df_caderno.loc[df_caderno.data_pub == (datetime.today() - timedelta(days=1)).date()]
        df_caderno = df_caderno.drop_duplicates(subset=['caderno'], keep='last').set_index('caderno').sort_values('views', ascending=False)

        """## Atualizar sheet"""
        ws.append_rows([[df_append[0], df_append[1], df_append[2], df_append[3], total_views]], table_range='A1:E1000', value_input_option='USER_ENTERED')
        ws_materias1.append_rows(df_ontem.drop(columns=['datetime', 'views_data']).values.tolist(), value_input_option='USER_ENTERED')
        ws_materias2.append_rows(df_anteontem.drop(columns=['datetime', 'views_data']).values.tolist(), value_input_option='USER_ENTERED')
        ws_cadernos.append_rows(df_caderno.reset_index().astype(str).values.tolist(), value_input_option='USER_ENTERED')
        
        oculta.update('A1', datetime.now().strftime('%Y-%m-%d'), value_input_option='USER_ENTERED')

        print('Dados atualizados: https://docs.google.com/spreadsheets/d/1HC4tsTBXcipU4E1ZGO6fvb7c')