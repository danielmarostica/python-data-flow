import sys
import time
import traceback
import smtplib
import gspread

sys.path.insert(1, 'modules/')
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.oauth2 import service_account
from module1 import func1
from module2 import func2
from module3 import func3
from module4 import func4
from module5 import func5
from conector import connect

# google auth
credentials = service_account.Credentials.from_service_account_file('gspread-connection.json')
scoped_credentials = credentials.with_scopes(['https://www.googleapis.com/auth/spreadsheets'])

# email configs
sender_address = 'sender@gmail.com'
sender_pass = 'senderpass'
receiver_address = 'name.surname@gruponzn.com'
receiver_address_2 = 'name2.surname2@gruponzn.com'

attempt = 0
while attempt < 3:
    try:
        # gspread
        gc = gspread.authorize(scoped_credentials)

        # conexão
        cursor = connect()

        # funções
        tec(gc, cursor)
        nexperts(gc, cursor)
        daily_tec(gc, cursor)
        mega(gc, cursor)
        daily_mega(gc, cursor)
        baixaki(gc, cursor)

    except Exception as err:
        print('Error: ', err)

        # tentar novamente
        attempt += 1
        if attempt < 3:
            print(f'Houve um erro na execução. Tentando novamente ({attempt}/2)...')
            time.sleep(10)
        
        else:
            # enviar email com o traceback
            message = MIMEMultipart()
            message['From'] = sender_address
            message['To'] = receiver_address
            message['Subject'] = 'Ocorreu um problema na execução da automação'
            message.attach(MIMEText(traceback.format_exc(), 'plain'))
            session = smtplib.SMTP('smtp.gmail.com', 587)
            session.starttls() # security
            session.login(sender_address, sender_pass)
            text = message.as_string()
            session.sendmail(sender_address, receiver_address, text)
            session.sendmail(sender_address, receiver_address_2, text)
            session.quit()
            print('Houve um erro na execução e não será realizada uma nova tentativa.')
            print(f'Email com os logs enviado para {receiver_address}.')
    else:
        sys.exit('\nPipeline finalizado.')
