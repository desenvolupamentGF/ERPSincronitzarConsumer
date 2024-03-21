# To connect to MySQL databases
import mysql.connector

# To connect to SQLServer databases
import pyodbc

# Imports to send emails
import smtplib
from email.message import EmailMessage

# Imports environment values
import os

# Email constants
EMAIL_SMTP = os.environ['EMAIL_SMTP']
EMAIL_PORT = os.environ['EMAIL_PORT']
EMAIL_USER_FROM = os.environ['EMAIL_USER_FROM']
EMAIL_USER_TO = os.environ['EMAIL_USER_TO']
EMAIL_PASS = os.environ['EMAIL_PASS']

# Send email
def send_email(subject, environment, startTime, endTime, executionResult):
    diff = endTime - startTime

    strEnvironment = "TEST"
    if environment == 1:
        strEnvironment = "PROD"

    msg = EmailMessage()
    msg.set_content("Execució finalitzada del programa " + subject + ". Entorn: " + strEnvironment + ".")
    msg['Subject'] = subject + ": " + executionResult + " (Temps execució: " + str(diff) + ")"
    msg['From'] = EMAIL_USER_FROM
    msg['To'] = EMAIL_USER_TO

    s = smtplib.SMTP(EMAIL_SMTP, port=EMAIL_PORT)
    s.starttls()
    s.login(EMAIL_USER_FROM, EMAIL_PASS)
    s.send_message(msg)
    s.quit()

def connectMySQL(user, password, host, database):
    return mysql.connector.connect(user=user, password=password,
                                   host=host, database=database)            
       
def disconnectMySQL(db):
    try:
        db.rollback()
        db.close()
    except Exception as e:              
        None

def connectSQLServer(user, password, host, database):
    return pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};\
                           SERVER='+host+';\
                           DATABASE='+database+';\
                           UID='+user+';\
                           PWD='+ password)       
       
def disconnectSQLServer(db):
    try:
        db.rollback()
        db.close()
    except Exception as e:              
        None
