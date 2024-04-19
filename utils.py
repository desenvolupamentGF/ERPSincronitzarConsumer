# For logging purposes
import logging

# To connect to MySQL databases
import mysql.connector

# To connect to SQLServer databases
import pyodbc

# Import needed library for HTTP requests
import requests

# Imports to send emails
import smtplib
from email.message import EmailMessage

# Imports environment values
import os

# Token creation constants
TOKEN_URL_TEST = os.environ['TOKEN_URL_TEST']
TOKEN_URL_PROD = os.environ['TOKEN_URL_PROD']
TOKEN_CLIENT_ID = os.environ['TOKEN_CLIENT_ID']
TOKEN_CLIENT_SECRET = os.environ['TOKEN_CLIENT_SECRET']
TOKEN_GRANT_TYPE = os.environ['TOKEN_GRANT_TYPE']
TOKEN_USERNAME = os.environ['TOKEN_USERNAME']
TOKEN_PASSWORD = os.environ['TOKEN_PASSWORD']
TOKEN_SCOPE = os.environ['TOKEN_SCOPE']
TOKEN_EMAIL = os.environ['TOKEN_EMAIL']

# Email constants
EMAIL_SMTP = os.environ['EMAIL_SMTP']
EMAIL_PORT = os.environ['EMAIL_PORT']
EMAIL_USER_FROM = os.environ['EMAIL_USER_FROM']
EMAIL_USER_TO = os.environ['EMAIL_USER_TO']
EMAIL_PASS = os.environ['EMAIL_PASS']

# Function to generate token (not to call it directly --> externally use calculate_access_token below)
def get_access_token(url, client_id, client_secret, grant_type, username, password, scope, email):
    data={
        "grant_type": grant_type,
        "username": username,
        "password": password,
        "scope": scope,
        "email": email
    }
    response = requests.post(
        url,
        data=data,
        auth=(client_id, client_secret),
    )
    return response.json()["access_token"]

# Function to generate token (to be called externally. Only one parameter: 0 for test, 1 for production)
def calculate_access_token(environment):
    if environment == 0:
        token = get_access_token(TOKEN_URL_TEST, TOKEN_CLIENT_ID, TOKEN_CLIENT_SECRET, TOKEN_GRANT_TYPE, TOKEN_USERNAME, TOKEN_PASSWORD, TOKEN_SCOPE, TOKEN_EMAIL)
    else:
        token = get_access_token(TOKEN_URL_PROD, TOKEN_CLIENT_ID, TOKEN_CLIENT_SECRET, TOKEN_GRANT_TYPE, TOKEN_USERNAME, TOKEN_PASSWORD, TOKEN_SCOPE, TOKEN_EMAIL)
    return token

# Example of use
token = calculate_access_token(0)
print (token)

# Only the token as part of the Bearer (to be used for instance when sending files)
def calculate_bearer_header(token):
    headers = {
        "Authorization": "Bearer " + token
    }
    return headers

# Token in the Bearer plus content type
def calculate_json_header(token):
    headers = {
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json"
    }
    return headers

# To check returned values of a request execution
def response_details(response):
    # Check status code for response received (success code - 200)
    logging.error('Response code:' + str(response.status_code))

    # Print content of the response
    logging.error('Response message:' + str(response.content))

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
    return pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};\
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
