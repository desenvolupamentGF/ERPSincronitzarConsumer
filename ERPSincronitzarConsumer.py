# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
ENVIRONMENT = 0
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!

# for logging purposes
import logging

# for hash/encrypt reasons
import hashlib

# Import to work with json data
import json

# for RabbitMQ messaging between publishers and consumers
import pika

# extra imports
import sys
import datetime
from utils import send_email, connectMySQL, disconnectMySQL
from utils import calculate_access_token, calculate_json_header
import os

# Import needed library for HTTP requests
import requests

# End points URLs
URL_FAMILIES = '/productFamilies'
URL_PRODUCTS = '/products'
URL_FORMATS = '/formats'
URL_COSTS = '/standardCosts'

# Other constants
CONN_TIMEOUT = 50

GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']
GLAMSUITE_DEFAULT_ZONE_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_ID']
GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID']

PIKA_USER = os.environ['PIKA_USER']
PIKA_PASSWORD = os.environ['PIKA_PASSWORD']

RABBIT_URL = os.environ['RABBIT_URL']
RABBIT_PORT = os.environ['RABBIT_PORT']
RABBIT_QUEUE_FAMILIES = os.environ['RABBIT_QUEUE_FAMILIES']
RABBIT_QUEUE_PROJECTS = os.environ['RABBIT_QUEUE_PROJECTS']
RABBIT_QUEUE_PRODUCTS = os.environ['RABBIT_QUEUE_PRODUCTS']

MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']

URL_API = os.environ['URL_API_TEST']
if ENVIRONMENT == 1:
    URL_API = os.environ['URL_API_PROD']

class RabbitPublisherService:

    def __init__(self, rabbit_url: str, rabbit_port: str, queue_name: str):
        self.rabbit_url = rabbit_url
        self.rabbit_port = rabbit_port
        self.queue_name = queue_name
        credentials = pika.PlainCredentials(PIKA_USER, PIKA_PASSWORD)
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_url, port=self.rabbit_port, credentials=credentials))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

def get_value_from_database(mycursor, correlation_id: str, url):
    mycursor.execute("SELECT erpGFId, hash FROM gfintranet.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = 'Mercaderies ERP GF' AND origin = 'Emmegi' AND correlationId = '" + str(correlation_id) + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
    myresult = mycursor.fetchall()

    erpGFId = None
    hash = None
    for x in myresult:
        erpGFId = str(x[0])
        hash = str(x[1])

    return erpGFId, hash

def update_value_from_database(dbOrigin, mycursor, correlation_id: str, erpGFId, hash, url):
    sql = "INSERT INTO gfintranet.ERPIntegration (companyId, endpoint, origin, correlationId, deploy, callType, erpGFId, hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE erpGFId=VALUES(erpGFId), hash=VALUES(hash)"
    val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), "Mercaderies ERP GF", "Emmegi", str(correlation_id), str(ENVIRONMENT), url, erpGFId, hash)
    mycursor.execute(sql, val)
    dbOrigin.commit()    

def delete_value_from_database(dbOrigin, mycursor, correlation_id: str, url):
    mycursor.execute("DELETE FROM gfintranet.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = 'Mercaderies ERP GF' AND origin = 'Emmegi' AND correlationId = '" + str(correlation_id) + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
    dbOrigin.commit()

def synch_by_database(dbOrigin, mycursor, headers, url: str, correlation_id: str, data: dict, filter_name: str = "", filter_value: str = ""):
    """
    Synchronize objects with the API.
    Always returns the specific Glam ID.
    """

    key = url + ":" + data['correlationId']
    #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
    data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
    glam_id, old_data_hash = get_value_from_database(mycursor, correlation_id, url)

    # POST if not exists.
    # PUT if hash changed.
    # Nothing if hash is the same
    if glam_id is None:
        try:
            req = requests.post(url=URL_API + url, data=json.dumps(data),     
                                headers=headers, verify=False, timeout=CONN_TIMEOUT)
        except Exception as e:
            logging.error('Error posting to GlamSuite with ' + key + '. Err: ' + str(e))
            return None, False
    elif glam_id is not None and str(old_data_hash) != str(data_hash):
        try:
            req = requests.put(url=URL_API + url + "/" + str(glam_id), data=json.dumps(data), 
                               headers=headers, verify=False, timeout=CONN_TIMEOUT)
        except Exception as e:
            logging.error('Error putting to GlamSuite with ' + key + '. Err: ' + str(e))
            return None, False
    else:
        return glam_id, False

    if req.status_code in [200, 201]:  # POST or PUT success 
        p_glam_id = req.json()['id']
        update_value_from_database(dbOrigin, mycursor, correlation_id, p_glam_id, str(data_hash), url)
        return p_glam_id, True
    elif req.status_code == 204:
        delete_value_from_database(dbOrigin, mycursor, correlation_id, url)
        return None, False
    elif req.status_code == 400:  # Bad request. If an element with the same code already exists, we will use it.
        try:
            get_req = requests.get(URL_API + url + f"?search={filter_value}", headers=headers,
                                   verify=False, timeout=CONN_TIMEOUT)
            if get_req.status_code == 200:                
                #if 'code' in data:
                #    item = next((i for i in get_req.json() if i[filter_name].casefold() == filter_value.casefold()
                #                and i['code'].casefold() == data['code'].casefold()), None)
                #else:
                #    item = next((i for i in get_req.json() if i[filter_name].casefold() == filter_value.casefold()), None)
                item = next((i for i in get_req.json() if i[filter_name].casefold() == filter_value.casefold()), None)

                if item is not None:
                    update_value_from_database(dbOrigin, mycursor, correlation_id, str(item['id']), str(data_hash), url)
                    return item['id'], True
                else:
                    logging.error('Error posting to GlamSuite with ' + key)
                    return None, False
        except Exception as err:
            return None, False
    elif req.status_code == 404:  # Not found. (PUT id not found)
        delete_value_from_database(dbOrigin, mycursor, correlation_id, url)
        return None, False
    else:
        logging.error(
            'Error sync:' + key + '\n    json:' + json.dumps(data) +
            '\n    HTTP Status: ' + str(req.status_code) + ' Content: ' + str(req.content))  
        return None, False

def sync_families(dbOrigin, mycursor, headers, data: dict):
    None

def sync_projects(dbOrigin, mycursor, headers, data: dict):
    None

def sync_products2(dbOrigin, mycursor, headers, data: dict):
    None

def sync_products(dbOrigin, mycursor, headers, data: dict):
        """
        :param data: dict -> {
            "correlationId": "02,
            "code": "02",
            "name": "Good 01 Material",
            "description": "Good 01 Material",
            "familyCorrelationId": 1,
            "costs": [
                {
                    "date": "2020-02-01",
                    "cost": 23.5
                }
            ],
            "formats": [
                {
                    "formatCorrelationId": 1,
                    "quantity": 2.5,
                    "barcodes": [               # Realment arribarà buida.
                        "1234512345",
                        "1234512347",
                        "1234512348"
                    ]
                }
            ]
        }
        :return None

        Opcions de productId:
            1: Material
            2: Product
            3: Service

        Opcions de product state:
            1. Draft
            2. Active
            3. Inactive
            4. Obsolete
        """

        if data['familyCorrelationId'] is None or str(data['familyCorrelationId']) == "0":
            logging.info('      ********** FAMILIA 0 **********')
            return

        correlation_id = data['correlationId']
        family_correlation_id = str(data['familyCorrelationId']).strip()
        format_correlation_id = str(data['formats'][0]['formatCorrelationId']).strip()  # Donant per suposat que hi haurà mínim un.

        # Get Glam Family id.
        glam_family_id, nothing_to_do = get_value_from_database(mycursor, correlation_id=family_correlation_id, url=URL_FAMILIES)
        if glam_family_id is None:
            logging.error('Error sync:' + URL_PRODUCTS + ":" + correlation_id + 
                          " Missing product family: " + URL_FAMILIES + ":" + family_correlation_id)
            return

        data["familyId"] = glam_family_id

        # Synchronize product.
        _glam_product_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_PRODUCTS, correlation_id=data['correlationId'], data=data, filter_name="code", filter_value=data['code'])

        # Update
        if _glam_product_id is not None:
            # PUT product fields.
            try:
                glam_id, old_put_data_hash = get_value_from_database(mycursor, correlation_id, URL_PRODUCTS + "/" + str(_glam_product_id) + "/PUT")

                put_data = {
                    "code": data['code'],
                    "name": data['name'],
                    "description": data['description'],
                    "familyId": data['familyId'],
                    "eanCode": ""
                }
                #put_data_hash = hash(str(put_data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
                put_data_hash = hashlib.sha256(str(put_data).encode('utf-8')).hexdigest()

                if old_put_data_hash is None or str(old_put_data_hash) != str(put_data_hash):
                    req = requests.put(url=URL_API + URL_PRODUCTS + '/' + str(_glam_product_id),
                                       data=json.dumps(put_data), headers=headers,
                                       verify=False, timeout=CONN_TIMEOUT)
                    if req.status_code != 200:
                        raise Exception('PUT with error')
                    
                    update_value_from_database(dbOrigin, mycursor, correlation_id, str(_glam_product_id), str(put_data_hash), URL_PRODUCTS + "/" + str(_glam_product_id) + "/PUT")

            except Exception as err:
                logging.error('Error sync:' + URL_PRODUCTS + ":" + correlation_id + ' With error: ' + str(err))

            # Sync product costs.
            try:
                for product_cost in data['costs']:
                    post_product_costs_url = URL_PRODUCTS + '/' + str(_glam_product_id) + URL_COSTS + "/" + str(product_cost['date'])
                    cost_glam_id, old_post_data_costs_hash = get_value_from_database(mycursor, correlation_id, post_product_costs_url)

                    post_product_costs_data = {"productId": _glam_product_id, "date": product_cost['date'], "cost": product_cost['cost']}
                    #post_product_cost_data_hash = hash(str(post_product_costs_data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor
                    post_product_cost_data_hash = hashlib.sha256(str(post_product_costs_data).encode('utf-8')).hexdigest()

                    if cost_glam_id is None:
                        req_post = requests.post(
                            url=URL_API + URL_PRODUCTS + '/' + str(_glam_product_id) + URL_COSTS,
                            data=json.dumps(post_product_costs_data), headers=headers,
                            verify=False, timeout=CONN_TIMEOUT)
                        if req_post.status_code == 201:
                            update_value_from_database(dbOrigin, mycursor, correlation_id, str(req_post.json()['id']), str(post_product_cost_data_hash), post_product_costs_url)
                        else:
                            _sync_cost(dbOrigin, mycursor, headers, correlation_id,
                                       post_product_costs_url, post_product_costs_data,
                                       post_product_cost_data_hash, _glam_product_id, product_cost, cost_glam_id)
                    else:
                        if old_post_data_costs_hash is None or str(old_post_data_costs_hash) != str(post_product_cost_data_hash):
                            req_put = requests.put(
                                url=URL_API + URL_PRODUCTS + '/' + str(_glam_product_id) + URL_COSTS + "/" + str(cost_glam_id),
                                data=json.dumps(post_product_costs_data), headers=headers,
                                verify=False, timeout=CONN_TIMEOUT)
                            if req_put.status_code == 200:
                                update_value_from_database(dbOrigin, mycursor, correlation_id, str(req_post.json()['id']), str(post_product_cost_data_hash), post_product_costs_url)
                            else:
                                _sync_cost(dbOrigin, mycursor, headers, correlation_id,
                                           post_product_costs_url, post_product_costs_data,
                                           post_product_cost_data_hash, _glam_product_id, product_cost, cost_glam_id)

            except Exception as err:
                logging.error('Error sync:' + URL_PRODUCTS + ":" + correlation_id + ' With error: ' + str(err))

            # Synchronize product formats.
            try:
                sync_format_data = {
                    "correlationId": format_correlation_id,
                    "formatCode": format_correlation_id,
                    "quantity": 1
                }
                synch_by_database(dbOrigin, mycursor, headers, url=URL_PRODUCTS + '/' + str(_glam_product_id) + URL_FORMATS, correlation_id=format_correlation_id,
                                  data=sync_format_data, filter_name="formatCode", filter_value=format_correlation_id)

            except Exception as err:
                logging.error('Error synch: ' + URL_PRODUCTS + '/' + str(_glam_product_id) + URL_FORMATS + ":" + correlation_id + ' With error: ' + str(err))

        # Activate synchronized product.
        if _has_been_posted is not None and _has_been_posted is True:
            try:
                patch_data = {"action": "ChangeState", "stateId": 2}  # 2 = State active.
                req = requests.patch(url=URL_API + URL_PRODUCTS + '/' + str(_glam_product_id),
                                     data=json.dumps(patch_data), headers=headers,
                                     verify=False, timeout=CONN_TIMEOUT)
                if req.status_code != 200:
                    raise Exception('PATCH with error')
            except Exception as err:
                logging.error('Error synch: ' + URL_PRODUCTS + ':' + correlation_id + " With error: " + str(err))

def _sync_cost(dbOrigin, mycursor, headers, correlation_id, product_cost_url, product_cost_data, product_cost_data_hash, product_glam_id, product_cost, cost_glam_id):
    # Obtain element to update
    req_get = requests.get(
        url=URL_API + URL_PRODUCTS + '/' + str(product_glam_id) + URL_COSTS,
        headers=headers, verify=False, timeout=CONN_TIMEOUT)

    if req_get.status_code == 200:
        item = next((i for i in req_get.json() if
                     datetime.datetime.strptime(i['date'], "%Y-%m-%dT%H:%M:%S").date() ==
                     datetime.datetime.strptime(product_cost['date'], "%Y-%m-%d").date())
                    , None)

        if item is not None:
            item_costs_data = {"productId": product_glam_id, "date": product_cost['date'],
                               "cost": product_cost['cost']}
            #item_costs_data_hash = hash(str(item_costs_data))      # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            item_costs_data_hash = hashlib.sha256(str(item_costs_data).encode('utf-8')).hexdigest()

            if item_costs_data_hash != product_cost_data_hash:
                # Update element.
                req_put = requests.put(
                    url=URL_API + URL_PRODUCTS + '/' + str(product_glam_id) + URL_COSTS + "/" + item.id,
                    data=json.dumps(product_cost_data), headers=headers,
                    verify=False, timeout=CONN_TIMEOUT)
                if req_put.status_code == 200:
                     update_value_from_database(dbOrigin, mycursor, correlation_id, str(item['id']), str(product_cost_data_hash), product_cost_url)
                else:
                    raise Exception('PUT cost with error')
            update_value_from_database(dbOrigin, mycursor, correlation_id, str(item['id']), str(product_cost_data_hash), product_cost_url)
        else:
            if cost_glam_id is not None:
                delete_value_from_database(dbOrigin, mycursor, correlation_id, product_cost_url)
            logging.error('Error processing Cost with url:' + product_cost_url)

def synchronize_queues(dbOrigin, mycursor, now):
    logging.info('   Syncronizing queues...')

    try:
        # Preparing message queues
        myRabbit_MERCADERIES_families = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE_FAMILIES)
        myRabbit_MERCADERIES_projects = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE_PROJECTS)
        myRabbit_MERCADERIES_products = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE_PRODUCTS)

        def callback_MERCADERIES_families(ch, method, properties, body):
            # Calculate access token and header for the request
            token = calculate_access_token(ENVIRONMENT)
            headers = calculate_json_header(token)

            sync_families(dbOrigin, mycursor, headers,
                data=json.loads(body) # Faig un json.loads per convertir d'String a diccionari
            )

        def callback_MERCADERIES_projects(ch, method, properties, body):
            # Calculate access token and header for the request
            token = calculate_access_token(ENVIRONMENT)
            headers = calculate_json_header(token)

            sync_projects(dbOrigin, mycursor, headers,
                data=json.loads(body) # Faig un json.loads per convertir d'String a diccionari
            )

        def callback_MERCADERIES_products(ch, method, properties, body):
            # Calculate access token and header for the request
            token = calculate_access_token(ENVIRONMENT)
            headers = calculate_json_header(token)

            sync_products2(dbOrigin, mycursor, headers,
                data=json.loads(body) # Faig un json.loads per convertir d'String a diccionari
            )

        myRabbit_MERCADERIES_families.channel.queue_declare(queue=myRabbit_MERCADERIES_families.queue_name)
        myRabbit_MERCADERIES_families.channel.basic_consume(queue=myRabbit_MERCADERIES_families.queue_name, on_message_callback=callback_MERCADERIES_families, auto_ack=True)

        myRabbit_MERCADERIES_projects.channel.queue_declare(queue=myRabbit_MERCADERIES_projects.queue_name)
        myRabbit_MERCADERIES_projects.channel.basic_consume(queue=myRabbit_MERCADERIES_projects.queue_name, on_message_callback=callback_MERCADERIES_projects, auto_ack=True)

        myRabbit_MERCADERIES_products.channel.queue_declare(queue=myRabbit_MERCADERIES_products.queue_name)
        myRabbit_MERCADERIES_products.channel.basic_consume(queue=myRabbit_MERCADERIES_products.queue_name, on_message_callback=callback_MERCADERIES_products, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        myRabbit_MERCADERIES_families.channel.start_consuming()
        myRabbit_MERCADERIES_projects.channel.start_consuming()
        myRabbit_MERCADERIES_products.channel.start_consuming()

    except Exception as e:
        logging.error('   Unexpected error processing queued messages: ' + str(e))
        send_email("ERPSincronitzarConsumer", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbOrigin)
        sys.exit(1)

def main():

    executionResult = "OK"

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename="log/ERPSincronitzarConsumer.log", level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP Sincronitzar Consumer - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to sincro database (EMMEGI - MySQL)
    dbOrigin = None
    try:
        dbOrigin = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        mycursor = dbOrigin.cursor()
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL emmegi database: ' + str(e))
        send_email("ERPSincronitzarConsumer", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbOrigin)
        sys.exit(1)

    synchronize_queues(dbOrigin, mycursor, now)    

    # Send email with execution summary
    send_email("ERPSincronitzarConsumer", ENVIRONMENT, now, datetime.datetime.now(), executionResult)

    logging.info('END ERP Sincronitzar Consumer - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('')

    # Closing database
    mycursor.close()
    dbOrigin.close()

    sys.exit(0)

    #logging.debug('debug message')
    #logging.info('info message')
    #logging.warning('warn message')
    #logging.error('error message')
    #logging.critical('critical message')

if __name__ == '__main__':
    main()