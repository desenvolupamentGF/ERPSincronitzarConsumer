# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
# TEST (0) O PRODUCCIÓ (1) ... BE CAREFUL!!!
ENVIRONMENT = 1
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
import time
from utils import send_email, connectMySQL, disconnectMySQL
from utils import calculate_access_token, calculate_json_header
import os

# Import needed library for HTTP requests
import requests

# End points URLs
URL_FAMILIES = '/productFamilies'
URL_LOCATIONS = '/locations'
URL_CONTAINERS = '/containers'
URL_PRODUCTS = '/products'
URL_FORMATS = '/formats'
URL_COSTS = '/standardCosts'
URL_DEPARTMENTS = '/departments'
URL_WORKERS = '/workers'
URL_CONTRACTS = '/contracts'
URL_USERS = '/users'
URL_ORGANIZATIONS = '/organizations'

URL_ZONES = '/zones'
URL_WAREHOUSES = '/warehouses'
URL_PLANTS = '/plants'
URL_GEOLOCATIONS = '/geolocations'

URL_COUNTRIES = '/countries'

# Glam Suite constants
GLAMSUITE_DEFAULT_COMPANY_ID = os.environ['GLAMSUITE_DEFAULT_COMPANY_ID']
GLAMSUITE_DEFAULT_ZONE_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_ID']
GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID = os.environ['GLAMSUITE_DEFAULT_CONTAINER_TYPE_ID']

GLAMSUITE_DEFAULT_ZONE_EPI_ID = os.environ['GLAMSUITE_DEFAULT_ZONE_EPI_ID']

# Rabbit constants for messaging
RABBIT_URL = os.environ['RABBIT_URL']
RABBIT_PORT = os.environ['RABBIT_PORT']
RABBIT_QUEUE = os.environ['RABBIT_QUEUE']

# Database constants
MYSQL_USER = os.environ['MYSQL_USER']
MYSQL_PASSWORD = os.environ['MYSQL_PASSWORD']
MYSQL_HOST = os.environ['MYSQL_HOST']
MYSQL_DATABASE = os.environ['MYSQL_DATABASE']

# Other constants
CONN_TIMEOUT = 50

# Some global values
glo_warehouse_location_mask = ""
glo_zone_code = ""
glo_warehouse_code = ""
glo_plant_code = ""
glo_geolocation_code = ""
glo_aisle_code = ""
glo_rack_code = ""
glo_shelf_code = ""

glo_warehouse_location_mask_epi = ""
glo_zone_code_epi = ""
glo_warehouse_code_epi = ""
glo_plant_code_epi = ""
glo_geolocation_code_epi = ""
glo_aisle_code_epi = ""
glo_rack_code_epi = ""
glo_shelf_code_epi = ""

URL_API = os.environ['URL_API_TEST']
if ENVIRONMENT == 1:
    URL_API = os.environ['URL_API_PROD']

####################################################################################################

def mask_letter(mask, letter, replacement):
    ret = mask

    position = mask.find(letter + "(")
    if position >= 0:   # If we find the letter inside mask
        startBracket = mask.find("(")
        endBracket = mask.find(")")
        if startBracket >= 0 and endBracket >= 0 and startBracket < endBracket:     # If the letter is followed but brackets and a number inside the brackets
            numberPositionsReplacement = int(mask[startBracket+1:endBracket])
            newValue = replacement[-numberPositionsReplacement:]    # We take as many positions as needed from the replacement starting from the right
            newValue = newValue.zfill(numberPositionsReplacement)   # Filling with as many 0s on the left as needed
            ret = ret.replace(letter + "(" + str(numberPositionsReplacement) + ")", newValue)   # Finally the mask is replaced with the new calculated value

    return ret

def calculate_mask_value(warehouse_location_mask, zone_code, warehouse_code, plant_code, geolocation_code, aisle_code, rack_code, shelf_code, position_code):
    # warehouse_location_mask can be formed with letters and some other characters as brackets, dots, etc.
    # Need to deal with the following letters in the mask:
    # G(n)  -> Geolocation where n in the number of positions of the code I will replace with (left filled with 0s)
    # PT(n) -> Plant where n in the number of positions of the code I will replace with (left filled with 0s)
    # W(n)  -> Warehouse where n in the number of positions of the code I will replace with (left filled with 0s)
    # Z(n)  -> Zone where n in the number of positions of the code I will replace with (left filled with 0s)
    # H(n)  -> Aisle where n in the number of positions of the code I will replace with (left filled with 0s)
    # C(n)  -> Rack where n in the number of positions of the code I will replace with (left filled with 0s)
    # HT(n) -> Shelf where n in the number of positions of the code I will replace with (left filled with 0s)
    # P(n)  -> Position where n in the number of positions of the code I will replace with (left filled with 0s)
    # Example warehouse_location_mask being G(1)W(1)Z(2)-P(5). This means:
    #          First position starting on the right of the Geolocation code
    #          First position starting on the right of the Warehouse code
    #          First two positions starting on the right of the Zone code (filled with zeros on the left to complete 2 positions if needed)
    #          The character "-"
    #          First 5 positions starting on the right of the project/location code (filled with zeros on the left to complete 5 positions if needed)
    #          So... being the geolocation code GS, with G(1) we will use "S"
    #                being the warehouse code WS, with W(1) we will use "S"
    #                being zone code ZS, with Z(2) we will use "ZS"
    #                and being position code 123, with P(5) we will use "00123"
    #          In this particular example detailed above, mask G(1)W(1)Z(2)-P(5) becomes "SSZS-00123"    
    ret = warehouse_location_mask

    warehouse_location_mask_aux = warehouse_location_mask.replace(")", ")|")    # I add a pipe "|" immediately after every ")"
    maskList = warehouse_location_mask_aux.split("|")   # I use the pipe "|" to separate the mask in a list of submasks
    for mask in maskList:   # I process every submask applying each of the possible letters I can find on it
        if mask != "":
            ret = ret.replace(mask, mask_letter(mask, "G", geolocation_code))
            ret = ret.replace(mask, mask_letter(mask, "PT", plant_code))
            ret = ret.replace(mask, mask_letter(mask, "W", warehouse_code))
            ret = ret.replace(mask, mask_letter(mask, "Z", zone_code))
            ret = ret.replace(mask, mask_letter(mask, "H", aisle_code))
            ret = ret.replace(mask, mask_letter(mask, "C", rack_code))
            ret = ret.replace(mask, mask_letter(mask, "HT", shelf_code))
            ret = ret.replace(mask, mask_letter(mask, "P", position_code))
    return ret

####################################################################################################

class RabbitPublisherService():

    def __init__(self, rabbit_url: str, rabbit_port: str, queue_name: str):
        self.rabbit_url = rabbit_url
        self.rabbit_port = rabbit_port
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_url, port=self.rabbit_port))
        self.channel = self.connection.channel()

####################################################################################################

def get_value_from_database(mycursor, correlation_id: str, url, endPoint, origin):
    mycursor.execute("SELECT erpGFId, hash FROM gfintranet.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND origin = '" + str(origin) + "' AND correlationId = '" + str(correlation_id).replace("'", "''") + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
    myresult = mycursor.fetchall()

    erpGFId = None
    hash = None
    for x in myresult:
        erpGFId = str(x[0])
        hash = str(x[1])

    return erpGFId, hash

def update_value_from_database(dbOrigin, mycursor, correlation_id: str, erpGFId, hash, url, endPoint, origin):
    sql = "INSERT INTO gfintranet.ERPIntegration (companyId, endpoint, origin, correlationId, deploy, callType, erpGFId, hash) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE erpGFId=VALUES(erpGFId), hash=VALUES(hash)"
    val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), str(endPoint), str(origin), str(correlation_id), str(ENVIRONMENT), url, erpGFId, hash)
    mycursor.execute(sql, val)
    dbOrigin.commit()    

def delete_value_from_database(dbOrigin, mycursor, correlation_id: str, url, endPoint, origin):
    mycursor.execute("DELETE FROM gfintranet.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) +"' AND origin = '" + str(origin) + "' AND correlationId = '" + str(correlation_id).replace("'", "''") + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
    dbOrigin.commit()

####################################################################################################

def synch_by_database(dbOrigin, mycursor, headers, url: str, correlation_id: str, producerData: dict, data: dict, filter_name: str = "", filter_value: str = "", endPoint = "", origin = ""):
    """
    Synchronize objects with the API.
    Always returns the specific Glam ID.
    """

    key = url + ":" + data['correlationId']
    #data_hash = hash(str(producerData))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
    data_hash = hashlib.sha256(str(producerData).encode('utf-8')).hexdigest()
    glam_id, old_data_hash = get_value_from_database(mycursor, correlation_id, url, endPoint, origin)

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
        if endPoint == "Users ERP GF": # This endpoint is not returning "id" as the others
            p_glam_id = req.json()['userName']
        else:
            p_glam_id = req.json()['id']
        update_value_from_database(dbOrigin, mycursor, correlation_id, p_glam_id, str(data_hash), url, endPoint, origin)
        return p_glam_id, True
    elif req.status_code == 204:
        delete_value_from_database(dbOrigin, mycursor, correlation_id, url, endPoint, origin)
        return None, False
    elif req.status_code in [400, 422]:  # Bad request. If an element with the same code already exists, we will use it.
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
                    if endPoint == "Users ERP GF": # This endpoint is not returning "id" as the others
                        id = str(item['userName'])
                    else:
                        id = str(item['id'])
                    update_value_from_database(dbOrigin, mycursor, correlation_id, id, str(data_hash), url, endPoint, origin)
                    return id, True
                else:
                    logging.error('Error posting to GlamSuite with ' + key)
                    return None, False
        except Exception as err:
            return None, False
    elif req.status_code == 404:  # Not found. (PUT id not found)
        delete_value_from_database(dbOrigin, mycursor, correlation_id, url, endPoint, origin)
        return None, False
    else:
        logging.error(
            'Error sync:' + key + '\n    json:' + json.dumps(data) +
            '\n    HTTP Status: ' + str(req.status_code) + ' Content: ' + str(req.content))  
        return None, False

####################################################################################################

def sync_usuaris(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    print ("New message: usuari")
    """
    :param data: dict -> {
        "userName": "aezcurra",
        "name": "Amador", 
        "surname": "Ezcurra",
        "email": "aezcurra@garciafaura.com",
        "active": "1",
        "phoneNumber": "93 662 14 41",
        "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
        "roleId": GLAMSUITE_DEFAULT_GUEST_ROLE,
        "password": "aezcurraGf123!",
        "correlationId": "7"
    }
    :return None
    """
    # Synchronize users
    p_glam_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_USERS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin)

    if _has_been_posted is not None and _has_been_posted is True:
        try:
            if data['active'] == "1":
                dataStatus = { "action": "changeState", "stateId": "1"}
            else:
                dataStatus = { "action": "changeState", "stateId": "2"}

            req = requests.patch(url=URL_API + URL_USERS + '/' + str(p_glam_id), data=json.dumps(dataStatus), headers=headers)
            if (req.status_code != 200 and req.status_code != 400): # (success code - 200 or 400)
                    raise Exception('PATCH with error when activating/deactivating user')
        except Exception as err:
            logging.error('Error synch activating/deactivating with error: ' + str(err))          

####################################################################################################

def sync_departaments(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    print ("New message: departament")
    """
    :param data: dict -> {
        "name": "COMERCIAL",
        "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
        "calendarId": GLAMSUITE_DEFAULT_CALENDAR_ID,
        "correlationId": "7"
    }
    :return None
    """
    # Synchronize department
    synch_by_database(dbOrigin, mycursor, headers, url=URL_DEPARTMENTS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin)

def sync_treballadors(dbOrigin, mycursor, headers, maskValue, data: dict, endPoint, origin):
    print ("New message: treballador")
    """
    :param data: dict -> {
        "name": "ARCOS ESPINOSA JESUS",
        "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
        "nationality": "ES",
        "identificationTypeId": 0,
        "identificationNumber": "46457469E",
        "address": "CL ONZE DE SETEMBRE 6 2 4",
        "postalCode": "08840",
        "city": "VILADECANS",
        "region": "BARCELONA",
        "countryId": "ES",
        "linkedInProfile": " ", 
        "iban": "ESXXXXXXXXXXXXXXXXXXXXXX",
        "costs": [
            {
                "exercise": "2020",
                "cost": 30000
            }
        ],        
        "correlationId": "533"
    },
    :param dataContract: dict -> {
        "contractNumber": "100/21",
        "contractTypeId": 1,
        "startDate": "2024-02-22T12:38:41.440Z",
        "endDate": "2024-02-22T12:38:41.440Z",
        "departmentId": departmentId,
        "workforceId": "",
        "workingHours": 40,
        "correlationId": "533"
    }
    :return None
    """

    dataAux = data.copy() # copy of the original data received from producer. I need it for hash purposes cos I will make changes on it.

    # We need the GUID for the nationality
    get_req = requests.get(URL_API + URL_COUNTRIES + f"?search={data['nationality']}", headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
    
    if get_req.status_code == 200:                
        item = next((i for i in get_req.json() if i["isoAlfa2"].casefold() == data['nationality'].casefold()), None)

        if item is not None:
            data['nationality'] = item["id"]
        else:
            logging.error('Error nationality not found:' + data['nationality'])
            return            

    # We need the GUID for the country
    get_req = requests.get(URL_API + URL_COUNTRIES + f"?search={data['countryId']}", headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
    
    if get_req.status_code == 200:                
        item = next((i for i in get_req.json() if i["isoAlfa2"].casefold() == data['countryId'].casefold()), None)

        if item is not None:
            data['countryId'] = item["id"]
        else:
            logging.error('Error country not found:' + data['countryId'])
            return            

    # Synchronize worker
    _glam_worker_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_WORKERS, correlation_id=data['correlationId'], producerData=dataAux, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin)

    if _glam_worker_id is not None:  

        # Workers API Post don't allow linkedin and iban fields
        # Let's synchronize it with the PUT request.
        # We have to GET the element, compare it and modify it if needed.
        try:
            req = requests.get(url=URL_API + URL_WORKERS + '/' + str(_glam_worker_id), headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
            _glam_linkedInProfile = req.json()['linkedInProfile']
            _glam_iban = req.json()['iban']
        except Exception as err:
            logging.error('Error sync:' + URL_WORKERS + ":" + str(_glam_worker_id) + " With error: " + str(err))
            return

        if data['linkedInProfile'] != _glam_linkedInProfile or data['iban'] != _glam_iban:
            try:
                req = requests.put(url=URL_API + URL_WORKERS + '/' + str(_glam_worker_id),
                                   data=json.dumps(data), headers=headers,
                                   verify=False, timeout=CONN_TIMEOUT)
                if req.status_code != 200:
                    raise Exception('PUT with error')
            except Exception as err:
                logging.error('Error sync:' + URL_WORKERS + ":" + str(_glam_worker_id) + " With error: " + str(err))

        if _has_been_posted is not None and _has_been_posted is True:

            # Sync worker costs
            try:
                patch_data = {"costs": data['costs']}   
                req = requests.patch(url=URL_API + URL_WORKERS + '/' + str(_glam_worker_id),
                                     data=json.dumps(patch_data), headers=headers,
                                     verify=False, timeout=CONN_TIMEOUT)
                if req.status_code != 200:
                    raise Exception('PATCH with error')
            except Exception as err:
                logging.error('Error synch: ' + URL_WORKERS + ':' + str(_glam_worker_id) + " With error: " + str(err))

        # Sync worker contract
        dataContract = data['dataContract']
        if dataContract['contractNumber'] != "":

            # Get Glam Department id
            glam_department_id, nothing_to_do = get_value_from_database(mycursor, correlation_id=dataContract['departmentId'], url=URL_DEPARTMENTS, endPoint=endPoint, origin=origin)
            if glam_department_id is None:
                logging.error('Error sync:' + URL_CONTRACTS + ":" + str(dataContract['contractNumber']) + 
                              " Missing department: " + URL_DEPARTMENTS + ":" + str(dataContract['departmentId']))
                return

            dataContract["departmentId"] = glam_department_id

            # Synchronize contract
            _glam_contract_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_WORKERS + '/' + str(_glam_worker_id) + URL_CONTRACTS, correlation_id=dataContract['contractNumber'], producerData=dataContract, data=dataContract, filter_name="contractNumber", filter_value=dataContract['contractNumber'], endPoint=endPoint, origin=origin)

        # Sync EPI location
        dataLocation = data['dataLocation']
        p_correlation_id = dataLocation['correlationId']
        p_gf_description = dataLocation['description']
        p_glam_id, nothing_to_do = synch_by_database(dbOrigin, mycursor, headers, url=URL_LOCATIONS, correlation_id=p_correlation_id,
                                                     producerData=dataLocation, data=dataLocation, filter_name='code', filter_value=str(maskValue).strip(), endPoint=endPoint, origin=origin)
        if p_glam_id is None:  # Synchronization error.
            return

        # 11/22: Locations API Post don't allow description field.
        # Let's synchronize it with the PUT request.
        # We have to GET the element, compare it and modify it if needed.
        try:
            req = requests.get(url=URL_API + URL_LOCATIONS + '/' + str(p_glam_id), headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
            _glam_description = req.json()['description']
            _container_id = req.json()['container_id']

        except Exception as err:
            logging.error('Error sync:' + URL_LOCATIONS + ":" + p_correlation_id + " With error: " + str(err))
            return

        if p_gf_description != _glam_description:
            try:
                put_data = {"description": p_gf_description}
                req = requests.put(url=URL_API + URL_CONTAINERS + '/' + str(_container_id),
                                   data=json.dumps(put_data), headers=headers,
                                   verify=False, timeout=CONN_TIMEOUT)
                if req.status_code != 200:
                    raise Exception('PUT with error')
            except Exception as err:
                logging.error('Error sync:' + URL_LOCATIONS + ":" + p_correlation_id + " With error: " + str(err))

####################################################################################################

def sync_families(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    print ("New message: familia")
    """
    :param data: dict -> {
        "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
        "productTypeId": 1,                         # S'ha acordat internament que posarem tipus 'Material'.
        "batchTraceabilityId": 1,                   # S'ha acordat internament que posarem tipus 'Batch'.
        "batchSelectionCriteriaId": 1,              # S'ha acordat internament que posarem tipus 'FIFO'.        
        "code": "02",
        "name": "Good 01 Material",
        "correlationId": "02"
    }
    :return None
    """
    synch_by_database(dbOrigin, mycursor, headers, url=URL_FAMILIES, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin)

def sync_projects(dbOrigin, mycursor, headers, maskValue, data: dict, endPoint, origin):
    print ("New message: project")
    """
    Sincronitzem els projectes. A 11/22 només creem ubicacions.
    :param data: dict -> {
        "correlationId": "0523",
        "name": "Obra que hi ha a Brasil",
        "zoneId": "123ABC",
        "containerTypeId": "123ABC",
        "aisle": "A",
        "rack": "A",
        "shelf": "A",
        "position": "0523",
        "preferential": False
    }
    :return: None
    """
    p_correlation_id = data['correlationId']
    p_gf_description = data['description']
    p_glam_id, nothing_to_do = synch_by_database(dbOrigin, mycursor, headers, url=URL_LOCATIONS, correlation_id=p_correlation_id,
                                                 producerData=data, data=data, filter_name='code', filter_value=str(maskValue).strip(), endPoint=endPoint, origin=origin)
    if p_glam_id is None:  # Synchronization error.
        return

    # 11/22: Locations API Post don't allow description field.
    # Let's synchronize it with the PUT request.
    # We have to GET the element, compare it and modify it if needed.
    try:
        req = requests.get(url=URL_API + URL_LOCATIONS + '/' + str(p_glam_id), headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
        _glam_description = req.json()['description']
        _container_id = req.json()['container_id']
    except Exception as err:
        logging.error('Error sync:' + URL_LOCATIONS + ":" + p_correlation_id + " With error: " + str(err))
        return

    if p_gf_description != _glam_description:
        try:
            put_data = {"description": p_gf_description}
            req = requests.put(url=URL_API + URL_CONTAINERS + '/' + str(p_glam_id),
                               data=json.dumps(put_data), headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
            if req.status_code != 200:
                raise Exception('PUT with error')
        except Exception as err:
            logging.error('Error sync:' + URL_LOCATIONS + ":" + p_correlation_id + " With error: " + str(err))
    return

def sync_products(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    print ("New message: product")        
    """
    :param data: dict -> {
        "correlationId": "02",
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

    correlation_id = data['correlationId']
    format_correlation_id = str(data['formats'][0]['formatCorrelationId']).strip()  # Donant per suposat que hi haurà mínim un.

    # Synchronize product.
    _glam_product_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_PRODUCTS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="code", filter_value=data['code'], endPoint=endPoint, origin=origin)

    # Update
    if _glam_product_id is not None:
        # PUT product fields.
        try:
            glam_id, old_put_data_hash = get_value_from_database(mycursor, correlation_id, URL_PRODUCTS + "/" + str(_glam_product_id) + "/PUT", endPoint, origin)

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
                 
                update_value_from_database(dbOrigin, mycursor, correlation_id, str(_glam_product_id), str(put_data_hash), URL_PRODUCTS + "/" + str(_glam_product_id) + "/PUT", endPoint, origin)

        except Exception as err:
            logging.error('Error sync:' + URL_PRODUCTS + ":" + correlation_id + ' With error: ' + str(err))

        # Sync product costs.
        try:
            for product_cost in data['costs']:
                post_product_costs_url = URL_PRODUCTS + '/' + str(_glam_product_id) + URL_COSTS + "/" + str(product_cost['date'])
                cost_glam_id, old_post_data_costs_hash = get_value_from_database(mycursor, correlation_id, post_product_costs_url, endPoint, origin)

                post_product_costs_data = {"productId": _glam_product_id, "date": product_cost['date'], "cost": product_cost['cost']}
                #post_product_cost_data_hash = hash(str(post_product_costs_data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor
                post_product_cost_data_hash = hashlib.sha256(str(post_product_costs_data).encode('utf-8')).hexdigest()

                if cost_glam_id is None:
                    req_post = requests.post(
                        url=URL_API + URL_PRODUCTS + '/' + str(_glam_product_id) + URL_COSTS,
                        data=json.dumps(post_product_costs_data), headers=headers,
                        verify=False, timeout=CONN_TIMEOUT)
                    if req_post.status_code == 201:
                        update_value_from_database(dbOrigin, mycursor, correlation_id, str(req_post.json()['id']), str(post_product_cost_data_hash), post_product_costs_url, endPoint, origin)
                    else:
                        sync_cost(dbOrigin, mycursor, headers, correlation_id,
                                  post_product_costs_url, post_product_costs_data,
                                  post_product_cost_data_hash, _glam_product_id, product_cost, cost_glam_id, 
                                  endPoint, origin)
                else:
                    if old_post_data_costs_hash is None or str(old_post_data_costs_hash) != str(post_product_cost_data_hash):
                        req_put = requests.put(
                            url=URL_API + URL_PRODUCTS + '/' + str(_glam_product_id) + URL_COSTS + "/" + str(cost_glam_id),
                            data=json.dumps(post_product_costs_data), headers=headers,
                            verify=False, timeout=CONN_TIMEOUT)
                        if req_put.status_code == 200:
                            update_value_from_database(dbOrigin, mycursor, correlation_id, str(req_put.json()['id']), str(post_product_cost_data_hash), post_product_costs_url, endPoint, origin)
                        else:
                            sync_cost(dbOrigin, mycursor, headers, correlation_id,
                                      post_product_costs_url, post_product_costs_data,
                                      post_product_cost_data_hash, _glam_product_id, product_cost, cost_glam_id, 
                                      endPoint, origin)

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
                              producerData=sync_format_data, data=sync_format_data, filter_name="formatCode", filter_value=format_correlation_id, endPoint=endPoint, origin=origin)

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

def sync_cost(dbOrigin, mycursor, headers, correlation_id, product_cost_url, product_cost_data, product_cost_data_hash, product_glam_id, product_cost, cost_glam_id, endPoint, origin):
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
                     update_value_from_database(dbOrigin, mycursor, correlation_id, str(item['id']), str(product_cost_data_hash), product_cost_url, endPoint, origin)
                else:
                    raise Exception('PUT cost with error')
            update_value_from_database(dbOrigin, mycursor, correlation_id, str(item['id']), str(product_cost_data_hash), product_cost_url, endPoint, origin)
        else:
            if cost_glam_id is not None:
                delete_value_from_database(dbOrigin, mycursor, correlation_id, product_cost_url, endPoint, origin)
            logging.error('Error processing Cost with url:' + product_cost_url)

####################################################################################################

def sync_proveidors(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    print ("New message: proveïdor")
    """
    :param data: dict -> {
        "code": "GF1506",
        "legalName": "ASTIGLASS S.L", 
        "tradeName": "ASTIGLASS S.L",
        "countryId": "ESP",
        "identificationType": {typeId: "3", number: "B41610775"},
        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
        "correlationId": "GF1506"
    }
    :return None
    """

    dataAux = data.copy() # copy of the original data received from producer. I need it for hash purposes cos I will make changes on it.

    # We need the GUID for the country
    get_req = requests.get(URL_API + URL_COUNTRIES + f"?search={data['countryId']}", headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
    
    if get_req.status_code == 200:                
        item = next((i for i in get_req.json() if i["isoAlfa3"].casefold() == data['countryId'].casefold()), None)

        if item is not None:
            data['countryId'] = item["id"]
        else:
            logging.error('Error country not found:' + data['countryId'])
            return            

    # Synchronize proveïdors
    p_glam_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_ORGANIZATIONS, correlation_id=data['correlationId'], producerData=dataAux, data=data, filter_name="tradeName", filter_value=str(data['tradeName']).strip(), endPoint=endPoint, origin=origin)

    if _has_been_posted is not None and _has_been_posted is True:
        try:
            req = requests.patch(url=URL_API + URL_ORGANIZATIONS + '/' + str(p_glam_id) + "/activate", headers=headers)
            if (req.status_code != 200 and req.status_code != 400): 
                    raise Exception('PATCH with error when activating proveïdor')
        except Exception as err:
            logging.error('Error synch activating proveïdor with error: ' + str(err))          

####################################################################################################

def sync_clients(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    print ("New message: client")
    """
    :param data: dict -> {
        "code": "000164",
        "legalName": "RUBATEC, SA", 
        "tradeName": "RUBATEC, SA",
        "countryId": "ESP",
        "identificationType": {typeId: "3", number: "A60744216"},
        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
        "correlationId": "000164"
    }
    :return None
    """

    dataAux = data.copy() # copy of the original data received from producer. I need it for hash purposes cos I will make changes on it.

    # We need the GUID for the country
    get_req = requests.get(URL_API + URL_COUNTRIES + f"?search={data['countryId']}", headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
    
    if get_req.status_code == 200:                
        item = next((i for i in get_req.json() if i["isoAlfa3"].casefold() == data['countryId'].casefold()), None)

        if item is not None:
            data['countryId'] = item["id"]
        else:
            logging.error('Error country not found:' + data['countryId'])
            return            

    # Synchronize clients
    p_glam_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_ORGANIZATIONS, correlation_id=data['correlationId'], producerData=dataAux, data=data, filter_name="tradeName", filter_value=str(data['tradeName']).strip(), endPoint=endPoint, origin=origin)

    if _has_been_posted is not None and _has_been_posted is True:
        try:
            req = requests.patch(url=URL_API + URL_ORGANIZATIONS + '/' + str(p_glam_id) + "/activate", headers=headers)
            if (req.status_code != 200 and req.status_code != 400): 
                    raise Exception('PATCH with error when activating client')
        except Exception as err:
            logging.error('Error synch activating client with error: ' + str(err))          

####################################################################################################

def global_values():
    # Calculate access token and header for the request
    token = calculate_access_token(ENVIRONMENT)
    headers = calculate_json_header(token)

    # Zone data
    req = requests.get(URL_API + URL_ZONES + '/' + GLAMSUITE_DEFAULT_ZONE_ID, headers=headers,
                       verify=False, timeout=CONN_TIMEOUT)
    global glo_zone_code 
    glo_zone_code = req.json()['code']
    warehouse_id = req.json()['warehouseId']

    # Warehouse data
    req = requests.get(URL_API + URL_WAREHOUSES + '/' + warehouse_id, headers=headers,
                       verify=False, timeout=CONN_TIMEOUT)
    global glo_warehouse_code
    glo_warehouse_code = req.json()['code']
    global glo_warehouse_location_mask
    glo_warehouse_location_mask = req.json()['locationMask']
    plant_id = req.json()['plantId']

    # Plant data
    req = requests.get(URL_API + URL_PLANTS + '/' + plant_id, headers=headers,
                       verify=False, timeout=CONN_TIMEOUT)
    global glo_plant_code
    glo_plant_code = req.json()['code']
    geolocation_id = req.json()['geolocationId']

    # Geolocation data
    req = requests.get(URL_API + URL_GEOLOCATIONS + '/' + geolocation_id, headers=headers,
                       verify=False, timeout=CONN_TIMEOUT)
    global glo_geolocation_code
    glo_geolocation_code = req.json()['code']

    # Aisle, rack, shelf
    global glo_aisle_code
    glo_aisle_code = "A"
    global glo_rack_code
    glo_rack_code = "A"
    global glo_shelf_code
    glo_shelf_code = "A"

    # Zone EPI data
    req = requests.get(URL_API + URL_ZONES + '/' + GLAMSUITE_DEFAULT_ZONE_EPI_ID, headers=headers,
                       verify=False, timeout=CONN_TIMEOUT)
    global glo_zone_code_epi 
    glo_zone_code_epi = req.json()['code']
    warehouse_id = req.json()['warehouseId']

    # Warehouse data
    req = requests.get(URL_API + URL_WAREHOUSES + '/' + warehouse_id, headers=headers,
                       verify=False, timeout=CONN_TIMEOUT)
    global glo_warehouse_code_epi
    glo_warehouse_code_epi = req.json()['code']
    global glo_warehouse_location_mask_epi
    glo_warehouse_location_mask_epi = req.json()['locationMask']
    plant_id = req.json()['plantId']

    # Plant data
    req = requests.get(URL_API + URL_PLANTS + '/' + plant_id, headers=headers,
                       verify=False, timeout=CONN_TIMEOUT)
    global glo_plant_code_epi
    glo_plant_code_epi = req.json()['code']
    geolocation_id = req.json()['geolocationId']

    # Geolocation data
    req = requests.get(URL_API + URL_GEOLOCATIONS + '/' + geolocation_id, headers=headers,
                       verify=False, timeout=CONN_TIMEOUT)
    global glo_geolocation_code_epi
    glo_geolocation_code_epi = req.json()['code']

    # Aisle, rack, shelf
    global glo_aisle_code_epi
    glo_aisle_code_epi = "A"
    global glo_rack_code_epi
    glo_rack_code_epi = "A"
    global glo_shelf_code_epi
    glo_shelf_code_epi = "A"

####################################################################################################
        
def main():

    # current date and time
    now = datetime.datetime.now() 

    # set up logging
    logging.basicConfig(filename=os.environ['LOG_FILE'], level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

    logging.info('START ERP Sincronitzar Consumer - ENVIRONMENT: ' + str(ENVIRONMENT))
    logging.info('   Connecting to database')

    # connecting to database (EMMEGI - MySQL)
    dbOrigin = None
    try:
        dbOrigin = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
        mycursor = dbOrigin.cursor()        
    except Exception as e:
        logging.error('   Unexpected error when connecting to MySQL emmegi database: ' + str(e))
        send_email("ERPSincronitzarConsumer", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbOrigin)
        sys.exit(1)
        
    try:
        # Populate some global values
        global_values()
    except Exception as e:
        logging.error('   Unexpected error calculating global values: ' + str(e))
        send_email("ERPSincronitzarConsumer", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")
        disconnectMySQL(dbOrigin)
        sys.exit(1)

    # Preparing message queue
    myRabbit = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)

    while True: # infinite loop

        try:

            def callback_message(ch, method, properties, body):
                # Calculate access token and header for the request
                token = calculate_access_token(ENVIRONMENT)
                headers = calculate_json_header(token)

                data = json.loads(body) # Faig un json.loads per convertir d'String a diccionari

                # Mercaderies
                if data['queueType'] == "MERCADERIES_FAMILIES":
                    sync_families(dbOrigin, mycursor, headers, data, 'Mercaderies ERP GF', 'Emmegi')
                if data['queueType'] == "MERCADERIES_PROJECTES":
                    maskValue = calculate_mask_value(glo_warehouse_location_mask, glo_zone_code, glo_warehouse_code, glo_plant_code, glo_geolocation_code, glo_aisle_code, glo_rack_code, glo_shelf_code, str(data['correlationId']).strip())
                    sync_projects(dbOrigin, mycursor, headers, maskValue, data, 'Mercaderies ERP GF', 'Emmegi')
                if data['queueType'] == "MERCADERIES_PRODUCTES":
                    sync_products(dbOrigin, mycursor, headers, data, 'Mercaderies ERP GF', 'Emmegi')

                # Treballadors
                if data['queueType'] == "TREBALLADORS_DEPARTAMENTS":
                    sync_departaments(dbOrigin, mycursor, headers, data, 'Treballadors ERP GF', 'Biostar')
                if data['queueType'] == "TREBALLADORS_TREBALLADORS":
                    maskValue = calculate_mask_value(glo_warehouse_location_mask_epi, glo_zone_code_epi, glo_warehouse_code_epi, glo_plant_code_epi, glo_geolocation_code_epi, glo_aisle_code_epi, glo_rack_code_epi, glo_shelf_code_epi, str(data['correlationId']).strip())
                    sync_treballadors(dbOrigin, mycursor, headers, maskValue, data, 'Treballadors ERP GF', 'Biostar')
                
                # Usuaris
                if data['queueType'] == "USERS_USERS":
                    sync_usuaris(dbOrigin, mycursor, headers, data, 'Users ERP GF', 'Emmegi')

                # Proveïdors
                if data['queueType'] == "ORGANIZATIONS_PROVEIDORS":
                    sync_proveidors(dbOrigin, mycursor, headers, data, 'Organizations ERP GF', 'Sage')
                if data['queueType'] == "ORGANIZATIONS_CLIENTS":
                    sync_clients(dbOrigin, mycursor, headers, data, 'Organizations ERP GF', 'Sage')

            myRabbit.channel.queue_declare(queue=myRabbit.queue_name)
            myRabbit.channel.basic_consume(queue=myRabbit.queue_name, on_message_callback=callback_message, auto_ack=True)
            myRabbit.channel.start_consuming()
        
        except Exception as e:
            logging.error('   Unexpected error processing queued messages: ' + str(e))
            send_email("ERPSincronitzarConsumer", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")            

            reconnect = False
            while not reconnect:
                logging.info('   Sleeping 60 seconds to reconnect with database&rabbit and retry...')
                time.sleep(60) 
            
                try:
                    dbOrigin = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
                    mycursor = dbOrigin.cursor()        
                    myRabbit = RabbitPublisherService(RABBIT_URL, RABBIT_PORT, RABBIT_QUEUE)
                    reconnect = True
                    logging.info('   Successfully reconnected. Execution continues...')
                    send_email("ERPSincronitzarConsumer - SUCCESSFULLY RECONNECTED", ENVIRONMENT, now, datetime.datetime.now(), "OK")  
                except Exception as e:
                    logging.error('   Unexpected error reconnecting to database&rabbit: ' + str(e))

    #logging.debug('debug message')
    #logging.info('info message')
    #logging.warning('warn message')
    #logging.error('error message')
    #logging.critical('critical message')

if __name__ == '__main__':
    main()