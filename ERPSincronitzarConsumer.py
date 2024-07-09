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
from utils import send_email, connectMySQL, disconnectMySQL, replaceCharacters
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
URL_WORKERS = '/workers'
URL_CONTRACTS = '/contracts'
URL_SALARIES = '/salaries'
URL_USERS = '/users'
URL_ORGANIZATIONS = '/organizations'
URL_PROVIDERS = '/providers'
URL_CUSTOMERS = '/customers'
URL_PERSONS = "/persons"
URL_CONTACTS = "/contacts"
URL_ADDRESS = "/address"
URL_PAYMENTMETHODS = '/paymentMethods'
URL_COMMERCIALCONDITIONS = '/commercialConditions'
URL_CALENDARS = '/calendars'
URL_HOLIDAYS = '/holidays'
URL_DEPARTMENTS = '/departments'
URL_TIMETABLES = '/timetables'
URL_WORKFORCES = '/workforces'
URL_CREDITRISKS = '/creditRisks'

URL_ZONES = '/zones'
URL_WAREHOUSES = '/warehouses'
URL_PLANTS = '/plants'
URL_GEOLOCATIONS = '/geolocations'

URL_COUNTRIES = '/countries'
URL_CUSTOMFIELDS = '/customFields'

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

# Global values
global GLOBAL_ENDPOINT
global GLOBAL_ORIGIN
global GLOBAL_CORRELATIONID
global GLOBAL_CALLTYPE

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
    mycursor.execute("SELECT erpGFId, hash FROM ERP_GF.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) + "' AND origin = '" + str(origin) + "' AND correlationId = '" + str(correlation_id).replace("'", "''") + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
    myresult = mycursor.fetchall()

    erpGFId = None
    hash = None
    for x in myresult:
        erpGFId = str(x[0])
        hash = str(x[1])

    return erpGFId, hash

def update_value_from_database(dbOrigin, mycursor, correlation_id: str, erpGFId, hash, url, endPoint, origin, helper):
    sql = "INSERT INTO ERP_GF.ERPIntegration (companyId, endpoint, origin, correlationId, deploy, callType, erpGFId, hash, helper) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE erpGFId=VALUES(erpGFId), hash=VALUES(hash), helper=VALUES(helper)"
    val = (str(GLAMSUITE_DEFAULT_COMPANY_ID), str(endPoint), str(origin), str(correlation_id), str(ENVIRONMENT), str(url), str(erpGFId), str(hash), str(helper))
    mycursor.execute(sql, val)
    dbOrigin.commit()    

def delete_value_from_database(dbOrigin, mycursor, correlation_id: str, url, endPoint, origin):
    mycursor.execute("DELETE FROM ERP_GF.ERPIntegration WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) +"' AND origin = '" + str(origin) + "' AND correlationId = '" + str(correlation_id).replace("'", "''") + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
    dbOrigin.commit()

# Due to database failures, some structures are not completely syncronized so better to update the hash for another retry
def reinit_hash(dbOrigin, mycursor, correlation_id: str, url, endPoint, origin):
    mycursor.execute("UPDATE ERP_GF.ERPIntegration SET hash = '123' WHERE companyId = '" + str(GLAMSUITE_DEFAULT_COMPANY_ID) + "' AND endpoint = '" + str(endPoint) +"' AND origin = '" + str(origin) + "' AND correlationId = '" + str(correlation_id).replace("'", "''") + "' AND deploy = " + str(ENVIRONMENT) + " AND callType = '" + str(url) + "'")
    dbOrigin.commit()

####################################################################################################

def synch_by_database(dbOrigin, mycursor, headers, url: str, correlation_id: str, producerData: dict, data: dict, filter_name: str = "", filter_value: str = "", endPoint = "", origin = "", helper = ""):
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

    if req.status_code in [200, 201]:  # PUT or POST success 
        if endPoint == "Users ERP GF": # This endpoint is not returning "id" as the others
            p_glam_id = req.json()['userName']
        else:
            p_glam_id = req.json()['id']
        update_value_from_database(dbOrigin, mycursor, correlation_id, p_glam_id, str(data_hash), url, endPoint, origin, helper)
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
                    update_value_from_database(dbOrigin, mycursor, correlation_id, id, str(data_hash), url, endPoint, origin, helper)
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
    logging.info('New message: usuari')
    """
    :param data: dict -> {
        "userName": "aezcurra",
        "name": "Amador", 
        "surname": "Ezcurra",
        "email": "aezcurra@garciafaura.com",
        "active": "1",
        "phoneNumber": "93 662 14 41",
        "changePassword": True,
        "languageId": GLAMSUITE_DEFAULT_LANGUAGE_CATALA,
        "roleId": GLAMSUITE_DEFAULT_GUEST_ROLE,
        "password": "aezcurraGf123!",
        "correlationId": "7"
    }
    :return None
    """

    dataAux = data.copy() # copy of the original data received from producer. I need it for hash purposes cos I will make changes on it.

    # We check if the user already exists
    get_req = requests.get(URL_API + URL_USERS + f"?search={data['userName']}", headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
    
    if get_req.status_code == 200:                
        item = next((i for i in get_req.json() if i["userName"].casefold() == data['userName'].casefold()), None)

        if item is not None: # If already exists in ERP GF, we keep current role, current state and current changePassword
            if item["roleId"] != "":
                data['roleId'] = item["roleId"]
            if str(item["stateId"]) == "2": 
                data['active'] = "0" # Inactive   
            else: # str(item["stateId"]) == "1"
                data['active'] = "1" # Active   
            data['changePassword'] = item["changePassword"]
        else: # If does not exist in ERP GF, we will create it as inactive and role guest (no need to change guest cos producer creates them as guest)
            data['active'] = "0" # Inactive
            
    # Synchronize users
    p_glam_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_USERS, correlation_id=data['correlationId'], producerData=dataAux, data=data, filter_name="userName", filter_value=str(data['userName']).strip(), endPoint=endPoint, origin=origin, helper="")

    if _has_been_posted is not None and _has_been_posted is True:
        try:
            if data['active'] == "1":
                dataStatus = { "action": "changeState", "stateId": "1"} # Active
            else:
                dataStatus = { "action": "changeState", "stateId": "2"} # Inactive

            req = requests.patch(url=URL_API + URL_USERS + '/' + str(p_glam_id), data=json.dumps(dataStatus), headers=headers)
            if (req.status_code != 200 and req.status_code != 400): # (success code - 200 or 400)
                    raise Exception('PATCH with error when activating/deactivating user')
        except Exception as err:
            logging.error('Error synch activating/deactivating with error: ' + str(err))          

####################################################################################################

def sync_treballadors(dbOrigin, mycursor, headers, maskValue, data: dict, endPoint, origin):
    logging.info('New message: treballador')
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
                "date": "2024-06-27T06:07:21.492Z",
                "annualGrossSalary": 30000,
                "annualSocialSecurityContribution": 2000,
                "annualOtherExpenses": 0,
                "correlationId": "46457469E"                
            }
        ],        
        "contracts": [
            {
                "contractNumber": "100/21",
                "contractTypeId": 1,
                "startDate": "2024-02-22T12:38:41.440Z",
                "endDate": "2024-02-22T12:38:41.440Z",
                "departmentId": "Logística",
                "workforceId": "Cap de departament",
                "calendarId": "1598773e-cf84-4cc0-9cc0-08dc339cf820",
                "annualWorkingHours": 1920,
                "timetableId": "aa315dbb-11d5-4feb-c106-08dc8bc773d2",
                "correlationId": "46457469E"
            }
        ],        
        "correlationId": "46457469E"
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
    _glam_worker_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_WORKERS, correlation_id=data['correlationId'], producerData=dataAux, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")

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
            costs = data['costs']   
            for cost in costs:
                _glam_cost_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_WORKERS + '/' + str(_glam_worker_id) + URL_SALARIES, correlation_id=cost['date'], producerData=cost, data=cost, filter_name="date", filter_value=cost['date'].replace('Z',''), endPoint=endPoint, origin=origin, helper="")

            # Sync worker contracts
            contracts = data['contracts']
            for contract in contracts:

                # We check if the contract already exists
                get_req = requests.get(URL_API + URL_WORKERS + '/' + str(_glam_worker_id) + URL_CONTRACTS, headers=headers,
                                       verify=False, timeout=CONN_TIMEOUT)

                if get_req.status_code == 200:                
                    item = next((i for i in get_req.json() if i["contractNumber"].casefold() == contract['contractNumber'].casefold() and i["startDate"].replace('Z','').casefold() == contract['startDate'].replace('Z','').casefold()), None)

                    if item is not None: # If already exists in ERP GF, we keep current department, workforce, calendar and timetable
                        contract['departmentId'] = item["departmentId"]
                        contract['workforceId'] = item["workforceId"]
                        contract['calendarId'] = item["calendarId"]
                        contract['timetableId'] = item["timetableId"]
                    else: 
                        # We need the GUID for the department
                        get_req = requests.get(URL_API + URL_DEPARTMENTS + f"?search={contract['departmentId']}", headers=headers,
                                               verify=False, timeout=CONN_TIMEOUT)
    
                        if get_req.status_code == 200:                
                            item = next((i for i in get_req.json() if i["name"].casefold() == contract['departmentId'].casefold()), None)

                            if item is not None:
                                contract['departmentId'] = item["id"]
                            else:
                                logging.error('Error department not found:' + contract['departmentId'])
                                return            

                        # We need the GUID for the workforce
                        get_req = requests.get(URL_API + URL_WORKFORCES + f"?search={contract['workforceId']}", headers=headers,
                                               verify=False, timeout=CONN_TIMEOUT)
    
                        if get_req.status_code == 200:                
                            item = next((i for i in get_req.json() if i["name"].casefold() == contract['workforceId'].casefold()), None)

                            if item is not None:
                                contract['workforceId'] = item["id"]
                            else:
                               logging.error('Error workforce not found:' + contract['workforceId'])
                               return            

                    # Synchronize contract
                    _glam_contract_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_WORKERS + '/' + str(_glam_worker_id) + URL_CONTRACTS, correlation_id=contract['startDate'], producerData=contract, data=contract, filter_name="startDate", filter_value=contract['startDate'].replace('Z',''), endPoint=endPoint, origin=origin, helper="")

        # Sync EPI location
        dataLocation = data['dataLocation']
        p_correlation_id = dataLocation['correlationId']
        p_gf_description = dataLocation['description']
        p_glam_id, nothing_to_do = synch_by_database(dbOrigin, mycursor, headers, url=URL_LOCATIONS, correlation_id=p_correlation_id,
                                                     producerData=dataLocation, data=dataLocation, filter_name='code', filter_value=str(maskValue).strip(), endPoint=endPoint, origin=origin, helper="")
        if p_glam_id is None:  # Synchronization error.
            return

        # 11/22: Locations API Post don't allow description field.
        # Let's synchronize it with the PUT request.
        # We have to GET the element, compare it and modify it if needed.
        try:
            req = requests.get(url=URL_API + URL_LOCATIONS + '/' + str(p_glam_id), headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
            _glam_description = req.json()['description']
            _containerId = req.json()['containerId']

        except Exception as err:
            logging.error('Error sync:' + URL_LOCATIONS + ":" + p_correlation_id + " With error: " + str(err))
            return

        if p_gf_description != _glam_description:
            try:
                put_data = {"description": p_gf_description}
                req = requests.put(url=URL_API + URL_CONTAINERS + '/' + str(_containerId),
                                   data=json.dumps(put_data), headers=headers,
                                   verify=False, timeout=CONN_TIMEOUT)
                if req.status_code != 200:
                    raise Exception('PUT with error')
            except Exception as err:
                logging.error('Error sync:' + URL_LOCATIONS + ":" + p_correlation_id + " With error: " + str(err))

####################################################################################################

def sync_families(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: familia')
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

    synch_by_database(dbOrigin, mycursor, headers, url=URL_FAMILIES, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")

def sync_projects(dbOrigin, mycursor, headers, maskValue, data: dict, endPoint, origin):
    logging.info('New message: project')
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
                                                 producerData=data, data=data, filter_name='code', filter_value=str(maskValue).strip(), endPoint=endPoint, origin=origin, helper="")
    if p_glam_id is None:  # Synchronization error.
        return

    # 11/22: Locations API Post don't allow description field.
    # Let's synchronize it with the PUT request.
    # We have to GET the element, compare it and modify it if needed.
    try:
        req = requests.get(url=URL_API + URL_LOCATIONS + '/' + str(p_glam_id), headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
        _glam_description = req.json()['description']
        _containerId = req.json()['containerId']
    except Exception as err:
        logging.error('Error sync:' + URL_LOCATIONS + ":" + p_correlation_id + " With error: " + str(err))
        return

    if p_gf_description != _glam_description:
        try:
            put_data = {"description": p_gf_description}
            req = requests.put(url=URL_API + URL_CONTAINERS + '/' + str(_containerId),
                               data=json.dumps(put_data), headers=headers,
                               verify=False, timeout=CONN_TIMEOUT)
            if req.status_code != 200:
                raise Exception('PUT with error')
        except Exception as err:
            logging.error('Error sync:' + URL_LOCATIONS + ":" + p_correlation_id + " With error: " + str(err))
    return

def sync_products(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: product')
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
    _glam_product_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_PRODUCTS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="code", filter_value=data['code'], endPoint=endPoint, origin=origin, helper="")

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
                              producerData=sync_format_data, data=sync_format_data, filter_name="formatCode", filter_value=format_correlation_id, endPoint=endPoint, origin=origin, helper="")

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
                raise Exception('PATCH with error when activating product')
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

def sync_paymentMethods(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: payment method')
    """
    :param data: dict -> {
        "companyId": GLAMSUITE_DEFAULT_COMPANY_ID,
        "ibanPrintMethodId": 0,
        "code": "1",
        "name": "Rebut 30 dies",
        "correlationId": "1"
    }
    :return None
    """

    synch_by_database(dbOrigin, mycursor, headers, url=URL_PAYMENTMETHODS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")

def sync_organizations(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: organization')
    """
    :param data: dict -> {
        "code": "06645940013",
        "legalName": "MECAL SRL", 
        "tradeName": "MECAL SRL",
        "countryId": "ITA",
        "identificationType": {typeId: "3", number: "06645940013"},
        "accountP": "4004000013",
        "accountD": "4300042994",
        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
        "active": "YES",
        "correlationId": "06645940013",
        "dataProveedor": {	
            "name": "Domicili Fiscal",
            "address": "CR/SANTA CREU DE CALAFELL,93,1-2",
            "postalCode": "08850",
            "city": "GAVA",
            "region": "BARCELONA",
            "phone": "62615405",
            "countryId": str(_codNacion).strip(),
            "languageId": "cbc36b65-e9af-4bf7-8146-08dc32d2fd05",
            "geolocation": "",
            "conditionTypeId": 2,
            "currencyId": "926b9441-12be-4fd0-77a8-08dc32d2fd0b",
            "invoicingTypeId": "5c037462-6da1-44b1-8128-08dc8155eff7",
            "warehouseId": "e0bed6ad-27e3-4411-942f-08dc83ccd0d3",
            "carrierId": "5bb8e236-1566-49b6-fffd-08dc81572e74",
            "incotermId": "6ebb8fbc-69f8-4320-0d6e-08dc744b792d",
            "rates": [],
            "specialDiscount": "10",
            "paymentInAdvanceDiscount": "10",
            "finantialCost": "10",
            "shippingDays": 0,
            "correlationId": "06645940013",
            "paymentMethodId": "b5a489b7-b883-43b6-1871-08dc82d66c97"
        },
        "dataCliente": {
            "name": "Domicili Fiscal",
            "address": "CR/SANTA CREU DE CALAFELL,93,1-2",
            "postalCode": "08850",
            "city": "GAVA",
            "region": "BARCELONA",
            "phone": "62615405",
            "countryId": str(_codNacion).strip(),
            "languageId": "cbc36b65-e9af-4bf7-8146-08dc32d2fd05",
            "geolocation": "",
            "conditionTypeId": 1,
            "currencyId": "926b9441-12be-4fd0-77a8-08dc32d2fd0b",
            "invoicingTypeId": "5c037462-6da1-44b1-8128-08dc8155eff7",
            "warehouseId": "e0bed6ad-27e3-4411-942f-08dc83ccd0d3",
            "carrierId": "5bb8e236-1566-49b6-fffd-08dc81572e74",
            "incotermId": "6ebb8fbc-69f8-4320-0d6e-08dc744b792d",
            "rates": [],            
            "specialDiscount": "10",
            "paymentInAdvanceDiscount": "10",
            "finantialCost": "10",
            "shippingDays": 0,
            "amount": "200000",
            "insuranceCompany": "CESCE",
            "correlationId": "06645940013",            
            "paymentMethodId": "b5a489b7-b883-43b6-1871-08dc82d66c97"
        }
    }
    :return None
    """

    dataAux = data.copy() # copy of the original data received from producer. I need it for hash purposes cos I will make changes on it.
    dataProveedorAux = data['dataProveedor'].copy() # copy for the same reason as previous
    dataClienteAux = data['dataCliente'].copy() # copy for the same reason as previous

    # We need the GUID for the country
    get_req = requests.get(URL_API + URL_COUNTRIES + f"?search={data['countryId']}", headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
    
    if get_req.status_code == 200:                
        item = next((i for i in get_req.json() if i["isoAlfa3"].casefold() == data['countryId'].casefold()), None)

        if item is not None:
            data['countryId'] = item["id"]
            
            dataProveedor = data['dataProveedor'].copy()
            dataProveedor['countryId'] = item["id"]

            dataCliente = data['dataCliente'].copy()
            dataCliente['countryId'] = item["id"]
        else:
            logging.error('Error country not found:' + data['countryId'])
            return            

    # Synchronize organization
    helper = replaceCharacters(data["legalName"], [".",",","-","'"," "], True)
    p_glam_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_ORGANIZATIONS, correlation_id=data['correlationId'], producerData=dataAux, data=data, filter_name="tradeName", filter_value=str(data['tradeName']).strip(), endPoint=endPoint, origin=origin, helper=helper)
    if _has_been_posted is not None and _has_been_posted is True:
        try:
            if data['active'] == "YES":
                patch_data = {"action": "ChangeState", "stateId": 2}  # 2 = State active.
            else:
                patch_data = {"action": "ChangeState", "stateId": 3}  # 3 = Not active.
            req = requests.patch(url=URL_API + URL_ORGANIZATIONS + '/' + str(p_glam_id),
                                 data=json.dumps(patch_data), headers=headers,
                                 verify=False, timeout=CONN_TIMEOUT)
            if req.status_code != 200:
                logging.error('PATCH with error when activating/deactivating organization with error: ' + str(req.status_code))

            if data['accountP'] != "":
                # The organization is a provider too
                post_provider = {"organizationId": str(p_glam_id), "account": data['accountP'], "correlationId": data['correlationId'], }
                p_glam_provider_id, _provider_has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_PROVIDERS, correlation_id=data['correlationId'], producerData=post_provider, data=post_provider, filter_name="tradeName", filter_value=str(data['tradeName']).strip(), endPoint=endPoint, origin=origin, helper="")

                # We create an address for the organization/provider
                p_glam_address_id, _address_has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_ORGANIZATIONS + '/' + str(p_glam_id) + URL_ADDRESS, correlation_id=dataProveedor['correlationId'], producerData=dataProveedorAux, data=dataProveedor, filter_name="address", filter_value=str(dataProveedor['address']).strip(), endPoint=endPoint, origin=origin, helper="")

                if _address_has_been_posted is not None and _address_has_been_posted is True:
                    # Time to stablish the commercial conditions of the organization/provider
                    if dataProveedor['paymentMethodId'] != "":
                        dataProveedor['organizationAddressId'] = str(p_glam_address_id)
                        synch_by_database(dbOrigin, mycursor, headers, url=URL_ORGANIZATIONS + '/' + str(p_glam_id) + URL_COMMERCIALCONDITIONS, correlation_id=dataProveedor['correlationId'], producerData=dataProveedorAux, data=dataProveedor, filter_name="organizationAddressId", filter_value=str(dataProveedor['organizationAddressId']).strip(), endPoint=endPoint, origin=origin, helper="")

            if data['accountC'] != "":
                # The organization is a client too
                post_customer = {"organizationId": str(p_glam_id), "account": data['accountC'], "correlationId": data['correlationId'], }
                p_glam_customer_id, _customer_has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_CUSTOMERS, correlation_id=data['correlationId'], producerData=post_customer, data=post_customer, filter_name="tradeName", filter_value=str(data['tradeName']).strip(), endPoint=endPoint, origin=origin, helper="")

                # We create an address for the organization/client
                p_glam_address_id, _address_has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_ORGANIZATIONS + '/' + str(p_glam_id) + URL_ADDRESS, correlation_id=dataCliente['correlationId'], producerData=dataClienteAux, data=dataCliente, filter_name="address", filter_value=str(dataCliente['address']).strip(), endPoint=endPoint, origin=origin, helper="")

                if _address_has_been_posted is not None and _address_has_been_posted is True:
                    # Time to stablish the commercial conditions of the organization/client
                    if dataCliente['paymentMethodId'] != "":
                        dataCliente['organizationAddressId'] = str(p_glam_address_id)
                        synch_by_database(dbOrigin, mycursor, headers, url=URL_ORGANIZATIONS + '/' + str(p_glam_id) + URL_COMMERCIALCONDITIONS, correlation_id=dataCliente['correlationId'], producerData=dataClienteAux, data=dataCliente, filter_name="organizationAddressId", filter_value=str(dataCliente['organizationAddressId']).strip(), endPoint=endPoint, origin=origin, helper="")

                creditRisk = round(float(dataCliente['amount']), 2)
                if creditRisk == round(float(0), 2): # if 0 means that we don't really have a real value for the risk so we don't sync any value
                    None
                else:
                    if creditRisk == round(float(-1), 2): # if -1 means that the insurance company said the risk is 0
                        creditRisk = round(float(0), 2)

                    # Obtain most recent credit risk of the organization/client
                    req_get = requests.get(
                        url=URL_API + URL_CUSTOMERS + '/' + str(p_glam_customer_id) + URL_CREDITRISKS + "?o=date,desc", # descending order by data
                        headers=headers, verify=False, timeout=CONN_TIMEOUT)
                
                    amount = 0
                    insuranceCompany = ""
                    date = ""
                    if req_get.status_code == 200:
                        # Need the details of the most recent date
                        for i in req_get.json():
                            amount = i['amount']
                            insuranceCompany = i['insuranceCompany']
                            date = datetime.datetime.strptime(i['date'], "%Y-%m-%dT%H:%M:%S").date()
                            break # first retrieved credit risk is the one I need cos the descending order by date

                        newRisk = False
                        if date == "":
                            newRisk = True
                        else:
                            if round(float(amount), 2) != creditRisk or insuranceCompany != dataCliente['insuranceCompany']:
                                newRisk = True

                        if newRisk:
                            dataRisk = {"date": str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")), "amount": str(creditRisk), "insuranceCompany": dataCliente['insuranceCompany'], }
                            url = URL_CUSTOMERS + '/' + str(p_glam_customer_id) + URL_CREDITRISKS
                            #data_hash = hash(str(dataRisk))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
                            data_hash = hashlib.sha256(str(dataRisk).encode('utf-8')).hexdigest()
                            req = requests.post(url=URL_API + url, data=json.dumps(dataRisk),     
                                                headers=headers, verify=False, timeout=CONN_TIMEOUT)
                            if req.status_code == 201:                            
                                update_value_from_database(dbOrigin, mycursor, req.json()['id'], p_glam_customer_id, str(data_hash), url, endPoint, origin, "")

        except Exception as err:
            logging.error('Error synch activating organization with error: ' + str(err))          

def sync_clientsContactes(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: clientContacte')
    """
    :param data: dict -> {
        "name": "JESUS GARCIA",
        "organizationId": "bedbf4be-1ae1-4d33-6356-08dc80f1e342",
        "phone": "611448303",
        "email": "jgarcia@celurba.es",
        "languageId": "cbc36b65-e9af-4bf7-8146-08dc32d2fd05",
        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
        "position": "CAP D'OBRA",
        "comments": "",
        "correlationId":        
    }
    :return None
    """

    # Synchronize person
    p_glam_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_PERSONS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")

    if _has_been_posted is not None and _has_been_posted is True:
        try:
            post_data = {"personId": str(p_glam_id), "position": data['position'], "comments": data['comments']} 
            req = requests.post(url=URL_API + URL_ORGANIZATIONS + '/' + str(data['organizationId']) + URL_CONTACTS, data=json.dumps(post_data),     
                                headers=headers, verify=False, timeout=CONN_TIMEOUT)
            if req.status_code != 201:
                raise Exception('POST with error when assigning person as contact of the organization')

        except Exception as err:
            logging.error('Error when assigning person as contact of the organization/client with error: ' + str(err))          

def sync_proveidorsContactes(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: proveïdorContacte')
    """
    :param data: dict -> {
        "name": "a.forcadellaccessoris@gmail.com",
        "nif": "B65574121",
        "phone": "No informat",
        "email": "a.forcadellaccessoris@gmail.com",
        "languageId": "cbc36b65-e9af-4bf7-8146-08dc32d2fd05",
        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
        "position": "Atenció al client (enviament de comandes)",
        "comments": "",
        "correlationId": "2"                       
    }
    :return None
    """

    # We need to check that the contact is not already created
    get_req = requests.get(URL_API + URL_PERSONS + f"?search={data['email']}", headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
    if get_req.status_code == 200:                
        item = next((i for i in get_req.json() if i['email'].casefold() == data['email'].casefold()), None)

        if item is not None:
            #data_hash = hash(str(data))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
            data_hash = hashlib.sha256(str(data).encode('utf-8')).hexdigest()
            update_value_from_database(dbOrigin, mycursor, data['correlationId'], "", str(data_hash), URL_PERSONS, endPoint, origin, "")
            logging.warning('Person already exists. Skip to next one.')
            return
    else:
        logging.error('Search for the person failed, check why: ' + str(data['email']) + ' ' + str(data['nif']))
        return 

    # We need to get the GUID for the organization
    get_req = requests.get(URL_API + URL_ORGANIZATIONS + f"?search={data['nif']}", headers=headers,
                           verify=False, timeout=CONN_TIMEOUT)
    
    if get_req.status_code == 200:                
        item = next((i for i in get_req.json() if i["identificationType"]["number"].casefold() == data['nif'].casefold()), None)

        if item is not None:
            organizationId = item["id"]
        else:
            logging.error('Error organization not found: ' + data['nif'])
            return            

    # Synchronize person
    p_glam_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_PERSONS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")

    if _has_been_posted is not None and _has_been_posted is True:
        try:
            post_data = {"personId": str(p_glam_id), "position": data['position'], "comments": data['comments']} 
            req = requests.post(url=URL_API + URL_ORGANIZATIONS + '/' + str(organizationId) + URL_CONTACTS, data=json.dumps(post_data),     
                                headers=headers, verify=False, timeout=CONN_TIMEOUT)
            if req.status_code != 201:
                raise Exception('POST with error when assigning person as contact of the organization')

        except Exception as err:
            logging.error('Error when assigning person as contact of the organization/provider with error: ' + str(err))          

# INICI CODE OBSOLET (NO TORNAR A ACTIVAR! - ES VA FER UNA EXECUCIÓ ÚNICA EL 27/06/2024)  
#
#
#def sync_proveidorsContactes_temp(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
#    logging.info('New message: proveïdorContacte')
#    """
#    :param data: dict -> {
#        "name": "Luis López",
#        "nif": "B81147951",
#        "phone": "620145093",
#        "email": "l.lopez@extol.es",
#        "languageId": "cbc36b65-e9af-4bf7-8146-08dc32d2fd05",
#        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
#        "position": "Director comercial",
#        "comments": "",
#        "correlationId": "CONTACTE_1"                       
#    }
#    :return None
#    """
#
#    # We need to check that the contact is not already created
#    get_req = requests.get(URL_API + URL_PERSONS + f"?search={data['email']}", headers=headers,
#                           verify=False, timeout=CONN_TIMEOUT)
#    if get_req.status_code == 200:                
#        item = next((i for i in get_req.json() if i['email'].casefold() == data['email'].casefold()), None)
#
#        if item is not None:
#            logging.warning('Person already exists. Skip to next one.')
#            return
#    else:
#        logging.error('Search for the person failed, check why: ' + str(data['email']) + ' ' + str(data['nif']))
#        return 
#    
#    # We need to get the GUID for the organization
#    get_req = requests.get(URL_API + URL_ORGANIZATIONS + f"?search={data['nif']}", headers=headers,
#                           verify=False, timeout=CONN_TIMEOUT)
#    
#    if get_req.status_code == 200:                
#        item = next((i for i in get_req.json() if i["identificationType"]["number"].casefold() == data['nif'].casefold()), None)
#
#        if item is not None:
#            organizationId = item["id"]
#        else:
#            logging.error('Error organization not found: ' + data['nif'])
#            return            
#
#    # Synchronize person
#    p_glam_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_PERSONS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")
#
#    if _has_been_posted is not None and _has_been_posted is True:
#        try:
#            post_data = {"personId": str(p_glam_id), "position": data['position'], "comments": data['comments']} 
#            req = requests.post(url=URL_API + URL_ORGANIZATIONS + '/' + str(organizationId) + URL_CONTACTS, data=json.dumps(post_data),     
#                                headers=headers, verify=False, timeout=CONN_TIMEOUT)
#            if req.status_code != 201:
#                raise Exception('POST with error when assigning person as contact of the organization')
#
#        except Exception as err:
#            logging.error('Error when assigning person as contact of the organization/provider with error: ' + str(err))          
#
#def sync_proveidorsCampsPersonalitzats_temp(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
#    logging.info('New message: proveïdorCampsPersonalitzats')
#    """
#    :param data: dict -> {
#        "nif": "04537481H",
#        "Tipus_ABC": "4665e311-b6d6-45d1-fe25-08dc985fd06e",
#        "Pagaments_ABC": "ccf5acab-c803-4666-fe28-08dc985fd06e",
#        "Terminis_de_lliurament_ABC": "d7484937-5c20-4074-fe2b-08dc985fd06e",
#        "Preus_ABC": "a6cea6c7-59ac-4fc5-fe2e-08dc985fd06e",
#        "Família": "XAPES ALUMINI",
#        "Producte": "XAPES ALUMINI, PERFILS ALUMINI",
#        "Fàbrica": "Aluminios Andalucía",
#        "Web": "www.alumisan.com",        
#        "correlationId": "PERSONALITZAT_1"                        
#    }
#    :return None
#    """
#
#    dataAux = data.copy() # copy of the original data received from producer. I need it for hash purposes cos I will make changes on it.
#
#    # We need to get the GUID for the "Família" (or create it if not existing)
#    if data['Família'] != "":
#        get_req = requests.get(URL_API + URL_CUSTOMFIELDS + "/" + "e26740a3-fbf5-43b7-547e-08dc985fcbef" + "/values", headers=headers,
#                               verify=False, timeout=CONN_TIMEOUT)
#    
#        if get_req.status_code == 200:                
#            item = next((i for i in get_req.json() if i["name"].casefold() == data['Família'].casefold()), None)
#
#            if item is not None:
#                data['Família'] = item["id"]
#            else:
#                post_data = {"name": data['Família']} 
#                req = requests.post(url=URL_API + URL_CUSTOMFIELDS + '/' + "e26740a3-fbf5-43b7-547e-08dc985fcbef" + "/values", data=json.dumps(post_data),     
#                                    headers=headers, verify=False, timeout=CONN_TIMEOUT)
#                if req.status_code != 201:
#                    raise Exception('POST with error when creating "Família": ' + str(data['Família']))
#                else:
#                    data['Família'] = str(req.json()['id'])
#
#    # We need to get the GUID for the "Producte" (or create it if not existing)
#    if data['Producte'] != "":
#        get_req = requests.get(URL_API + URL_CUSTOMFIELDS + "/" + "697b054c-aa25-462a-547f-08dc985fcbef" + "/values", headers=headers,
#                               verify=False, timeout=CONN_TIMEOUT)
#    
#        if get_req.status_code == 200:                
#            item = next((i for i in get_req.json() if i["name"].casefold() == data['Producte'].casefold()), None)
#
#            if item is not None:
#                data['Producte'] = item["id"]
#            else:
#                post_data = {"name": data['Producte']} 
#                req = requests.post(url=URL_API + URL_CUSTOMFIELDS + '/' + "697b054c-aa25-462a-547f-08dc985fcbef" + "/values", data=json.dumps(post_data),     
#                                    headers=headers, verify=False, timeout=CONN_TIMEOUT)
#                if req.status_code != 201:
#                    raise Exception('POST with error when creating "Producte": ' + str(data['Producte']))
#                else:
#                    data['Producte'] = str(req.json()['id'])
#
#    # We need to get the GUID for the "Fàbrica" (or create it if not existing)
#    if data['Fàbrica'] != "":
#        get_req = requests.get(URL_API + URL_CUSTOMFIELDS + "/" + "9dbac501-4faf-453d-5480-08dc985fcbef" + "/values", headers=headers,
#                               verify=False, timeout=CONN_TIMEOUT)
#    
#        if get_req.status_code == 200:                
#            item = next((i for i in get_req.json() if i["name"].casefold() == data['Fàbrica'].casefold()), None)
#
#            if item is not None:
#                data['Fàbrica'] = item["id"]
#            else:
#                post_data = {"name": data['Fàbrica']} 
#                req = requests.post(url=URL_API + URL_CUSTOMFIELDS + '/' + "9dbac501-4faf-453d-5480-08dc985fcbef" + "/values", data=json.dumps(post_data),     
#                                    headers=headers, verify=False, timeout=CONN_TIMEOUT)
#                if req.status_code != 201:
#                    raise Exception('POST with error when creating "Fàbrica": ' + str(data['Fàbrica']))
#                else:
#                    data['Fàbrica'] = str(req.json()['id'])
#
#    # We need to get the GUID for the organization
#    get_req = requests.get(URL_API + URL_ORGANIZATIONS + f"?search={data['nif']}", headers=headers,
#                           verify=False, timeout=CONN_TIMEOUT)
#    
#    if get_req.status_code == 200:                
#        item = next((i for i in get_req.json() if i["identificationType"]["number"].casefold() == data['nif'].casefold()), None)
#
#        if item is not None:
#            organizationId = item["id"]
#        else:
#            logging.error('Error organization not found: ' + data['nif'])
#            return            
#
#    # We also need the account of the organization/provider
#    try:
#        get_req = requests.get(URL_API + URL_PROVIDERS + '/' + str(organizationId), headers=headers,
#                               verify=False, timeout=CONN_TIMEOUT)
#        if get_req.status_code == 404:
#            logging.error('Error account of the organization/provider not found')          
#            return
#        else:
#            account = get_req.json()['account']
#    except Exception as err:
#        logging.error('Error when retreaving account of the organization/provider with error: ' + str(err))          
#        return
#
#    try:
#        post_data = {"account": str(account), "customerCode": "", "customFieldValues": data} 
#        req = requests.put(url=URL_API + URL_PROVIDERS + '/' + str(organizationId), data=json.dumps(post_data),     
#                           headers=headers, verify=False, timeout=CONN_TIMEOUT)
#        if req.status_code != 200:
#            raise Exception('PUT with error when assigning personalized fields to the organization')
#        else:
#            #data_hash = hash(str(dataAux))    # Perquè el hash era diferent a cada execució encara que s'apliqués al mateix valor 
#            data_hash = hashlib.sha256(str(dataAux).encode('utf-8')).hexdigest()
#            update_value_from_database(dbOrigin, mycursor, data['correlationId'], str(req.json()['id']), str(data_hash), URL_PERSONS, endPoint, origin, "")
#
#    except Exception as err:
#        logging.error('Error when assigning personalized fields to the organization with error: ' + str(err))          
#
#
# FINAL CODE OBSOLET (NO TORNAR A ACTIVAR! - ES VA FER UNA EXECUCIÓ ÚNICA EL 27/06/2024) 

####################################################################################################

def sync_calendarisLaborals(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: calendariLaboral')
    """
    :param data: dict -> [{
        "name": "GARCIA FAURA | Barcelona",
        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
        "holidays": [
                        {
                            "date": "2024-01-01",
                            "reasonId": 0
                        },
                        {
                            "date": "2024-01-06",
                            "reasonId": 0
                        },
                        ...
                    ],
        "correlationId": "6328f08b-5375-48f3-a9a0-449389546370"       
    }] X N
    :return None
    """

    # Synchronize calendar
    p_glam_id, _has_been_posted = synch_by_database(dbOrigin, mycursor, headers, url=URL_CALENDARS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")

    if _has_been_posted is not None and _has_been_posted is True:
        try:
            for holiday in data["holidays"]:
                synch_by_database(dbOrigin, mycursor, headers, url=URL_CALENDARS + '/' + str(p_glam_id) + URL_HOLIDAYS, correlation_id=holiday['correlationId'], producerData=holiday, data=holiday, filter_name="date", filter_value=str(holiday['date']).strip(), endPoint=endPoint, origin=origin, helper="")                

        except Exception as err:
            logging.error('Error when assigning holidays to the calendar with error: ' + str(err))          

def sync_departments(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: departments')
    """
    :param data: dict -> [{
        "name": "Control estratègic I Compres",
        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
        "correlationId": "056c8fae-5d21-4444-904e-2f8386725ab9"       
    }] X N
    :return None
    """

    # Synchronize department
    synch_by_database(dbOrigin, mycursor, headers, url=URL_DEPARTMENTS, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")

def sync_timetables(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: timetables')
    """
    :param data: dict -> [{
        "name": "Jornada parcial | 5 h",
        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
        "correlationId": "2fcd866f-5a0d-4497-b275-84bafe75322d"       
    }] X N
    :return None
    """

    # Synchronize timetable
    synch_by_database(dbOrigin, mycursor, headers, url=URL_TIMETABLES, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")

def sync_workforces(dbOrigin, mycursor, headers, data: dict, endPoint, origin):
    logging.info('New message: workforces')
    """
    :param data: dict -> [{
        "name": "Tècnic Senior RRHH",
        "companyId": "2492b776-1548-4485-3019-08dc339adb32",
        "correlationId": "142972df79b39d5b282f53837cc506bd0c7c7cf85aedcea5b07550d07e90de88"       
    }] X N
    :return None
    """

    # Synchronize timetable
    synch_by_database(dbOrigin, mycursor, headers, url=URL_WORKFORCES, correlation_id=data['correlationId'], producerData=data, data=data, filter_name="name", filter_value=str(data['name']).strip(), endPoint=endPoint, origin=origin, helper="")

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

                global GLOBAL_ENDPOINT, GLOBAL_ORIGIN, GLOBAL_CORRELATIONID, GLOBAL_CALLTYPE

                # Calculate access token and header for the request
                token = calculate_access_token(ENVIRONMENT)
                headers = calculate_json_header(token)

                data = json.loads(body) # Faig un json.loads per convertir d'String a diccionari

                # Mercaderies
                if data['queueType'] == "MERCADERIES_FAMILIES":
                    GLOBAL_ENDPOINT = 'Mercaderies ERP GF'
                    GLOBAL_ORIGIN = 'Emmegi'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_FAMILIES                    
                    sync_families(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                if data['queueType'] == "MERCADERIES_PROJECTES":
                    GLOBAL_ENDPOINT = 'Mercaderies ERP GF'
                    GLOBAL_ORIGIN = 'Emmegi'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_LOCATIONS                    
                    maskValue = calculate_mask_value(glo_warehouse_location_mask, glo_zone_code, glo_warehouse_code, glo_plant_code, glo_geolocation_code, glo_aisle_code, glo_rack_code, glo_shelf_code, str(data['correlationId']).strip())
                    sync_projects(dbOrigin, mycursor, headers, maskValue, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                if data['queueType'] == "MERCADERIES_PRODUCTES":
                    GLOBAL_ENDPOINT = 'Mercaderies ERP GF'
                    GLOBAL_ORIGIN = 'Emmegi'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_PRODUCTS                    
                    sync_products(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)

                # Treballadors
                if data['queueType'] == "TREBALLADORS_TREBALLADORS":
                    GLOBAL_ENDPOINT = 'Treballadors ERP GF'
                    GLOBAL_ORIGIN = 'Sesame/Sage'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_WORKERS                    
                    maskValue = calculate_mask_value(glo_warehouse_location_mask_epi, glo_zone_code_epi, glo_warehouse_code_epi, glo_plant_code_epi, glo_geolocation_code_epi, glo_aisle_code_epi, glo_rack_code_epi, glo_shelf_code_epi, str(data['correlationId']).strip())
                    sync_treballadors(dbOrigin, mycursor, headers, maskValue, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                
                # Usuaris
                if data['queueType'] == "USERS_USERS":
                    GLOBAL_ENDPOINT = 'Users ERP GF'
                    GLOBAL_ORIGIN = 'Emmegi'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_USERS                    
                    sync_usuaris(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)

                # Organizations
                if data['queueType'] == "ORGANIZATIONS_PAYMENTMETHODS":
                    GLOBAL_ENDPOINT = 'Organizations ERP GF'
                    GLOBAL_ORIGIN = 'Sage'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_PAYMENTMETHODS                    
                    sync_paymentMethods(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                if data['queueType'] == "ORGANIZATIONS_ORGANIZATIONS":
                    GLOBAL_ENDPOINT = 'Organizations ERP GF'
                    GLOBAL_ORIGIN = 'Sage'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_ORGANIZATIONS                    
                    sync_organizations(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                if data['queueType'] == "CLIENTS_CONTACTES":
                    GLOBAL_ENDPOINT = 'Clients ERP GF'
                    GLOBAL_ORIGIN = 'Pipedrive'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_PERSONS                    
                    sync_clientsContactes(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                if data['queueType'] == "PROVEIDORS_CONTACTES":
                    GLOBAL_ENDPOINT = 'Proveidors ERP GF'
                    GLOBAL_ORIGIN = 'Emmegi/GFIntranet'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_PERSONS                    
                    sync_proveidorsContactes(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                # INICI CODE OBSOLET (NO TORNAR A ACTIVAR! - ES VA FER UNA EXECUCIÓ ÚNICA EL 27/06/2024) 
                # 
                #                    
                #if data['queueType'] == "PROVEIDORS_CONTACTES_TEMP":
                #    sync_proveidorsContactes_temp(dbOrigin, mycursor, headers, data, 'Proveidors ERP GF', 'Excel')
                #if data['queueType'] == "PROVEIDORS_CAMPSPERSONALITZATS_TEMP":
                #    sync_proveidorsCampsPersonalitzats_temp(dbOrigin, mycursor, headers, data, 'Proveidors ERP GF', 'Excel')
                #
                #
                # FINAL CODE OBSOLET (NO TORNAR A ACTIVAR! - ES VA FER UNA EXECUCIÓ ÚNICA EL 27/06/2024)                  

                # Recursos Humans
                if data['queueType'] == "RRHH_CALENDARISLABORALS":
                    GLOBAL_ENDPOINT = 'Recursos Humans ERP GF'
                    GLOBAL_ORIGIN = 'Sesame'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_CALENDARS                    
                    sync_calendarisLaborals(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                if data['queueType'] == "RRHH_DEPARTMENTS":
                    GLOBAL_ENDPOINT = 'Recursos Humans ERP GF'
                    GLOBAL_ORIGIN = 'Sesame'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_DEPARTMENTS                    
                    sync_departments(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                if data['queueType'] == "RRHH_TIMETABLES":
                    GLOBAL_ENDPOINT = 'Recursos Humans ERP GF'
                    GLOBAL_ORIGIN = 'Sesame'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_TIMETABLES                    
                    sync_timetables(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
                if data['queueType'] == "RRHH_WORKFORCES":
                    GLOBAL_ENDPOINT = 'Recursos Humans ERP GF'
                    GLOBAL_ORIGIN = 'Sesame'
                    GLOBAL_CORRELATIONID = data['correlationId']
                    GLOBAL_CALLTYPE = URL_WORKFORCES                    
                    sync_workforces(dbOrigin, mycursor, headers, data, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)

            myRabbit.channel.queue_declare(queue=myRabbit.queue_name)
            myRabbit.channel.basic_consume(queue=myRabbit.queue_name, on_message_callback=callback_message, auto_ack=True)
            myRabbit.channel.start_consuming()
        
        except Exception as e:
            logging.error('   Unexpected error processing queued messages: ' + str(e))
            send_email("ERPSincronitzarConsumer", ENVIRONMENT, now, datetime.datetime.now(), "ERROR")            

            reconnect = False
            while not reconnect:
                logging.info('   Sleeping 10 seconds to reconnect with database&rabbit and retry...')
                time.sleep(10) 
            
                try:
                    dbOrigin = connectMySQL(MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
                    mycursor = dbOrigin.cursor()        
                    reinit_hash(dbOrigin, mycursor, GLOBAL_CORRELATIONID, GLOBAL_CALLTYPE, GLOBAL_ENDPOINT, GLOBAL_ORIGIN)
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