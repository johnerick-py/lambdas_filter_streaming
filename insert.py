import json
import boto3
import psycopg2
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configurações
MAX_RETRIES = 3
RETRY_DELAY = 2
BATCH_SIZE = 1000
MAX_WORKERS = 10

s3 = boto3.client('s3')
DLQ_BUCKET = 'dlq-sms-user-stream'

def send_to_dlq(event, reason):
    try:
        timestamp = int(time.time())
        key = f"failed_events/{timestamp}.json"
        body = json.dumps({"event": event, "error": reason})
        s3.put_object(Bucket=DLQ_BUCKET, Key=key, Body=body.encode('utf-8'))
        logger.warning(f"Evento enviado para DLQ no S3: {key}")
    except Exception as e:
        logger.error(f"Falha ao enviar evento para DLQ S3: {e}")

def read_s3_file_with_retry(bucket, key):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            logger.warning(f"[RETRY {attempt}] Falha ao ler {key}: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    logger.error(f"Falha ao acessar {key} após {MAX_RETRIES} tentativas")
    return None

def lambda_handler(event, context):
    timeout_limit = context.get_remaining_time_in_millis() / 1000 - 30
    start_time = time.time()

    logger.info(f"Iniciando execução com {timeout_limit:.2f}s disponíveis")

    db_name = os.environ['DBNAME']
    host = os.environ['HOST']
    port = os.environ['PORT']
    user = os.environ['USER']
    password = os.environ['PASSWORD']

    user_events = []

    try:
        for record in event['Records']:
            if time.time() - start_time > timeout_limit * 0.5:
                logger.warning("Atingido 50% do tempo disponível, interrompendo leitura")
                break

            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            logger.info(f"Processando arquivo {key}")

            content = read_s3_file_with_retry(bucket, key)
            if not content:
                continue

            lines = content.strip().split('\n')

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(parse_update_line, line) for line in lines if line.strip()]

                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        user_events.append(result)

        if user_events:
            remaining_time = timeout_limit - (time.time() - start_time)
            if remaining_time < 60:
                logger.warning("Tempo restante insuficiente para DB")
                return {'statusCode': 200, 'body': json.dumps('Tempo insuficiente para update')}

            process_to_aurora(user_events, db_name, host, port, user, password)

        logger.info(f"Execução concluída em {time.time() - start_time:.2f}s")
        return {'statusCode': 200, 'body': json.dumps('Eventos atualizados com sucesso')}

    except Exception as e:
        logger.error(f"Erro geral: {e}")
        send_to_dlq(event, str(e))
        return {'statusCode': 500, 'body': json.dumps(str(e))}

def parse_update_line(line):
    try:
        data = json.loads(line)
        meta = data.get('meta', {})
        action = meta.get('action', '')
        json_data = data.get('data', {})
        attributes = json_data.get('attributes', {})

        user_id = attributes.get('refId') or json_data.get('user_id')
        if not user_id:
            logger.warning("Evento sem user_id válido ignorado.")
            return None

        addresses = []
        for item in attributes.get('addressList', []):
            addr = item.get('address', {})
            addresses.append({
                'user_address_id': user_id,
                'address_type': addr.get('addressType'),
                'full_address': addr.get('street', {}).get('line1'),
                'city': addr.get('city'),
                'neighborhood': addr.get('neighborhood'),
                'postal_code': addr.get('postalCode'),
                'building_site_number': addr.get('buildingSiteNumber'),
                'county_id': addr.get('county', {}).get('refId')
            })

        phones = []
        for item in attributes.get('phoneNumberList', []):
            phone = item.get('phoneNumber', {})
            phones.append({
                'user_phone_id': user_id,
                'phone_number': phone.get('number'),
                'phone_type': phone.get('phoneNumberType')
            })

        emails = []
        for item in attributes.get('personEmailList', []):
            email = item.get('personEmail', {}).get('email')
            emails.append({
                'user_email_id': user_id,
                'email': email
            })

        return {
            'action': action,
            'data': {
                'user_id': user_id,
                'country_id': attributes.get('country', {}).get('refId', None),
                'gender': attributes.get('sex', None),
                'first_name': attributes.get('name', {}).get('firstName', None),
                'second_name': attributes.get('name', {}).get('middleName', None),
                'last_name': attributes.get('name', {}).get('lastName', None),
                'birth_date': attributes.get('birthDate', None),
                'language_code': attributes.get('language', {}).get('refId', None),
                'uno_user_name': attributes.get('user', {}).get('username', None),
                'active_status': attributes.get('user', {}).get('active') == 1,
                'oficial_id': attributes.get('oficialId', None),
                'addresses': addresses,
                'phones': phones,
                'emails': emails
            }
        }
    except Exception as e:
        logger.error(f"Erro ao analisar linha JSON: {e}")
        return None

def process_to_aurora(user_events, dbname, host, port, user, password):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = psycopg2.connect(
                dbname=dbname, host=host, port=port,
                user=user, password=password, connect_timeout=10
            )
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.execute("SET statement_timeout = 300000")

            for user_event in user_events:
                perform_upserts(cursor, user_event)

            conn.commit()
            logger.info("Commit concluído no Aurora")
            break
        except Exception as e:
            logger.error(f"[Tentativa {attempt}] Erro no DB: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY)
        finally:
            try:
                cursor.close()
                conn.close()
            except:
                pass

def perform_upserts(cursor, user_event):
    action = user_event['action']
    user = user_event['data']

    if action == 'user_created':
        insert_user(cursor, user)
    elif action == 'user_updated':
        update_user(cursor, user)

    if user.get('addresses'):
        for address in user['addresses']:
            if action == 'user_created':
                insert_address(cursor, address)
            elif action == 'user_updated':
                update_address(cursor, address)

    if user.get('phones'):
        for phone in user['phones']:
            if action == 'user_created':
                insert_phone(cursor, phone)
            elif action == 'user_updated':
                update_phone(cursor, phone)

    if user.get('emails'):
        for email in user['emails']:
            if action == 'user_created':
                insert_email(cursor, email)
            elif action == 'user_updated':
                update_email(cursor, email)

# Funções de insert e update

def insert_user(cursor, user):
    query = """
        INSERT INTO gold.dim_user (
            user_id, country_id, gender, first_name, second_name, last_name,
            full_name, birth_date, language_code, uno_user_name, active_status, oficial_id
        ) VALUES (
            %(user_id)s, %(country_id)s, %(gender)s, %(first_name)s, %(second_name)s, %(last_name)s,
            CONCAT_WS(' ', %(first_name)s, %(second_name)s, %(last_name)s),
            %(birth_date)s, %(language_code)s, %(uno_user_name)s, %(active_status)s, %(oficial_id)s
        )
    """
    cursor.execute(query, user)

    query = """
        INSERT INTO baa.dim_user (
            user_id, country_id, gender, first_name, second_name, last_name,
            full_name, birth_date, language_code, uno_user_name, active_status, oficial_id
        ) VALUES (
            %(user_id)s, %(country_id)s, %(gender)s, %(first_name)s, %(second_name)s, %(last_name)s,
            CONCAT_WS(' ', %(first_name)s, %(second_name)s, %(last_name)s),
            %(birth_date)s, %(language_code)s, %(uno_user_name)s, %(active_status)s, %(oficial_id)s
        )
    """
    cursor.execute(query, user)

def update_user(cursor, user):
    query = """
        UPDATE gold.dim_user SET
            country_id = %(country_id)s,
            gender = %(gender)s,
            first_name = %(first_name)s,
            second_name = %(second_name)s,
            last_name = %(last_name)s,
            full_name = CONCAT_WS(' ', %(first_name)s, %(second_name)s, %(last_name)s),
            birth_date = %(birth_date)s,
            language_code = %(language_code)s,
            uno_user_name = %(uno_user_name)s,
            active_status = %(active_status)s,
            oficial_id = %(oficial_id)s
        WHERE user_id = %(user_id)s
    """
    cursor.execute(query, user)

    query = """
        UPDATE baa.dim_user SET
            country_id = %(country_id)s,
            gender = %(gender)s,
            first_name = %(first_name)s,
            second_name = %(second_name)s,
            last_name = %(last_name)s,
            full_name = CONCAT_WS(' ', %(first_name)s, %(second_name)s, %(last_name)s),
            birth_date = %(birth_date)s,
            language_code = %(language_code)s,
            uno_user_name = %(uno_user_name)s,
            active_status = %(active_status)s,
            oficial_id = %(oficial_id)s
        WHERE user_id = %(user_id)s
    """
    cursor.execute(query, user)

def insert_address(cursor, address):
    query = """
        INSERT INTO gold.dim_user_address (
            user_address_id, address_type, full_address, city,
            neighborhood, postal_code, building_site_number, county_id
        ) VALUES (
            %(user_address_id)s, %(address_type)s, %(full_address)s, %(city)s,
            %(neighborhood)s, %(postal_code)s, %(building_site_number)s, %(county_id)s
        )
    """
    cursor.execute(query, address)

    query = """
        INSERT INTO baa.dim_user_address (
            user_address_id, address_type, full_address, city,
            neighborhood, postal_code, building_site_number, county_id
        ) VALUES (
            %(user_address_id)s, %(address_type)s, %(full_address)s, %(city)s,
            %(neighborhood)s, %(postal_code)s, %(building_site_number)s, %(county_id)s
        )
    """
    cursor.execute(query, address)

def update_address(cursor, address):
    query = """
        UPDATE gold.dim_user_address SET
            address_type = %(address_type)s,
            full_address = %(full_address)s,
            city = %(city)s,
            neighborhood = %(neighborhood)s,
            postal_code = %(postal_code)s,
            building_site_number = %(building_site_number)s,
            county_id = %(county_id)s
        WHERE user_address_id = %(user_address_id)s
    """
    cursor.execute(query, address)


    query = """
        UPDATE baa.dim_user_address SET
            address_type = %(address_type)s,
            full_address = %(full_address)s,
            city = %(city)s,
            neighborhood = %(neighborhood)s,
            postal_code = %(postal_code)s,
            building_site_number = %(building_site_number)s,
            county_id = %(county_id)s
        WHERE user_address_id = %(user_address_id)s
    """
    cursor.execute(query, address)

def insert_phone(cursor, phone):
    query = """
        INSERT INTO gold.dim_user_phone (
            user_phone_id, phone_number, phone_type
        ) VALUES (
            %(user_phone_id)s, %(phone_number)s, %(phone_type)s
        )
    """
    cursor.execute(query, phone)

    query = """
        INSERT INTO baa.dim_user_phone (
            user_phone_id, phone_number, phone_type
        ) VALUES (
            %(user_phone_id)s, %(phone_number)s, %(phone_type)s
        )
    """
    cursor.execute(query, phone)

def update_phone(cursor, phone):
    query = """
        UPDATE gold.dim_user_phone SET
            phone_number = %(phone_number)s,
            phone_type = %(phone_type)s
        WHERE user_phone_id = %(user_phone_id)s
    """
    cursor.execute(query, phone)

    query = """
        UPDATE baa.dim_user_phone SET
            phone_number = %(phone_number)s,
            phone_type = %(phone_type)s
        WHERE user_phone_id = %(user_phone_id)s
    """
    cursor.execute(query, phone)

def insert_email(cursor, email):
    query = """
        INSERT INTO gold.dim_user_email (
            user_email_id, email
        ) VALUES (
            %(user_email_id)s, %(email)s
        )
    """
    cursor.execute(query, email)

    query = """
        INSERT INTO baa.dim_user_email (
            user_email_id, email
        ) VALUES (
            %(user_email_id)s, %(email)s
        )
    """
    cursor.execute(query, email)

def update_email(cursor, email):
    query = """
        UPDATE gold.dim_user_email SET
            email = %(email)s
        WHERE user_email_id = %(user_email_id)s
    """
    cursor.execute(query, email)

    query = """
        UPDATE baa.dim_user_email SET
            email = %(email)s
        WHERE user_email_id = %(user_email_id)s
    """
    cursor.execute(query, email)
