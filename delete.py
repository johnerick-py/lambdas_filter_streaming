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
MAX_WORKERS = 10
BATCH_SIZE = 1000

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

    user_ids = set()

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
                futures = [executor.submit(parse_line, line) for line in lines if line.strip()]
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        user_ids.add(result)

        if user_ids:
            remaining_time = timeout_limit - (time.time() - start_time)
            if remaining_time < 60:
                logger.warning("Tempo restante insuficiente para DB")
                return {'statusCode': 200, 'body': json.dumps('Tempo insuficiente para delete')}

            process_batch_deletion(user_ids, db_name, host, port, user, password)

        logger.info(f"Execução finalizada em {time.time() - start_time:.2f}s")
        return {'statusCode': 200, 'body': json.dumps('Eventos de deleção processados com sucesso')}

    except Exception as e:
        logger.error(f"Erro geral: {e}")
        send_to_dlq(event, str(e))
        return {'statusCode': 500, 'body': json.dumps(str(e))}

def parse_line(line):
    try:
        data = json.loads(line)
        meta = data.get('meta', {})
        action = meta.get('action', '')
        json_data = data.get('data', {})
        attributes = json_data.get('attributes', {})
        user_id = attributes.get('refId') or json_data.get('user_id')
        if action == 'user_deleted' and user_id:
            return user_id
        return None
    except Exception as e:
        logger.error(f"Erro ao analisar linha JSON: {e}")
        return None

def process_batch_deletion(user_ids, dbname, host, port, user, password):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = psycopg2.connect(
                dbname=dbname, host=host, port=port,
                user=user, password=password, connect_timeout=10
            )
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.execute("SET statement_timeout = 300000")

            ids = list(user_ids)
            for i in range(0, len(ids), BATCH_SIZE):
                batch = tuple(ids[i:i+BATCH_SIZE])
                delete_in_order(cursor, batch)

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

def delete_in_order(cursor, batch):
    if not batch:
        return

    logger.info(f"Deletando batch de {len(batch)} usuários")
    
    cursor.execute("DELETE FROM gold.dim_user_email WHERE user_email_id IN %s", (batch,))
    cursor.execute("DELETE FROM gold.dim_user_phone WHERE user_phone_id IN %s", (batch,))
    cursor.execute("DELETE FROM gold.dim_user_address WHERE user_address_id IN %s", (batch,))
    cursor.execute("DELETE FROM gold.dim_user WHERE user_id IN %s", (batch,))

    cursor.execute("DELETE FROM baa.dim_user_email WHERE user_email_id IN %s", (batch,))
    cursor.execute("DELETE FROM baa.dim_user_phone WHERE user_phone_id IN %s", (batch,))
    cursor.execute("DELETE FROM baa.dim_user_address WHERE user_address_id IN %s", (batch,))
    cursor.execute("DELETE FROM baa.dim_user WHERE user_id IN %s", (batch,))
