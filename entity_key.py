"""
This code refers to the first filter lambda, we separate it by entityKey and send it to the respective buckets
"""

import json
import boto3
import urllib.parse
import gzip
import io
import time
from datetime import datetime

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()

        if key.endswith('.gz'):
            with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as f:
                content = f.read()

        text_content = content.decode('utf-8')

        eventos_por_tipo = {}
        eventos_processados = 0

        for line in text_content.splitlines():
            if not line.strip():
                continue

            try:
                json_event = json.loads(line)
                eventos_processados += 1

                meta = json_event.get('meta', {})
                entity_key = meta.get('entityKey', 'UNKNOWN').upper()
                action = meta.get('action', 'UNKNOWN')

                chave = (entity_key, action)
                if chave not in eventos_por_tipo:
                    eventos_por_tipo[chave] = []

                eventos_por_tipo[chave].append(json_event)

            except json.JSONDecodeError as e:
                print(f"Erro ao analisar JSON: {e}")
                print(f"Linha problemática: {line[:200]}...")

        eventos_salvos = 0
        for (entity_key, action), eventos in eventos_por_tipo.items():
            if eventos:
                batch_salvos = salvar_eventos_em_lote(entity_key, action, eventos)
                eventos_salvos += batch_salvos

        print(f"Arquivo {key}: processados {eventos_processados} eventos, salvos {eventos_salvos} eventos")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Arquivo {key}: processados {eventos_processados} eventos, salvos {eventos_salvos} eventos')
        }

    except Exception as e:
        print(f"Erro ao processar objeto S3 {key} do bucket {bucket}: {e}")
        raise e


def salvar_eventos_em_lote(entity_key, action, eventos):
    salvos = 0

    buckets_por_entidade = {
        'USER': 'sms-streaming-user',
        'SCHOOLCLASS': 'sms-streaming-schoolclass',
        'SUBJECTSCHOOLCLASS': 'sms-streaming-subjectschoolclass',
        'USERSCHOOLCLASS': 'sms-streaming-userschoolclass',
        'UNOSCHOOL': 'sms-streaming-unoschool',
        'SCHOOLGRADEGROUP': 'sms-streaming-schoolgradegroup',
        'SCHOOLLEVELSESSION': 'sms-streaming-schoollevelsession',
        'USERGROUP':  'sms-streaming-usergroup',
        'USERROLE': 'sms-streaming-userrole',
        'SESSION': 'sms-streaming-session',
    }
        

    target_bucket = buckets_por_entidade.get(entity_key, 'sms-streaming-unknown')
    subpasta = action.lower()
    batch_size = 25

    for i in range(0, len(eventos), batch_size):
        batch = eventos[i:i + batch_size]
        if not batch:
            continue

        try:
            first_event = batch[0]
            first_data = first_event.get('data', {})
            attributes = first_data.get('attributes', {})
            entity_id = attributes.get('refId', 'unknown')

            timestamp = int(time.time())
            qtd_eventos = len(batch)

            jsonl_content = "\n".join(json.dumps(e, ensure_ascii=False) for e in batch)

            target_key = f"{subpasta}/{entity_key.lower()}_{qtd_eventos}_{entity_id}_{timestamp}.json"

            s3_client.put_object(
                Bucket=target_bucket,
                Key=target_key,
                Body=jsonl_content,
                ContentType='application/json'
            )

            salvos += qtd_eventos
            print(f"Batch de {qtd_eventos} eventos salvo em s3://{target_bucket}/{target_key}")

        except Exception as e:
            print(f"Erro ao salvar eventos {entity_key}-{action}: {e}")

    print(f"Salvos {salvos} eventos do tipo {entity_key}-{action}")
    return salvos
