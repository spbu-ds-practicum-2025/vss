from fastapi import FastAPI, UploadFile, File, Path
from fastapi.responses import PlainTextResponse, FileResponse

from typing import Annotated, Optional
from pydantic import BaseModel
import os, shutil, uuid
import libs.storage_client.client as storage_client
import asyncio
import pika
import json

API_Gateaway=FastAPI()

credentials = pika.PlainCredentials('admin', 'password')
params = pika.ConnectionParameters(host='localhost', port=5672, virtual_host='/', credentials=credentials)
conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue='user_requests', durable=True)

@API_Gateaway.post('/files/upload', response_model=str)
async def upload_file(file: Annotated[UploadFile, File(...)])->str:
    delivered = False
    MAX_RETRIES = 5
    RETRY_DELAY = 2 
    content = await file.read()
    text_content = content.decode('utf-8')
    task_id = str(uuid.uuid4())
    with open('process.txt', 'w', encoding='utf-8') as f:
        f.write(text_content)

    for attempt in range(MAX_RETRIES):
        storage_client.upload_file(local_path='process.txt', bucket='mapreduce', project_name=task_id, filename='process.txt')
        delivered = storage_client.file_exists(bucket='mapreduce', key='process.txt', project_name=task_id)

        if delivered:
            os.remove('process.txt')

            task = {
                "task_id": task_id,
                }
            body = json.dumps(task)
            props = pika.BasicProperties(delivery_mode=2, content_type='application/json')
            ch.basic_publish(exchange='', routing_key='user_requests', body=body, properties=props)
            conn.close()

            return f"File uploaded with task_id: {task_id}"
        else:
            await asyncio.sleep(RETRY_DELAY)

    raise Exception("Failed to upload file after multiple attempts.")

@API_Gateaway.post('/files/upload_by_name', response_model=str)
async def upload_file_name(name:str) -> str:
    task_id = str(uuid.uuid4())
    key = storage_client._build_key(task_id, filename='process.txt')

    delivered = False
    MAX_RETRIES = 5
    RETRY_DELAY = 2 
    for attempt in range(MAX_RETRIES):
        storage_client.move_file_with_rename(bucket='mapreduce', source_key=name, destination=key, new_filename='process.txt')
        delivered = storage_client.file_exists(bucket='mapreduce', key='process.txt', project_name=task_id)
        if delivered:
            task = {
                "task_id": task_id,
                }
            body = json.dumps(task)
            props = pika.BasicProperties(delivery_mode=2, content_type='application/json')
            ch.basic_publish(exchange='', routing_key='user_requests', body=body, properties=props)
            conn.close()

            return f"File uploaded with task_id: {task_id}"
        else:
            await asyncio.sleep(RETRY_DELAY)

    raise Exception("Failed to upload file after multiple attempts.")

@API_Gateaway.get('/files/status')
async def get_status():
    current_dir = os.getcwd()
    result_file_path = os.path.join(current_dir, 'result.txt')

    if os.path.exists(os.path.join(current_dir, 'result.txt')):
        result = FileResponse(
            path=os.path.join(current_dir, 'result.txt'),
            filename='result.txt',
            media_type='text/plain'
        )
        return result
    else:
        return "Still Processing"



