# services/API_Gateaway/server.py
from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
from pydantic import BaseModel
import uuid
import json, os
import threading
import pika
import libs.storage_client.client as storage_client
from typing import Optional

class TaskInfo(BaseModel):
    task_id: str
    status: str = "processing"
    filename: Optional[str] = None
    result_key: Optional[str] = None

tasks: Dict[str, TaskInfo] = {}

API_Gateaway = FastAPI(title="File Processing API")

# CORS — чтобы сайт работал
API_Gateaway.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def download_result_file(task_id: str):
    task = tasks.get(task_id)
    if not task or task.status != "completed":
        raise HTTPException(status_code=400, detail="Task not completed")

    result_key = task.result_key or f"{task_id}/output/result.jsonl"

    try:
        # Стримим напрямую из MinIO — без временных файлов и ошибок на Windows
        s3 = storage_client.get_s3_client()
        response = s3.get_object(Bucket='mapreduce', Key=result_key)
        streaming_body = response['Body']

        filename = f"result_{task.filename or 'file'}.jsonl"

        return StreamingResponse(
            streaming_body.iter_chunks(chunk_size=8192),
            media_type='application/jsonlines',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download error: {str(e)}")

@API_Gateaway.post('/files/upload')
async def upload_process_file(file: UploadFile = File(...)) -> str:
    task_id = str(uuid.uuid4())
    tasks[task_id] = TaskInfo(task_id=task_id, status="processing", filename=file.filename)

    content = await file.read()
    local_path = 'process.txt'
    with open(local_path, 'wb') as f:
        f.write(content)

    try:
        storage_client.upload_process_file(
            local_path=local_path,
            bucket='mapreduce',
            project_name=task_id,
            filename='process.txt'
        )
        os.remove(local_path)

        conn = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost', port=7672,
            credentials=pika.PlainCredentials('admin', 'password'),
            virtual_host='/'
        ))
        ch = conn.channel()
        ch.queue_declare(queue='user_requests', durable=True)
        ch.basic_publish(
            exchange='',
            routing_key='user_requests',
            body=json.dumps({
                "task_id": task_id,
                "action": "process",
                "original_filename": file.filename
            }),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        conn.close()

        return f"File uploaded with task_id: {task_id}"

    except Exception as e:
        if os.path.exists(local_path):
            os.remove(local_path)
        tasks[task_id].status = "error"
        raise HTTPException(status_code=500, detail=str(e))

@API_Gateaway.post('/files/upload_by_name')
async def upload_file_name(name: str) -> str:
    task_id = str(uuid.uuid4())
    tasks[task_id] = TaskInfo(task_id=task_id, status="processing", filename=os.path.basename(name))

    source_key = name.lstrip("/")
    destination_key = f"{task_id}/input_file/process.txt"

    try:
        storage_client.move_file(
            bucket='mapreduce',
            source_key=source_key,
            destination_key=destination_key,
            project_name=None,
            delete_source=True
        )

        conn = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost', port=7672,
            credentials=pika.PlainCredentials('admin', 'password'),
            virtual_host='/'
        ))
        ch = conn.channel()
        ch.queue_declare(queue='user_requests', durable=True)
        ch.basic_publish(
            exchange='',
            routing_key='user_requests',
            body=json.dumps({
                "task_id": task_id,
                "action": "process",
                "original_filename": os.path.basename(name)
            }),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        conn.close()

        return f"File {source_key} processed with task_id: {task_id}"

    except Exception as e:
        tasks[task_id].status = "error"
        raise HTTPException(status_code=500, detail=str(e))

@API_Gateaway.get('/files/status')
async def get_status(task_id: str = Query(...), download: bool = Query(False)):
    task = tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    result_key = f"{task_id}/output/result.jsonl"

    if storage_client.file_exists(bucket='mapreduce', key=result_key, project_name=None):
        task.status = "completed"
        task.result_key = result_key

        if download:
            return download_result_file(task_id)
        else:
            return {
                "status": "completed",
                "task_id": task_id,
                "original_filename": task.filename,
                "message": "Processing completed",
                "download_url": f"/files/status?task_id={task_id}&download=true"
            }
    else:
        return {
            "status": task.status,
            "task_id": task_id,
            "original_filename": task.filename,
            "message": "Processing in progress"
        }

# Уведомления о завершении
def listen_for_completion():
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(
            host='localhost', port=7672,
            credentials=pika.PlainCredentials('admin', 'password'),
            virtual_host='/'
        ))
        ch = conn.channel()
        ch.queue_declare(queue='job_completion', durable=True)

        def callback(ch, method, properties, body):
            try:
                msg = json.loads(body)
                task_id = msg.get("task_id")
                if task_id in tasks:
                    tasks[task_id].status = "completed"
                    tasks[task_id].result_key = msg.get("result_key")
                    print(f"[API] Task {task_id} COMPLETED → {msg.get('result_key')}")
            except Exception as e:
                print(f"[API] Completion callback error: {e}")
            finally:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        ch.basic_consume(queue='job_completion', on_message_callback=callback, auto_ack=False)
        print("[API] Listening for job_completion queue...")
        ch.start_consuming()
    except Exception as e:
        print(f"[API] Completion listener crashed: {e}")

threading.Thread(target=listen_for_completion, daemon=True).start()