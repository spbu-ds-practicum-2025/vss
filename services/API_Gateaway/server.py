from fastapi import FastAPI, UploadFile, File, Path, Query, HTTPException
from fastapi.responses import PlainTextResponse, FileResponse, JSONResponse, StreamingResponse
from typing import Annotated, Optional, Dict
from pydantic import BaseModel
import os, shutil, uuid, json, asyncio, io
import libs.storage_client.client as storage_client
import pika

# Модели данных
class TaskInfo(BaseModel):
    task_id: str
    status: str = "processing"  # processing, completed, error
    filename: Optional[str] = None
    result_key: Optional[str] = None

# Хранилище задач в памяти
tasks: Dict[str, TaskInfo] = {}

API_Gateaway = FastAPI(title="File Processing API")

def download_result_file(task_id: str):
    """
    Скачивает результат обработки и возвращает как StreamingResponse
    """
    try:
        # Получаем информацию о задаче
        task = tasks.get(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        
        if task.status != "completed":
            raise HTTPException(
                status_code=400,
                detail=f"File not ready for download. Current status: {task.status}"
            )
        
        # Путь к результату в хранилище
        result_key = f"{task_id}/result.txt"
        
        # Скачиваем файл из хранилища в память
        import tempfile
        
        # Создаем временный файл
        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.txt', delete=False) as temp_file:
            temp_path = temp_file.name
            
            try:
                # Скачиваем из хранилища
                storage_client.download_file(
                    bucket='mapreduce',
                    key=result_key,
                    local_path=temp_path
                )
                
                # Читаем содержимое файла
                with open(temp_path, 'rb') as f:
                    file_content = f.read()
                
                # Создаем имя файла для скачивания
                original_name = task.filename or "file"
                if original_name.endswith('.txt'):
                    result_filename = f"result_{original_name}"
                else:
                    result_filename = f"result_{original_name}.txt"
                
                # Удаляем временный файл
                os.unlink(temp_path)
                
                # Возвращаем файл как поток
                return StreamingResponse(
                    io.BytesIO(file_content),
                    media_type='text/plain',
                    headers={
                        'Content-Disposition': f'attachment; filename="{result_filename}"',
                        'X-Task-ID': task_id,
                        'X-Filename': result_filename
                    }
                )
                
            except Exception as e:
                # Удаляем временный файл при ошибке
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                raise e
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error downloading file: {str(e)}"
        )

@API_Gateaway.post('/files/upload')
async def upload_process_file(file: Annotated[UploadFile, File(...)]) -> str:
    """
    Загружает файл и создает задачу на обработку.
    Возвращает task_id как строку (как в оригинале).
    """
    delivered = False
    MAX_RETRIES = 5
    RETRY_DELAY = 2 
    
    # Читаем и декодируем содержимое файла
    content = await file.read()
    text_content = content.decode('utf-8')
    
    # Генерируем уникальный task_id
    task_id = str(uuid.uuid4())
    
    # Сохраняем информацию о задаче
    tasks[task_id] = TaskInfo(
        task_id=task_id,
        status="processing",
        filename=file.filename
    )
    
    # Сохраняем файл локально как process.txt (как в оригинале)
    with open('process.txt', 'w', encoding='utf-8') as f:
        f.write(text_content)

    # Создаем подключение к RabbitMQ (как в оригинале, но с улучшениями)
    credentials = pika.PlainCredentials('admin', 'password')
    params = pika.ConnectionParameters(host='localhost', port=5672, virtual_host='/', credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue='user_requests', durable=True)

    for attempt in range(MAX_RETRIES):
        try:
            # Загружаем файл в хранилище (как в оригинале)
            storage_client.upload_process_file(
                local_path='process.txt', 
                bucket='mapreduce', 
                project_name=task_id, 
                filename='process.txt'
            )
            
            # Проверяем загрузку (как в оригинале)
            delivered = storage_client.file_exists(
                bucket='mapreduce', 
                key='process.txt', 
                project_name=task_id
            )

            if delivered:
                # Удаляем локальный файл
                os.remove('process.txt')

                # Отправляем задачу в RabbitMQ (с расширенными данными)
                task_data = {
                    "task_id": task_id,
                    "action": "process",
                    "original_filename": file.filename
                }
                body = json.dumps(task_data)
                props = pika.BasicProperties(
                    delivery_mode=2, 
                    content_type='application/json'
                )
                ch.basic_publish(
                    exchange='', 
                    routing_key='user_requests', 
                    body=body, 
                    properties=props
                )
                conn.close()

                # Возвращаем строку как в оригинале
                return f"File uploaded with task_id: {task_id}"
            else:
                await asyncio.sleep(RETRY_DELAY)

        except Exception as e:
            print(f"Upload attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(RETRY_DELAY)

    # Если не удалось загрузить
    if os.path.exists('process.txt'):
        os.remove('process.txt')
    
    tasks[task_id].status = "error"
    raise Exception("Failed to upload file after multiple attempts.")

@API_Gateaway.post('/files/upload_by_name')
async def upload_file_name(name: str) -> str:
    """
    Перемещает существующий файл в хранилище как process.txt.
    Возвращает task_id как строку.
    """
    task_id = str(uuid.uuid4())
    
    # Сохраняем информацию о задаче
    tasks[task_id] = TaskInfo(
        task_id=task_id,
        status="processing",
        filename=name.split('/')[-1] if '/' in name else name
    )
    
    delivered = False
    MAX_RETRIES = 5
    RETRY_DELAY = 2 
    
    # Создаем подключение к RabbitMQ
    credentials = pika.PlainCredentials('admin', 'password')
    params = pika.ConnectionParameters(host='localhost', port=5672, virtual_host='/', credentials=credentials)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue='user_requests', durable=True)
    
    for attempt in range(MAX_RETRIES):
        try:
            # Используем оригинальный подход: просто перемещаем файл
            # Формируем целевую директорию
            destination_dir = f"{task_id}/input_file/"
            
            # Перемещаем и переименовываем файл
            storage_client.move_file_with_rename(
                bucket='mapreduce',
                source_key=name,
                destination=destination_dir,
                new_filename='process.txt'
            )
            
            # Проверяем успешность
            delivered = storage_client.file_exists(
                bucket='mapreduce',
                key='process.txt',
                project_name=task_id
            )
            
            if delivered:
                # Отправляем задачу в RabbitMQ
                task_data = {
                    "task_id": task_id,
                    "action": "process",
                    "original_filename": name.split('/')[-1] if '/' in name else name
                }
                body = json.dumps(task_data)
                props = pika.BasicProperties(
                    delivery_mode=2, 
                    content_type='application/json'
                )
                ch.basic_publish(
                    exchange='', 
                    routing_key='user_requests', 
                    body=body, 
                    properties=props
                )
                conn.close()

                return f"File uploaded with task_id: {task_id}"
            else:
                await asyncio.sleep(RETRY_DELAY)
                
        except Exception as e:
            print(f"Upload attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(RETRY_DELAY)

    # Если не удалось
    tasks[task_id].status = "error"
    raise Exception("Failed to upload file after multiple attempts.")

@API_Gateaway.get('/files/status')
async def get_status(
    task_id: str = Query(..., description="ID задачи полученный при загрузке файла"),
    download: bool = Query(False, description="Скачать файл автоматически при завершении")
):
    """
    Получает статус обработки файла по task_id.
    """
    # Проверяем существование задачи
    if task_id not in tasks:
        return JSONResponse(
            status_code=404,
            content={"status": "not_found", "message": "Task not found"}
        )
    
    task = tasks[task_id]
    
    # Проверяем, есть ли результат в хранилище
    try:
        # Путь к результату всегда: task_id/result.txt
        result_key = f"{task_id}/result.txt"
        
        if storage_client.file_exists(bucket='mapreduce', key=result_key):
            task.status = "completed"
            task.result_key = result_key
            
            # Если запрошено скачивание - возвращаем файл
            if download:
                return download_result_file(task_id)
            else:
                # Возвращаем информацию о завершении
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "completed",
                        "task_id": task_id,
                        "original_filename": task.filename,
                        "result_filename": "result.txt",
                        "message": "File processing completed",
                        "download_url": f"/files/status?task_id={task_id}&download=true"
                    }
                )
        else:
            # Проверяем наличие входного файла process.txt
            input_exists = storage_client.file_exists(
                bucket='mapreduce',
                key='process.txt',
                project_name=task_id
            )
            
            if not input_exists:
                task.status = "error"
                return JSONResponse(
                    status_code=500,
                    content={
                        "status": "error",
                        "message": "Input file not found, processing failed"
                    }
                )
            
            # Файл еще обрабатывается
            return JSONResponse(
                status_code=200,
                content={
                    "status": "processing",
                    "task_id": task_id,
                    "original_filename": task.filename,
                    "message": "File is being processed",
                    "check_again_url": f"/files/status?task_id={task_id}"
                }
            )
            
    except Exception as e:
        task.status = "error"
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": f"Error checking status: {str(e)}"
            }
        )

@API_Gateaway.get('/files/status_with_auto_download')
async def get_status_with_auto_download(
    task_id: str = Query(..., description="ID задачи")
):
    """
    Автоматически скачивает файл result.txt если обработка завершена.
    """
    if task_id not in tasks:
        return JSONResponse(
            status_code=404,
            content={"status": "not_found", "message": "Task not found"}
        )
    
    task = tasks[task_id]
    
    # Проверяем наличие результата result.txt
    result_key = f"{task_id}/result.txt"
    
    if storage_client.file_exists(bucket='mapreduce', key=result_key):
        task.status = "completed"
        task.result_key = result_key
        
        # Автоматически возвращаем файл
        return download_result_file(task_id)
    else:
        # Файл еще обрабатывается
        return JSONResponse(
            status_code=200,
            content={
                "status": "processing",
                "task_id": task_id,
                "original_filename": task.filename,
                "message": "File is being processed",
                "auto_download_url": f"/files/status_with_auto_download?task_id={task_id}"
            }
        )

