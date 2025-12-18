from fastapi import FastAPI, UploadFile, File, Path
from fastapi.responses import PlainTextResponse, FileResponse

from typing import Annotated, Optional
from pydantic import BaseModel
import os, shutil
import "libs/storage_client.upload_file" as storage_upload_file

API_Gateaway=FastAPI()

@API_Gateaway.post('/files/upload', response_model=str)
async def upload_file(file: Annotated[UploadFile, File(...)])->str:
    content = await file.read()
    text_content = content.decode('utf-8')
    try:
        os.remove('result.txt')
    except:
        pass
    with open('process.txt', 'w', encoding='utf-8') as f:
        f.write(text_content)
    storage_upload_file('process.txt', 'mapreduce', 'process.txt')
    os.remove('process.txt')

    return "File delivered"



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



