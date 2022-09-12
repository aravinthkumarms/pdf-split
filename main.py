
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import json
import logging
from PyPDF2 import PdfFileWriter, PdfFileReader
import os
from google.cloud import storage
import fitz
import py_eureka_client.eureka_client as eureka_client
from fastapi import FastAPI, UploadFile, HTTPException
import uvicorn
import shutil
import http3
from PIL import Image
import urllib.request
import nest_asyncio
import requests
from dotenv import load_dotenv

import os
from utils import *

load_dotenv()


app = FastAPI()


origins = ["http://localhost:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage_client = storage.Client.from_service_account_json(
    "key.json")
bucketName = "pdf-split-ingestion"
bucket = storage_client.get_bucket(bucketName)
splitBucketName = "pdf-split-files"
splitBucket = storage_client.get_bucket(splitBucketName)
createPDFBucketName = "create-pdf-delivery"
createPDFBucket = storage_client.get_bucket(createPDFBucketName)


logging.basicConfig(level=logging.INFO)
client = http3.AsyncClient()
PORT = os.environ.get('PORT', 7000)
host = 'localhost'


def on_err(err_type: str, err: Exception):
    if err_type in (eureka_client.ERROR_REGISTER, eureka_client.ERROR_DISCOVER):
        eureka_client.stop()
    else:
        print(f"{err_type}::{err}")


# eureka_client.init(eureka_server="http://localhost:8761/eureka",
#                    app_name="PDF-SPLIT-SERVICE",
#                    instance_port=PORT,
#                    on_error=on_err
#                    )
# logging.info("Eureka client initialized")

# logging.info("PDF-SPLIT-SERVICE is running on port: {}".format(port))
# ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif'}
# def allowed_file(filename):
#     return '.' in filename and \
#            filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# @app.get("/download/file/?filename={filename}")
# async def read(filename: str):
#     fileName = filename
#     try:
#         blob = bucket.blob("{}.pdf".format(fileName))
#         blob.download_to_filename("{}.pdf".format(fileName))
#         logging.info("Downloaded file {}.pdf".format(fileName))
#         return jsonify({"success": True}), 200
#     except Exception as e:
#         return f"An Error Occurred: {e}", 500

async def call_api(url: str):
    r = await client.get(url)
    return r.text


@app.get("/try/connect")
async def download():
    res = {"connection": "success",
           "message": "Connection Successful"}
    return res


@app.get("/download/file/{filename}")
async def download(filename: str):
    fileName = filename
    try:
        blob = bucket.blob("{}".format(fileName))
        blob.download_to_filename("temp/{}".format(fileName))
        logging.info("Downloaded file {}".format(fileName))
        return {"success": True}
    except Exception as e:
        logging.error(e)
        return raiseExceptionInternalServerError("Internal Server Error")


async def getFileId(fileName: str):
    try:
        res = requests.get(
            f"http://host.docker.internal:8000/blob/response/v2/?fileName={fileName}")
        jsonRes = dict(json.loads(res.text))
        logging.info(jsonRes)
    except Exception as e:
        logging.error(e)
        return raiseExceptionInternalServerError("Internal Server Error")
    return jsonRes['fileId']


@app.post('/upload')
async def upload(files: UploadFile):
    filename = files.filename
    fileId = ''
    totalPages = 0
    fileSize = 0
    if filename:
        try:
            fileLocation = f"temp/{filename}"
            with open(fileLocation, "wb+") as file_object:
                shutil.copyfileobj(files.file, file_object)
            blob = bucket.blob("{}".format(filename))
            totalPages = PdfFileReader(fileLocation).numPages
            fileSize = os.stat(fileLocation).st_size
            request = {
                "fileName": filename,
                "filePath": f"gs://{bucketName}/{filename}",
                "totalPages": totalPages,
                "fileSize": fileSize
            }
            res = requests.post(
                os.environ["upload_api"], json=request)
            logging.info(
                "Hitting Data Api to upload file details")
            logging.info(res.json())
            fileId = await getFileId(filename)
            startedWrkFlwRequest = {
                "fileId": fileId,
                "workFlowStepType": "Upload",
                "workFlowStatusType": "Started"}
            await updateWrkFlwDtl(fileId, startedWrkFlwRequest)
            blob.upload_from_filename(filename=f"temp/{filename}")
            completedWrkFlwRequest = {
                "fileId": fileId,
                "workFlowStepType": "Upload",
                "workFlowStatusType": "Completed"}
            await updateWrkFlwDtl(fileId, completedWrkFlwRequest)
        except Exception as e:
            logging.exception(e)
            request = {
                "fileName": filename,
                "filePath": None,
                "totalPages": 0,
                "fileSize": 0
            }
            res = requests.post(
                os.environ["upload_api"], json=request)
            logging.info(
                "Hitting Data Api to upload file details")
            logging.info(res.json())
            fileId = await getFileId(filename)
            startedWrkFlwRequest = {
                "fileId": fileId,
                "workFlowStepType": "Upload",
                "workFlowStatusType": "Started"}
            await updateWrkFlwDtl(fileId, startedWrkFlwRequest)
            exceptionWrkFlwRequest = {
                "fileId": fileId,
                "workFlowStepType": "Upload",
                "workFlowStatusType": "Exception",
                "errorDescription": "EXC-01 Unable to upload file to GCS"}
            await updateWrkFlwDtl(fileId, exceptionWrkFlwRequest)
            logging.error(e)
            os.remove(f"temp/{filename}")
            return raiseExceptionInternalServerError("Internal Server Error")
        os.remove(f"temp/{filename}")
        return json.loads(res.text)
    else:
        logging.warning("No file found")
        return raiseExceptionBadRequest("No file found")


@DeprecationWarning
@app.get("/split/file/v1/{filename}")
async def split(filename: str):
    fileName = filename
    request = await download(fileName)
    if request:
        try:
            with open(fileName, "rb") as f:
                inputpdf = PdfFileReader(f, "rb")
                for i in range(inputpdf.numPages):
                    output = PdfFileWriter()
                    output.addPage(inputpdf.getPage(i))
                    with open("temp/{}_page{}.pdf".format(fileName, i), "wb") as outputStream:
                        output.write(outputStream)
            for i in range(inputpdf.numPages):
                blob = splitBucket.blob(
                    "{}/{}_page{}.pdf".format(fileName, fileName, i))
                blob.upload_from_filename(
                    "temp/{}_page{}.pdf".format(fileName, i))
                os.remove("temp/{}_page{}.pdf".format(fileName, i))
            os.remove(fileName)
            return {"success": "File Splited and Uploaded"}
        except Exception as e:
            logging.error(e)
            return raiseExceptionInternalServerError("Internal Server Error")


@app.get("/split/file/v2/{filename}")
async def split(filename: str):
    fileName = filename
    request = await download(fileName)
    if request:
        try:
            inputpdf = r"temp/{}".format(fileName)
            logging.info(f"Input file: {inputpdf}, getting splitted")
            with fitz.open(inputpdf) as pages:
                i = 1
                logging.info("Splitting started")
                for page in pages:
                    imageName = "Page_" + str(i) + ".png"
                    thumbnailName = "Page_" + str(i) + ".webp"
                    mat = fitz.Matrix(2, 2)
                    pix = page.get_pixmap(matrix=mat)
                    pix.save(f"temp/{imageName}", "webp")
                    image = Image.open(f"temp/{imageName}")
                    image = image.convert('RGB')
                    image.save(f"temp/{thumbnailName}", 'PNG')
                    logging.info(f"Splitted {imageName}, {thumbnailName}")
                    image = splitBucket.blob(f"{fileName}/{imageName}")
                    i += 1
                logging.info("Splitting completed")
            return {"connection": "success",
                    "message": "PDF Splitting Successful"}
        except Exception as e:
            logging.error(e)
            return raiseExceptionInternalServerError("Internal Server Error")


@app.get("/upload/file/v1/{fileName}")
async def upload(fileName: str):
    try:
        fileLocation = f"temp/{fileName}"
        totalPages = PdfFileReader(fileLocation).numPages
        for j in range(1, totalPages+1):
            imageName = f"Page_{j}.png"
            thumbnailName = f"Page_{j}.webp"
            image = splitBucket.blob(f"{fileName}/{imageName}")
            thumbnail = splitBucket.blob(
                f"{fileName}/thumbanails/{thumbnailName}")
            image.upload_from_filename(f"temp/{imageName}")
            thumbnail.upload_from_filename(f"temp/{thumbnailName}")
            logging.info(
                f"Uploaded to {splitBucketName}/{fileName}/{imageName} and {splitBucketName}/{fileName}/thumbnails/{thumbnailName}")
            os.remove(f"temp/{imageName}")
            os.remove(f"temp/{thumbnailName}")
            logging.info(f"removed {imageName} and {thumbnailName}")
        os.remove(f"temp/{fileName}")
        return {"connection": "success",
                "message": "Uploading Splitted Images Successful"}
    except Exception as e:
        logging.error(e)
        return raiseExceptionInternalServerError("Internal Server Error")


@app.get("/createPDF/v1/{fileName}")
async def createPDf(fileName: str):
    try:
        prefix = fileName
        local = 'temp/'
        imageList = []
        blobs = splitBucket.list_blobs(prefix=prefix, delimiter="")
        print(blobs)
        for blob in blobs:
            filename = blob.name.replace('/', '').replace(fileName, "")
            blob.download_to_filename(local + filename)  # Download
            imageList.append(filename)
            if fileName+"/thumbanails/" in blob.name:
                os.remove(local+filename)
                imageList.remove(filename)
        pdfImageList = []
        for i in range(len(imageList)):
            tempImage = Image.open(r'{}{}'.format(local, imageList[i]))
            temp = tempImage.convert('RGB')
            pdfImageList.append(temp)
        print(pdfImageList)
        pdfImageList[0].save(r'{}{}'.format(local, fileName),
                             save_all=True, append_images=pdfImageList[1:])
        return "success"
    except Exception as e:
        logging.error(e)
        return raiseExceptionInternalServerError("Internal Server Error")


class Body(BaseModel):
    pageOrder: list


@app.post("/createPDF/v2/{fileName}")
async def createPDFV2(fileName: str, pageOrder: Body):
    try:
        pdfImageList = []
        for i in pageOrder.pageOrder:
            imageName = f"Page_{i}.png"
            logging.info(f"Downloading image {imageName} ")
            blob = splitBucket.blob(fileName+"/"+imageName)
            local = 'temp/'
            blob.download_to_filename(local + imageName)
            tempImage = Image.open(r'{}{}'.format(local, imageName))
            temp = tempImage.convert('RGB')
            pdfImageList.append(temp)
            newFileName = fileName[:-4]+"New.pdf"
        logging.info(
            f"creating PDF for the request of pageOrder {pageOrder.pageOrder}")
        pdfImageList[0].save(r'{}{}'.format(local, newFileName),
                             save_all=True, append_images=pdfImageList[1:])
        logging.info(f"PDF Created with fileName {newFileName}")
        createPDFPath = createPDFBucket.blob(
            f"{fileName}/{newFileName}")
        createPDFPath.upload_from_filename(f"temp/{newFileName}")
        logging.info("PDF Uploaded to GCS Successfully")
        os.remove(f"temp/{newFileName}")
        for i in range(len(pageOrder.pageOrder)):
            os.remove(f"temp/Page_{i+1}.png")
        return "success"

    except Exception as e:
        logging.error(e)
        return raiseExceptionInternalServerError("Internal Server Error")


@app.get("/get/file/v1/{filename}")
async def post(filename: str):
    try:
        return await getFileId(filename)
    except urllib.request.HTTPError as e:
        print(e)


if __name__ == '__main__':
    nest_asyncio.apply()
    uvicorn.run(app, port=PORT, debug=True, host=host)
