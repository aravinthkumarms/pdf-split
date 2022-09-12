from importlib.resources import path
from google.cloud import storage
from fileinput import filename
from pdf2image import convert_from_path
from PyPDF2 import PdfFileWriter, PdfFileReader
import fitz
import os
from PIL import Image
import PIL
# pdfs = r"ARAVINTHKUMARRESUME.pdf"
# pages = convert_from_path(
#     pdfs, 350, poppler_path=r'C:\\Program Files\\Poppler\\poppler-0.68.0\\bin')

# i = 1
# for page in pages:
#     image_name = "Page_" + str(i) + ".jpg"
#     page.save(image_name, "JPEG")
#     i = i+1

fileName = "AravinthDocuments"


# inputpdf = r"{}.pdf".format(fileName)
# pages = convert_from_path(
#     inputpdf, 350, poppler_path=r"./poppler-0.68.0/bin")
# i = 1
# for page in pages:
#     image_name = "Page_" + str(i) + ".jpg"
#     page.save(image_name, "JPEG")
#     i = i+1
storage_client = storage.Client.from_service_account_json(
    "key.json")
bucketName = "pdf-split-ingestion"
bucket = storage_client.get_bucket(bucketName)
splitBucketName = "pdf-split-files"
splitBucket = storage_client.get_bucket(splitBucketName)
# PyMuPDF, imported as fitz for backward compatibility reasons
# file_path = "temp/AravinthDocuments.pdf"
# with fitz.open(file_path) as pages:
#     i = 1  # open document
#     for page in pages:
#         pix = page.get_pixmap()  # render page to an image
#         pix.save(f"page_{i}.png")
#         image = Image.open(f"page_{i}.png")
#         image = image.convert('RGB')
#         image.save('new-format-image-from-png.webp', 'webp')
#         i += 1
# os.remove(file_path)
blob = splitBucket.blob(f"{fileName}/")
path = r"temp/page_1.jpg"
os.system(f"attrib -h {path}")
file_ = open(path, "wb")

blob.upload_from_filename(
    file_)
