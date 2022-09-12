from fastapi import HTTPException
import requests
from datetime import datetime
import logging


def errorMessage(message, statusCode):
    return {
        "timeStamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": statusCode,
        "type": "Error",
        "message": message
    }


def raiseExceptionInternalServerError(message):
    raise HTTPException(
        status_code=500, detail=errorMessage(message, 500))


def raiseExceptionBadRequest(message):
    raise HTTPException(
        status_code=400, detail=errorMessage(message, 400))


async def wrkFlwReq(step, fileId):
    try:
        if step == "splitStarted":
            splitingStartedRequest = {
                "fileId": fileId,
                "workFlowStepType": "Split PDF",
                "workFlowStatusType": "Started"}
            return await updateWrkFlwDtl(fileId, splitingStartedRequest)

        elif step == "splitCompleted":
            splitingCompletedRequest = {
                "fileId": fileId,
                "workFlowStepType": "Split PDF",
                "workFlowStatusType": "Completed"}
            return await updateWrkFlwDtl(fileId, splitingCompletedRequest)

        elif step == "deliveryStarted":
            deliveryPdfStartedRequest = {
                "fileId": fileId,
                "workFlowStepType": "PDF Delivery",
                "workFlowStatusType": "Started"}
            return await updateWrkFlwDtl(fileId, deliveryPdfStartedRequest)

        elif step == "deliveryCompleted":
            deliveryCompletedRequest = {
                "fileId": fileId,
                "workFlowStepType": "PDF Delivery",
                "workFlowStatusType": "Completed"}
            return await updateWrkFlwDtl(fileId, deliveryCompletedRequest)
    except Exception as e:
        logging.exception(e)
        return f"An Error Occurred: {e}", 500


async def updateWrkFlwDtl(fileId, request):
    try:
        res = requests.post(
            f"http://localhost:8000/blob/createwrkflw/{fileId}/v1", json=request)
        logging.info(
            "Hitting Data API for workflow details")
        logging.info(res.json())
    except Exception as e:
        logging.exception(e)
        return raiseExceptionInternalServerError("Internal Server Error")
