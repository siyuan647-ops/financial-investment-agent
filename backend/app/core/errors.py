from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException


class AppError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message


def error_response(request: Request, status: int, code: str, message: str):
    headers = {"WWW-Authenticate": "Bearer"} if status == 401 else {}
    return JSONResponse(
        status_code=status,
        content={
            "error": {"code": code, "message": message},
            "request_id": request.state.request_id,
        },
        headers=headers,
    )


async def app_error_handler(request: Request, exc: AppError):
    return error_response(request, exc.status, exc.code, exc.message)


async def http_error_handler(request: Request, exc: HTTPException):
    codes = {404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED", 401: "UNAUTHENTICATED", 403: "FORBIDDEN"}
    return error_response(
        request,
        exc.status_code,
        codes.get(exc.status_code, "HTTP_ERROR"),
        "Request could not be processed",
    )


async def validation_error_handler(request: Request, exc: RequestValidationError):
    return error_response(request, 422, "VALIDATION_ERROR", "Invalid request")
