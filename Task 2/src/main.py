import time
import uuid
import json
from datetime import datetime, timezone
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from src.routers.products import router as products_router
from src.routers.orders import router as orders_router
from src.routers.auth import router as auth_router, get_current_user

app = FastAPI(title="Marketplace API", version="1.0.0")


async def set_body(request: Request, body: bytes):
    async def receive():
        return {"type": "http.request", "body": body}

    request._receive = receive


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content={
            "error_code": "VALIDATION_ERROR",
            "message": "Validation error for input data",
            "details": exc.errors()
        }
    )


@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    start_time = time.time()

    body_bytes = b""
    if request.method in ["POST", "PUT", "DELETE"]:
        body_bytes = await request.body()
        await set_body(request, body_bytes)

    response = await call_next(request)
    duration_ms = int((time.time() - start_time) * 1000)

    body_log = None
    if body_bytes:
        try:
            body_dict = json.loads(body_bytes)
            if "password" in body_dict:
                body_dict["password"] = "***"
            body_log = body_dict
        except json.JSONDecodeError:
            pass

    log_data = {
        "request_id": request_id,
        "method": request.method,
        "endpoint": request.url.path,
        "status_code": response.status_code,
        "duration_ms": duration_ms,
        "user_id": None,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    if request.method in ["POST", "PUT", "DELETE"] and body_log is not None:
        log_data["body"] = body_log

    print(json.dumps(log_data))
    response.headers["X-Request-Id"] = request_id
    return response


@app.get("/ping", tags=["System"])
async def ping():
    return {"status": "ok"}


app.include_router(auth_router, prefix="/auth", tags=["Auth"])
app.include_router(products_router, prefix="/products", tags=["Products"])
app.include_router(orders_router, prefix="/orders", tags=["Orders"])