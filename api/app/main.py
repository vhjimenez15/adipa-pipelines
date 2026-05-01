from fastapi import FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from fastapi import Depends

from app.auth import authenticate, create_token
from app.routers import orders, kpis, sync_log

app = FastAPI(
    title="ADIPA Pipelines API",
    description="KPIs de ventas multi-país — CL, MX, CO",
    version="1.0.0",
    # root_path necesario cuando corre detrás de nginx en /api
    root_path="/api",
)


@app.post(
    "/auth/login",
    tags=["Auth"],
    summary="Login — returns a Bearer token",
    response_model=dict,
)
def login(form: OAuth2PasswordRequestForm = Depends()):
    if not authenticate(form.username, form.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Wrong username or password",
        )
    token = create_token(form.username)
    return {"access_token": token, "token_type": "bearer"}


app.include_router(orders.router)
app.include_router(kpis.router)
app.include_router(sync_log.router)
