import os
from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

SECRET_KEY = os.environ["JWT_SECRET"]
ALGORITHM = "HS256"
EXPIRE_HOURS = int(os.environ.get("JWT_EXPIRE_HOURS", 24))

API_USER = os.environ["API_USER"]
API_PASSWORD = os.environ["API_PASSWORD"]

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


def create_token(username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=EXPIRE_HOURS)
    return jwt.encode({"sub": username, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    credentials_error = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if not username:
            raise credentials_error
        return username
    except JWTError:
        raise credentials_error


def authenticate(username: str, password: str) -> bool:
    return username == API_USER and password == API_PASSWORD
