import httpx
from jose import jwt, JWTError
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from typing import Dict

KEYCLOAK_URL = "http://localhost:8080" #server
REALM_URI = f"{KEYCLOAK_URL}/realms/master"
ALGORITHM = "RS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Cache della chiave pubblica
jwks_cache = {}

async def get_public_key():
    if not jwks_cache.get("keys"):
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{REALM_URI}/protocol/openid-connect/certs")
            if resp.status_code != 200:
                raise RuntimeError("Impossibile recuperare le chiavi pubbliche da Keycloak.")
            jwks_cache["keys"] = resp.json()["keys"]
    return jwks_cache["keys"]

async def verify_token(token: str = Depends(oauth2_scheme)) -> Dict:
    keys = await get_public_key()
    
    for key in keys:
        try:
            public_key = jwt.construct_rsa_public_key(key)
            payload = jwt.decode(token, public_key, algorithms=[ALGORITHM], options={"verify_aud": False})
            return payload
        except JWTError:
            continue  # Prova con la prossima chiave se ce ne sono più di una

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token non valido o firma non verificabile",
        headers={"WWW-Authenticate": "Bearer"},
    )
