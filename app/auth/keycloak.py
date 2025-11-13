import httpx
from jose import jwt, JWTError
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from dotenv import load_dotenv
import os

load_dotenv() 

KEYCLOAK_URL = os.environ["KEYCLOAK_URL"]
REALM = os.environ["REALM_RELATIVE_PATH"]
ALGORITHM = os.environ["ALGORITHM"]
REALM_URI = f"{KEYCLOAK_URL}{REALM}"

oauth2_scheme = HTTPBearer()
jwks_cache = {}

# ===== Utils =====


async def get_public_keys():
    if not jwks_cache.get("keys"):
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{REALM_URI}/protocol/openid-connect/certs")
            if resp.status_code != 200:
                raise RuntimeError(
                    "Impossibile recuperare le chiavi pubbliche da Keycloak."
                )
            jwks_cache["keys"] = resp.json()["keys"]
    return jwks_cache["keys"]


async def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(oauth2_scheme),
) -> dict:
    token = credentials.credentials
    keys = await get_public_keys()

    for key in keys:
        try:
            payload = jwt.decode(
                token,
                key,
                algorithms=[ALGORITHM],
                options={"verify_aud": False},  # Imposta a True se vuoi verificare aud
            )
            return payload
        except JWTError:
            continue  # Prova la prossima chiave

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token non valido o firma non verificabile",
        headers={"WWW-Authenticate": "Bearer"},
    )


def has_role_in_client(payload: dict, client: str, role: str) -> bool:
    """
    Verifica se l'utente ha un determinato ruolo in uno specifico client Keycloak.

    :param payload: Il dizionario decodificato del token JWT.
    :param client: Il nome del client (es. 'van-gogh-dna').
    :param role: Il ruolo da cercare (es. 'van-gogh-admin').
    :return: True se l'utente ha quel ruolo nel client, False altrimenti.
    """
    roles = payload.get("resource_access", {}).get(client, {}).get("roles", [])
    return role in roles


async def require_admin(token_data: dict = Depends(verify_token)) -> dict:
    if not has_role_in_client(token_data, "van-gogh-dna", "van-gogh-admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Permessi insufficienti: ruolo van-gogh-admin richiesto.",
        )
        
    return token_data
