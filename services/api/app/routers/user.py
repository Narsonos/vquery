#Fastapi
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm

#Project files
from app.utils.common import json_serializer
from app.models.common import User,UserCreationModel, UserLoginModel, TokenResponse
from app.config import Config
from app.db import RedisDependency, DBDependency, get_users
import app.utils.security as security
import app.utils.exceptions as exc

#SQLAlchemy/SQLModel
from sqlmodel import select, insert, delete
import sqlalchemy.exc as sqlexc

#Pydantic/Typing
from typing import Annotated









router = APIRouter(
    prefix="/users",
    tags = ["users"],
    responses={404: {"description": "Requested resource is not found"}}
    )

import logging
logger = logging.getLogger('applogger')






@router.post("/token", responses={
    401: {"description":"Bad credentials"},
    422: {"description":"Form data has bad format (PydanticValidation)"},
    })
async def login(dbsession: DBDependency, redis: RedisDependency, form_data: Annotated[OAuth2PasswordRequestForm,Depends()]) -> TokenResponse:
    given_user = UserLoginModel(username=form_data.username,password=form_data.password)
    user = await security.authenticate_user(dbsession, given_user)
    if not user:
        raise exc.CredentialsException

    
    access_token, refresh_token, session_id = security.create_access_and_refresh(user_id=user.id, return_session_id=True)
    #Here we're gonna load user and load session - the sessions and cached_users are separate One to Many
    user_session_key = f"user_session:{user.id}:{session_id}" #One record per SESSION
    await redis.hset(user_session_key, mapping={"access_token":access_token,"refresh_token":refresh_token})

    #Had to change it a bit due to the way oauth works - for better testing
    return TokenResponse(access_token=access_token, refresh_token=refresh_token,token_type="bearer")



@router.get("/logout")
async def logout(token_data: Annotated[dict,Depends(security.exctract_token_data)], redis: RedisDependency) -> JSONResponse:
    user_id, session_id = token_data
    await redis.delete(f"user_session:{user_id}:{session_id}") #delete session from "session" storage
    return JSONResponse(status_code=200, content={"msg":"Token successfully deactivated!"})


@router.post("/refresh", responses={
    401: {"description":"Logged out or expired/wrong token"},
    })
async def refresh(refresh_token: Annotated[str, Depends(security.oauth2)], redis: RedisDependency) -> TokenResponse:
    logger.info(f'[REFRESH] Token = {refresh_token}')
    user_id,session_id = await security.exctract_token_data(refresh_token, refresh=True) 
    user_data = await redis.hgetall(f"user:{user_id}:{session_id}")
    
    #If user in redis - refresh tokens
    if user_data.get("refresh_token") == refresh_token:
        access_token, refresh_token = security.create_access_and_refresh(user_id=user_id, session_id=session_id)
        await redis.hset(f"user:{user_id}:{session_id}", mapping={
            'access_token':access_token,
            'refresh_token':refresh_token
            })

        #Return renewed tokens
        return TokenResponse(access_token=access_token, refresh_token=refresh_token,token_type="bearer")

    #Else user is considered logged out (bc they're not in redis)
    raise exc.LoggedOutException



########################################
#        GETTING USER PROFILE          #
########################################


#for testing purposes only
@router.get("/")
async def read_users(dbsession: DBDependency) -> list[User]:
    users = await get_users(dbsession)
    return users

@router.get("/me")
async def whoami(current_user:security.CurrentUserDependency) -> User:
    return current_user



########################################
#     USER CREATION & VERIFICATION     #
########################################



# Note: User creation & deletion is considered as admin only right.
@router.post("/new", tags=['Admin'], responses= {
    200: {"description":"Created successfully"},
    409: {"description":"User already exists"},
    })
async def create_user(
    dbsession: DBDependency,
    signup_user: UserCreationModel,
    current_user: security.CurrentUserDependency
    )  -> JSONResponse:

    if not current_user.role == 'admin':
        raise exc.NotAllowed

    user = (await get_users(dbsession, username=signup_user.username)).one_or_none()
    if user:
        raise exc.UserAlreadyExistsError

    user = User(
        username=signup_user.username,
        description=signup_user.description,
        password_hash=security.hash_password(signup_user.password)
    )

    dbsession.add(user)
    await dbsession.commit()
    return JSONResponse(status_code=200, content={"msg":"User successfully created!"})

@router.post("/update", tags=['Admin'], responses= {
    200: {"description":"User updated"},
    403: {"description":"Invalid token"},
    409: {"description":"Account with this username exists"},
    })
async def update_user(
    dbsession: DBDependency,
    target_user_id: int,
    edited_user: UserCreationModel, 
    current_user: security.CurrentUserDependency,
    )  -> JSONResponse:


    #If not admin, you can change only your own profile
    if not current_user.role == 'admin':
        target_user = current_user
    else:
        target_user = (await get_users(dbsession, id=target_user_id)).one_or_none()
        if not target_user:
            raise exc.UserDoesNotExist

    target_user.username = edited_user.username
    target_user.password_hash = security.hash_password(edited_user.password)
    target_user.description = edited_user.description

    try:
        target_user = await dbsession.merge(target_user)
        await dbsession.commit()
        await dbsession.refresh(target_user)
        return JSONResponse(status_code=200, content={'msg': "User successfully updated!"})
    except sqlexc.IntegrityError: #If username violates unique constraint
        raise exc.UserAlreadyExistsError


@router.post('/delete', tags=['Admin'], responses= {
    200: {"description":"User successfully deleted"},
    502: {"description":"Failed to delete user"},
    })
async def delete_user(
    dbsession:DBDependency,
    redis:RedisDependency,
    current_user:security.CurrentUserDependency,
    target_user_id: int | None,
    ) -> JSONResponse:

    #If not admin, you can change only your own profile
    if not target_user_id or not current_user.role == 'admin':
        target_user = current_user
    else:
        target_user = (await get_users(dbsession, id=target_user_id)).one_or_none()
        if not target_user:
            raise exc.UserDoesNotExist
    
    #delete all sessions from redis
    _,keys = await redis.scan(cursor=0, match=f"user_session:{target_user.id}:*")
    async with redis.pipeline() as pipe:
        for key in keys:
            logger.debug(f'[APP] Session ...{key[:10]} of user {target_user.id} logged out')
            await pipe.delete(key)
        await pipe.execute()

    try:
        logger.debug(f'[APP] User({target_user.id}) is about to be deleted')
        await dbsession.execute(delete(User).where(User.id==target_user.id)) 
        await dbsession.commit()
        return JSONResponse(status_code=200, content={"msg":f"User (id={target_user.id}) was successfully deleted!"})
    except Exception as e:
        logger.debug(exc.format_exception_string(e))
        raise HTTPException(status_code=502, detail={"msg":"Failed to delete user. It might have been deleted already!"})


    





