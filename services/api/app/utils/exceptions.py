from fastapi import HTTPException, status
import traceback

def format_exception_string(e: Exception,source:str = "APP", comment: str = ""): #For loggers
    return f'[{source}: Exception] {comment}\n\nTraceback:\n{traceback.format_exception(e)}'

class AppBaseException(Exception):
    """Global base exception"""
    pass

class CustomDatabaseException(AppBaseException):
    """Base for exceptions raised manually in database-related functions"""
    pass

class TableNameIsTooLong(CustomDatabaseException):
    """If table name that is generated automatically based off of .xlsx sheet and file name is too long"""
    pass

class UnsupportedDialectException(CustomDatabaseException):
    """Can be used to show that chosen SQL dialect is not supported by the function"""
    pass

class SQLException(AppBaseException):
    '''Base for exceptions raised by JSON-SQL constructions'''
    def __init__(self, detail: dict):
        self.detail = detail

    def get_http_exception(self):
        return HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=self.detail
        )

class ValueNotAllowed(SQLException):
    '''Raised mostly when some is not in ENUM'''
    pass



#HTTP Exceptions
CredentialsException = HTTPException(
		status_code=status.HTTP_401_UNAUTHORIZED,
		detail="Could not validate credentials",
		headers={"WWW-Authenticate":"Bearer"}
	)

TokenExpiredException = HTTPException(
		status_code=status.HTTP_401_UNAUTHORIZED,
		detail="AccessToken expired",
		headers={"WWW-Authenticate":"Bearer"}
	)

LoggedOutException = HTTPException(
		status_code=status.HTTP_401_UNAUTHORIZED,
		detail="Token is valid but User logged out",
		headers={"WWW-Authenticate":"Bearer"}
	)

UnsupportedImageType = HTTPException(
		status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
		detail="Something is wrong with the image type or Content-type header! PNG, JPEG, JPG"
	)

MoreThanOneRequest = HTTPException(
		status_code=status.HTTP_429_TOO_MANY_REQUESTS, 
		detail={"msg":"This operation allows 1 request being processed at once"}
	)

UserAlreadyExistsError = HTTPException(
        status_code=status.HTTP_409_CONFLICT, 
        detail={"msg":f"User with this username already exists!"}
    )

UserDoesNotExist = HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, 
        detail={"msg":f"User does not exist!"}
    )

NotAllowed = HTTPException(
    status_code=status.HTTP_403_FORBIDDEN,
    detail={"msg":'You do not have enough rights for this operation!'}
)

WrongAliasTarget = HTTPException(
    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    detail={"msg":'Alias cannot point at target, name of which contains double underscore, i.e. __table'}
)

AliasTargetNotExists = HTTPException(
    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    detail={"msg":'Alias points at nonexistent target'}
)

AliasAlreadyExists = HTTPException(
    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    detail={"msg":'Alias already exists'}   
)

TableNotExists = HTTPException(
    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    detail={"msg":'Table not exists'}   
)

InvalidQuery = HTTPException(
    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    detail={"msg":'Query failed, action cancelled'}   
)

QueryAlreadyExists = HTTPException(
    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    detail={"msg":'Query with this id already exists'}   
)

QueryNotExists = HTTPException(
    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    detail={"msg":'Query with this id does not exist'}   
)

TableInsteadOfColumn = HTTPException(
    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    detail={"msg":'Table operand can not be used within an expression. If you create an alias for a table -> it must not be within an expression. TableOperand as target'}   
)


