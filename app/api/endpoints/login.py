from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

router = APIRouter()

class LoginRequest(BaseModel):
    username: str
    password: str

class UserInfo(BaseModel):
    roles: List[str]
    introduction: str
    avatar: str
    name: str

class TokenResponse(BaseModel):
    code: int
    data: Dict[str, str]

class UserInfoResponse(BaseModel):
    code: int
    data: UserInfo

class LogoutResponse(BaseModel):
    code: int
    message: str

@router.post("/login", response_model=TokenResponse)
async def login(data: LoginRequest):
    """
    Authenticate user and return token.
    Mirrors the original login function.
    """
    # This function is simplified to match the original basic implementation
    # In a real application, you would verify credentials against a database
    if data.username == 'tuadmin' and data.password == 'Tu1Museum!to0L':
        return {"code": 20000, "data": {"token": "ad234Vdk3234"}}
    elif data.username == 'ajones27@tulane.edu' and data.password == '8dL*u9Kb2j':
        return {"code": 20000, "data": {"token": "edsdfkE323fj"}}
    elif data.username == 'pburch@tulane.edu' and data.password == '9dL@k24vds':
        return {"code": 20000, "data": {"token": "ed29vcmDske"}}
    elif data.username == 'rbrill@tulane.edu' and data.password == 'nvd8*mLaSe':
        return {"code": 20000, "data": {"token": "ed90d3jsdfsd"}}
    else:
        raise HTTPException(
            status_code=401,
            detail="Wrong Username or Password."
        )

@router.get("/getInfo", response_model=UserInfoResponse)
async def get_info(token: Optional[str] = None):
    """
    Get user information based on token.
    Mirrors the original getInfo function.
    """
    if token == 'ad234Vdk3234':
        return {
            "code": 20000,
            "data": {
                "roles": ["admin"],
                "introduction": "I am a super administrator",
                "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
                "name": "Super Admin"
            }
        }
    elif token == 'edsdfkE323fj':
        return {
            "code": 20000,
            "data": {
                "roles": ["editor"],
                "introduction": "I am an editor",
                "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
                "name": "Andrew Jones"
            }
        }
    elif token == 'ed29vcmDske':
        return {
            "code": 20000,
            "data": {
                "roles": ["editor"],
                "introduction": "I am an editor",
                "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
                "name": "Paula Burch"
            }
        }
    elif token == 'ed90d3jsdfsd':
        return {
            "code": 20000,
            "data": {
                "roles": ["editor"],
                "introduction": "I am an editor",
                "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
                "name": "Ross Brill"
            }
        }
    else:
        raise HTTPException(
            status_code=500,
            detail="Wrong Token"
        )

@router.post("/logout", response_model=LogoutResponse)
async def logout():
    """
    Log out user.
    Mirrors the original logout function.
    """
    return {
        "code": 20000,
        "message": "log out success"
    }