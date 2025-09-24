from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from app.services.auth_client import auth_client

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

def map_permissions_to_roles(permissions: List[str], is_superuser: bool = False) -> List[str]:
    """
    Map unified authentication permissions to Vue Admin Template roles.
    """
    if is_superuser:
        return ["admin"]
    
    roles = []
    if any(perm in permissions for perm in ["project.admin", "data.manage"]):
        roles.append("admin")
    elif any(perm in permissions for perm in ["data.edit", "data.upload"]):
        roles.append("editor")
    else:
        roles.append("viewer")
    
    return roles

def generate_introduction(permissions: List[str], is_superuser: bool = False) -> str:
    """
    Generate user introduction based on permissions.
    """
    if is_superuser:
        return "I am a super administrator"
    elif any(perm in permissions for perm in ["project.admin", "data.manage"]):
        return "I am a museum administrator"
    elif any(perm in permissions for perm in ["data.edit", "data.upload"]):
        return "I am a museum data editor"
    else:
        return "I am a museum user"

@router.post("/login", response_model=TokenResponse)
async def login(data: LoginRequest):
    """
    Redirect to unified authentication system.
    The Vue frontend should use SSO login instead of this endpoint.
    """
    # For backward compatibility, we'll keep supporting direct login temporarily
    # but encourage using SSO through the frontend
    raise HTTPException(
        status_code=400,
        detail="Please use the 'Sign in with Unified Authentication' button for secure login."
    )

@router.get("/getInfo", response_model=UserInfoResponse)
async def get_info(token: Optional[str] = Query(None)):
    """
    Get user information based on unified authentication token.
    """
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Token is required"
        )
    
    try:
        # Verify token with unified authentication center
        user_data = await auth_client.verify_token(token)
        
        if not user_data:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired token"
            )
        
        # Extract user information
        username = user_data.get("username", "Unknown User")
        display_name = user_data.get("display_name", username)
        is_superuser = user_data.get("is_superuser", False)
        permissions = user_data.get("permissions", [])
        
        # Map to Vue Admin Template format
        roles = map_permissions_to_roles(permissions, is_superuser)
        introduction = generate_introduction(permissions, is_superuser)
        
        return {
            "code": 20000,
            "data": {
                "roles": roles,
                "introduction": introduction,
                "avatar": "https://wpimg.wallstcn.com/f778738c-e4f8-4870-b634-56703b4acafe.gif",
                "name": display_name
            }
        }
    except Exception as e:
        # Token验证失败，返回401
        raise HTTPException(
            status_code=401,
            detail=f"Token verification failed: {str(e)}"
        )

@router.post("/logout", response_model=LogoutResponse)
async def logout():
    """
    Log out user from FMMT backend.
    The actual SSO logout is handled by the frontend.
    """
    return {
        "code": 20000,
        "message": "log out success"
    }