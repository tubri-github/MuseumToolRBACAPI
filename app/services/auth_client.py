"""
Authentication client for connecting to the unified authentication center.
"""
import httpx
import logging
from typing import Optional, Dict, Any
from app.core.config import settings

logger = logging.getLogger(__name__)


class AuthenticationClient:
    """Client for communicating with the unified authentication center."""
    
    def __init__(self):
        self.auth_center_url = settings.AUTH_CENTER_URL
        self.api_key = settings.AUTH_CENTER_API_KEY
        self.project_code = settings.PROJECT_CODE
    
    async def verify_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Verify a token with the authentication center and get user info.
        
        Args:
            token: The authentication token to verify
            
        Returns:
            User information if token is valid, None otherwise
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.auth_center_url}/api/v1/sso/verify-token",
                    headers={
                        "Content-Type": "application/json"
                    },
                    json={
                        "token": token,
                        "project": self.project_code
                    },
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get("success"):
                        return result.get("data")
                    else:
                        logger.warning(f"Token verification failed: {result.get('message')}")
                else:
                    logger.warning(f"Token verification request failed with status {response.status_code}")
                    
        except httpx.TimeoutException:
            logger.error("Token verification timeout")
        except httpx.RequestError as e:
            logger.error(f"Token verification request error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during token verification: {e}")
            
        return None
    
    async def get_user_permissions(self, user_id: str) -> list:
        """
        Get user permissions for this project from the authentication center.
        
        Args:
            user_id: The user ID
            
        Returns:
            List of permission codes for this project
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.auth_center_url}/api/v1/service/user-permissions/{user_id}",
                    headers={
                        "X-API-Key": self.api_key
                    },
                    params={
                        "project": self.project_code
                    },
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get("success"):
                        return result.get("data", {}).get("permissions", [])
                    else:
                        logger.warning(f"Get user permissions failed: {result.get('message')}")
                else:
                    logger.warning(f"Get user permissions request failed with status {response.status_code}")
                    
        except Exception as e:
            logger.error(f"Error getting user permissions: {e}")
            
        return []


# Global authentication client instance
auth_client = AuthenticationClient()