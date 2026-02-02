from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
import os
import logging
from typing import Dict, List, Optional

# Configure logger
logger = logging.getLogger(__name__)

class KeyVaultClient:
    def __init__(self, vault_url: str, client_id: str, client_secret: str, tenant_id: str):
        """
        Initialize KeyVault client with service principal credentials
        
        Args:
            vault_url: Azure Key Vault URL
            client_id: Service principal client ID
            client_secret: Service principal client secret
            tenant_id: Azure AD tenant ID
        """
        self.vault_url = vault_url
        self.credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        self.client = SecretClient(vault_url=vault_url, credential=self.credential)
        logger.info(f"KeyVault client initialized for vault: {vault_url}")
    
    def get_secret(self, secret_name: str) -> Optional[str]:
        """
        Retrieve a single secret from Key Vault
        
        Args:
            secret_name: Name of the secret to retrieve
            
        Returns:
            Secret value or None if not found
        """
        try:
            secret = self.client.get_secret(secret_name)
            logger.info(f"Successfully retrieved secret: {secret_name}")
            return secret.value
        except Exception as e:
            logger.error(f"Error retrieving secret '{secret_name}': {e}")
            return None
    
    def get_secrets(self, secret_names: List[str]) -> Dict[str, str]:
        """
        Retrieve multiple secrets from Key Vault
        
        Args:
            secret_names: List of secret names to retrieve
            
        Returns:
            Dictionary mapping secret names to their values
        """
        secrets = {}
        for secret_name in secret_names:
            secret_value = self.get_secret(secret_name)
            if secret_value:
                secrets[secret_name] = secret_value
            else:
                logger.warn(f"Failed to retrieve secret: {secret_name}")
        
        logger.info(f"Retrieved {len(secrets)}/{len(secret_names)} secrets successfully")
        return secrets

def create_keyvault_client() -> KeyVaultClient:
    """
    Create KeyVault client using environment variables
    
    Required environment variables:
    - AZURE_VAULT_URL
    - AZURE_CLIENT_ID
    - AZURE_CLIENT_SECRET
    - AZURE_TENANT_ID
    """
    vault_url = os.getenv('AZURE_VAULT_URL')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    
    if not all([vault_url, client_id, client_secret, tenant_id]):
        logger.error("Missing required environment variables for Azure Key Vault connection")
        raise ValueError("Missing required environment variables for Azure Key Vault connection")
    
    logger.info("Creating KeyVault client from environment variables")
    return KeyVaultClient(vault_url, client_id, client_secret, tenant_id)
