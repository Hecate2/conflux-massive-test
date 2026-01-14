"""
Conflux JSON-RPC Client

A simple client for interacting with Conflux nodes via JSON-RPC.
"""

import json
import random
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

import requests

from loguru import logger


class ConfluxRpcError(Exception):
    """Error from Conflux RPC"""
    
    def __init__(self, code: int, message: str, data: Any = None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"RPC Error {code}: {message}")


class ConfluxRpcClient:
    """
    Client for Conflux JSON-RPC API.
    
    Provides methods for common Conflux operations.
    """
    
    DEFAULT_TIMEOUT = 30  # seconds
    
    def __init__(self, url: str, timeout: int = DEFAULT_TIMEOUT):
        """
        Initialize the RPC client.
        
        Args:
            url: JSON-RPC endpoint URL
            timeout: Request timeout in seconds
        """
        self.url = url
        self.timeout = timeout
        self._request_id = 0
    
    def _next_id(self) -> int:
        """Get next request ID"""
        self._request_id += 1
        return self._request_id
    
    def call(self, method: str, params: Optional[List[Any]] = None) -> Any:
        """
        Make an RPC call.
        
        Args:
            method: RPC method name
            params: Method parameters
            
        Returns:
            Result from RPC call
            
        Raises:
            ConfluxRpcError: If RPC returns an error
        """
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": method,
            "params": params or [],
        }
        
        try:
            response = requests.post(
                self.url,
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            
            result = response.json()
            
            if "error" in result:
                error = result["error"]
                raise ConfluxRpcError(
                    code=error.get("code", -1),
                    message=error.get("message", "Unknown error"),
                    data=error.get("data"),
                )
            
            return result.get("result")
            
        except requests.RequestException as e:
            raise ConfluxRpcError(-1, f"Request failed: {e}")
    
    # ==================== Status Methods ====================
    
    def get_status(self) -> Dict[str, Any]:
        """Get node status"""
        return self.call("cfx_getStatus")
    
    def get_sync_phase(self) -> str:
        """Get current sync phase"""
        return self.call("debug_currentSyncPhase")
    
    def get_epoch_number(self, epoch_tag: str = "latest_state") -> int:
        """
        Get current epoch number.
        
        Args:
            epoch_tag: Epoch tag (latest_state, latest_mined, etc.)
            
        Returns:
            Epoch number
        """
        result = self.call("cfx_epochNumber", [epoch_tag])
        return int(result, 16)
    
    def get_block_count(self) -> int:
        """Get total block count"""
        return self.call("test_getBlockCount")
    
    def get_peer_count(self) -> int:
        """Get number of connected peers"""
        result = self.call("net_peerCount")
        return int(result, 16)
    
    def get_goodput(self) -> Dict[str, Any]:
        """Get goodput statistics"""
        return self.call("test_getGoodPut")
    
    # ==================== Node Identity ====================
    
    def get_node_id(self) -> str:
        """
        Get node's public key (node ID).
        
        Uses test_getNodeId with a random challenge.
        
        Returns:
            Node public key hex string
        """
        # Use eth_keys.datatypes.Signature for signature recovery
        from eth_utils.hexadecimal import decode_hex, encode_hex
        import struct
        
        # Generate random challenge
        challenge = random.randint(0, 2**32 - 1)
        challenge_bytes = struct.pack(">I", challenge)
        
        # Get signature
        signature = self.call("test_getNodeId", [encode_hex(challenge_bytes)])
        
        # Recover public key from signature
        sig_bytes = decode_hex(signature)
        
        # Parse signature (r, s, v)
        r = int.from_bytes(sig_bytes[0:32], 'big')
        s = int.from_bytes(sig_bytes[32:64], 'big')
        v = sig_bytes[64]
        
        # Create recoverable signature
        from eth_keys.datatypes import Signature
        from eth_hash.auto import keccak
        
        # Hash the challenge
        msg_hash = keccak(challenge_bytes)
        
        # Recover public key
        sig = Signature(vrs=(v, r, s))
        public_key = sig.recover_public_key_from_msg_hash(msg_hash)
        
        return public_key.to_hex()
    
    # ==================== Peer Management ====================
    
    def add_peer(self, enode: Optional[str]) -> bool:
        """
        Add a peer to the node. Returns False immediately if enode is None.
        
        Args:
            enode: Enode URL (cfxnode://pubkey@host:port)
            
        Returns:
            True if successful
        """
        if not enode:
            return False
        try:
            self.call("admin_addPeer", [enode])
            return True
        except ConfluxRpcError:
            return False
    
    def get_peers(self) -> List[Dict[str, Any]]:
        """Get list of connected peers"""
        return self.call("admin_peers") or []
    
    # ==================== Block Operations ====================
    
    def get_best_block_hash(self) -> str:
        """Get hash of the best block"""
        return self.call("cfx_getBestBlockHash")
    
    def get_block_by_hash(
        self, 
        block_hash: str, 
        include_txs: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get block by hash.
        
        Args:
            block_hash: Block hash
            include_txs: Whether to include full transactions
            
        Returns:
            Block data or None
        """
        return self.call("cfx_getBlockByHash", [block_hash, include_txs])
    
    def get_block_by_epoch(
        self, 
        epoch: int, 
        include_txs: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get block by epoch number.
        
        Args:
            epoch: Epoch number
            include_txs: Whether to include full transactions
            
        Returns:
            Block data or None
        """
        epoch_hex = hex(epoch)
        return self.call("cfx_getBlockByEpochNumber", [epoch_hex, include_txs])
    
    def generate_block(
        self, 
        num_txs: int = 0,
        block_size_limit: Optional[int] = None,
    ) -> str:
        """
        Generate a block (test method).
        
        Args:
            num_txs: Number of transactions to include
            block_size_limit: Block size limit
            
        Returns:
            Block hash
        """
        params = [num_txs]
        if block_size_limit:
            params.append(block_size_limit)
        return self.call("test_generateBlockWithFakeTxs", params)
    
    def generate_block_with_parent(
        self, 
        parent_hash: str,
        referees: Optional[List[str]] = None,
    ) -> str:
        """
        Generate a block with specific parent.
        
        Args:
            parent_hash: Parent block hash
            referees: List of referee block hashes
            
        Returns:
            Block hash
        """
        return self.call("test_generateBlockWithParent", [parent_hash, referees or []])
    
    def generate_one_block(
        self,
        num_txs: int = 0,
        block_size_limit: int = 300000,
    ) -> str:
        """
        Generate one block (simpler version).
        
        Args:
            num_txs: Number of transactions
            block_size_limit: Block size limit
            
        Returns:
            Block hash
        """
        return self.call("generateoneblock", [num_txs, block_size_limit])
    
    # ==================== Transaction Operations ====================
    
    def get_nonce(self, address: str) -> int:
        """
        Get account nonce.
        
        Args:
            address: Account address
            
        Returns:
            Nonce
        """
        result = self.call("cfx_getNextNonce", [address])
        return int(result, 16)
    
    def get_balance(self, address: str) -> int:
        """
        Get account balance.
        
        Args:
            address: Account address
            
        Returns:
            Balance in Drip
        """
        result = self.call("cfx_getBalance", [address])
        return int(result, 16)
    
    def send_raw_transaction(self, raw_tx: str) -> str:
        """
        Send a raw transaction.
        
        Args:
            raw_tx: Signed transaction hex
            
        Returns:
            Transaction hash
        """
        return self.call("cfx_sendRawTransaction", [raw_tx])
    
    def get_transaction(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Get transaction by hash.
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Transaction data or None
        """
        return self.call("cfx_getTransactionByHash", [tx_hash])
    
    def get_transaction_receipt(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """
        Get transaction receipt.
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Receipt data or None
        """
        return self.call("cfx_getTransactionReceipt", [tx_hash])
    
    # ==================== Test Methods ====================
    
    def send_usable_genesis_accounts(self, start_index: int) -> None:
        """
        Initialize transaction generator with genesis accounts.
        
        Args:
            start_index: Starting account index
        """
        self.call("test_sendUsableGenesisAccounts", [start_index])
    
    def get_confirmation_risk(self, block_hash: str) -> int:
        """
        Get confirmation risk for a block.
        
        Args:
            block_hash: Block hash
            
        Returns:
            Risk value (lower is better)
        """
        result = self.call("cfx_getConfirmationRiskByHash", [block_hash])
        if result:
            return int(result, 16)
        return -1
