from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import json
import logging
from datetime import datetime
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Rakbank POC Kafka User Transaction Webhook",
    description="Receives user transaction data from Confluent Kafka topic via HTTP Sink",
    version="1.0.0"
)

# Pydantic model for incoming data (key field excluded as requested)
class UserTransaction(BaseModel):
    authorizer_usrnbr: Optional[int] = None
    creat_usrnbr: Optional[int] = None
    creat_time: Optional[str] = None
    data: Optional[str] = None
    usrname: Optional[str] = None

# In-memory storage for POC (in production, use database)
received_transactions = []

@app.get("/")
async def root():
    return {
        "message": "Rakbank POC Kafka User Transaction Webhook Service",
        "status": "running",
        "timestamp": datetime.now().isoformat(),
        "total_received": len(received_transactions),
        "service_info": {
            "source": "Confluent Cloud Flink",
            "topic": "user_trans_hst_avro",
            "purpose": "POC webhook endpoint"
        }
    }

@app.post("/webhook/user-transactions")
async def receive_user_transaction(transaction: UserTransaction):
    """
    Main webhook endpoint for receiving user transaction data from Kafka HTTP Sink
    Key field is excluded from processing as requested
    """
    try:
        # Add metadata when received
        transaction_dict = transaction.dict()
        transaction_dict["received_at"] = datetime.now().isoformat()
        transaction_dict["poc_id"] = len(received_transactions) + 1
        
        # Store transaction (in production, save to database/ADLS)
        received_transactions.append(transaction_dict)
        
        # Enhanced logging for POC monitoring
        logger.info(f"[RAKBANK POC] Transaction received - "
                   f"User: {transaction.usrname}, "
                   f"Creator: {transaction.creat_usrnbr}, "
                   f"Data: {transaction.data}, "
                   f"Time: {transaction.creat_time}")
        
        return {
            "status": "success", 
            "message": "User transaction received successfully",
            "poc_id": transaction_dict["poc_id"],
            "user": transaction.usrname,
            "received_at": transaction_dict["received_at"]
        }
        
    except Exception as e:
        logger.error(f"[RAKBANK POC ERROR] Failed to process transaction: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing transaction: {str(e)}")

@app.get("/webhook/user-transactions")
async def get_received_transactions():
    """
    Get all received transactions for POC monitoring and testing
    """
    return {
        "total_count": len(received_transactions),
        "last_10_transactions": received_transactions[-10:] if received_transactions else [],
        "last_updated": datetime.now().isoformat(),
        "poc_status": "active"
    }

@app.post("/webhook/user-transactions/batch")
async def receive_batch_transactions(transactions: list[UserTransaction]):
    """
    Handle batch transactions from Kafka Connect (if connector sends batches)
    """
    try:
        received_count = 0
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for transaction in transactions:
            transaction_dict = transaction.dict()
            transaction_dict["received_at"] = datetime.now().isoformat()
            transaction_dict["batch_id"] = batch_id
            transaction_dict["poc_id"] = len(received_transactions) + 1
            
            received_transactions.append(transaction_dict)
            received_count += 1
            
            logger.info(f"[RAKBANK POC BATCH] Transaction received - User: {transaction.usrname}")
        
        return {
            "status": "success",
            "message": f"Batch of {received_count} transactions received successfully",
            "batch_id": batch_id,
            "total_received": len(received_transactions)
        }
        
    except Exception as e:
        logger.error(f"[RAKBANK POC ERROR] Failed to process batch: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing batch: {str(e)}")

@app.get("/health")
async def health_check():
    return {
        "status": "healthy", 
        "timestamp": datetime.now().isoformat(),
        "service": "Rakbank POC Kafka Webhook",
        "total_transactions": len(received_transactions)
    }

@app.get("/poc/stats")
async def poc_statistics():
    """
    POC specific statistics endpoint
    """
    if not received_transactions:
        return {
            "message": "No transactions received yet",
            "total_count": 0,
            "status": "waiting_for_data"
        }
    
    # Basic statistics for POC
    users = [t.get("usrname") for t in received_transactions if t.get("usrname")]
    unique_users = list(set(users))
    
    return {
        "total_transactions": len(received_transactions),
        "unique_users": len(unique_users),
        "user_list": unique_users,
        "first_transaction": received_transactions[0]["received_at"] if received_transactions else None,
        "last_transaction": received_transactions[-1]["received_at"] if received_transactions else None,
        "poc_status": "collecting_data"
    }

# For Azure App Service
if __name__ == "__main__":
    uvicorn.run(
        "main:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=False,
        log_level="info"
    )
