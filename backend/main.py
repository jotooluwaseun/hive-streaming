"""
Backend Service

This backend service provides a FastAPI endpoint for receiving telemetry events. 
It validates incoming payloads using a Pydantic model, logs each event to a JSONL file, and 
returns an acknowledgment with a timestamp. It also enables CORS so events can be sent from 
any client.

"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
import json

app = FastAPI(title="Telemetry Receiver API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic Model for Validation
class TelemetryEvent(BaseModel):
    event_type: str
    playback_position: Optional[float] = None
    quality: Optional[Dict[str, Any]] = None
    buffering: Optional[Dict[str, Any]] = None

    # Metadata from collector
    sessionId: str
    viewerId: str
    eventId: str
    timestamp: int


# In-memory or file storage
LOG_FILE = "telemetry.log"


def save_event(event: dict):
    """Append event to a JSONL log file."""
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")


# API Endpoint
@app.post("/telemetry")
async def receive_telemetry(event: TelemetryEvent):
    event_dict = event.dict()

    # Print to console
    print("\n📡 Received Telemetry Event:")
    print(json.dumps(event_dict, indent=2))

    # Save to file
    save_event(event_dict)

    return {"status": "ok", "received_at": datetime.utcnow().isoformat()}
