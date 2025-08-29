"""
Google ADK Vertex AI Agent Engine Service for eCFR Analytics
Provides conversational AI interface using Google's Agent Engine
"""

import os
import uuid
from typing import List, Dict, Any
from datetime import datetime

from vertexai import agent_engines
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# Initialize Vertex AI
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
REGION = os.getenv("REGION", "us-central1")
AGENT_ENGINE_ID = "projects/321175517523/locations/us-central1/reasoningEngines/3532937568230309888"  # Your Agent Engine Resource ID

if not PROJECT_ID:
    raise RuntimeError("PROJECT_ID environment variable is required")

app = FastAPI(title="eCFR AI Assistant", version="2.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    message: str
    conversation_history: List[Dict[str, str]] = []
    date: str = "2025-08-22"
    max_context_sections: int = 5

class ChatResponse(BaseModel):
    response: str
    sources: List[Dict[str, Any]]
    context_used: List[str]

class AgentSession:
    """Manages Agent Engine sessions."""
    
    def __init__(self):
        self.sessions = {}  # user_id -> session_id
    
    def get_or_create_session(self, agent_engine_service, user_id: str = None) -> str:
        """Get existing session or create a new one."""
        if not user_id:
            user_id = "default_user"
        
        if user_id not in self.sessions:
            # Create a new session with the Agent Engine
            session = agent_engine_service.reasoning_engine.create_session(user_id=user_id)
            session_id = session["id"]
            self.sessions[user_id] = session_id
        
        return self.sessions[user_id]
    
    def clear_session(self, user_id: str = None):
        """Clear session for user."""
        if not user_id:
            user_id = "default_user"
        
        if user_id in self.sessions:
            del self.sessions[user_id]

agent_session = AgentSession()

class AgentEngineService:
    """Service for interacting with Google ADK Agent Engine."""
    
    def __init__(self):
        self.reasoning_engine = None
        self._initialize_engine()
    
    def _initialize_engine(self):
        """Initialize the reasoning engine."""
        try:
            self.reasoning_engine = agent_engines.get(AGENT_ENGINE_ID)
            print(f"Successfully initialized Agent Engine: {AGENT_ENGINE_ID}")
        except Exception as e:
            print(f"Failed to initialize Agent Engine: {e}")
            self.reasoning_engine = None
    
    def query_agent(self, user_id: str, session_id: str, message: str) -> str:
        """Query the Agent Engine using the correct API."""
        if not self.reasoning_engine:
            raise HTTPException(status_code=503, detail="Agent Engine not available")
        
        try:
            # Try different methods to interact with the Agent Engine
            #print(f"Available methods on reasoning_engine: {dir(self.reasoning_engine)}")
            agent_engine = self.reasoning_engine
            response_text = ""
            print(f"Querying Agent Engine with message: {message[:100]}...")
            print(f"User ID: {user_id}, Session ID: {session_id}")

            for event in agent_engine.stream_query(
                user_id=user_id, session_id=session_id, message=message
            ):
                if event:
                    print(f"Agent event: {event}")
                if "content" in event and "parts" in event["content"]:
                    for part in event["content"]["parts"]:
                        if "text" in part:
                            response_text += part["text"]
            
            print(f"Raw Agent response: {response_text}")

            return response_text
        except Exception as e:
            print(f"Agent Engine query failed: {e}")
            print(f"Exception type: {type(e)}")
            # Return a helpful fallback response
            return f"I'm having trouble connecting to the Agent Engine right now. This appears to be a question about {message[:100]}... Please try again later or contact support."

agent_engine_service = AgentEngineService()

@app.get("/")
def root():
    return {"message": "eCFR AI Assistant API (Agent Engine)", "status": "ready", "agent_engine_id": AGENT_ENGINE_ID}

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Main chat endpoint for regulatory AI assistant using Agent Engine."""
    
    try:
        # Get or create session for this user
        user_id = "ecfr_user"  # Could be parameterized later
        
        session_id = agent_session.get_or_create_session(agent_engine_service, user_id=user_id)

        print(f"Processing chat request - User: {user_id}, Session: {session_id}")
        print(f"Message: {request.message}")
        
        # Add context about the date and domain to the message
        enhanced_message = f"""
        Context: This is a question about federal regulations (Code of Federal Regulations - CFR) for date {request.date}. 
        The user is asking about regulatory compliance, burden analysis, or legal requirements.
        
        User Question: {request.message}
        
        Please provide a comprehensive answer focusing on:
        1. Specific regulatory citations and requirements
        2. Compliance implications and regulatory burden
        3. Practical guidance for understanding the regulation
        4. Any relevant enforcement or penalty information
        """
        
        # Query the Agent Engine
        ai_response = agent_engine_service.query_agent(user_id, session_id, enhanced_message)
        
        # Since Agent Engine handles context internally, we don't need to provide explicit sources
        # But we can extract any citations mentioned in the response
        sources = extract_citations_from_response(ai_response)
        context_used = [f"Agent Engine Session: {session_id}"]
        
        return ChatResponse(
            response=ai_response,
            sources=sources,
            context_used=context_used
        )
        
    except Exception as e:
        print(f"Chat error: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

def extract_citations_from_response(response: str) -> List[Dict[str, Any]]:
    """Extract CFR citations from Agent response for source display."""
    import re
    
    sources = []
    
    # Look for CFR citations in the response
    cfr_pattern = r'(\d+\s+CFR\s+§?\s*[\d.]+[A-Za-z]*(?:-[\d.]+[A-Za-z]*)?)'
    citations = re.findall(cfr_pattern, response)
    
    for i, citation in enumerate(set(citations[:5])):  # Max 5 unique citations
        # Extract title and part from citation
        title_match = re.search(r'(\d+)\s+CFR', citation)
        title_num = int(title_match.group(1)) if title_match else 0
        
        source = {
            "citation": citation,
            "title": title_num,
            "part": "N/A",  # Agent Engine handles the lookup
            "agency": "Various",
            "heading": "Regulatory Reference",
            "burden_score": 0,  # Agent Engine provides context-aware analysis
            "relevance": 100,
            "summary": f"Referenced in Agent Engine response"
        }
        sources.append(source)
    
    return sources

@app.delete("/chat/session")
def clear_chat_session(user_id: str = "ecfr_user"):
    """Clear chat session."""
    try:
        agent_session.clear_session(user_id)
        return {"message": f"Session cleared for user {user_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear session: {str(e)}")

@app.get("/health")
def health():
    """Health check endpoint."""
    try:
        # Test if Agent Engine is accessible
        engine_status = "connected" if agent_engine_service.reasoning_engine else "unavailable"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "agent_engine": engine_status,
                "agent_engine_id": AGENT_ENGINE_ID
            }
        }
    except Exception as e:
        return {
            "status": "degraded",
            "timestamp": datetime.now().isoformat(),
            "error": str(e),
            "services": {
                "agent_engine": "error"
            }
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)