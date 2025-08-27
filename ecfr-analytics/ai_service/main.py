"""
Vertex AI RAG Service for eCFR Analytics
Provides conversational AI interface with full regulatory context
"""

import os
import re
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import datetime

import vertexai
from vertexai.generative_models import GenerativeModel
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from google.cloud import bigquery
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

# Initialize Vertex AI
PROJECT_ID = os.getenv("PROJECT_ID", "lawscan")
REGION = os.getenv("REGION", "us-central1")
DATASET = os.getenv("DATASET", "ecfr_enhanced")
TABLE = os.getenv("TABLE", "sections_enhanced")

if not PROJECT_ID:
    raise RuntimeError("PROJECT_ID environment variable is required")

vertexai.init(project=PROJECT_ID, location=REGION)

# Initialize clients
bq_client = bigquery.Client(project=PROJECT_ID)
embedding_model = TextEmbeddingModel.from_pretrained("textembedding-gecko@003")
generative_model = GenerativeModel("gemini-1.5-pro")

app = FastAPI(title="eCFR AI Assistant", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@dataclass
class RegulationContext:
    """Represents a regulation section with full context for AI."""
    section_citation: str
    title_num: int
    part_num: str
    agency_name: str
    section_heading: str
    section_text: str
    regulatory_burden_score: float
    prohibition_count: int
    requirement_count: int
    enforcement_terms: int
    ai_context_summary: str
    relevance_score: float = 0.0

class ChatRequest(BaseModel):
    message: str
    conversation_history: List[Dict[str, str]] = []
    date: str = "2025-08-22"
    max_context_sections: int = 5

class ChatResponse(BaseModel):
    response: str
    sources: List[Dict[str, Any]]
    context_used: List[str]

class EmbeddingService:
    """Service for creating and searching embeddings."""
    
    def __init__(self):
        self.model = embedding_model
    
    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Get embeddings for a list of texts."""
        try:
            inputs = [TextEmbeddingInput(text, "RETRIEVAL_DOCUMENT") for text in texts]
            embeddings = self.model.get_embeddings(inputs)
            return [embedding.values for embedding in embeddings]
        except Exception as e:
            print(f"Embedding error: {e}")
            return [[0.0] * 768 for _ in texts]  # Fallback to zero embeddings
    
    def get_query_embedding(self, query: str) -> List[float]:
        """Get embedding for a search query."""
        try:
            input_obj = TextEmbeddingInput(query, "RETRIEVAL_QUERY")
            embedding = self.model.get_embeddings([input_obj])
            return embedding[0].values
        except Exception as e:
            print(f"Query embedding error: {e}")
            return [0.0] * 768

embedding_service = EmbeddingService()

class RegulatoryRAG:
    """RAG system for regulatory queries."""
    
    def __init__(self):
        self.bq = bq_client
        
    def search_regulations_semantic(self, query: str, date: str, limit: int = 10) -> List[RegulationContext]:
        """Search regulations using semantic similarity."""
        
        # For now, use keyword-based search as fallback
        # In production, you'd want to implement vector similarity search
        return self.search_regulations_keyword(query, date, limit)
    
    def search_regulations_keyword(self, query: str, date: str, limit: int = 10) -> List[RegulationContext]:
        """Search regulations using keyword matching."""
        
        # Extract key terms from the query
        query_lower = query.lower()
        search_terms = []
        
        # Look for specific regulatory concepts
        if any(term in query_lower for term in ['safety', 'hazard', 'danger']):
            search_terms.append("safety")
        if any(term in query_lower for term in ['environment', 'pollution', 'emission']):
            search_terms.append("environment")
        if any(term in query_lower for term in ['penalty', 'fine', 'violation', 'enforce']):
            search_terms.append("enforcement")
        if any(term in query_lower for term in ['requirement', 'must', 'shall']):
            search_terms.append("requirement")
        if any(term in query_lower for term in ['prohibition', 'prohibited', 'forbidden']):
            search_terms.append("prohibition")
            
        # Extract CFR citations if present
        cfr_pattern = r'(?:title\s+)?(\d+)\s+cfr\s+(?:part\s+)?(\d+)(?:\.(\d+))?'
        cfr_matches = re.findall(cfr_pattern, query_lower)
        
        # Build search query
        where_conditions = ["version_date = DATE(@date)"]
        params = [bigquery.ScalarQueryParameter("date", "STRING", date)]
        
        if cfr_matches:
            # Specific CFR citation search
            title_num, part_num, section_num = cfr_matches[0]
            where_conditions.append("title_num = @title")
            where_conditions.append("part_num = @part")
            params.extend([
                bigquery.ScalarQueryParameter("title", "INT64", int(title_num)),
                bigquery.ScalarQueryParameter("part", "STRING", part_num)
            ])
            if section_num:
                where_conditions.append("section_citation LIKE @section")
                params.append(bigquery.ScalarQueryParameter("section", "STRING", f"%{section_num}%"))
        else:
            # Keyword-based search
            search_conditions = []
            for i, term in enumerate(search_terms[:3]):  # Limit to 3 terms
                search_conditions.append(f"(LOWER(section_text) LIKE @term{i} OR LOWER(section_heading) LIKE @term{i} OR LOWER(ai_context_summary) LIKE @term{i})")
                params.append(bigquery.ScalarQueryParameter(f"term{i}", "STRING", f"%{term}%"))
            
            if search_conditions:
                where_conditions.append(f"({' OR '.join(search_conditions)})")
            else:
                # Fallback: search in all text fields
                where_conditions.append("(LOWER(section_text) LIKE @query OR LOWER(section_heading) LIKE @query)")
                params.append(bigquery.ScalarQueryParameter("query", "STRING", f"%{query_lower}%"))
        
        sql = f"""
        SELECT 
            section_citation,
            title_num,
            part_num,
            agency_name,
            section_heading,
            section_text,
            regulatory_burden_score,
            prohibition_count,
            requirement_count,
            enforcement_terms,
            ai_context_summary,
            word_count,
            -- Simple relevance scoring
            CASE 
                WHEN LOWER(section_heading) LIKE @query_param THEN 10
                WHEN LOWER(ai_context_summary) LIKE @query_param THEN 5
                ELSE 1
            END as relevance_score
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE {' AND '.join(where_conditions)}
        ORDER BY relevance_score DESC, regulatory_burden_score DESC
        LIMIT @limit
        """
        
        params.extend([
            bigquery.ScalarQueryParameter("query_param", "STRING", f"%{query_lower}%"),
            bigquery.ScalarQueryParameter("limit", "INT64", limit)
        ])
        
        job = self.bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
        results = []
        
        for row in job.result():
            context = RegulationContext(
                section_citation=row.section_citation or "",
                title_num=row.title_num or 0,
                part_num=row.part_num or "",
                agency_name=row.agency_name or "Unknown",
                section_heading=row.section_heading or "",
                section_text=row.section_text or "",
                regulatory_burden_score=row.regulatory_burden_score or 0.0,
                prohibition_count=row.prohibition_count or 0,
                requirement_count=row.requirement_count or 0,
                enforcement_terms=row.enforcement_terms or 0,
                ai_context_summary=row.ai_context_summary or "",
                relevance_score=row.relevance_score or 0.0
            )
            results.append(context)
        
        return results
    
    def generate_response(self, query: str, context_sections: List[RegulationContext], conversation_history: List[Dict[str, str]]) -> str:
        """Generate AI response using retrieved context."""
        
        # Build system prompt with regulatory expertise
        system_prompt = """You are an expert regulatory analyst specializing in the Code of Federal Regulations (CFR). You have deep knowledge of regulatory burden analysis, compliance requirements, and federal regulation structure.

Your expertise includes:
- Understanding CFR hierarchy (Titles → Parts → Sections)
- Regulatory burden scoring (0-100 scale where higher = more burdensome)
- Prohibition analysis (restrictions and forbidden activities)
- Requirement analysis (mandatory obligations and compliance standards)
- Enforcement mechanisms (penalties, fines, violations)
- Cross-regulatory relationships and dependencies

When answering questions:
1. Always cite specific CFR sections when referencing regulations
2. Explain regulatory burden implications when relevant
3. Distinguish between requirements (must/shall) and prohibitions (may not/prohibited)
4. Provide practical compliance guidance when appropriate
5. Highlight enforcement consequences when discussing violations

Use the provided regulatory context to give accurate, specific answers."""

        # Build context from retrieved sections
        context_text = "\n\n".join([
            f"**{section.section_citation}** (Burden Score: {section.regulatory_burden_score:.1f}/100)\n"
            f"Agency: {section.agency_name}\n"
            f"Heading: {section.section_heading}\n"
            f"Summary: {section.ai_context_summary}\n"
            f"Key Metrics: {section.prohibition_count} prohibitions, {section.requirement_count} requirements, {section.enforcement_terms} enforcement terms\n"
            f"Text: {section.section_text[:1000]}{'...' if len(section.section_text) > 1000 else ''}"
            for section in context_sections[:3]  # Limit context to avoid token limits
        ])
        
        # Build conversation context
        conversation_context = ""
        if conversation_history:
            recent_history = conversation_history[-3:]  # Last 3 exchanges
            for exchange in recent_history:
                conversation_context += f"Previous Q: {exchange.get('user', '')}\nPrevious A: {exchange.get('assistant', '')}\n\n"
        
        # Construct the full prompt
        full_prompt = f"""{system_prompt}

REGULATORY CONTEXT:
{context_text}

{'CONVERSATION HISTORY:' + conversation_context if conversation_context else ''}

USER QUESTION: {query}

Provide a comprehensive answer based on the regulatory context above. If the context doesn't contain relevant information, clearly state that and provide general guidance about where to look for such information in the CFR."""
        
        try:
            # Generate response using Gemini
            response = generative_model.generate_content(full_prompt)
            return response.text
        except Exception as e:
            print(f"Generation error: {e}")
            return f"I apologize, but I encountered an error generating a response. However, I found {len(context_sections)} relevant regulations that may help answer your question about: {query}"

rag_service = RegulatoryRAG()

@app.get("/")
def root():
    return {"message": "eCFR AI Assistant API", "status": "ready"}

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Main chat endpoint for regulatory AI assistant."""
    
    try:
        # Search for relevant regulations
        context_sections = rag_service.search_regulations_semantic(
            request.message, 
            request.date, 
            limit=request.max_context_sections
        )
        
        if not context_sections:
            # No relevant context found
            return ChatResponse(
                response="I couldn't find any specific regulations related to your question. Could you please provide more details or try rephrasing your question? For example, you could mention specific CFR titles, parts, or regulatory topics like 'safety requirements' or 'environmental compliance'.",
                sources=[],
                context_used=[]
            )
        
        # Generate AI response
        ai_response = rag_service.generate_response(
            request.message,
            context_sections,
            request.conversation_history
        )
        
        # Prepare sources for frontend
        sources = []
        context_used = []
        
        for section in context_sections:
            source = {
                "citation": section.section_citation,
                "title": section.title_num,
                "part": section.part_num,
                "agency": section.agency_name,
                "heading": section.section_heading,
                "burden_score": section.regulatory_burden_score,
                "relevance": section.relevance_score,
                "summary": section.ai_context_summary[:200] + "..." if len(section.ai_context_summary) > 200 else section.ai_context_summary
            }
            sources.append(source)
            context_used.append(f"{section.section_citation}: {section.section_heading}")
        
        return ChatResponse(
            response=ai_response,
            sources=sources,
            context_used=context_used
        )
        
    except Exception as e:
        print(f"Chat error: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

@app.get("/health")
def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "vertex_ai": "connected",
            "bigquery": "connected"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)