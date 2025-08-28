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
generative_model = GenerativeModel("gemini-1.0-pro")

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
        
    def search_regulations_semantic(self, query: str, date: str, limit: int = 10, conversation_history: List[Dict[str, str]] = None) -> List[RegulationContext]:
        """Search regulations using semantic similarity."""
        
        # For now, use keyword-based search as fallback
        # In production, you'd want to implement vector similarity search
        return self.search_regulations_keyword(query, date, limit, conversation_history)
    
    def search_regulations_keyword(self, query: str, date: str, limit: int = 10, conversation_history: List[Dict[str, str]] = None) -> List[RegulationContext]:
        """Search regulations using advanced keyword matching and semantic concepts."""
        
        # Check if this is a follow-up question
        is_followup = self.detect_followup_question_simple(query)
        previous_citations = self.extract_previous_citations(conversation_history) if conversation_history else []
        
        print(f"Search - Query: {query}, Is followup: {is_followup}, Previous citations: {previous_citations}")
        
        # For follow-up questions, search specifically for the previously mentioned sections
        if is_followup and previous_citations:
            return self.search_specific_citations(previous_citations, date)
        
        # Otherwise, use the original search method
        return self.search_regulations_keyword_original(query, date, limit)
    
    def detect_followup_question_simple(self, query: str) -> bool:
        """Simple follow-up detection for search purposes."""
        query_lower = query.lower()
        return any(indicator in query_lower for indicator in [
            'tell me more', 'more about', 'it', 'that', 'this', 'details', 'summary', 'elaborate'
        ])
    
    def search_specific_citations(self, citations: List[str], date: str) -> List[RegulationContext]:
        """Search for specific CFR citations."""
        results = []
        for citation in citations:
            # Parse citation to extract title and part
            match = re.search(r'(\d+)\s+CFR\s+§\s+([\d.]+)', citation)
            if match:
                title_num = int(match.group(1))
                section_parts = match.group(2).split('.')
                
                sql = f"""
                SELECT 
                    section_citation, title_num, part_num, agency_name, section_heading, 
                    section_text, regulatory_burden_score, prohibition_count, requirement_count,
                    enforcement_terms, ai_context_summary, word_count
                FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
                WHERE version_date = DATE(@date) 
                  AND title_num = @title
                  AND section_citation = @citation
                LIMIT 1
                """
                
                job = self.bq.query(sql, job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("date", "STRING", date),
                        bigquery.ScalarQueryParameter("title", "INT64", title_num),
                        bigquery.ScalarQueryParameter("citation", "STRING", citation)
                    ]
                ))
                
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
                        relevance_score=100.0  # High relevance for exact matches
                    )
                    results.append(context)
        
        return results
    
    def search_regulations_keyword_original(self, query: str, date: str, limit: int = 10, conversation_history: List[Dict[str, str]] = None) -> List[RegulationContext]:
        """Original keyword search method (renamed to avoid conflicts)."""
        
        # Extract key terms from the query
        query_lower = query.lower()
        search_terms = []
        
        # Enhanced regulatory concept detection
        regulatory_concepts = {
            'compliance': ['compliance', 'conform', 'adhere', 'meet', 'satisfy'],
            'safety': ['safety', 'hazard', 'danger', 'risk', 'secure', 'protection'],
            'environment': ['environment', 'pollution', 'emission', 'waste', 'air', 'water', 'soil', 'contamination'],
            'enforcement': ['penalty', 'fine', 'violation', 'enforce', 'sanction', 'punishment', 'citation'],
            'requirement': ['requirement', 'must', 'shall', 'required', 'mandatory', 'obligation'],
            'prohibition': ['prohibition', 'prohibited', 'forbidden', 'banned', 'not permitted', 'shall not'],
            'disclosure': ['disclosure', 'report', 'notify', 'inform', 'submit', 'file'],
            'ethical': ['ethical', 'ethics', 'conduct', 'conflict', 'integrity', 'standards'],
            'financial': ['financial', 'cost', 'fee', 'payment', 'dollar', 'money', 'budget'],
            'management': ['management', 'administration', 'oversight', 'supervision', 'control'],
            'security': ['security', 'classified', 'confidential', 'access', 'clearance'],
            'training': ['training', 'education', 'instruction', 'course', 'program']
        }
        
        # Find matching concepts
        detected_concepts = []
        for concept, terms in regulatory_concepts.items():
            if any(term in query_lower for term in terms):
                detected_concepts.append(concept)
                search_terms.extend(terms[:2])  # Add top 2 terms for each concept
        
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
            # Enhanced keyword-based search with concept detection
            search_conditions = []
            
            # If we detected concepts, search for them
            if detected_concepts:
                for i, concept in enumerate(detected_concepts[:3]):
                    concept_terms = regulatory_concepts[concept][:2]  # Top 2 terms per concept
                    concept_condition = []
                    for j, term in enumerate(concept_terms):
                        param_name = f"concept{i}_{j}"
                        concept_condition.append(f"(LOWER(section_text) LIKE @{param_name} OR LOWER(section_heading) LIKE @{param_name})")
                        params.append(bigquery.ScalarQueryParameter(param_name, "STRING", f"%{term}%"))
                    search_conditions.append(f"({' OR '.join(concept_condition)})")
            
            # Always include direct query terms
            query_words = [word.strip() for word in query_lower.split() if len(word.strip()) > 2][:5]  # Max 5 words
            for i, word in enumerate(query_words):
                param_name = f"word{i}"
                search_conditions.append(f"(LOWER(section_text) LIKE @{param_name} OR LOWER(section_heading) LIKE @{param_name})")
                params.append(bigquery.ScalarQueryParameter(param_name, "STRING", f"%{word}%"))
            
            if search_conditions:
                where_conditions.append(f"({' OR '.join(search_conditions)})")
            else:
                # Ultimate fallback: search entire query
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
            -- Enhanced relevance scoring with multiple factors
            (
                CASE WHEN LOWER(section_heading) LIKE @query_param THEN 15 ELSE 0 END +
                CASE WHEN LOWER(section_text) LIKE @query_param THEN 10 ELSE 0 END +
                CASE WHEN regulatory_burden_score > 50 THEN 5 ELSE 0 END +
                CASE WHEN prohibition_count > 0 THEN 3 ELSE 0 END +
                CASE WHEN requirement_count > 2 THEN 2 ELSE 0 END +
                CASE WHEN enforcement_terms > 0 THEN 4 ELSE 0 END +
                CASE WHEN word_count > 100 THEN 2 ELSE 1 END
            ) as relevance_score
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
        
        # Fallback intelligent response system when Vertex AI is unavailable
        if not context_sections:
            return "I couldn't find any specific regulations related to your question. Could you please provide more details or try rephrasing your question? For example, you could mention specific CFR titles, parts, or regulatory topics."
        
        try:
            # Generate response using Gemini
            print(f"Attempting Vertex AI generation with {len(context_sections)} context sections...")
            
            # Build system prompt
            system_prompt = """You are an expert regulatory analyst specializing in the Code of Federal Regulations (CFR). Provide comprehensive analysis with specific citations, burden scores, and practical compliance guidance."""

            # Build context from retrieved sections
            context_text = "\n\n".join([
                f"**{section.section_citation}** (Burden Score: {section.regulatory_burden_score:.1f}/100)\n"
                f"Agency: {section.agency_name}\n"
                f"Heading: {section.section_heading}\n"
                f"Key Metrics: {section.prohibition_count} prohibitions, {section.requirement_count} requirements, {section.enforcement_terms} enforcement terms\n"
                f"Text: {section.section_text[:800]}{'...' if len(section.section_text) > 800 else ''}"
                for section in context_sections[:3]
            ])
            
            full_prompt = f"{system_prompt}\n\nREGULATORY CONTEXT:\n{context_text}\n\nUSER QUESTION: {query}\n\nProvide a comprehensive answer with specific citations and burden analysis."
            
            response = generative_model.generate_content(full_prompt)
            print(f"Vertex AI generation successful")
            return response.text
            
        except Exception as e:
            print(f"Vertex AI generation failed ({type(e).__name__}): {e}")
            # Fallback to intelligent template-based response
            return self.generate_intelligent_fallback_response(query, context_sections, conversation_history)
    
    def generate_intelligent_fallback_response(self, query: str, context_sections: List[RegulationContext], conversation_history: List[Dict[str, str]] = None) -> str:
        """Generate intelligent responses without external AI using templates and analysis."""
        
        # Analyze the query to determine response type
        query_lower = query.lower()
        
        # Check if this is a follow-up question by analyzing conversation history
        is_followup = self.detect_followup_question(query, conversation_history)
        previous_citations = self.extract_previous_citations(conversation_history) if conversation_history else []
        
        print(f"Query: {query}")
        print(f"Is followup: {is_followup}")
        print(f"Previous citations: {previous_citations}")
        print(f"Available sections: {[s.section_citation for s in context_sections]}")
        
        # Sort sections by relevance and burden score
        sorted_sections = sorted(context_sections, key=lambda x: (x.relevance_score, x.regulatory_burden_score), reverse=True)
        
        # If it's a follow-up and we have previous citations, prioritize those sections
        if is_followup and previous_citations:
            # Filter to sections matching previous citations first
            matching_sections = [s for s in sorted_sections if s.section_citation in previous_citations]
            print(f"Matching sections for followup: {[s.section_citation for s in matching_sections]}")
            if matching_sections:
                sorted_sections = matching_sections + [s for s in sorted_sections if s.section_citation not in previous_citations]
        
        top_section = sorted_sections[0] if sorted_sections else None
        
        if not top_section:
            return "I couldn't find any specific regulations related to your question."
        
        # Handle different types of queries
        if self.is_summary_request(query):
            return self.generate_summary_response(top_section, query, is_followup)
        elif self.is_detail_request(query):
            return self.generate_detailed_response(top_section, query, is_followup)
        else:
            return self.generate_standard_response(query, sorted_sections, is_followup)
    
    def detect_followup_question(self, query: str, conversation_history: List[Dict[str, str]]) -> bool:
        """Detect if this is a follow-up question to a previous query."""
        if not conversation_history:
            return False
        
        query_lower = query.lower()
        followup_indicators = [
            'tell me more', 'more about', 'explain that', 'what about', 'how about',
            'details', 'summary', 'elaborate', 'clarify', 'it', 'that', 'this',
            'what does it', 'how does it', 'why does it', 'when does it'
        ]
        
        return any(indicator in query_lower for indicator in followup_indicators)
    
    def extract_previous_citations(self, conversation_history: List[Dict[str, str]]) -> List[str]:
        """Extract CFR citations from previous conversation."""
        citations = []
        if not conversation_history:
            return citations
        
        # Look at the last assistant response
        last_response = conversation_history[-1].get('assistant', '') if conversation_history else ''
        
        # Look for the first/primary citation mentioned (usually the main topic)
        cfr_pattern = r'(\d+\s+CFR\s+§\s+[\d.]+[A-Za-z]*(?:-[\d.]+[A-Za-z]*)?)'
        matches = re.findall(cfr_pattern, last_response)
        
        if matches:
            # Take the first citation as the primary one
            citations.append(matches[0])
        
        return citations
    
    def is_summary_request(self, query: str) -> bool:
        """Check if user is asking for a summary."""
        query_lower = query.lower()
        summary_keywords = ['summary', 'summarize', 'simple', 'brief', 'short', 'one line', 'tldr', 'essence', 'gist']
        return any(keyword in query_lower for keyword in summary_keywords)
    
    def is_detail_request(self, query: str) -> bool:
        """Check if user is asking for more details."""
        query_lower = query.lower()
        detail_keywords = ['more', 'detail', 'elaborate', 'explain', 'tell me about', 'describe', 'how', 'why', 'what']
        return any(keyword in query_lower for keyword in detail_keywords)
    
    def generate_summary_response(self, section: RegulationContext, query: str, is_followup: bool) -> str:
        """Generate a concise summary response."""
        burden_level = self.get_burden_level(section.regulatory_burden_score)
        
        if is_followup:
            # For follow-ups, provide a direct summary
            summary_parts = [
                f"**{section.section_citation}** establishes {section.section_heading.lower().replace('§ ' + section.section_citation.split('§ ')[1], '').strip()}",
                f"Burden: {section.regulatory_burden_score:.1f}/100 ({burden_level})"
            ]
            
            if section.requirement_count > 0 or section.prohibition_count > 0:
                metrics = []
                if section.requirement_count > 0:
                    metrics.append(f"{section.requirement_count} requirements")
                if section.prohibition_count > 0:
                    metrics.append(f"{section.prohibition_count} prohibitions")
                summary_parts.append(f"Contains {', '.join(metrics)}")
        else:
            summary_parts = [
                f"**{section.section_citation}** - {section.section_heading}",
                f"This is a {burden_level.lower()} regulation with {section.requirement_count} requirements and {section.enforcement_terms} enforcement mechanisms."
            ]
        
        return " • ".join(summary_parts)
    
    def generate_detailed_response(self, section: RegulationContext, query: str, is_followup: bool) -> str:
        """Generate a detailed response with comprehensive information."""
        burden_level = self.get_burden_level(section.regulatory_burden_score)
        
        response_parts = []
        
        if is_followup:
            response_parts.append(f"Here are the details about **{section.section_citation}**:")
        else:
            response_parts.append(f"**{section.section_citation}** ({section.agency_name}) - **{section.section_heading}**")
        
        # Regulatory analysis
        response_parts.append(f"**Regulatory Analysis:** {section.regulatory_burden_score:.1f}/100 ({burden_level})")
        
        # Detailed breakdown
        if section.requirement_count > 0 or section.prohibition_count > 0 or section.enforcement_terms > 0:
            breakdown = []
            if section.requirement_count > 0:
                breakdown.append(f"**{section.requirement_count} requirements** - mandatory compliance obligations")
            if section.prohibition_count > 0:
                breakdown.append(f"**{section.prohibition_count} prohibitions** - restricted or forbidden activities")
            if section.enforcement_terms > 0:
                breakdown.append(f"**{section.enforcement_terms} enforcement mechanisms** - penalties or sanctions")
            
            response_parts.append("**Components:**\n" + "\n".join([f"• {item}" for item in breakdown]))
        
        # Include summary if available and substantial
        if section.ai_context_summary and len(section.ai_context_summary) > 50:
            response_parts.append(f"**Context:** {section.ai_context_summary}")
        
        # Compliance guidance
        if section.regulatory_burden_score > 50:
            response_parts.append("⚠️ **High Burden:** This regulation requires significant compliance effort. Consider professional consultation.")
        
        return "\n\n".join(response_parts)
    
    def generate_standard_response(self, query: str, sorted_sections: List[RegulationContext], is_followup: bool) -> str:
        """Generate a standard response for general queries."""
        top_section = sorted_sections[0]
        burden_level = self.get_burden_level(top_section.regulatory_burden_score)
        
        response_parts = []
        
        if is_followup:
            response_parts.append(f"Regarding **{top_section.section_citation}** - **{top_section.section_heading}**")
        else:
            response_parts.append(f"I found **{top_section.section_citation}** ({top_section.agency_name}) - **{top_section.section_heading}**")
        
        # Burden and metrics
        if top_section.regulatory_burden_score > 0:
            response_parts.append(f"**Regulatory Burden:** {top_section.regulatory_burden_score:.1f}/100 ({burden_level})")
        
        # Key components
        metrics = []
        if top_section.requirement_count > 0:
            metrics.append(f"{top_section.requirement_count} requirements")
        if top_section.prohibition_count > 0:
            metrics.append(f"{top_section.prohibition_count} prohibitions")
        if top_section.enforcement_terms > 0:
            metrics.append(f"{top_section.enforcement_terms} enforcement terms")
        
        if metrics:
            response_parts.append(f"**Key Components:** {', '.join(metrics)}")
        
        # Context-aware analysis
        query_lower = query.lower()
        if any(term in query_lower for term in ['requirement', 'must', 'shall', 'required']):
            if top_section.requirement_count > 0:
                response_parts.append(f"This regulation contains **{top_section.requirement_count} specific requirements** for compliance.")
        
        if any(term in query_lower for term in ['penalty', 'fine', 'violation', 'enforcement']):
            if top_section.enforcement_terms > 0:
                response_parts.append(f"**Enforcement:** Contains **{top_section.enforcement_terms} enforcement mechanisms** with potential penalties.")
        
        # Related sections (only for non-follow-up questions)
        if not is_followup and len(sorted_sections) > 1:
            other_sections = sorted_sections[1:min(3, len(sorted_sections))]
            other_refs = [f"{s.section_citation} (Burden: {s.regulatory_burden_score:.1f})" for s in other_sections]
            response_parts.append(f"**Related:** {', '.join(other_refs)}")
        
        return "\n\n".join(response_parts)
    
    def get_burden_level(self, score: float) -> str:
        """Determine regulatory burden level."""
        if score <= 25: return "Low Risk"
        elif score <= 50: return "Moderate Risk"
        elif score <= 75: return "High Risk"
        else: return "Critical Risk"

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
            limit=request.max_context_sections,
            conversation_history=request.conversation_history
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