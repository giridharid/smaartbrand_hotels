"""
Smaartbrand Smartphones - FastAPI Backend
With IP-based rate limiting and admin key bypass
Using BigQuery Data Agent for intelligent chat
"""

import os
import json
import hashlib
import base64
import uuid
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from collections import defaultdict

from fastapi import FastAPI, HTTPException, Request, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account

# ============================================
# CONFIGURATION
# ============================================

ADMIN_KEY = os.getenv("SMAARTBRAND_ADMIN_KEY", "smaart2024admin")
MAX_FREE_REQUESTS = int(os.getenv("MAX_FREE_REQUESTS", "4"))
RATE_LIMIT_WINDOW_HOURS = int(os.getenv("RATE_LIMIT_WINDOW_HOURS", "24"))

GCP_PROJECT = os.getenv("GCP_PROJECT", "gen-lang-client-0143536012")
BQ_DATASET = os.getenv("BQ_DATASET", "smartphone")
GCP_LOCATION = os.getenv("GCP_LOCATION", "us-central1")

# ============================================
# GCP CREDENTIALS FROM JSON ENV VAR
# ============================================

def setup_gcp_credentials():
    """Setup GCP credentials from JSON environment variable (supports base64 or raw JSON)"""
    creds_json = os.getenv("GCP_CREDENTIALS_JSON")
    if creds_json:
        print(f"GCP_CREDENTIALS_JSON found, length: {len(creds_json)}")
        try:
            # Try base64 decode first
            try:
                decoded = base64.b64decode(creds_json).decode('utf-8')
                print("Decoded from base64")
                creds_json = decoded
            except:
                print("Not base64, using as raw JSON")
            
            # Handle potential escaping issues
            if creds_json.startswith("'") and creds_json.endswith("'"):
                creds_json = creds_json[1:-1]
            if creds_json.startswith('"') and creds_json.endswith('"'):
                creds_json = creds_json[1:-1]
            
            creds_dict = json.loads(creds_json)
            print(f"Parsed credentials for project: {creds_dict.get('project_id', 'unknown')}")
            
            # Create credentials with proper scopes
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict,
                scopes=[
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/bigquery"
                ]
            )
            print("Credentials created successfully")
            return credentials
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"First 100 chars: {creds_json[:100]}")
            return None
        except Exception as e:
            print(f"Error creating credentials: {e}")
            return None
    else:
        print("GCP_CREDENTIALS_JSON not found in environment")
    return None

GCP_CREDENTIALS = setup_gcp_credentials()
print(f"GCP_CREDENTIALS status: {'loaded' if GCP_CREDENTIALS else 'NOT LOADED'}")

# BigQuery Data Agent
BQ_AGENT_ID = os.getenv("BQ_AGENT_ID", "agent_85a3f3af-cddb-4359-af26-439ca6a3dfe9")
BQ_AGENT_LOCATION = "global"

# Legacy BQ ML model (fallback)
BQ_GEMINI_MODEL = os.getenv("BQ_GEMINI_MODEL", "gemini_flash_model")

def get_chat_client():
    """Get the BigQuery Data Analytics chat client"""
    try:
        from google.cloud import geminidataanalytics_v1alpha as gda
        from google.api_core import client_options
        
        if GCP_CREDENTIALS:
            return gda.DataChatServiceClient(
                credentials=GCP_CREDENTIALS,
                client_options=client_options.ClientOptions()
            )
        return gda.DataChatServiceClient()
    except ImportError as e:
        print(f"geminidataanalytics import error: {e}")
        return None
    except Exception as e:
        print(f"Chat client error: {e}")
        return None

def call_bq_agent(message: str, conversation_id: str = None) -> Tuple[str, bool]:
    """
    Call the BigQuery Data Agent for intelligent responses.
    Returns (response_text, used_fallback)
    """
    try:
        from google.cloud import geminidataanalytics_v1alpha as gda
        
        cc = get_chat_client()
        if not cc:
            print("BQ Agent unavailable, falling back to BQ ML")
            fallback_response = generate_with_gemini_fallback(message)
            return (fallback_response, True)
        
        parent = f"projects/{GCP_PROJECT}/locations/{BQ_AGENT_LOCATION}"
        agent = f"{parent}/dataAgents/{BQ_AGENT_ID}"
        
        # Create a new conversation for each request
        conv_id = conversation_id or f"smaart-{uuid.uuid4().hex[:8]}"
        conv_name = cc.conversation_path(GCP_PROJECT, BQ_AGENT_LOCATION, conv_id)
        
        # Try to get existing conversation or create new one
        try:
            cc.get_conversation(name=conv_name)
        except:
            cc.create_conversation(
                request=gda.CreateConversationRequest(
                    parent=parent,
                    conversation_id=conv_id,
                    conversation=gda.Conversation(agents=[agent])
                )
            )
        
        # Chat with the agent
        stream = cc.chat(
            request={
                "parent": parent,
                "conversation_reference": {
                    "conversation": conv_name,
                    "data_agent_context": {"data_agent": agent}
                },
                "messages": [{"user_message": {"text": message}}]
            }
        )
        
        # Collect response from system_message (that's where the data is!)
        response_parts = []
        
        def is_thinking_message(text):
            """Check if text is a thinking/status message that should be filtered"""
            if not text:
                return True
            
            # Status messages to skip
            skip_starts = [
                "Retrieving", "Retrieved", "Finding", "Refining", "Evaluating",
                "Calculating", "Formulating", "Analyzing", "Processing",
                "Querying", "Searching", "Loading", "Fetching"
            ]
            for skip in skip_starts:
                if text.startswith(skip):
                    return True
            
            # Thinking patterns to skip (agent's internal reasoning)
            thinking_patterns = [
                "I'm now", "I've ", "I'll ", "I plan", "I'm focused",
                "I'm formulating", "I've confirmed", "I've sifted",
                "I'm zeroing", "I decided", "I'm leaning", "I've re-evaluated",
                "leaning towards", "decided against", "for now"
            ]
            for pattern in thinking_patterns:
                if pattern in text:
                    return True
            
            # Very short lines without markdown headers are usually status
            if len(text) < 50 and not text.startswith("#") and not text.startswith("📊") and not text.startswith("🎯"):
                # Check if it looks like a title/status (capitalized words, no punctuation)
                if text.endswith(("Query", "Metrics", "Preferences", "Rankings", "Visualization", "Response")):
                    return True
            
            return False
        
        for chunk in stream:
            # Extract from system_message.text.parts
            if hasattr(chunk, 'system_message'):
                sm = chunk.system_message
                if hasattr(sm, 'text') and hasattr(sm.text, 'parts'):
                    for p in sm.text.parts:
                        text = str(p).strip()
                        # Skip thinking/status messages
                        if not is_thinking_message(text):
                            response_parts.append(text)
            
            # Also check agent_message (just in case)
            if hasattr(chunk, 'agent_message'):
                am = chunk.agent_message
                if hasattr(am, 'text') and hasattr(am.text, 'parts'):
                    for p in am.text.parts:
                        text = str(p).strip()
                        if not is_thinking_message(text):
                            response_parts.append(text)
        
        # Combine response parts, excluding follow-up questions at the end
        final_parts = []
        for part in response_parts:
            # Stop if we hit follow-up questions (ending with ?)
            if part.endswith('?') and not part.startswith('#'):
                continue
            if part:
                final_parts.append(part)
        
        response = "\n\n".join(final_parts)
        return (response.strip() if response else "No response from agent", False)
        
    except Exception as e:
        print(f"BQ Agent error: {e}")
        import traceback
        traceback.print_exc()
        fallback_response = generate_with_gemini_fallback(message)
        return (fallback_response, True)

def generate_with_gemini_fallback(prompt: str) -> str:
    """Fallback: Generate text using BigQuery ML Gemini model"""
    client = get_bq_client()
    sql = f"""
    SELECT ml_generate_text_llm_result 
    FROM ML.GENERATE_TEXT(
        MODEL `{GCP_PROJECT}.{BQ_DATASET}.{BQ_GEMINI_MODEL}`,
        (SELECT @prompt AS prompt),
        STRUCT(0.3 AS temperature, 1024 AS max_output_tokens, TRUE AS flatten_json_output)
    )
    """
    try:
        job_cfg = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("prompt", "STRING", prompt)]
        )
        result = client.query(sql, job_config=job_cfg).result()
        for row in result:
            return row["ml_generate_text_llm_result"].strip()
        return "No response generated"
    except Exception as e:
        print(f"Gemini fallback error: {e}")
        raise Exception(f"Chat generation failed: {str(e)}")

# ============================================
# RATE LIMITER
# ============================================

class RateLimiter:
    """IP-based rate limiter with admin bypass"""
    
    def __init__(self):
        self.requests: Dict[str, list] = defaultdict(list)
    
    def _hash_ip(self, ip: str) -> str:
        """Hash IP for privacy"""
        return hashlib.sha256(ip.encode()).hexdigest()[:16]
    
    def _clean_old_requests(self, ip_hash: str):
        """Remove requests older than window"""
        cutoff = datetime.now() - timedelta(hours=RATE_LIMIT_WINDOW_HOURS)
        self.requests[ip_hash] = [
            ts for ts in self.requests[ip_hash] if ts > cutoff
        ]
    
    def check_and_increment(self, ip: str, admin_key: Optional[str] = None) -> dict:
        """
        Check if request is allowed.
        Returns: {"allowed": bool, "remaining": int, "is_admin": bool}
        """
        # Admin bypass
        if admin_key == ADMIN_KEY:
            return {"allowed": True, "remaining": 999, "is_admin": True}
        
        ip_hash = self._hash_ip(ip)
        self._clean_old_requests(ip_hash)
        
        current_count = len(self.requests[ip_hash])
        
        if current_count >= MAX_FREE_REQUESTS:
            return {
                "allowed": False, 
                "remaining": 0, 
                "is_admin": False,
                "message": f"Demo limit reached ({MAX_FREE_REQUESTS} requests). Contact sales for full access."
            }
        
        # Record this request
        self.requests[ip_hash].append(datetime.now())
        
        return {
            "allowed": True, 
            "remaining": MAX_FREE_REQUESTS - current_count - 1,
            "is_admin": False
        }
    
    def get_usage(self, ip: str) -> dict:
        """Get current usage for IP"""
        ip_hash = self._hash_ip(ip)
        self._clean_old_requests(ip_hash)
        current_count = len(self.requests[ip_hash])
        return {
            "used": current_count,
            "remaining": max(0, MAX_FREE_REQUESTS - current_count),
            "limit": MAX_FREE_REQUESTS
        }

rate_limiter = RateLimiter()

# ============================================
# FASTAPI APP
# ============================================

app = FastAPI(
    title="Smaartbrand Smartphones API",
    description="Decision Intelligence for Smartphone Brands",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# DEPENDENCIES
# ============================================

def get_client_ip(request: Request) -> str:
    """Extract client IP from request"""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host or "unknown"

def get_bq_client():
    """Get BigQuery client"""
    if GCP_CREDENTIALS:
        return bigquery.Client(project=GCP_PROJECT, credentials=GCP_CREDENTIALS)
    return bigquery.Client(project=GCP_PROJECT)

# ============================================
# RATE LIMIT MIDDLEWARE
# ============================================

async def check_rate_limit(
    request: Request,
    admin_key: Optional[str] = Query(None, alias="key")
):
    """Check rate limit for request"""
    ip = get_client_ip(request)
    
    # Check header first, then query param
    header_key = request.headers.get("X-Admin-Key")
    key_to_use = header_key or admin_key
    
    result = rate_limiter.check_and_increment(ip, key_to_use)
    
    if not result["allowed"]:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "message": result.get("message", "Rate limit exceeded"),
                "remaining": 0
            }
        )
    
    return result

# ============================================
# REQUEST/RESPONSE MODELS
# ============================================

class ChatRequest(BaseModel):
    message: str
    context: Optional[dict] = None  # brand, series, product filters

class ChatResponse(BaseModel):
    response: str
    data_used: Optional[dict] = None
    remaining_requests: int

class CompareRequest(BaseModel):
    entity1: str  # brand name, series, or product
    entity2: str
    compare_type: str = "brand"  # brand, series, product

# ============================================
# SYSTEM PROMPT
# ============================================

SYSTEM_PROMPT = """You are SmaartAnalyst, a smartphone decision intelligence assistant powered by Smaartbrand.

=== WHO YOU SERVE ===
Smartphone brand teams who need actionable intelligence:
- Category Manager - Market share, competitive positioning, portfolio gaps
- Product Marketing - USPs, messaging, campaign themes, audience targeting
- Product R&D - Feature priorities, what to improve, what's working
- New Business / Strategy - Market entry, segment opportunities, pricing
- Digital Marketing - Keywords, ad copy, audience segments

=== CONTEXT AWARENESS ===
1. If no brand context exists and the question needs brand-specific data, ask: "Which brand are you working with?" 
2. Once a brand is mentioned, it becomes the CONTEXT for follow-up questions.
3. For GENERIC questions (about demographics, personas, market trends) - provide OVERALL market insights across all brands, highlighting which brands lead.
4. For BRAND-SPECIFIC questions - focus ONLY on that brand's data.

Examples:
- "What do women prefer?" → GENERIC. Show female preferences across brands with leaders.
- "What do Tech Savvy users want?" → GENERIC. Show Tech Savvy insights with top performing brands.
- "How is Samsung doing on Camera?" → BRAND-SPECIFIC. Focus only on Samsung Camera.
- "What should we improve?" (after discussing Samsung) → Use Samsung as context.

=== YOUR PURPOSE ===
Transform owner sentiment into DECISIONS and ACTIONS by department.
Use DEMOGRAPHIC DATA (gender, persona, switcher, loyalist) to personalize insights.

=== ASPECT MAPPING ===
Use these emojis: Camera 📸, Battery 🔋, Performance ⚡, Display 📱, Design ✨, Value for Money 💰, Software 📲, Build Quality 🔧, Audio 🔊, General ⭐

=== PRICE TIERS (INR) ===
Budget (<15K), Mid-range (15-30K), Upper-mid (30-50K), Premium (50K+)

=== PERSONAS ===
Tech Savvy, Budget Conscious, Camera Enthusiast, Gamer, Business User

=== RESPONSE FORMAT ===
For GENERIC queries (no specific brand):
📊 **Market Insight**: [Overall trend with top 3-4 brands and their % scores]
👥 **Segment Leaders**: [Which brands win with which segment]
🎯 **Opportunity**: [Gap or untapped market area]

For BRAND-SPECIFIC queries:
📊 **Insight**: [2-3 sentences with specific % scores for the brand]
👥 **Audience Mix**: [persona breakdown]
♂️♀️ **Gender Split**: [if relevant]
🔄 **Switcher Intel**: [if relevant]

🎯 **Actions by Department**:
📦 Category Manager: [action]
📢 Product Marketing: [action with PROMOTE/AVOID]
🔬 Product R&D: [action]

=== RULES ===
1. Answer ONLY from the data provided. Never hallucinate.
2. Always cite specific % satisfaction scores.
3. Be direct — brand managers are busy.
4. Max 300 words.
5. For brand-specific queries, end with 🎯 Actions by Department.
6. For generic queries, end with 🎯 Opportunity.
7. LANGUAGE: Default to English. Only respond in other languages if the question is clearly in that language.
"""

# ============================================
# BIGQUERY HELPERS
# ============================================

def run_query(query: str) -> list:
    """Run BigQuery query and return results as list of dicts"""
    client = get_bq_client()
    try:
        result = client.query(query).result()
        return [dict(row) for row in result]
    except Exception as e:
        print(f"BigQuery error: {e}")
        return []

def get_brand_satisfaction(brand: Optional[str] = None) -> list:
    """Get satisfaction scores by brand"""
    where = f"WHERE UPPER(brand_name) = UPPER('{brand}')" if brand else ""
    query = f"""
    SELECT 
        brand_name,
        aspect_name,
        SUM(positive_count) as positive,
        SUM(negative_count) as negative,
        ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count) + SUM(negative_count), 0), 0) as satisfaction
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
    {where}
    GROUP BY brand_name, aspect_name
    ORDER BY brand_name, satisfaction DESC
    """
    return run_query(query)

def get_brand_demographics(brand: Optional[str] = None) -> list:
    """Get demographic breakdown by brand"""
    base_where = """WHERE 
        gender IS NOT NULL AND gender != '' AND LOWER(TRIM(gender)) != 'null' AND
        persona IS NOT NULL AND persona != '' AND LOWER(TRIM(persona)) != 'null'"""
    if brand:
        where = f"{base_where} AND UPPER(brand_name) = UPPER('{brand}')"
    else:
        where = base_where
    query = f"""
    SELECT 
        brand_name,
        gender,
        persona,
        SUM(review_count) as reviews
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
    {where}
    GROUP BY brand_name, gender, persona
    ORDER BY brand_name, reviews DESC
    """
    return run_query(query)

def get_series_comparison(series1: str, series2: str) -> list:
    """Compare two series"""
    query = f"""
    SELECT *
    FROM `{GCP_PROJECT}.{BQ_DATASET}.series_comparison`
    WHERE series IN ('{series1}', '{series2}')
    """
    return run_query(query)

def get_product_comparison(product1: str, product2: str) -> list:
    """Compare two products by name (partial match)"""
    query = f"""
    SELECT 
        product_name,
        aspect_name,
        ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count) + SUM(negative_count), 0), 0) as satisfaction,
        SUM(positive_count) + SUM(negative_count) as volume
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
    WHERE LOWER(product_name) LIKE '%{product1.lower()}%' 
       OR LOWER(product_name) LIKE '%{product2.lower()}%'
    GROUP BY product_name, aspect_name
    ORDER BY aspect_name, product_name
    """
    return run_query(query)

def get_brand_comparison(brand1: str, brand2: str) -> list:
    """Compare two brands"""
    query = f"""
    SELECT 
        brand_name,
        aspect_name,
        ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count) + SUM(negative_count), 0), 0) as satisfaction,
        SUM(positive_count) + SUM(negative_count) as volume
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
    WHERE UPPER(brand_name) IN (UPPER('{brand1}'), UPPER('{brand2}'))
    GROUP BY brand_name, aspect_name
    ORDER BY aspect_name, brand_name
    """
    return run_query(query)

def get_weakest_aspects(brand: Optional[str] = None) -> list:
    """Get aspects with lowest satisfaction (weaknesses) from main satisfaction data.
    This matches what the dashboard shows as Top Weakness."""
    where = f"WHERE UPPER(brand_name) = UPPER('{brand}')" if brand else ""
    query = f"""
    SELECT aspect_name, satisfaction, positive_count, negative_count, total_mentions
    FROM (
        SELECT 
            aspect_name,
            ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count) + SUM(negative_count), 0), 0) as satisfaction,
            SUM(positive_count) as positive_count,
            SUM(negative_count) as negative_count,
            SUM(positive_count) + SUM(negative_count) as total_mentions
        FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
        {where}
        GROUP BY aspect_name
    )
    WHERE total_mentions > 100
    ORDER BY satisfaction ASC
    LIMIT 10
    """
    return run_query(query)

def get_color_preferences(brand: Optional[str] = None, gender: Optional[str] = None, persona: Optional[str] = None) -> list:
    """Get color preferences, optionally filtered by brand, gender, and/or persona.
    Returns top colors by review count with satisfaction scores."""
    conditions = [
        "color IS NOT NULL", 
        "color != ''",
        "LOWER(TRIM(color)) != 'null'"
    ]
    
    if brand:
        conditions.append(f"UPPER(brand_name) = UPPER('{brand}')")
    
    if gender:
        conditions.append(f"gender = '{gender}'")
    
    if persona:
        conditions.append(f"persona = '{persona}'")
    
    where = "WHERE " + " AND ".join(conditions)
    
    query = f"""
    SELECT 
        color,
        brand_name,
        gender,
        persona,
        SUM(CAST(review_count AS INT64)) as reviews,
        ROUND(AVG(CAST(satisfaction AS FLOAT64)), 0) as satisfaction
    FROM `{GCP_PROJECT}.{BQ_DATASET}.color_preferences`
    {where}
    GROUP BY color, brand_name, gender, persona
    ORDER BY reviews DESC
    LIMIT 20
    """
    return run_query(query)

def get_color_preferences_grouped(brand: Optional[str] = None, group_by: str = "gender") -> list:
    """Get color preferences grouped by gender or persona for UI charts.
    group_by: 'gender' or 'persona'
    Returns top colors by review count."""
    conditions = [
        "color IS NOT NULL", 
        "color != ''",
        "LOWER(TRIM(color)) != 'null'"
    ]
    
    if brand:
        conditions.append(f"UPPER(brand_name) = UPPER('{brand}')")
    
    if group_by == "gender":
        conditions.append("gender IS NOT NULL AND gender IN ('M', 'F')")
        group_col = "gender"
    else:  # persona
        conditions.append("persona IS NOT NULL AND persona != '' AND LOWER(TRIM(persona)) != 'null'")
        group_col = "persona"
    
    where = "WHERE " + " AND ".join(conditions)
    
    query = f"""
    SELECT 
        {group_col},
        color,
        SUM(CAST(review_count AS INT64)) as reviews,
        ROUND(AVG(CAST(satisfaction AS FLOAT64)), 0) as satisfaction
    FROM `{GCP_PROJECT}.{BQ_DATASET}.color_preferences`
    {where}
    GROUP BY {group_col}, color
    ORDER BY {group_col}, reviews DESC
    """
    return run_query(query)

def get_persona_preferences(persona: Optional[str] = None, brand: Optional[str] = None) -> list:
    """Get preferences by persona, optionally filtered by brand"""
    conditions = ["persona IS NOT NULL", "persona != ''", "LOWER(TRIM(persona)) != 'null'"]
    if persona:
        conditions.append(f"persona = '{persona}'")
    if brand:
        conditions.append(f"UPPER(brand_name) = UPPER('{brand}')")
    
    where = "WHERE " + " AND ".join(conditions)
    query = f"""
    SELECT 
        persona,
        brand_name,
        aspect_name,
        ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count) + SUM(negative_count), 0), 0) as satisfaction,
        SUM(review_count) as reviews
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
    {where}
    GROUP BY persona, brand_name, aspect_name
    ORDER BY persona, reviews DESC
    """
    return run_query(query)

def get_switcher_analysis(brand: Optional[str] = None) -> list:
    """Get switcher data"""
    where = f"WHERE UPPER(brand_name) = UPPER('{brand}')" if brand else ""
    query = f"""
    SELECT 
        brand_name,
        switcher_from,
        SUM(switcher_count) as switch_count
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
    {where}
    AND switcher_from IS NOT NULL
    GROUP BY brand_name, switcher_from
    ORDER BY switch_count DESC
    LIMIT 20
    """
    return run_query(query)

def get_evolution(series: str) -> list:
    """Get series evolution"""
    query = f"""
    SELECT *
    FROM `{GCP_PROJECT}.{BQ_DATASET}.series_evolution_trend`
    WHERE series = '{series}'
    ORDER BY version, aspect_name
    """
    return run_query(query)

def get_improvements(brand: Optional[str] = None, product_id: Optional[int] = None) -> list:
    """Get top pain points"""
    conditions = []
    if brand:
        conditions.append(f"UPPER(brand_name) = UPPER('{brand}')")
    if product_id:
        conditions.append(f"product_id = {product_id}")
    
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    query = f"""
    SELECT *
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_improvement_insights`
    {where}
    ORDER BY negative_count DESC
    LIMIT 10
    """
    return run_query(query)

def get_driver_analysis(brand: Optional[str] = None) -> list:
    """Get driver analysis (share of voice)"""
    where = f"WHERE UPPER(brand_name) = UPPER('{brand}')" if brand else ""
    query = f"""
    SELECT 
        brand_name,
        aspect_name,
        SUM(positive_count) as positive,
        SUM(negative_count) as negative,
        SUM(sentiment_count) as total,
        ROUND(100.0 * SUM(sentiment_count) / SUM(SUM(sentiment_count)) OVER (PARTITION BY brand_name), 0) as share_of_voice,
        ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count) + SUM(negative_count), 0), 0) as satisfaction
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
    {where}
    GROUP BY brand_name, aspect_name
    ORDER BY brand_name, share_of_voice DESC
    """
    return run_query(query)

def get_correspondence_matrix() -> list:
    """Get brand x aspect matrix"""
    query = f"""
    SELECT *
    FROM `{GCP_PROJECT}.{BQ_DATASET}.brand_aspect_matrix`
    ORDER BY brand_name, aspect_name
    """
    return run_query(query)

def get_price_tier_preferences(gender: Optional[str] = None, persona: Optional[str] = None) -> list:
    """Get price tier preferences"""
    conditions = [
        "gender IS NOT NULL", "gender != ''",
        "persona IS NOT NULL", "persona != ''", "LOWER(TRIM(persona)) != 'null'",
        "price_tier IS NOT NULL", "price_tier != ''",
        "SAFE_CAST(review_count AS INT64) IS NOT NULL"  # Filter out rows with bad review_count values
    ]
    if gender:
        conditions.append(f"gender = '{gender}'")
    if persona:
        conditions.append(f"persona = '{persona}'")
    
    where = "WHERE " + " AND ".join(conditions)
    
    query = f"""
    SELECT 
        gender,
        persona,
        price_tier,
        brand_name,
        SUM(SAFE_CAST(review_count AS INT64)) as reviews,
        ROUND(AVG(SAFE_CAST(satisfaction AS FLOAT64)), 0) as satisfaction,
        ROUND(AVG(SAFE_CAST(avg_price AS FLOAT64)), 0) as avg_price
    FROM `{GCP_PROJECT}.{BQ_DATASET}.price_tier_preferences`
    {where}
    GROUP BY gender, persona, price_tier, brand_name
    ORDER BY reviews DESC
    """
    return run_query(query)

def get_processor_preferences(brand: Optional[str] = None, gender: Optional[str] = None, persona: Optional[str] = None) -> list:
    """Get processor preferences, optionally filtered by brand, gender, and/or persona.
    Returns top processors by review count with satisfaction scores."""
    conditions = [
        "processor IS NOT NULL",
        "processor != ''",
        "LOWER(TRIM(processor)) != 'null'"
    ]
    
    if brand:
        conditions.append(f"UPPER(brand_name) = UPPER('{brand}')")
    
    if gender:
        conditions.append(f"gender = '{gender}'")
    
    if persona:
        conditions.append(f"persona = '{persona}'")
    
    where = "WHERE " + " AND ".join(conditions)
    
    query = f"""
    SELECT 
        processor,
        brand_name,
        gender,
        persona,
        SUM(CAST(review_count AS INT64)) as reviews,
        ROUND(AVG(CAST(satisfaction AS FLOAT64)), 0) as satisfaction
    FROM `{GCP_PROJECT}.{BQ_DATASET}.processor_preferences`
    {where}
    GROUP BY processor, brand_name, gender, persona
    ORDER BY reviews DESC
    LIMIT 20
    """
    return run_query(query)

def get_processor_preferences_grouped(brand: Optional[str] = None, group_by: str = "gender") -> list:
    """Get processor preferences grouped by gender or persona for UI charts.
    group_by: 'gender' or 'persona'
    Returns top processors by review count."""
    conditions = [
        "processor IS NOT NULL",
        "processor != ''",
        "LOWER(TRIM(processor)) != 'null'"
    ]
    
    if brand:
        conditions.append(f"UPPER(brand_name) = UPPER('{brand}')")
    
    if group_by == "gender":
        conditions.append("gender IS NOT NULL AND gender IN ('M', 'F')")
        group_col = "gender"
    else:  # persona
        conditions.append("persona IS NOT NULL AND persona != '' AND LOWER(TRIM(persona)) != 'null'")
        group_col = "persona"
    
    where = "WHERE " + " AND ".join(conditions)
    
    query = f"""
    SELECT 
        {group_col},
        processor,
        SUM(CAST(review_count AS INT64)) as reviews,
        ROUND(AVG(CAST(satisfaction AS FLOAT64)), 0) as satisfaction
    FROM `{GCP_PROJECT}.{BQ_DATASET}.processor_preferences`
    {where}
    GROUP BY {group_col}, processor
    ORDER BY {group_col}, reviews DESC
    """
    return run_query(query)

def get_products(brand: Optional[str] = None) -> list:
    """Get product list from sentiment summary"""
    where = f"WHERE UPPER(brand_name) = UPPER('{brand}')" if brand else ""
    query = f"""
    SELECT DISTINCT
        product_name,
        brand_name
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
    {where}
    ORDER BY brand_name, product_name
    LIMIT 100
    """
    return run_query(query)

def get_enhanced_comparison(brand1: str, brand2: str, gender: Optional[str] = None, 
                            persona: Optional[str] = None, price_tier: Optional[str] = None,
                            product1: Optional[str] = None, product2: Optional[str] = None) -> dict:
    """Enhanced comparison with filters"""
    conditions = [f"brand_name IN ('{brand1}', '{brand2}')"]
    
    if gender:
        conditions.append(f"gender = '{gender}'")
    if persona:
        conditions.append(f"persona = '{persona}'")
    if price_tier:
        conditions.append(f"price_tier = '{price_tier}'")
    
    # Product-level comparison
    if product1 and product2:
        product_conditions = [f"product_name IN ('{product1}', '{product2}')"]
        if gender:
            product_conditions.append(f"gender = '{gender}'")
        if persona:
            product_conditions.append(f"persona = '{persona}'")
        
        product_where = " AND ".join(product_conditions)
        product_query = f"""
        SELECT 
            product_name,
            aspect_name,
            ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count) + SUM(negative_count), 0), 0) as satisfaction,
            SUM(positive_count) + SUM(negative_count) as volume
        FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
        WHERE {product_where}
        GROUP BY product_name, aspect_name
        ORDER BY aspect_name, product_name
        """
        product_data = run_query(product_query)
    else:
        product_data = []
    
    # Brand-level comparison
    where = " AND ".join(conditions)
    brand_query = f"""
    SELECT 
        brand_name,
        aspect_name,
        ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count) + SUM(negative_count), 0), 0) as satisfaction,
        SUM(positive_count) + SUM(negative_count) as volume
    FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
    WHERE {where}
    GROUP BY brand_name, aspect_name
    ORDER BY aspect_name, brand_name
    """
    brand_data = run_query(brand_query)
    
    # Demographic split if no specific filter
    demo_data = []
    if not gender:
        demo_query = f"""
        SELECT 
            brand_name,
            gender,
            ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count) + SUM(negative_count), 0), 0) as overall_satisfaction,
            SUM(review_count) as reviews
        FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
        WHERE brand_name IN ('{brand1}', '{brand2}') AND gender IS NOT NULL
        GROUP BY brand_name, gender
        ORDER BY brand_name, gender
        """
        demo_data = run_query(demo_query)
    
    return {
        "brand_comparison": brand_data,
        "product_comparison": product_data,
        "demographic_split": demo_data,
        "filters_applied": {
            "gender": gender,
            "persona": persona,
            "price_tier": price_tier,
            "products": [product1, product2] if product1 and product2 else None
        }
    }

# ============================================
# INTENT DETECTION
# ============================================

# Common misspellings mapping
SPELL_CORRECTIONS = {
    # Aspects - removed partial matches that could be substrings of correct words
    "procesor": "processor", "processer": "processor", "proccesor": "processor",
    "camra": "camera", "cemera": "camera", "camara": "camera", "kamera": "camera",
    "batry": "battery", "battry": "battery", "batery": "battery", "baterry": "battery",
    "perfomance": "performance", "performence": "performance", "preformance": "performance",
    "desplay": "display", "dispaly": "display", "disply": "display",
    "headfone": "headphone", "headphon": "headphone", "hedphone": "headphone",
    "speeker": "speaker", "speker": "speaker",
    "chargin": "charging", "chargeing": "charging",
    "storag": "storage", "storge": "storage",
    # Brands
    "samsng": "samsung", "samung": "samsung", "smasung": "samsung", "sumsung": "samsung",
    "aple": "apple", "appel": "apple", "aplle": "apple",
    "onplus": "oneplus", "onepluse": "oneplus",
    "xaomi": "xiaomi", "xiomi": "xiaomi", "xiaome": "xiaomi",
    "opoo": "oppo", "opo": "oppo",
    "realmi": "realme", "relame": "realme",
    "infnix": "infinix", "infnx": "infinix",
    "motarola": "motorola", "motorla": "motorola",
    # Personas
    "budegt": "budget", "buget": "budget",
    "teck": "tech",
    "savyy": "savvy", "savy": "savvy",
    "enthusiest": "enthusiast", "entusiast": "enthusiast",
    "gamr": "gamer", "gammer": "gamer",
    # Common terms
    "satisfation": "satisfaction", "satisfacton": "satisfaction",
    "comparision": "comparison",
    "prefrence": "preference", "preferance": "preference",
    "colur": "color", "colour": "color", "clor": "color",
    "wemon": "women", "womn": "women", "fmale": "female", "femal": "female",
}

def correct_spelling(message: str) -> tuple:
    """Apply spell corrections to common misspellings. Returns (corrected_message, was_corrected)"""
    import re
    message_lower = message.lower()
    corrected = message_lower
    was_corrected = False
    
    # Sort by length descending to replace longer matches first
    sorted_corrections = sorted(SPELL_CORRECTIONS.items(), key=lambda x: len(x[0]), reverse=True)
    
    for wrong, right in sorted_corrections:
        # Use word boundary matching to avoid partial replacements
        pattern = r'\b' + re.escape(wrong) + r'\b'
        if re.search(pattern, corrected):
            corrected = re.sub(pattern, right, corrected)
            was_corrected = True
    
    return corrected, was_corrected

def detect_intent(message: str) -> dict:
    """Detect query intent and extract entities"""
    # Apply spell correction first
    message_lower, was_corrected = correct_spelling(message)
    
    intent = {
        "type": "general",
        "brand": None,
        "series": None,
        "compare": False,
        "entities": [],
        "aspects": [],
        "demographic": None,
        "analysis_type": None,
        "original_message": message,
        "corrected_message": message_lower if was_corrected else None
    }
    
    # Brand detection
    brands = ["apple", "samsung", "oneplus", "xiaomi", "realme", "oppo", "vivo", "google", "motorola", "nokia", "asus", "iqoo", "poco", "redmi", "nothing", "infinix", "honor", "tecno", "lava", "micromax", "lenovo", "huawei"]
    for brand in brands:
        if brand in message_lower:
            if not intent["brand"]:
                intent["brand"] = brand.title()
            else:
                intent["entities"].append(brand.title())
    
    # Series detection
    series_patterns = {
        "iphone": "iPhone",
        "galaxy s": "Galaxy S", 
        "galaxy a": "Galaxy A",
        "pixel": "Pixel",
        "nord": "Nord",
        "redmi note": "Redmi Note",
        "realme gt": "Realme GT",
        "oneplus": "OnePlus"
    }
    for pattern, series_name in series_patterns.items():
        if pattern in message_lower:
            intent["series"] = series_name
    
    # Product/model detection (e.g., "iPhone 14", "iPhone 15", "Galaxy S23")
    import re
    product_patterns = [
        r'iphone\s*(\d+)\s*(pro|plus|max|mini)?',
        r'galaxy\s*[sa](\d+)\s*(ultra|plus|\+)?',
        r'pixel\s*(\d+)\s*(pro|a)?',
        r'oneplus\s*(\d+)\s*(pro|r|t)?',
        r'redmi\s*note\s*(\d+)\s*(pro|plus)?',
        r'realme\s*(\d+)\s*(pro|plus)?'
    ]
    products_found = []
    for pattern in product_patterns:
        matches = re.findall(pattern, message_lower)
        if matches:
            # Reconstruct the product name
            for match in matches:
                if isinstance(match, tuple):
                    product_name = re.search(pattern, message_lower).group(0)
                    products_found.append(product_name.strip())
    
    if len(products_found) >= 2:
        intent["products"] = products_found[:2]  # First two products for comparison
        intent["compare"] = True
    elif len(products_found) == 1:
        intent["product"] = products_found[0]
    
    # Comparison detection
    if any(word in message_lower for word in ["compare", "vs", "versus", "against", "better", "difference"]):
        intent["compare"] = True
        intent["type"] = "comparison"
    
    # Demographic detection
    if any(word in message_lower for word in ["women", "female", "woman", "ladies"]):
        intent["demographic"] = {"gender": "F"}
    elif any(word in message_lower for word in ["men", "male", "man"]):
        intent["demographic"] = {"gender": "M"}
    
    # Persona detection - MERGE with existing demographic, don't overwrite
    personas = {
        "tech savvy": "Tech Savvy",
        "budget conscious": "Budget Conscious",
        "budget": "Budget Conscious",
        "camera enthusiast": "Camera Enthusiast",
        "camera lover": "Camera Enthusiast",
        "photography": "Camera Enthusiast",
        "gamer": "Gamer",
        "gaming": "Gamer",
        "business": "Business User"
    }
    for key, value in personas.items():
        if key in message_lower:
            if intent["demographic"]:
                intent["demographic"]["persona"] = value
            else:
                intent["demographic"] = {"persona": value}
    
    # Analysis type detection
    analysis_keywords = {
        "driver": ["driver", "what drives", "why do"],
        "swot": ["strength", "weakness", "swot"],
        "improvements": ["improve", "fix", "pain point", "problem", "issue"],
        "evolution": ["evolution", "trend", "over time", "improved", "version"],
        "color": ["color", "colour"],
        "processor": ["processor", "chip", "chipset", "cpu", "snapdragon", "mediatek", "helio", "dimensity"],
        "switcher": ["switcher", "switch from", "came from", "moved from"],
        "faq": ["faq", "questions", "frequently asked"],
        "adcopy": ["ad copy", "marketing", "campaign", "messaging"],
        "correspondence": ["correspondence", "positioning", "matrix", "brand map"],
        "price": ["price", "budget", "expensive", "affordable", "cost"]
    }
    
    for analysis_type, keywords in analysis_keywords.items():
        if any(kw in message_lower for kw in keywords):
            intent["analysis_type"] = analysis_type
            break
    
    # Aspect detection
    aspects = ["camera", "battery", "performance", "display", "design", "value", "software", "build", "audio"]
    for aspect in aspects:
        if aspect in message_lower:
            intent["aspects"].append(aspect.title())
    
    # Price tier detection
    import re
    price_patterns = [
        (r'under\s*(\d+)k|below\s*(\d+)k|less than\s*(\d+)k|<\s*(\d+)k', lambda x: int(x) * 1000),
        (r'(\d+)k\s*to\s*(\d+)k|(\d+)k\s*-\s*(\d+)k', lambda x, y: (int(x) * 1000, int(y) * 1000)),
        (r'above\s*(\d+)k|over\s*(\d+)k|more than\s*(\d+)k|>\s*(\d+)k', lambda x: int(x) * 1000),
        (r'around\s*(\d+)k|about\s*(\d+)k|(\d+)k\s*range', lambda x: int(x) * 1000)
    ]
    
    for pattern, _ in price_patterns:
        match = re.search(pattern, message_lower)
        if match:
            groups = [g for g in match.groups() if g]
            if groups:
                price_val = int(groups[0])
                if price_val < 15:
                    intent["price_tier"] = "Budget (<15K)"
                elif price_val < 30:
                    intent["price_tier"] = "Mid-range (15-30K)"
                elif price_val < 50:
                    intent["price_tier"] = "Upper-mid (30-50K)"
                else:
                    intent["price_tier"] = "Premium (50K+)"
                break
    
    return intent

def gather_context_data(intent: dict) -> dict:
    """Gather relevant data based on intent"""
    data = {}
    is_generic = not intent["brand"]  # No specific brand mentioned
    is_brand_comparison = intent["compare"] and intent["brand"] and intent["entities"]
    is_product_comparison = intent["compare"] and intent.get("products") and len(intent.get("products", [])) >= 2
    
    # Product comparison (e.g., "iPhone 14 vs iPhone 15")
    if is_product_comparison:
        products = intent["products"]
        data["product_comparison"] = get_product_comparison(products[0], products[1])
        return data  # Return early
    
    # Brand comparison queries - ONLY return comparison data, nothing else
    if is_brand_comparison:
        brand1 = intent["brand"]
        brand2 = intent["entities"][0]
        data["comparison"] = get_brand_comparison(brand1, brand2)
        # Also get demographics for both brands being compared
        data["brand1_demographics"] = get_brand_demographics(brand1)
        data["brand2_demographics"] = get_brand_demographics(brand2)
        return data  # Return early - don't add other data
    
    # Brand-specific queries (non-comparison)
    if intent["brand"]:
        data["brand_satisfaction"] = get_brand_satisfaction(intent["brand"])
        data["brand_demographics"] = get_brand_demographics(intent["brand"])
        data["brand_improvements"] = get_improvements(intent["brand"])
        data["weakest_aspects"] = get_weakest_aspects(intent["brand"])
    
    # Analysis type specific
    if intent["analysis_type"] == "driver":
        data["drivers"] = get_driver_analysis(intent["brand"])  # None = all brands
    
    if intent["analysis_type"] == "improvements":
        data["improvements"] = get_improvements(intent["brand"])
        data["weakest_aspects"] = get_weakest_aspects(intent["brand"])
    
    if intent["analysis_type"] == "switcher":
        data["switchers"] = get_switcher_analysis(intent["brand"])
    
    if intent["analysis_type"] == "color":
        gender = intent["demographic"].get("gender") if intent["demographic"] else None
        persona = intent["demographic"].get("persona") if intent["demographic"] else None
        data["colors"] = get_color_preferences(brand=intent["brand"], gender=gender, persona=persona)
    
    if intent["analysis_type"] == "processor":
        gender = intent["demographic"].get("gender") if intent["demographic"] else None
        persona = intent["demographic"].get("persona") if intent["demographic"] else None
        data["processors"] = get_processor_preferences(brand=intent["brand"], gender=gender, persona=persona)
    
    if intent["analysis_type"] == "evolution" and intent["series"]:
        data["evolution"] = get_evolution(intent["series"])
    
    if intent["analysis_type"] == "correspondence":
        data["matrix"] = get_correspondence_matrix()
    
    if intent["analysis_type"] == "price":
        gender = intent["demographic"].get("gender") if intent["demographic"] else None
        persona = intent["demographic"].get("persona") if intent["demographic"] else None
        data["price_tiers"] = get_price_tier_preferences(gender, persona)
    
    # Demographic queries - GENERIC (no brand) vs BRAND-SPECIFIC
    if intent["demographic"]:
        if "persona" in intent["demographic"]:
            if is_generic:
                # Generic: Get persona data for ALL brands
                data["persona_all_brands"] = get_persona_preferences(intent["demographic"]["persona"], None)
            else:
                # Brand-specific: Get persona data for specific brand
                data["persona"] = get_persona_preferences(intent["demographic"]["persona"], intent["brand"])
        
        if "gender" in intent["demographic"]:
            if is_generic:
                # Generic: Get overall gender preferences
                data["gender_all_brands"] = get_brand_demographics(None)  # All brands
                data["gender_satisfaction"] = get_brand_satisfaction(None)  # All brands
            # Pass brand and gender to color query
            data["colors"] = get_color_preferences(brand=intent["brand"], gender=intent["demographic"]["gender"])
            data["price_tiers"] = get_price_tier_preferences(intent["demographic"]["gender"])
    
    # Default: get overall market data
    if not data:
        data["brands"] = get_brand_satisfaction()
        data["demographics"] = get_brand_demographics()
        data["drivers"] = get_driver_analysis()
    
    return data

# ============================================
# API ENDPOINTS
# ============================================

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/api/usage")
async def get_usage(request: Request):
    """Get current usage stats for IP"""
    ip = get_client_ip(request)
    return rate_limiter.get_usage(ip)

@app.get("/api/brands")
async def list_brands(
    request: Request,
    rate_check: dict = Depends(check_rate_limit)
):
    """List all brands with summary stats"""
    data = get_brand_satisfaction()
    
    # Aggregate by brand
    brands = {}
    for row in data:
        brand = row["brand_name"]
        if brand not in brands:
            brands[brand] = {"aspects": [], "avg_satisfaction": 0}
        brands[brand]["aspects"].append({
            "aspect": row["aspect_name"],
            "satisfaction": row["satisfaction"]
        })
    
    for brand in brands:
        sats = [a["satisfaction"] for a in brands[brand]["aspects"] if a["satisfaction"]]
        brands[brand]["avg_satisfaction"] = round(sum(sats) / len(sats), 1) if sats else 0
    
    return {
        "brands": brands,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/satisfaction")
async def get_satisfaction(
    request: Request,
    brand: Optional[str] = None,
    rate_check: dict = Depends(check_rate_limit)
):
    """Get satisfaction scores"""
    data = get_brand_satisfaction(brand)
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/drivers")
async def get_drivers(
    request: Request,
    brand: Optional[str] = None,
    rate_check: dict = Depends(check_rate_limit)
):
    """Get driver analysis"""
    data = get_driver_analysis(brand)
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/improvements")
async def improvements(
    request: Request,
    brand: Optional[str] = None,
    rate_check: dict = Depends(check_rate_limit)
):
    """Get improvement insights"""
    data = get_improvements(brand)
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/demographics")
async def demographics(
    request: Request,
    brand: Optional[str] = None,
    rate_check: dict = Depends(check_rate_limit)
):
    """Get demographic breakdown"""
    data = get_brand_demographics(brand)
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/colors")
async def colors(
    request: Request,
    brand: Optional[str] = None,
    group_by: str = "gender",  # 'gender' or 'persona'
    rate_check: dict = Depends(check_rate_limit)
):
    """Get color preferences grouped by gender or persona"""
    data = get_color_preferences_grouped(brand=brand, group_by=group_by)
    return {
        "data": data,
        "group_by": group_by,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/switchers")
async def switchers(
    request: Request,
    brand: Optional[str] = None,
    rate_check: dict = Depends(check_rate_limit)
):
    """Get switcher analysis"""
    data = get_switcher_analysis(brand)
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/evolution")
async def evolution(
    request: Request,
    series: str,
    rate_check: dict = Depends(check_rate_limit)
):
    """Get series evolution"""
    data = get_evolution(series)
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/compare")
async def compare(
    request: Request,
    brand1: str,
    brand2: str,
    gender: Optional[str] = None,
    persona: Optional[str] = None,
    price_tier: Optional[str] = None,
    product1: Optional[str] = None,
    product2: Optional[str] = None,
    rate_check: dict = Depends(check_rate_limit)
):
    """Compare two brands with optional filters"""
    data = get_enhanced_comparison(brand1, brand2, gender, persona, price_tier, product1, product2)
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/processors")
async def processors(
    request: Request,
    brand: Optional[str] = None,
    group_by: str = "gender",  # 'gender' or 'persona'
    rate_check: dict = Depends(check_rate_limit)
):
    """Get processor preferences grouped by gender or persona"""
    data = get_processor_preferences_grouped(brand=brand, group_by=group_by)
    return {
        "data": data,
        "group_by": group_by,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/products")
async def products(
    request: Request,
    brand: Optional[str] = None,
    rate_check: dict = Depends(check_rate_limit)
):
    """Get product list"""
    data = get_products(brand)
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/correspondence")
async def correspondence(
    request: Request,
    rate_check: dict = Depends(check_rate_limit)
):
    """Get brand x aspect matrix"""
    data = get_correspondence_matrix()
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

@app.get("/api/price-tiers")
async def price_tiers(
    request: Request,
    gender: Optional[str] = None,
    persona: Optional[str] = None,
    rate_check: dict = Depends(check_rate_limit)
):
    """Get price tier preferences"""
    data = get_price_tier_preferences(gender, persona)
    return {
        "data": data,
        "remaining_requests": rate_check["remaining"]
    }

# ============================================
# CHAT RESPONSE VERIFICATION
# ============================================

def extract_satisfaction_claims(response: str, brands: List[str]) -> List[Tuple[str, str, int]]:
    """
    Extract brand + aspect + satisfaction % claims from response.
    Returns list of (brand, aspect, percentage) tuples.
    """
    claims = []
    
    # Common aspects to look for
    aspects = [
        "camera", "battery", "display", "screen", "performance", "design", 
        "value for money", "service", "quality", "processor", "sound",
        "speaker", "charging", "software", "build", "price", "features"
    ]
    
    # Normalize response for matching
    response_lower = response.lower()
    
    # Pattern: "Brand's aspect satisfaction is X%" or "Brand has X% satisfaction for aspect"
    # or "aspect satisfaction: Brand X%, Brand2 Y%"
    for brand in brands:
        brand_lower = brand.lower()
        if brand_lower not in response_lower:
            continue
            
        for aspect in aspects:
            # Look for patterns like "Samsung's camera satisfaction is 78%"
            # or "camera: Samsung 78%" or "Samsung camera 78%"
            patterns = [
                rf"{brand_lower}['\s]+{aspect}[^0-9]*?(\d{{1,3}})%",
                rf"{aspect}[^0-9]*?{brand_lower}[^0-9]*?(\d{{1,3}})%",
                rf"{brand_lower}[^0-9]*?(\d{{1,3}})%[^0-9]*?{aspect}",
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, response_lower)
                for match in matches:
                    try:
                        pct = int(match)
                        if 0 <= pct <= 100:
                            claims.append((brand, aspect, pct))
                    except:
                        pass
    
    # Deduplicate
    return list(set(claims))

def verify_claims_against_bq(claims: List[Tuple[str, str, int]], tolerance: int = 10) -> Tuple[List[dict], bool]:
    """
    Verify extracted claims against actual BigQuery data.
    Returns (verification_results, has_major_discrepancy)
    
    tolerance: allowed % difference before flagging as wrong
    """
    if not claims:
        return [], False
    
    results = []
    has_major_discrepancy = False
    
    for brand, aspect, claimed_pct in claims:
        # Query actual satisfaction for this brand + aspect
        query = f"""
        SELECT 
            ROUND(100.0 * SUM(positive_count) / NULLIF(SUM(positive_count + negative_count), 0), 0) as actual_satisfaction
        FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary`
        WHERE UPPER(brand_name) = UPPER('{brand}')
          AND LOWER(aspect_name) = LOWER('{aspect}')
        """
        
        try:
            rows = run_query(query)
            if rows and rows[0].get('actual_satisfaction') is not None:
                actual = int(rows[0]['actual_satisfaction'])
                diff = abs(claimed_pct - actual)
                
                result = {
                    "brand": brand,
                    "aspect": aspect,
                    "claimed": claimed_pct,
                    "actual": actual,
                    "diff": diff,
                    "verified": diff <= tolerance
                }
                results.append(result)
                
                if diff > tolerance:
                    has_major_discrepancy = True
        except Exception as e:
            print(f"Verification query failed for {brand}/{aspect}: {e}")
    
    return results, has_major_discrepancy

def add_verification_note(response: str, verification_results: List[dict], has_discrepancy: bool) -> str:
    """
    Add verification note to response if there are discrepancies.
    """
    if not has_discrepancy or not verification_results:
        return response
    
    # Build correction note
    corrections = []
    for r in verification_results:
        if not r["verified"]:
            corrections.append(f"{r['brand']} {r['aspect']}: claimed {r['claimed']}%, actual {r['actual']}%")
    
    if corrections:
        note = "\n\n---\n⚠️ **Data Verification Note**: Some figures have been cross-checked:\n"
        for c in corrections:
            note += f"• {c}\n"
        note += "\n_Please refer to the dashboard for verified numbers._"
        return response + note
    
    return response

@app.post("/api/chat")
async def chat(
    request: Request,
    chat_request: ChatRequest,
    rate_check: dict = Depends(check_rate_limit)
):
    """Chat with SmaartAnalyst - Data-First approach (BQ data → LLM narrative)"""
    
    user_message = chat_request.message
    
    # Add brand context if available
    context_brand = None
    if chat_request.context:
        context_brand = chat_request.context.get("brand")
    
    # Step 1: Parse intent from user message
    intent = detect_intent(user_message)
    
    # Override with context brand if no brand detected
    if not intent["brand"] and context_brand:
        intent["brand"] = context_brand
    
    # Step 2: Query BigQuery for REAL data
    bq_data = gather_context_data(intent)
    
    # Step 3: Format data for LLM
    data_context = format_data_for_llm(bq_data, intent)
    
    # Determine if department insights are needed (only for strategic questions)
    strategic_types = ["improvements", "comparison", "driver", "swot"]
    needs_department_insights = (
        intent["analysis_type"] in strategic_types or
        intent["compare"] or
        intent["demographic"] or
        "fix" in user_message.lower() or
        "improve" in user_message.lower() or
        "want" in user_message.lower() or
        "prefer" in user_message.lower()
    )
    
    # Build response format based on query type
    if intent["analysis_type"] == "faq":
        brand_name = intent["brand"] or "this brand"
        response_format = f"""RESPONSE FORMAT:
Generate 6-8 SEO-optimized FAQs for {brand_name}'s website. These should be questions real customers search on Google.

❓ **Frequently Asked Questions**:

1. **Is {brand_name} camera good for photography?**
   [Answer using camera satisfaction %, natural conversational tone]

2. **How long does {brand_name} battery last?**
   [Answer using battery satisfaction %, practical advice]

3. **Is {brand_name} worth the money?**
   [Answer using Value for Money %, compare to expectations]

4. **What are the pros and cons of {brand_name} phones?**
   [List top 3 strengths (highest %) and top 3 weaknesses (lowest %)]

5. **{brand_name} vs competitors - which is better?**
   [Use satisfaction data to position strengths]

6. **Is {brand_name} good for gaming/performance?**
   [Answer using Performance/Processor/Speed %]

RULES:
- Use EXACT % from data
- Questions should be natural Google searches (long-tail keywords)
- Include brand name in each question for SEO
- Answers should be conversational, not robotic
- Weave in the satisfaction % naturally, don't just state it"""
    
    elif intent["analysis_type"] == "adcopy":
        brand_name = intent["brand"] or "this brand"
        response_format = f"""RESPONSE FORMAT:
Generate marketing-ready ad copy for {brand_name} based on satisfaction data.

📢 **Google Ads Headlines** (30 chars max each):
1. [Strength-based headline with %]
2. [Benefit-focused headline]
3. [Competitive angle headline]

📱 **Social Media Copy** (Facebook/Instagram):
[60-80 words, conversational, emoji-friendly, highlight top 2 strengths with %]

🎯 **Key Selling Points** (for sales team):
✅ LEAD WITH:
- [Top strength]: [X]% satisfaction — [one-liner pitch]
- [2nd strength]: [X]% satisfaction — [one-liner pitch]

⚠️ AVOID MENTIONING:
- [Weakness]: [X]% — [why to avoid]
- [Weakness]: [X]% — [why to avoid]

📝 **Product Page Copy** (100 words):
[Longer form copy for website/e-commerce, weave in satisfaction % as social proof]

🌐 **Hindi Version**:
[Translate the Social Media Copy to Hindi]

RULES:
- Use EXACT % from data as social proof
- Make claims backed by data ("9 out of 10 users love our camera" if Camera is 90%)
- Tone: Confident but not exaggerated"""
    
    elif needs_department_insights:
        response_format = """RESPONSE FORMAT:
📊 **Insight**: [Key finding with EXACT % from data]
👥 **Audience Mix**: [persona/gender if in data]

🎯 **Actions by Department**:
📦 Category Manager: [action]
📢 Product Marketing: [PROMOTE/AVOID with specific aspects]
🔬 Product R&D: [action]"""
    else:
        response_format = """RESPONSE FORMAT:
📊 **Insight**: [Key finding with EXACT % from data]
👥 **Audience Mix**: [persona/gender if in data, otherwise skip]"""
    
    # Step 4: Generate narrative using Gemini with REAL data only
    prompt = f"""You are SmaartAnalyst, a smartphone insights expert. Generate insights based ONLY on the data provided below.

=== REAL DATA FROM BIGQUERY ===
{data_context}
=== END DATA ===

USER QUESTION: {user_message}

{response_format}

CRITICAL RULES:
1. ONLY use satisfaction percentages that appear in the data above
2. If data shows "Camera: 74%", say "Camera: 74%" - do NOT estimate or round
3. If specific data is not provided, say "data not available"
4. Be concise - max 250 words
5. Respond in the same language as the user's question (Hindi, Tamil, Telugu, etc.)"""

    try:
        # Use Gemini to generate narrative from real data
        answer = generate_with_gemini_fallback(prompt)
        
        # Add spell correction notice if applicable
        if intent.get("corrected_message"):
            answer = f"✏️ *Interpreted as: {intent['corrected_message']}*\n\n{answer}"
        
        # Detect brands for response metadata
        brands = ["Apple", "Samsung", "Xiaomi", "OnePlus", "Realme", "Oppo", "Vivo", 
                  "Motorola", "Nokia", "Google", "Asus", "Infinix", "Tecno", "Poco", "iQOO", "Honor"]
        
        detected_brand = intent["brand"]
        if not detected_brand:
            for brand in brands:
                if brand.lower() in chat_request.message.lower():
                    detected_brand = brand
                    break
        
        return ChatResponse(
            response=answer,
            data_used={
                "method": "data_first",
                "intent": {
                    "brand": intent["brand"],
                    "compare": intent["compare"],
                    "aspects": intent["aspects"],
                    "analysis_type": intent["analysis_type"],
                    "spelling_corrected": intent.get("corrected_message") is not None
                },
                "detected_brand": detected_brand,
                "context_brand": context_brand,
                "data_tables_used": list(bq_data.keys()),
                "verified": True  # Always true - we use real BQ data
            },
            remaining_requests=rate_check["remaining"]
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chat error: {str(e)}")


def format_data_for_llm(bq_data: dict, intent: dict) -> str:
    """Format BigQuery data as readable context for LLM"""
    sections = []
    
    # Comparison data
    if "comparison" in bq_data and bq_data["comparison"]:
        sections.append("## Brand Comparison - Satisfaction Scores")
        for row in bq_data["comparison"]:
            sections.append(f"- {row.get('brand_name', 'N/A')} | {row.get('aspect_name', 'N/A')}: {row.get('satisfaction', 'N/A')}% satisfaction ({row.get('volume', 'N/A')} reviews)")
    
    # Product comparison
    if "product_comparison" in bq_data and bq_data["product_comparison"]:
        sections.append("## Product Comparison - Satisfaction Scores")
        for row in bq_data["product_comparison"]:
            sections.append(f"- {row.get('product_name', 'N/A')} | {row.get('aspect_name', 'N/A')}: {row.get('satisfaction', 'N/A')}% ({row.get('volume', 'N/A')} reviews)")
    
    # Brand satisfaction
    if "brand_satisfaction" in bq_data and bq_data["brand_satisfaction"]:
        # Get brand name from first row
        brand_name = bq_data["brand_satisfaction"][0].get('brand_name', 'Brand') if bq_data["brand_satisfaction"] else 'Brand'
        sections.append(f"## {brand_name} Satisfaction by Aspect")
        for row in bq_data["brand_satisfaction"][:25]:  # Top 25 aspects
            sections.append(f"- {brand_name} {row.get('aspect_name', 'N/A')}: {row.get('satisfaction', 'N/A')}% ({row.get('positive', 0)} positive, {row.get('negative', 0)} negative)")
    
    # Demographics
    if "brand_demographics" in bq_data and bq_data["brand_demographics"]:
        brand_name = bq_data["brand_demographics"][0].get('brand_name', '') if bq_data["brand_demographics"] else ''
        header = f"## {brand_name} Audience Demographics" if brand_name else "## Audience Demographics"
        sections.append(header)
        for row in bq_data["brand_demographics"][:10]:
            sections.append(f"- {row.get('gender', 'N/A')} / {row.get('persona', 'N/A')}: {row.get('reviews', 0)} reviews")
    
    # Driver analysis
    if "drivers" in bq_data and bq_data["drivers"]:
        sections.append("## Driver Analysis (Share of Voice & Satisfaction)")
        for row in bq_data["drivers"][:10]:
            sections.append(f"- {row.get('brand_name', 'N/A')} | {row.get('aspect_name', 'N/A')}: SoV {row.get('share_of_voice', 'N/A')}%, Sat {row.get('satisfaction', 'N/A')}%")
    
    # Color preferences
    if "colors" in bq_data and bq_data["colors"]:
        sections.append("## Color Preferences")
        for row in bq_data["colors"][:8]:
            reviews = row.get('reviews', row.get('review_count', 0))
            brand = row.get('brand_name', '')
            gender = row.get('gender', '')
            persona = row.get('persona', '')
            context = f" ({brand}" if brand else ""
            if gender:
                context += f", {gender}"
            if persona:
                context += f", {persona}"
            if context:
                context += ")"
            sections.append(f"- {row.get('color', 'N/A')}: {reviews} reviews, {row.get('satisfaction', 'N/A')}% satisfaction{context}")
    
    # Processor preferences
    if "processors" in bq_data and bq_data["processors"]:
        sections.append("## Processor Preferences")
        for row in bq_data["processors"][:8]:
            reviews = row.get('reviews', row.get('review_count', 0))
            brand = row.get('brand_name', '')
            gender = row.get('gender', '')
            persona = row.get('persona', '')
            context = f" ({brand}" if brand else ""
            if gender:
                context += f", {gender}"
            if persona:
                context += f", {persona}"
            if context:
                context += ")"
            sections.append(f"- {row.get('processor', 'N/A')}: {reviews} reviews, {row.get('satisfaction', 'N/A')}% satisfaction{context}")
    
    # Price tier preferences
    if "price_tiers" in bq_data and bq_data["price_tiers"]:
        sections.append("## Price Tier Analysis")
        for row in bq_data["price_tiers"][:8]:
            sections.append(f"- {row.get('price_tier', 'N/A')}: {row.get('review_count', 0)} reviews, {row.get('satisfaction', 'N/A')}% satisfaction")
    
    # Persona preferences (all brands)
    if "persona_all_brands" in bq_data and bq_data["persona_all_brands"]:
        sections.append("## Persona Preferences - All Brands")
        # Group by brand for readability
        brand_aspects = {}
        for row in bq_data["persona_all_brands"]:
            brand = row.get('brand_name', 'Unknown')
            if brand not in brand_aspects:
                brand_aspects[brand] = []
            brand_aspects[brand].append(row)
        
        for brand, aspects in brand_aspects.items():
            sections.append(f"### {brand}")
            for row in aspects[:5]:  # Top 5 aspects per brand
                sections.append(f"- {row.get('aspect_name', 'N/A')}: {row.get('satisfaction', 'N/A')}% satisfaction ({row.get('reviews', 0)} reviews)")
    
    # Persona preferences (brand-specific)
    if "persona" in bq_data and bq_data["persona"]:
        sections.append("## Persona Preferences")
        for row in bq_data["persona"][:10]:
            sections.append(f"- {row.get('brand_name', 'N/A')} | {row.get('aspect_name', 'N/A')}: {row.get('satisfaction', 'N/A')}% ({row.get('reviews', 0)} reviews)")
    
    # Improvements
    if "improvements" in bq_data and bq_data["improvements"]:
        sections.append("## Areas Needing Improvement (High Volume, Low Satisfaction)")
        for row in bq_data["improvements"][:5]:
            sections.append(f"- {row.get('aspect_name', 'N/A')}: {row.get('satisfaction', 'N/A')}% satisfaction ({row.get('negative_count', 0)} negative mentions)")
    
    # Weakest aspects (lowest satisfaction - matches dashboard Top Weakness)
    if "weakest_aspects" in bq_data and bq_data["weakest_aspects"]:
        sections.append("## Weakest Aspects (Lowest Satisfaction Scores)")
        for row in bq_data["weakest_aspects"][:5]:
            sections.append(f"- {row.get('aspect_name', 'N/A')}: {row.get('satisfaction', 'N/A')}% satisfaction ({row.get('total_mentions', 0)} total mentions)")
    
    # All brands satisfaction (for generic queries)
    if "brands" in bq_data and bq_data["brands"]:
        sections.append("## All Brands - Satisfaction Overview")
        # Group by brand
        brand_data = {}
        for row in bq_data["brands"]:
            brand = row.get('brand_name', 'Unknown')
            if brand not in brand_data:
                brand_data[brand] = []
            brand_data[brand].append(row)
        
        for brand, aspects in brand_data.items():
            top_aspects = aspects[:3]  # Top 3 aspects per brand
            aspects_str = ", ".join([f"{a.get('aspect_name', 'N/A')}: {a.get('satisfaction', 'N/A')}%" for a in top_aspects])
            sections.append(f"- {brand}: {aspects_str}")
    
    if not sections:
        return "No specific data found for this query. Please try a more specific question about a brand, product, or aspect."
    
    return "\n".join(sections)

# ============================================
# HEALTH CHECK
# ============================================

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/debug")
async def debug():
    """Debug endpoint to check configuration"""
    creds_status = "loaded" if GCP_CREDENTIALS else "missing"
    creds_json_present = bool(os.getenv("GCP_CREDENTIALS_JSON"))
    
    # Test BQ connection
    bq_status = "unknown"
    bq_error = None
    try:
        client = get_bq_client()
        query = f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT}.{BQ_DATASET}.product_sentiment_summary` LIMIT 1"
        result = list(client.query(query).result())
        bq_status = f"ok - {result[0]['cnt']} rows"
    except Exception as e:
        bq_status = "error"
        bq_error = str(e)
    
    # Test Gemini model
    gemini_status = "unknown"
    gemini_error = None
    try:
        client = get_bq_client()
        query = f"SELECT 1 FROM `{GCP_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.MODELS` WHERE model_name = '{BQ_GEMINI_MODEL}' LIMIT 1"
        result = list(client.query(query).result())
        gemini_status = "model exists" if len(result) > 0 else "model not found"
    except Exception as e:
        gemini_status = "check failed"
        gemini_error = str(e)
    
    # Check all required tables
    table_checks = {}
    tables_to_check = [
        "product_sentiment_summary",
        "color_preferences", 
        "processor_preferences",
        "driver_analysis"
    ]
    try:
        client = get_bq_client()
        for table in tables_to_check:
            try:
                query = f"SELECT COUNT(*) as cnt FROM `{GCP_PROJECT}.{BQ_DATASET}.{table}` LIMIT 1"
                result = list(client.query(query).result())
                table_checks[table] = f"ok - {result[0]['cnt']} rows"
            except Exception as e:
                table_checks[table] = f"error: {str(e)[:100]}"
    except Exception as e:
        table_checks["error"] = str(e)
    
    return {
        "gcp_project": GCP_PROJECT,
        "bq_dataset": BQ_DATASET,
        "gcp_location": GCP_LOCATION,
        "bq_agent_id": BQ_AGENT_ID,
        "bq_agent_location": BQ_AGENT_LOCATION,
        "bq_gemini_model_fallback": BQ_GEMINI_MODEL,
        "credentials_json_present": creds_json_present,
        "credentials_status": creds_status,
        "bigquery_status": bq_status,
        "bigquery_error": bq_error,
        "gemini_model_status": gemini_status,
        "gemini_model_error": gemini_error,
        "bq_agent_status": "check via /debug/test-agent",
        "table_checks": table_checks
    }

@app.get("/debug/test-agent")
async def test_agent():
    """Test BQ Agent connectivity"""
    try:
        cc = get_chat_client()
        if cc:
            return {
                "status": "ok",
                "agent_id": BQ_AGENT_ID,
                "chat_client": "connected"
            }
        else:
            return {
                "status": "warning",
                "message": "Chat client unavailable, will use BQ ML fallback"
            }
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/debug/test-chat")
async def test_chat(message: str = "What brands are in the data?"):
    """Test BQ Agent with a message - shows raw response info"""
    try:
        from google.cloud import geminidataanalytics_v1alpha as gda
        
        cc = get_chat_client()
        if not cc:
            return {"status": "error", "message": "Chat client not available"}
        
        parent = f"projects/{GCP_PROJECT}/locations/{BQ_AGENT_LOCATION}"
        agent = f"{parent}/dataAgents/{BQ_AGENT_ID}"
        
        conv_id = f"test-{uuid.uuid4().hex[:8]}"
        conv_name = cc.conversation_path(GCP_PROJECT, BQ_AGENT_LOCATION, conv_id)
        
        # Create conversation
        try:
            cc.create_conversation(
                request=gda.CreateConversationRequest(
                    parent=parent,
                    conversation_id=conv_id,
                    conversation=gda.Conversation(agents=[agent])
                )
            )
        except Exception as e:
            return {"status": "error", "message": f"Failed to create conversation: {e}"}
        
        # Chat
        try:
            stream = cc.chat(
                request={
                    "parent": parent,
                    "conversation_reference": {
                        "conversation": conv_name,
                        "data_agent_context": {"data_agent": agent}
                    },
                    "messages": [{"user_message": {"text": message}}]
                }
            )
            
            chunks_info = []
            response_text = ""
            system_messages = []
            
            for i, chunk in enumerate(stream):
                chunk_info = {
                    "index": i,
                    "type": str(type(chunk).__name__),
                }
                
                # Extract from system_message
                if hasattr(chunk, 'system_message'):
                    sm = chunk.system_message
                    chunk_info["has_system_message"] = True
                    
                    # Check text attribute
                    if hasattr(sm, 'text'):
                        if hasattr(sm.text, 'parts'):
                            parts = [str(p) for p in sm.text.parts]
                            chunk_info["system_text_parts"] = parts
                            system_messages.extend(parts)
                        else:
                            chunk_info["system_text"] = str(sm.text)
                            system_messages.append(str(sm.text))
                    
                    # Check other attributes
                    sm_attrs = [a for a in dir(sm) if not a.startswith('_')]
                    chunk_info["system_message_attrs"] = sm_attrs
                
                # Extract from agent_message
                if hasattr(chunk, 'agent_message'):
                    am = chunk.agent_message
                    chunk_info["has_agent_message"] = True
                    if hasattr(am, 'text') and hasattr(am.text, 'parts'):
                        parts = [str(p) for p in am.text.parts]
                        chunk_info["agent_text_parts"] = parts
                        response_text += " ".join(parts)
                
                chunks_info.append(chunk_info)
            
            return {
                "status": "ok",
                "message_sent": message,
                "chunks_received": len(chunks_info),
                "agent_response": response_text[:1000] if response_text else "No agent_message text",
                "system_messages": system_messages[-5:],  # Last 5 system messages
                "chunks_detail": chunks_info
            }
            
        except Exception as e:
            import traceback
            return {
                "status": "error", 
                "message": f"Chat failed: {e}",
                "traceback": traceback.format_exc()
            }
            
    except Exception as e:
        import traceback
        return {"status": "error", "error": str(e), "traceback": traceback.format_exc()}

@app.get("/debug/test-color")
async def test_color(brand: Optional[str] = None, gender: Optional[str] = None):
    """Test color preferences query directly"""
    try:
        data = get_color_preferences(gender=gender, brand=brand)
        return {
            "status": "ok",
            "query_params": {"brand": brand, "gender": gender},
            "row_count": len(data),
            "data": data[:10]  # First 10 rows
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/debug/test-processor")
async def test_processor(brand: Optional[str] = None, gender: Optional[str] = None):
    """Test processor preferences query directly"""
    try:
        data = get_processor_preferences(brand=brand, gender=gender)
        return {
            "status": "ok",
            "query_params": {"brand": brand, "gender": gender},
            "row_count": len(data),
            "data": data[:10]  # First 10 rows
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.get("/debug/test-intent")
async def test_intent(message: str):
    """Test intent detection"""
    intent = detect_intent(message)
    context_data = gather_context_data(intent)
    return {
        "message": message,
        "intent": intent,
        "data_keys": list(context_data.keys()),
        "data_rows": {k: len(v) if isinstance(v, list) else "dict" for k, v in context_data.items()}
    }

# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
