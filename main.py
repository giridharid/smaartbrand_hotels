"""
Smaartbrand Hotels - FastAPI Backend
Matching smartphones dashboard architecture
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import base64
import re
from pydantic import BaseModel
from typing import Optional, List

app = FastAPI(title="Smaartbrand Hotels API")

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────
PROJECT = "gen-lang-client-0143536012"
DATASET = "analyst"

ASPECT_MAP = {
    1: "Dining",
    2: "Cleanliness", 
    3: "Amenities",
    4: "Staff",
    5: "Room",
    6: "Location",
    7: "Value for Money",
    8: "General"
}

ASPECT_ICONS = {
    "Dining": "🍽️",
    "Cleanliness": "🧹",
    "Amenities": "🏊",
    "Staff": "👨‍💼",
    "Room": "🛏️",
    "Location": "📍",
    "Value for Money": "💰",
    "General": "⭐"
}

# ─────────────────────────────────────────
# CREDENTIALS
# ─────────────────────────────────────────
def get_credentials():
    gcp_creds = os.environ.get("GCP_CREDENTIALS_JSON", "")
    if not gcp_creds:
        return None
    gcp_creds = gcp_creds.strip().strip('"').strip("'")
    try:
        if gcp_creds.startswith("{"):
            creds_dict = json.loads(gcp_creds)
        else:
            padding = 4 - len(gcp_creds) % 4
            if padding != 4:
                gcp_creds += "=" * padding
            creds_dict = json.loads(base64.b64decode(gcp_creds).decode('utf-8'))
        return service_account.Credentials.from_service_account_info(creds_dict)
    except Exception as e:
        print(f"Credential error: {e}")
        return None

def get_bq_client():
    credentials = get_credentials()
    if credentials:
        return bigquery.Client(credentials=credentials, project=credentials.project_id)
    try:
        return bigquery.Client(project=PROJECT)
    except:
        return None

client = get_bq_client()

# ─────────────────────────────────────────
# SPELL CORRECTIONS
# ─────────────────────────────────────────
SPELL_CORRECTIONS = {
    # Cities (from classification doc)
    "bangalore": "Bengaluru", "blr": "Bengaluru", "banglore": "Bengaluru",
    "bombay": "Mumbai", "madras": "Chennai", "calcutta": "Kolkata",
    "delhi": "New Delhi", "gurgaon": "Gurugram",
    # Hotel brands (common misspellings)
    "marriot": "Marriott", "marriot": "Marriott", "mariot": "Marriott",
    "oberoy": "Oberoi", "oberoyi": "Oberoi", "oberio": "Oberoi",
    "viventa": "Vivanta", "vivnta": "Vivanta",
    "lela": "Leela", "leala": "Leela", "leelaa": "Leela",
    "taaj": "Taj", "tajj": "Taj",
    "itc": "ITC", "radison": "Radisson", "raddison": "Radisson",
    "hyat": "Hyatt", "haytt": "Hyatt",
    "hilten": "Hilton", "hiltan": "Hilton",
    "novotle": "Novotel", "novatel": "Novotel",
    "holdiay": "Holiday", "holyday": "Holiday",
    "mahindra": "Mahindra", "mahinra": "Mahindra",
    "sterling": "Sterling", "sterlig": "Sterling",
    "pullman": "Pullman", "pulman": "Pullman",
    "ibis": "ibis", "ibiss": "ibis",
    "clarks": "Clarks", "clark": "Clarks",
    # Aspects (from classification doc)
    "food": "Dining", "resturant": "Dining", "restaurant": "Dining", 
    "breakfast": "Dining", "dinner": "Dining", "lunch": "Dining", "buffet": "Dining",
    "clean": "Cleanliness", "hygene": "Cleanliness", "hygiene": "Cleanliness",
    "dirty": "Cleanliness", "houskeeping": "Cleanliness",
    "price": "Value for Money", "cost": "Value for Money", "value": "Value for Money",
    "expensive": "Value for Money", "cheap": "Value for Money", "overpriced": "Value for Money",
    "service": "Staff", "staff": "Staff", "reception": "Staff", "receptionist": "Staff",
    "concierge": "Staff", "housekeeping": "Staff",
    "pool": "Amenities", "gym": "Amenities", "spa": "Amenities", 
    "wifi": "Amenities", "parking": "Amenities", "swiming": "Amenities",
    "bed": "Room", "bathroom": "Room", "ac": "Room", "aircon": "Room",
    "noise": "Room", "noisy": "Room", "quite": "Room", "quiet": "Room",
    "loaction": "Location", "locaton": "Location", "nearness": "Location",
    # Traveler types (from classification doc)
    "buisness": "Business", "bussiness": "Business", "bisness": "Business",
    "famly": "Family", "famliy": "Family", "familiy": "Family",
    "cuple": "Couple", "coupl": "Couple", "cupple": "Couple",
    "honeymoon": "Honeymoon", "honneymoon": "Honeymoon",
    # Stay purposes
    "liesure": "Leisure", "leisur": "Leisure",
    "vaccation": "Vacation", "vacaton": "Vacation",
    "confrence": "Conference", "conferance": "Conference",
    "weding": "Wedding", "weading": "Wedding",
}

def correct_spelling(message: str) -> tuple:
    """Correct common misspellings. Returns (corrected_message, was_corrected)"""
    corrected = message
    was_corrected = False
    
    # Sort by length descending to replace longer matches first
    sorted_corrections = sorted(SPELL_CORRECTIONS.items(), key=lambda x: len(x[0]), reverse=True)
    
    for wrong, right in sorted_corrections:
        pattern = r'\b' + re.escape(wrong) + r'\b'
        if re.search(pattern, corrected, re.IGNORECASE):
            corrected = re.sub(pattern, right, corrected, flags=re.IGNORECASE)
            was_corrected = True
    
    return corrected, was_corrected

# ─────────────────────────────────────────
# API ENDPOINTS
# ─────────────────────────────────────────

@app.get("/")
async def root():
    """Serve the main HTML page"""
    with open("index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/api/hotels")
async def get_hotels():
    """Get list of all hotels with metadata"""
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    query = f"""
    SELECT DISTINCT 
        pl.Name AS hotel_name,
        pd.Brand,
        pl.Star_Category AS star_category,
        pl.City,
        pd.Rating AS google_rating
    FROM `{PROJECT}.{DATASET}.product_list` pl
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE pd.Brand IS NOT NULL AND pl.Name IS NOT NULL AND pl.City IS NOT NULL
    ORDER BY pd.Brand, pl.Name
    """
    
    try:
        result = client.query(query).to_dataframe()
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/brands")
async def get_brands():
    """Get list of all hotel brands"""
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    query = f"""
    SELECT DISTINCT pd.Brand
    FROM `{PROJECT}.{DATASET}.product_description` pd
    WHERE pd.Brand IS NOT NULL
    ORDER BY pd.Brand
    """
    
    try:
        result = client.query(query).to_dataframe()
        return result['Brand'].tolist()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/cities")
async def get_cities():
    """Get list of all cities"""
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    query = f"""
    SELECT DISTINCT pl.City
    FROM `{PROJECT}.{DATASET}.product_list` pl
    WHERE pl.City IS NOT NULL
    ORDER BY pl.City
    """
    
    try:
        result = client.query(query).to_dataframe()
        return result['City'].tolist()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/satisfaction")
async def get_satisfaction(hotel: Optional[str] = None, brand: Optional[str] = None):
    """Get satisfaction scores by aspect for a hotel or brand"""
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    where_clause = ""
    if hotel:
        hotel_escaped = hotel.replace("'", "''")
        where_clause = f"WHERE pl.Name = '{hotel_escaped}'"
    elif brand:
        brand_escaped = brand.replace("'", "''")
        where_clause = f"WHERE pd.Brand = '{brand_escaped}'"
    else:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    query = f"""
    SELECT 
        s.aspect_id,
        SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) AS positive_count,
        SUM(CASE WHEN LOWER(s.sentiment_type) = 'negative' THEN 1 ELSE 0 END) AS negative_count,
        COUNT(*) AS total_mentions,
        ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(*), 0), 0) AS satisfaction
    FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
    JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    {where_clause}
    GROUP BY s.aspect_id
    ORDER BY s.aspect_id
    """
    
    try:
        result = client.query(query).to_dataframe()
        # Map aspect_id to aspect_name
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        result['icon'] = result['aspect_name'].map(ASPECT_ICONS)
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/drivers")
async def get_drivers(hotel: Optional[str] = None, brand: Optional[str] = None):
    """Get driver analysis (Share of Voice + Satisfaction) for a hotel or brand"""
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    where_clause = ""
    if hotel:
        hotel_escaped = hotel.replace("'", "''")
        where_clause = f"WHERE pl.Name = '{hotel_escaped}'"
    elif brand:
        brand_escaped = brand.replace("'", "''")
        where_clause = f"WHERE pd.Brand = '{brand_escaped}'"
    else:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    query = f"""
    WITH aspect_data AS (
        SELECT 
            s.aspect_id,
            SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) AS positive_count,
            SUM(CASE WHEN LOWER(s.sentiment_type) = 'negative' THEN 1 ELSE 0 END) AS negative_count,
            COUNT(*) AS mentions
        FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
        JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        {where_clause}
        GROUP BY s.aspect_id
    ),
    total AS (
        SELECT SUM(mentions) AS total_mentions FROM aspect_data
    )
    SELECT 
        a.aspect_id,
        a.positive_count,
        a.negative_count,
        a.mentions,
        ROUND(a.mentions * 100.0 / NULLIF(t.total_mentions, 0), 0) AS share_of_voice,
        ROUND(a.positive_count * 100.0 / NULLIF(a.positive_count + a.negative_count, 0), 0) AS satisfaction
    FROM aspect_data a, total t
    ORDER BY share_of_voice DESC
    """
    
    try:
        result = client.query(query).to_dataframe()
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        result['icon'] = result['aspect_name'].map(ASPECT_ICONS)
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/demographics")
async def get_demographics(hotel: Optional[str] = None, brand: Optional[str] = None):
    """Get demographics breakdown (traveler type, gender, stay purpose)
    
    Note: 75-90% of reviews have NULL traveler_type (expected).
    We show % of CLASSIFIED reviews only (excluding NULL).
    """
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    where_clause = ""
    if hotel:
        hotel_escaped = hotel.replace("'", "''")
        where_clause = f"WHERE pl.Name = '{hotel_escaped}'"
    elif brand:
        brand_escaped = brand.replace("'", "''")
        where_clause = f"WHERE pd.Brand = '{brand_escaped}'"
    else:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    # Total reviews (for context)
    total_query = f"""
    SELECT COUNT(DISTINCT e.id) AS total_reviews
    FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    {where_clause}
    """
    
    # Traveler type distribution (Family, Couple, Business, Solo, Group)
    # Excludes NULL - shows % among classified reviews only
    traveler_query = f"""
    SELECT 
        e.traveler_type,
        COUNT(DISTINCT e.id) AS review_count
    FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    {where_clause}
    AND e.traveler_type IS NOT NULL 
    AND e.traveler_type != ''
    AND e.traveler_type NOT IN ('Unknown', 'NULL')
    GROUP BY e.traveler_type
    ORDER BY review_count DESC
    """
    
    # Gender distribution (Male, Female - excludes Unknown)
    gender_query = f"""
    SELECT 
        e.inferred_gender AS gender,
        COUNT(DISTINCT e.id) AS review_count
    FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    {where_clause}
    AND e.inferred_gender IS NOT NULL 
    AND e.inferred_gender != ''
    AND e.inferred_gender NOT IN ('Unknown', 'NULL')
    GROUP BY e.inferred_gender
    ORDER BY review_count DESC
    """
    
    # Stay purpose distribution (Leisure, Honeymoon, Event, Business, Transit)
    purpose_query = f"""
    SELECT 
        e.stay_purpose,
        COUNT(DISTINCT e.id) AS review_count
    FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    {where_clause}
    AND e.stay_purpose IS NOT NULL 
    AND e.stay_purpose != ''
    AND e.stay_purpose NOT IN ('Unknown', 'NULL')
    GROUP BY e.stay_purpose
    ORDER BY review_count DESC
    """
    
    try:
        total_df = client.query(total_query).to_dataframe()
        total_reviews = int(total_df['total_reviews'].iloc[0]) if not total_df.empty else 0
        
        traveler_df = client.query(traveler_query).to_dataframe()
        gender_df = client.query(gender_query).to_dataframe()
        purpose_df = client.query(purpose_query).to_dataframe()
        
        # Calculate classified counts
        traveler_classified = int(traveler_df['review_count'].sum()) if not traveler_df.empty else 0
        gender_classified = int(gender_df['review_count'].sum()) if not gender_df.empty else 0
        purpose_classified = int(purpose_df['review_count'].sum()) if not purpose_df.empty else 0
        
        return {
            "total_reviews": total_reviews,
            "traveler_type": traveler_df.to_dict(orient='records'),
            "traveler_classified": traveler_classified,
            "traveler_null_pct": round((total_reviews - traveler_classified) * 100 / max(total_reviews, 1)),
            "gender": gender_df.to_dict(orient='records'),
            "gender_classified": gender_classified,
            "stay_purpose": purpose_df.to_dict(orient='records'),
            "purpose_classified": purpose_classified
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/phrases")
async def get_phrases(
    hotel: Optional[str] = None, 
    brand: Optional[str] = None,
    sentiment: Optional[str] = None,
    aspect: Optional[str] = None,
    limit: int = 10
):
    """Get top phrases with sentiment"""
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    where_clauses = []
    if hotel:
        hotel_escaped = hotel.replace("'", "''")
        where_clauses.append(f"pl.Name = '{hotel_escaped}'")
    elif brand:
        brand_escaped = brand.replace("'", "''")
        where_clauses.append(f"pd.Brand = '{brand_escaped}'")
    else:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    if sentiment:
        where_clauses.append(f"LOWER(s.sentiment_type) = '{sentiment.lower()}'")
    
    if aspect:
        # Find aspect_id from name
        aspect_id = None
        for aid, aname in ASPECT_MAP.items():
            if aname.lower() == aspect.lower():
                aspect_id = aid
                break
        if aspect_id:
            where_clauses.append(f"s.aspect_id = {aspect_id}")
    
    where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    query = f"""
    SELECT 
        s.treemap_name AS phrase,
        s.sentiment_type,
        s.aspect_id,
        COUNT(*) AS mention_count,
        ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(*), 0), 0) AS positive_pct
    FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
    JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    {where_clause}
    AND s.treemap_name IS NOT NULL AND s.treemap_name != ''
    GROUP BY s.treemap_name, s.sentiment_type, s.aspect_id
    ORDER BY mention_count DESC
    LIMIT {limit}
    """
    
    try:
        result = client.query(query).to_dataframe()
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/comparison")
async def get_comparison(hotels: str):
    """Get comparison data for multiple hotels (comma-separated)"""
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    hotel_list = [h.strip() for h in hotels.split(",") if h.strip()]
    if not hotel_list:
        raise HTTPException(status_code=400, detail="At least one hotel required")
    
    hotels_sql = "', '".join([h.replace("'", "''") for h in hotel_list])
    
    query = f"""
    SELECT 
        pl.Name AS hotel_name,
        s.aspect_id,
        SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) AS positive_count,
        SUM(CASE WHEN LOWER(s.sentiment_type) = 'negative' THEN 1 ELSE 0 END) AS negative_count,
        COUNT(*) AS total_mentions,
        ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(*), 0), 0) AS satisfaction
    FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
    JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    WHERE pl.Name IN ('{hotels_sql}')
    GROUP BY pl.Name, s.aspect_id
    ORDER BY pl.Name, s.aspect_id
    """
    
    try:
        result = client.query(query).to_dataframe()
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        
        # Pivot to hotel-centric view
        comparison = {}
        for hotel in hotel_list:
            hotel_data = result[result['hotel_name'] == hotel]
            comparison[hotel] = {
                "aspects": hotel_data.to_dict(orient='records'),
                "overall": {
                    "positive": int(hotel_data['positive_count'].sum()),
                    "negative": int(hotel_data['negative_count'].sum()),
                    "satisfaction": round(hotel_data['positive_count'].sum() * 100 / 
                                         max(hotel_data['positive_count'].sum() + hotel_data['negative_count'].sum(), 1))
                }
            }
        
        return comparison
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/weakest")
async def get_weakest_aspects(hotel: Optional[str] = None, brand: Optional[str] = None):
    """Get weakest aspects (lowest satisfaction)"""
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    where_clause = ""
    if hotel:
        hotel_escaped = hotel.replace("'", "''")
        where_clause = f"WHERE pl.Name = '{hotel_escaped}'"
    elif brand:
        brand_escaped = brand.replace("'", "''")
        where_clause = f"WHERE pd.Brand = '{brand_escaped}'"
    else:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    query = f"""
    SELECT aspect_id, aspect_name, satisfaction, positive_count, negative_count, total_mentions
    FROM (
        SELECT 
            s.aspect_id,
            SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) AS positive_count,
            SUM(CASE WHEN LOWER(s.sentiment_type) = 'negative' THEN 1 ELSE 0 END) AS negative_count,
            COUNT(*) AS total_mentions,
            ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
                  NULLIF(COUNT(*), 0), 0) AS satisfaction
        FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
        JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        {where_clause}
        GROUP BY s.aspect_id
    )
    WHERE total_mentions > 50
    ORDER BY satisfaction ASC
    LIMIT 5
    """
    
    try:
        result = client.query(query).to_dataframe()
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────
# CHAT ENDPOINT
# ─────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    hotel: Optional[str] = None
    brand: Optional[str] = None
    history: Optional[List[dict]] = []

def detect_intent(message: str, hotel: str = None, brand: str = None):
    """Detect intent from user message - matches smartphones approach"""
    message_lower = message.lower()
    
    intent = {
        "hotel": hotel,
        "brand": brand,
        "aspects": [],
        "traveler_type": None,
        "gender": None,
        "analysis_type": "general",
        "is_comparison": False
    }
    
    # Detect aspects
    for aspect_name in ASPECT_MAP.values():
        if aspect_name.lower() in message_lower:
            intent["aspects"].append(aspect_name)
    
    # Aspect synonyms
    aspect_synonyms = {
        "food": "Dining", "breakfast": "Dining", "restaurant": "Dining", "dinner": "Dining", 
        "lunch": "Dining", "buffet": "Dining", "meal": "Dining",
        "clean": "Cleanliness", "hygiene": "Cleanliness", "dirty": "Cleanliness", "housekeeping": "Cleanliness",
        "pool": "Amenities", "gym": "Amenities", "spa": "Amenities", "wifi": "Amenities", 
        "parking": "Amenities", "swimming": "Amenities",
        "service": "Staff", "reception": "Staff", "concierge": "Staff", "friendly": "Staff",
        "bed": "Room", "bathroom": "Room", "ac": "Room", "noise": "Room", "noisy": "Room",
        "price": "Value for Money", "cost": "Value for Money", "expensive": "Value for Money", 
        "cheap": "Value for Money", "worth": "Value for Money",
    }
    for word, aspect in aspect_synonyms.items():
        if word in message_lower and aspect not in intent["aspects"]:
            intent["aspects"].append(aspect)
    
    # Detect traveler type (from classification doc keywords)
    traveler_types = {
        "business": "Business", "corporate": "Business", "work trip": "Business", "conference": "Business",
        "family": "Family", "families": "Family", "kids": "Family", "children": "Family",
        "couple": "Couple", "couples": "Couple", "romantic": "Couple", "honeymoon": "Couple", "anniversary": "Couple",
        "solo": "Solo", "alone": "Solo", "by myself": "Solo",
        "group": "Group", "friends": "Group", "colleagues": "Group"
    }
    for word, ttype in traveler_types.items():
        if word in message_lower:
            intent["traveler_type"] = ttype
            break
    
    # Detect gender
    if any(w in message_lower for w in ["female", "women", "woman", "ladies"]):
        intent["gender"] = "Female"
    elif any(w in message_lower for w in ["male", "men", "man"]):
        intent["gender"] = "Male"
    
    # Detect analysis type (matching smartphones)
    if any(w in message_lower for w in ["compare", "vs", "versus", "comparison", "better than"]):
        intent["analysis_type"] = "comparison"
        intent["is_comparison"] = True
    elif any(w in message_lower for w in ["fix", "improve", "weak", "problem", "issue", "complaint", "pain", "worst"]):
        intent["analysis_type"] = "improvements"
    elif any(w in message_lower for w in ["strength", "good", "best", "love", "positive", "strong"]):
        intent["analysis_type"] = "strengths"
    elif any(w in message_lower for w in ["faq", "question", "frequently", "website faq"]):
        intent["analysis_type"] = "faq"
    elif any(w in message_lower for w in ["ad copy", "adcopy", "marketing", "campaign", "promote", "advertisement", "google ads"]):
        intent["analysis_type"] = "adcopy"
    elif any(w in message_lower for w in ["trend", "recent", "month", "quarter", "change", "last"]):
        intent["analysis_type"] = "trend"
    elif any(w in message_lower for w in ["driver", "sov", "share of voice", "what matters", "important"]):
        intent["analysis_type"] = "driver"
    
    return intent

def gather_context_data(intent: dict):
    """Gather real data from BigQuery based on intent - DATA FIRST approach"""
    if not client:
        return {}
    
    data = {}
    hotel = intent.get("hotel")
    brand = intent.get("brand")
    
    if not hotel and not brand:
        return data
    
    try:
        # Build WHERE clause
        if hotel:
            hotel_escaped = hotel.replace("'", "''")
            where = f"pl.Name = '{hotel_escaped}'"
        else:
            brand_escaped = brand.replace("'", "''")
            where = f"pd.Brand = '{brand_escaped}'"
        
        name = hotel or brand
        
        # 1. Get satisfaction data by aspect
        sat_query = f"""
        SELECT 
            s.aspect_id,
            SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) AS positive_count,
            SUM(CASE WHEN LOWER(s.sentiment_type) = 'negative' THEN 1 ELSE 0 END) AS negative_count,
            ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
                  NULLIF(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) + 
                         SUM(CASE WHEN LOWER(s.sentiment_type) = 'negative' THEN 1 ELSE 0 END), 0), 0) AS satisfaction
        FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
        JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {where}
        GROUP BY s.aspect_id
        ORDER BY satisfaction DESC
        """
        sat_df = client.query(sat_query).to_dataframe()
        sat_df['aspect_name'] = sat_df['aspect_id'].map(ASPECT_MAP)
        data['satisfaction'] = sat_df.to_dict(orient='records')
        
        # 2. Get weakest aspects (always include for improvements context)
        weak_query = f"""
        SELECT aspect_id, satisfaction, positive_count, negative_count, total_mentions
        FROM (
            SELECT 
                s.aspect_id,
                SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) AS positive_count,
                SUM(CASE WHEN LOWER(s.sentiment_type) = 'negative' THEN 1 ELSE 0 END) AS negative_count,
                COUNT(*) AS total_mentions,
                ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
                      NULLIF(COUNT(*), 0), 0) AS satisfaction
            FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
            JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
            JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
            JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
            WHERE {where}
            GROUP BY s.aspect_id
        )
        WHERE total_mentions > 50
        ORDER BY satisfaction ASC
        LIMIT 5
        """
        weak_df = client.query(weak_query).to_dataframe()
        weak_df['aspect_name'] = weak_df['aspect_id'].map(ASPECT_MAP)
        data['weakest'] = weak_df.to_dict(orient='records')
        
        # 3. Get demographics (traveler type, gender)
        demo_query = f"""
        SELECT 
            e.traveler_type,
            e.inferred_gender AS gender,
            COUNT(DISTINCT e.id) AS review_count
        FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {where}
        AND e.traveler_type IS NOT NULL AND e.traveler_type != '' AND e.traveler_type NOT IN ('Unknown', 'NULL')
        GROUP BY e.traveler_type, e.inferred_gender
        ORDER BY review_count DESC
        """
        demo_df = client.query(demo_query).to_dataframe()
        data['demographics'] = demo_df.to_dict(orient='records')
        
        # 4. Get top positive phrases
        pos_phrase_query = f"""
        SELECT s.treemap_name AS phrase, s.aspect_id, COUNT(*) AS mentions
        FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
        JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {where} AND LOWER(s.sentiment_type) = 'positive'
        AND s.treemap_name IS NOT NULL AND s.treemap_name != ''
        GROUP BY s.treemap_name, s.aspect_id
        ORDER BY mentions DESC
        LIMIT 10
        """
        pos_df = client.query(pos_phrase_query).to_dataframe()
        pos_df['aspect_name'] = pos_df['aspect_id'].map(ASPECT_MAP)
        data['positive_phrases'] = pos_df.to_dict(orient='records')
        
        # 5. Get top negative phrases
        neg_phrase_query = f"""
        SELECT s.treemap_name AS phrase, s.aspect_id, COUNT(*) AS mentions
        FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
        JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {where} AND LOWER(s.sentiment_type) = 'negative'
        AND s.treemap_name IS NOT NULL AND s.treemap_name != ''
        GROUP BY s.treemap_name, s.aspect_id
        ORDER BY mentions DESC
        LIMIT 10
        """
        neg_df = client.query(neg_phrase_query).to_dataframe()
        neg_df['aspect_name'] = neg_df['aspect_id'].map(ASPECT_MAP)
        data['negative_phrases'] = neg_df.to_dict(orient='records')
        
        # 6. If traveler type specified, get segment-specific data
        if intent.get("traveler_type"):
            ttype = intent["traveler_type"]
            segment_query = f"""
            SELECT 
                s.aspect_id,
                ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
                      NULLIF(COUNT(*), 0), 0) AS satisfaction
            FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
            JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
            JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
            JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
            WHERE {where} AND e.traveler_type = '{ttype}'
            GROUP BY s.aspect_id
            ORDER BY satisfaction DESC
            """
            seg_df = client.query(segment_query).to_dataframe()
            seg_df['aspect_name'] = seg_df['aspect_id'].map(ASPECT_MAP)
            data['segment_satisfaction'] = seg_df.to_dict(orient='records')
        
    except Exception as e:
        print(f"Error gathering context: {e}")
    
    return data

def format_data_for_llm(data: dict, hotel: str = None, brand: str = None):
    """Format data for LLM consumption - with hotel/brand context in each line"""
    name = hotel or brand or "Hotel"
    sections = []
    
    # Satisfaction by aspect (with hotel/brand name)
    if data.get('satisfaction'):
        sections.append(f"## {name} Satisfaction by Aspect")
        for item in data['satisfaction']:
            aspect = item.get('aspect_name', 'Unknown')
            sat = int(item.get('satisfaction', 0))
            sections.append(f"- {name} {aspect}: {sat}%")
    
    # Weakest aspects (critical for improvements)
    if data.get('weakest'):
        sections.append(f"\n## {name} Weakest Aspects (Areas to Improve)")
        for item in data['weakest']:
            aspect = item.get('aspect_name', 'Unknown')
            sat = int(item.get('satisfaction', 0))
            sections.append(f"- {name} {aspect}: {sat}%")
    
    # Demographics
    if data.get('demographics'):
        sections.append(f"\n## {name} Guest Demographics")
        
        # Aggregate by traveler type
        traveler_totals = {}
        gender_totals = {}
        for item in data['demographics']:
            ttype = item.get('traveler_type')
            gender = item.get('gender')
            count = item.get('review_count', 0)
            if ttype and ttype not in ('Unknown', 'NULL', ''):
                traveler_totals[ttype] = traveler_totals.get(ttype, 0) + count
            if gender and gender not in ('Unknown', 'NULL', ''):
                gender_totals[gender] = gender_totals.get(gender, 0) + count
        
        total_classified = sum(traveler_totals.values())
        if total_classified > 0:
            sections.append("### Traveler Type Mix (among classified reviews)")
            for ttype, count in sorted(traveler_totals.items(), key=lambda x: -x[1]):
                pct = round(count * 100 / total_classified)
                sections.append(f"- {ttype}: {pct}%")
            
            total_gender = sum(gender_totals.values())
            if total_gender > 0:
                sections.append("### Gender Mix")
                for gender, count in sorted(gender_totals.items(), key=lambda x: -x[1]):
                    pct = round(count * 100 / total_gender)
                    label = "Male" if gender == "Male" else "Female" if gender == "Female" else gender
                    sections.append(f"- {label}: {pct}%")
    
    # Segment-specific data
    if data.get('segment_satisfaction'):
        sections.append(f"\n## Segment-Specific Satisfaction")
        for item in data['segment_satisfaction']:
            aspect = item.get('aspect_name', 'Unknown')
            sat = int(item.get('satisfaction', 0))
            sections.append(f"- {aspect}: {sat}%")
    
    # Top phrases (what guests actually say)
    if data.get('positive_phrases'):
        sections.append(f"\n## What Guests Love (Top Positive Phrases)")
        for item in data['positive_phrases'][:5]:
            phrase = item.get('phrase', '')
            aspect = item.get('aspect_name', '')
            sections.append(f"- \"{phrase}\" ({aspect})")
    
    if data.get('negative_phrases'):
        sections.append(f"\n## What Guests Complain About (Top Negative Phrases)")
        for item in data['negative_phrases'][:5]:
            phrase = item.get('phrase', '')
            aspect = item.get('aspect_name', '')
            sections.append(f"- \"{phrase}\" ({aspect})")
    
    return "\n".join(sections)

@app.post("/api/chat")
async def chat(request: ChatRequest):
    """Chat endpoint with data-first approach - matching smartphones architecture"""
    if not client:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    # 1. Spell correction
    corrected_message, was_corrected = correct_spelling(request.message)
    
    # 2. Detect intent
    intent = detect_intent(corrected_message, request.hotel, request.brand)
    
    # 3. Gather REAL data from BigQuery
    bq_data = gather_context_data(intent)
    
    # 4. Format data for LLM (with hotel/brand context)
    data_context = format_data_for_llm(bq_data, request.hotel, request.brand)
    
    # 5. Determine query type for response format
    is_strategic = intent["analysis_type"] in ["improvements", "comparison", "driver"] or \
                   any(w in corrected_message.lower() for w in ["fix", "improve", "want", "prefer", "should", "recommend"])
    
    is_faq = intent["analysis_type"] == "faq"
    is_adcopy = intent["analysis_type"] == "adcopy"
    
    # 6. Build prompt (matching smartphones format)
    name = request.hotel or request.brand or "the hotel"
    
    system_prompt = f"""You are SmaartAnalyst, an AI hotel insights analyst. You help hotel managers understand guest sentiment and take action.

CRITICAL RULES:
1. ONLY use the exact numbers from the DATA section below - NEVER invent statistics
2. If data is not available for something, say so clearly
3. Round all percentages to whole numbers (67% not 67.3%)
4. Keep responses concise - under 200 words unless FAQ or AdCopy format
5. If the user writes in Hindi, Tamil, Telugu, or another Indian language, respond in that language
6. Always include the hotel/brand name when citing statistics

CURRENT CONTEXT: {name}

DATA (USE ONLY THESE NUMBERS - DO NOT HALLUCINATE):
{data_context}
"""

    # Add format-specific instructions
    if is_faq:
        system_prompt += f"""
RESPONSE FORMAT - SEO FAQs:
Generate 5-6 SEO-optimized FAQs. Each question should:
- Include "{name}" in the question (for SEO)
- Be phrased as natural Google search queries
- Be answered with specific % from the data above

Example format:
❓ **Frequently Asked Questions**

1. **Is {name} good for families?**
   [Answer using Family traveler data and relevant aspect scores]

2. **How is the food at {name}?**
   [Answer using Dining satisfaction %]

3. **Is {name} clean and hygienic?**
   [Answer using Cleanliness satisfaction %]

4. **What are the pros and cons of {name}?**
   [List top 3 strengths and top 3 weaknesses with %]

5. **Is {name} worth the price?**
   [Answer using Value for Money satisfaction %]
"""
    elif is_adcopy:
        system_prompt += f"""
RESPONSE FORMAT - Marketing Copy:
Generate ready-to-use marketing copy based on the data.

📢 **Google Ads Headlines** (30 chars max each):
1. [Strength-based headline]
2. [Guest phrase headline]  
3. [Competitive angle]

📱 **Social Media Copy** (Facebook/Instagram - 60-80 words):
[Conversational, emoji-friendly, highlight top strengths]

🎯 **Key Selling Points** (for sales team):
✅ LEAD WITH:
- [{name} top strength]: [X]% satisfaction — [one-liner pitch]
- [Second strength]: [X]% — [pitch]

⚠️ AVOID MENTIONING:
- [Weakness]: [X]% — [why to avoid in marketing]

📝 **Product Page Copy** (100 words):
[Longer form copy using guest phrases and satisfaction % as social proof]

🌐 **Hindi Version**:
[Translate the Social Media Copy to Hindi]
"""
    elif is_strategic:
        system_prompt += f"""
RESPONSE FORMAT - Strategic Insight:
📊 **Insight**: [2-3 sentences with EXACT % from data. What's the key finding?]

👥 **Guest Mix**: [Traveler type breakdown if available - Family X%, Business Y%, etc.]

🎯 **Actions by Department**:

📦 **Operations**: [Specific operational action based on weakest aspects]

📢 **Marketing**: 
   ✓ PROMOTE: [Top strength aspects with %]
   ✗ AVOID: [Weak aspects - don't highlight in marketing]

🛏️ **Housekeeping**: [If Room or Cleanliness is weak, specific action]

🍽️ **F&B**: [If Dining is relevant, specific action]

🛎️ **Front Desk**: [If Staff is relevant, specific action]
"""
    else:
        system_prompt += f"""
RESPONSE FORMAT - Simple Insight:
📊 **Insight**: [2-3 sentences with EXACT % from data. Answer the user's question directly.]

👥 **Guest Mix**: [Traveler type breakdown if available]
"""

    # 7. Generate response using BigQuery ML
    try:
        prompt_escaped = (system_prompt + f"\n\nUser Question: {corrected_message}").replace("'", "''").replace("\\", "\\\\")
        
        ml_query = f"""
        SELECT ml_generate_text_llm_result AS response
        FROM ML.GENERATE_TEXT(
            MODEL `{PROJECT}.{DATASET}.gemini_pro`,
            (SELECT '{prompt_escaped}' AS prompt),
            STRUCT(
                0.3 AS temperature,
                1500 AS max_output_tokens,
                TRUE AS flatten_json_output
            )
        )
        """
        
        result = client.query(ml_query).to_dataframe()
        
        if not result.empty and result['response'].iloc[0]:
            response_text = result['response'].iloc[0]
            
            # Add spell correction notice if applicable
            if was_corrected:
                response_text = f"✏️ *Interpreted as: {corrected_message}*\n\n" + response_text
            
            return {
                "response": response_text,
                "intent": intent,
                "corrected": was_corrected,
                "data_context": data_context[:500] + "..." if len(data_context) > 500 else data_context
            }
        else:
            # Fallback response with actual data
            fallback = f"📊 **{name} Overview**\n\n"
            if bq_data.get('satisfaction'):
                fallback += "**Satisfaction by Aspect:**\n"
                for item in bq_data['satisfaction'][:5]:
                    fallback += f"- {item.get('aspect_name')}: {int(item.get('satisfaction', 0))}%\n"
            if bq_data.get('weakest'):
                fallback += "\n**Areas to Improve:**\n"
                for item in bq_data['weakest'][:3]:
                    fallback += f"- {item.get('aspect_name')}: {int(item.get('satisfaction', 0))}%\n"
            
            if was_corrected:
                fallback = f"✏️ *Interpreted as: {corrected_message}*\n\n" + fallback
                
            return {
                "response": fallback,
                "intent": intent,
                "corrected": was_corrected
            }
            
    except Exception as e:
        print(f"Chat error: {e}")
        # Return data even if LLM fails
        fallback = f"I encountered an error generating the narrative, but here's the data for {name}:\n\n{data_context[:1000]}"
        return {
            "response": fallback,
            "intent": intent,
            "corrected": was_corrected,
            "error": str(e)
        }

@app.get("/api/usage")
async def get_usage():
    """Get API usage stats (for rate limiting display)"""
    return {"requests": 4, "limit": 4, "remaining": 4}

# ─────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "healthy", "database": "connected" if client else "disconnected"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
