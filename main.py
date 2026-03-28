"""
Smaartbrand Hotels - FastAPI Backend v3
With proper credential handling and logging
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import base64
import traceback
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
# CREDENTIALS & CLIENT
# ─────────────────────────────────────────
client = None

def init_client():
    global client
    if client is not None:
        return client
    
    gcp_creds = os.environ.get("GCP_CREDENTIALS_JSON", "")
    print(f"[DEBUG] GCP_CREDENTIALS_JSON length: {len(gcp_creds)}")
    
    if not gcp_creds:
        print("[ERROR] GCP_CREDENTIALS_JSON not set")
        return None
    
    gcp_creds = gcp_creds.strip().strip('"').strip("'")
    
    try:
        if gcp_creds.startswith("{"):
            print("[DEBUG] Parsing as raw JSON")
            creds_dict = json.loads(gcp_creds)
        else:
            print("[DEBUG] Parsing as base64")
            padding = 4 - len(gcp_creds) % 4
            if padding != 4:
                gcp_creds += "=" * padding
            decoded = base64.b64decode(gcp_creds).decode('utf-8')
            creds_dict = json.loads(decoded)
        
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        print(f"[SUCCESS] BigQuery client initialized for project: {credentials.project_id}")
        return client
    except Exception as e:
        print(f"[ERROR] Credential error: {e}")
        traceback.print_exc()
        return None

@app.on_event("startup")
async def startup():
    init_client()

def get_client():
    global client
    if client is None:
        init_client()
    return client

# ─────────────────────────────────────────
# API ENDPOINTS
# ─────────────────────────────────────────

@app.get("/")
async def root():
    try:
        with open("index.html", "r") as f:
            return HTMLResponse(content=f.read())
    except Exception as e:
        return HTMLResponse(content=f"<h1>Error loading page: {e}</h1>")

@app.get("/health")
async def health():
    c = get_client()
    gcp_creds = os.environ.get("GCP_CREDENTIALS_JSON", "")
    return {
        "status": "healthy" if c else "degraded",
        "database": "connected" if c else "disconnected",
        "env_var_set": bool(gcp_creds),
        "env_var_length": len(gcp_creds),
        "env_var_starts_with": gcp_creds[:20] if gcp_creds else "EMPTY",
        "all_env_keys": [k for k in os.environ.keys() if 'GCP' in k or 'GOOGLE' in k or 'CRED' in k]
    }

@app.get("/debug")
async def debug():
    """Debug endpoint to check credentials"""
    gcp_creds = os.environ.get("GCP_CREDENTIALS_JSON", "")
    
    result = {
        "env_var_set": bool(gcp_creds),
        "env_var_length": len(gcp_creds),
        "starts_with_brace": gcp_creds.strip().startswith("{") if gcp_creds else False,
        "starts_with_ey": gcp_creds.strip().startswith("ey") if gcp_creds else False,
        "has_quotes": gcp_creds.startswith('"') or gcp_creds.startswith("'") if gcp_creds else False,
        "client_initialized": client is not None
    }
    
    # Try to parse
    if gcp_creds:
        gcp_creds = gcp_creds.strip().strip('"').strip("'")
        try:
            if gcp_creds.startswith("{"):
                creds_dict = json.loads(gcp_creds)
                result["parse_method"] = "raw_json"
                result["project_id"] = creds_dict.get("project_id", "NOT_FOUND")
            else:
                padding = 4 - len(gcp_creds) % 4
                if padding != 4:
                    gcp_creds += "=" * padding
                decoded = base64.b64decode(gcp_creds).decode('utf-8')
                creds_dict = json.loads(decoded)
                result["parse_method"] = "base64"
                result["project_id"] = creds_dict.get("project_id", "NOT_FOUND")
        except Exception as e:
            result["parse_error"] = str(e)
    
    return result

@app.get("/debug/aspects")
async def debug_aspects():
    """Debug endpoint to check what aspect IDs exist in the database"""
    c = get_client()
    if not c:
        return {"error": "Database connection failed"}
    
    query = f"""
    SELECT DISTINCT aspect_id, COUNT(*) as count
    FROM `{PROJECT}.{DATASET}.product_user_review_sentiment`
    GROUP BY aspect_id
    ORDER BY aspect_id
    """
    
    try:
        result = c.query(query).to_dataframe()
        return {
            "aspect_ids_in_db": result.to_dict(orient='records'),
            "aspect_map_in_code": ASPECT_MAP
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/brands")
async def get_brands():
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed. Check GCP_CREDENTIALS_JSON.")
    
    query = f"""
    SELECT DISTINCT pd.Brand
    FROM `{PROJECT}.{DATASET}.product_description` pd
    WHERE pd.Brand IS NOT NULL AND pd.Brand != ''
    ORDER BY pd.Brand
    """
    
    try:
        result = c.query(query).to_dataframe()
        return result['Brand'].tolist()
    except Exception as e:
        print(f"[ERROR] get_brands: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/cities")
async def get_cities(brand: Optional[str] = None):
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    where_clause = ""
    if brand:
        brand_escaped = brand.replace("'", "''")
        where_clause = f"AND pd.Brand = '{brand_escaped}'"
    
    query = f"""
    SELECT DISTINCT pl.City
    FROM `{PROJECT}.{DATASET}.product_list` pl
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE pl.City IS NOT NULL AND pl.City != ''
    {where_clause}
    ORDER BY pl.City
    """
    
    try:
        result = c.query(query).to_dataframe()
        return result['City'].tolist()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/star_categories")
async def get_star_categories(brand: Optional[str] = None, city: Optional[str] = None):
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    where_clauses = ["pl.Star_Category IS NOT NULL"]
    if brand:
        where_clauses.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
    if city:
        where_clauses.append(f"pl.City = '{city.replace(chr(39), chr(39)+chr(39))}'")
    
    query = f"""
    SELECT DISTINCT pl.Star_Category
    FROM `{PROJECT}.{DATASET}.product_list` pl
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE {' AND '.join(where_clauses)}
    ORDER BY pl.Star_Category
    """
    
    try:
        result = c.query(query).to_dataframe()
        return [int(x) for x in result['Star_Category'].tolist()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hotels")
async def get_hotels(brand: Optional[str] = None, city: Optional[str] = None, star_category: Optional[int] = None):
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    where_clauses = ["pd.Brand IS NOT NULL", "pl.Name IS NOT NULL"]
    if brand:
        where_clauses.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
    if city:
        where_clauses.append(f"pl.City = '{city.replace(chr(39), chr(39)+chr(39))}'")
    if star_category:
        where_clauses.append(f"pl.Star_Category = {star_category}")
    
    query = f"""
    SELECT DISTINCT 
        pl.Name AS hotel_name,
        pd.Brand,
        pl.Star_Category AS star_category,
        pl.City
    FROM `{PROJECT}.{DATASET}.product_list` pl
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE {' AND '.join(where_clauses)}
    ORDER BY pl.Name
    """
    
    try:
        result = c.query(query).to_dataframe()
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/satisfaction")
async def get_satisfaction(
    hotel: Optional[str] = None, 
    brand: Optional[str] = None,
    traveler_type: Optional[str] = None,
    gender: Optional[str] = None
):
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    if not hotel and not brand:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    where_clauses = []
    if hotel:
        where_clauses.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
    elif brand:
        where_clauses.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
    
    if traveler_type:
        where_clauses.append(f"e.traveler_type = '{traveler_type.replace(chr(39), chr(39)+chr(39))}'")
    if gender:
        where_clauses.append(f"e.inferred_gender = '{gender.replace(chr(39), chr(39)+chr(39))}'")
    
    where_clause = "WHERE " + " AND ".join(where_clauses)
    
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
    ORDER BY satisfaction DESC
    """
    
    try:
        result = c.query(query).to_dataframe()
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        result['icon'] = result['aspect_name'].map(ASPECT_ICONS)
        return result.to_dict(orient='records')
    except Exception as e:
        print(f"[ERROR] get_satisfaction: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/drivers")
async def get_drivers(
    hotel: Optional[str] = None, 
    brand: Optional[str] = None,
    traveler_type: Optional[str] = None,
    gender: Optional[str] = None
):
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    if not hotel and not brand:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    where_clauses = []
    if hotel:
        where_clauses.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
    elif brand:
        where_clauses.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
    
    if traveler_type:
        where_clauses.append(f"e.traveler_type = '{traveler_type.replace(chr(39), chr(39)+chr(39))}'")
    if gender:
        where_clauses.append(f"e.inferred_gender = '{gender.replace(chr(39), chr(39)+chr(39))}'")
    
    where_clause = "WHERE " + " AND ".join(where_clauses)
    
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
        result = c.query(query).to_dataframe()
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        result['icon'] = result['aspect_name'].map(ASPECT_ICONS)
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/demographics")
async def get_demographics(hotel: Optional[str] = None, brand: Optional[str] = None):
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    if not hotel and not brand:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    where_clauses = []
    if hotel:
        where_clauses.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
    elif brand:
        where_clauses.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
    
    base_where = " AND ".join(where_clauses)
    
    traveler_query = f"""
    SELECT e.traveler_type, COUNT(DISTINCT e.id) AS review_count
    FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE {base_where} AND e.traveler_type IS NOT NULL AND e.traveler_type != ''
    GROUP BY e.traveler_type ORDER BY review_count DESC
    """
    
    gender_query = f"""
    SELECT e.inferred_gender AS gender, COUNT(DISTINCT e.id) AS review_count
    FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE {base_where} AND e.inferred_gender IS NOT NULL AND e.inferred_gender != ''
    GROUP BY e.inferred_gender ORDER BY review_count DESC
    """
    
    purpose_query = f"""
    SELECT e.stay_purpose, COUNT(DISTINCT e.id) AS review_count
    FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE {base_where} AND e.stay_purpose IS NOT NULL AND e.stay_purpose != ''
    GROUP BY e.stay_purpose ORDER BY review_count DESC
    """
    
    try:
        return {
            "traveler_type": c.query(traveler_query).to_dataframe().to_dict(orient='records'),
            "gender": c.query(gender_query).to_dataframe().to_dict(orient='records'),
            "stay_purpose": c.query(purpose_query).to_dataframe().to_dict(orient='records')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/traveler_preferences")
async def get_traveler_preferences(hotel: Optional[str] = None, brand: Optional[str] = None):
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    if not hotel and not brand:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    where_clauses = []
    if hotel:
        where_clauses.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
    elif brand:
        where_clauses.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
    
    query = f"""
    SELECT 
        e.traveler_type,
        s.aspect_id,
        ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(*), 0), 0) AS satisfaction,
        COUNT(*) AS mentions
    FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
    JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE {' AND '.join(where_clauses)}
    AND e.traveler_type IS NOT NULL AND e.traveler_type != ''
    GROUP BY e.traveler_type, s.aspect_id
    ORDER BY e.traveler_type, s.aspect_id
    """
    
    try:
        result = c.query(query).to_dataframe()
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        
        data = {}
        for _, row in result.iterrows():
            tt = row['traveler_type']
            if tt not in data:
                data[tt] = {'traveler_type': tt, 'aspects': {}}
            data[tt]['aspects'][row['aspect_name']] = {
                'satisfaction': int(row['satisfaction']),
                'mentions': int(row['mentions'])
            }
        return list(data.values())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stay_purpose_preferences")
async def get_stay_purpose_preferences(hotel: Optional[str] = None, brand: Optional[str] = None):
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    if not hotel and not brand:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    where_clauses = []
    if hotel:
        where_clauses.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
    elif brand:
        where_clauses.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
    
    query = f"""
    SELECT 
        e.stay_purpose,
        s.aspect_id,
        ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(*), 0), 0) AS satisfaction,
        COUNT(*) AS mentions
    FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
    JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE {' AND '.join(where_clauses)}
    AND e.stay_purpose IS NOT NULL AND e.stay_purpose != ''
    GROUP BY e.stay_purpose, s.aspect_id
    ORDER BY e.stay_purpose, s.aspect_id
    """
    
    try:
        result = c.query(query).to_dataframe()
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        
        data = {}
        for _, row in result.iterrows():
            sp = row['stay_purpose']
            if sp not in data:
                data[sp] = {'stay_purpose': sp, 'aspects': {}}
            data[sp]['aspects'][row['aspect_name']] = {
                'satisfaction': int(row['satisfaction']),
                'mentions': int(row['mentions'])
            }
        return list(data.values())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/comparison")
async def get_comparison(
    items: str,
    compare_by: str = "brand",
    traveler_type: Optional[str] = None,
    gender: Optional[str] = None
):
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    item_list = [i.strip() for i in items.split(",") if i.strip()]
    if len(item_list) < 2:
        raise HTTPException(status_code=400, detail="At least 2 items required")
    
    items_sql = "', '".join([i.replace("'", "''") for i in item_list])
    name_field = "pl.Name" if compare_by == "hotel" else "pd.Brand"
    
    extra_where = ""
    if traveler_type:
        extra_where += f" AND e.traveler_type = '{traveler_type.replace(chr(39), chr(39)+chr(39))}'"
    if gender:
        extra_where += f" AND e.inferred_gender = '{gender.replace(chr(39), chr(39)+chr(39))}'"
    
    query = f"""
    SELECT 
        {name_field} AS item_name,
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
    WHERE {name_field} IN ('{items_sql}') {extra_where}
    GROUP BY {name_field}, s.aspect_id
    ORDER BY {name_field}, s.aspect_id
    """
    
    try:
        result = c.query(query).to_dataframe()
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        
        comparison = {}
        for item in item_list:
            item_data = result[result['item_name'] == item]
            if not item_data.empty:
                total_pos = item_data['positive_count'].sum()
                total_neg = item_data['negative_count'].sum()
                comparison[item] = {
                    "aspects": item_data.to_dict(orient='records'),
                    "overall": {
                        "positive": int(total_pos),
                        "negative": int(total_neg),
                        "total_mentions": int(total_pos + total_neg),
                        "satisfaction": round(total_pos * 100 / max(total_pos + total_neg, 1))
                    }
                }
            else:
                comparison[item] = {"aspects": [], "overall": {"positive": 0, "negative": 0, "total_mentions": 0, "satisfaction": 0}}
        
        return comparison
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class ChatRequest(BaseModel):
    message: str
    hotel: Optional[str] = None
    brand: Optional[str] = None

@app.post("/api/chat")
async def chat(request: ChatRequest):
    return {
        "response": f"Chat functionality coming soon. You asked about: {request.message}",
        "hotel": request.hotel,
        "brand": request.brand
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
