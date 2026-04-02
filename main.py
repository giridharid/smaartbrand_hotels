"""
Smaartbrand Hotels - FastAPI Backend v3
With proper credential handling and logging
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import base64
import traceback
from pydantic import BaseModel
from typing import Optional, List

app = FastAPI(title="Smaartbrand Hotels API")

# Serve logo file
@app.get("/acquink_logo.png")
async def get_logo():
    return FileResponse("acquink_logo.png", media_type="image/jpeg")

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
    if city and city.strip():
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
    if city and city.strip():
        where_clauses.append(f"pl.City = '{city.replace(chr(39), chr(39)+chr(39))}'")
    if star_category:
        where_clauses.append(f"pl.Star_Category = {star_category}")
    
    query = f"""
    SELECT DISTINCT 
        pl.product_id,
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

@app.get("/api/hotels/all")
async def get_all_hotels():
    """Get all hotels for search cache"""
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    query = f"""
    SELECT DISTINCT 
        pl.product_id,
        pl.Name AS hotel_name,
        pd.Brand,
        pl.Star_Category AS star_category,
        pl.City
    FROM `{PROJECT}.{DATASET}.product_list` pl
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE pd.Brand IS NOT NULL AND pl.Name IS NOT NULL
    ORDER BY pl.Name
    """
    
    try:
        result = c.query(query).to_dataframe()
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/hotel_details")
async def get_hotel_details(
    hotel: Optional[str] = None, 
    brand: Optional[str] = None,
    city: Optional[str] = None,
    star: Optional[str] = None
):
    """Get details for a specific hotel or brand summary"""
    c = get_client()
    if not c:
        raise HTTPException(status_code=500, detail="Database connection failed")
    
    if hotel:
        query = f"""
        SELECT 
            pd.Name, pd.Brand, pd.About_Us, pd.Address, pd.Phone, pd.Website,
            pd.Rating, pd.Votes, pl.City, pl.Star_Category,
            pdt.review_count, pdt.positive_review_count, pdt.negative_review_count
        FROM `{PROJECT}.{DATASET}.product_description` pd
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON pd.product_id = pl.product_id
        LEFT JOIN `{PROJECT}.{DATASET}.product_detail` pdt ON pd.product_id = pdt.product_id
        WHERE pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'
        LIMIT 1
        """
    elif brand:
        # Build where clauses for brand filtering
        where_parts = [f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'"]
        if city and city.strip():
            where_parts.append(f"pl.City = '{city.replace(chr(39), chr(39)+chr(39))}'")
        if star and star.strip():
            where_parts.append(f"pl.Star_Category = {int(star)}")
        where_clause = " AND ".join(where_parts)
        
        query = f"""
        SELECT 
            pd.Brand as Name, pd.Brand,
            COUNT(DISTINCT pd.product_id) as hotel_count,
            ROUND(AVG(pd.Rating), 1) as Rating,
            SUM(pd.Votes) as Votes,
            SUM(pdt.review_count) as review_count,
            SUM(pdt.positive_review_count) as positive_review_count,
            SUM(pdt.negative_review_count) as negative_review_count
        FROM `{PROJECT}.{DATASET}.product_description` pd
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON pd.product_id = pl.product_id
        LEFT JOIN `{PROJECT}.{DATASET}.product_detail` pdt ON pd.product_id = pdt.product_id
        WHERE {where_clause}
        GROUP BY pd.Brand
        """
    else:
        raise HTTPException(status_code=400, detail="Either hotel or brand parameter required")
    
    try:
        result = c.query(query).to_dataframe()
        if result.empty:
            return {"error": "Not found"}
        return result.iloc[0].to_dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/satisfaction")
async def get_satisfaction(
    hotel: Optional[str] = None, 
    brand: Optional[str] = None,
    city: Optional[str] = None,
    star: Optional[str] = None,
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
    
    if city and city.strip():
        where_clauses.append(f"pl.City = '{city.replace(chr(39), chr(39)+chr(39))}'")
    if star and star.strip():
        where_clauses.append(f"pl.Star_Category = {int(star)}")
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
        # Convert aspect_id to int to match ASPECT_MAP keys
        result['aspect_id'] = result['aspect_id'].astype(int)
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
    city: Optional[str] = None,
    star: Optional[str] = None,
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
    
    if city and city.strip():
        where_clauses.append(f"pl.City = '{city.replace(chr(39), chr(39)+chr(39))}'")
    if star and star.strip():
        where_clauses.append(f"pl.Star_Category = {int(star)}")
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
        # Convert aspect_id to int to match ASPECT_MAP keys
        result['aspect_id'] = result['aspect_id'].astype(int)
        result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        result['icon'] = result['aspect_name'].map(ASPECT_ICONS)
        return result.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/demographics")
async def get_demographics(
    hotel: Optional[str] = None, 
    brand: Optional[str] = None,
    city: Optional[str] = None,
    star: Optional[str] = None
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
    
    if city and city.strip():
        where_clauses.append(f"pl.City = '{city.replace(chr(39), chr(39)+chr(39))}'")
    if star and star.strip():
        where_clauses.append(f"pl.Star_Category = {int(star)}")
    
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
async def get_traveler_preferences(
    hotel: Optional[str] = None, 
    brand: Optional[str] = None,
    city: Optional[str] = None,
    star: Optional[str] = None
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
    
    if city and city.strip():
        where_clauses.append(f"pl.City = '{city.replace(chr(39), chr(39)+chr(39))}'")
    if star and star.strip():
        where_clauses.append(f"pl.Star_Category = {int(star)}")
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
        # Convert aspect_id to int to match ASPECT_MAP keys
        result['aspect_id'] = result['aspect_id'].astype(int)
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
async def get_stay_purpose_preferences(
    hotel: Optional[str] = None, 
    brand: Optional[str] = None,
    city: Optional[str] = None,
    star: Optional[str] = None
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
    
    if city and city.strip():
        where_clauses.append(f"pl.City = '{city.replace(chr(39), chr(39)+chr(39))}'")
    if star and star.strip():
        where_clauses.append(f"pl.Star_Category = {int(star)}")
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
        # Convert aspect_id to int to match ASPECT_MAP keys
        result['aspect_id'] = result['aspect_id'].astype(int)
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
    
    # Support both ||| (new) and , (old) delimiters
    if '|||' in items:
        item_list = [i.strip() for i in items.split("|||") if i.strip()]
    else:
        item_list = [i.strip() for i in items.split(",") if i.strip()]
    
    if len(item_list) < 2:
        raise HTTPException(status_code=400, detail="At least 2 items required")
    
    print(f"[COMPARE] compare_by={compare_by}, items={item_list}")
    
    items_sql = "', '".join([i.replace("'", "''") for i in item_list])
    
    # For hotels, use product_id; for brands, use brand name
    if compare_by == "hotel":
        id_field = "pl.product_id"
        name_field = "pl.product_id"  # Return product_id as key
        # Also fetch hotel names for display
        select_extra = ", pl.Name AS display_name"
        group_extra = ", pl.Name"
    else:
        id_field = "pd.Brand"
        name_field = "pd.Brand"
        select_extra = ""
        group_extra = ""
    
    extra_where = ""
    if traveler_type:
        extra_where += f" AND e.traveler_type = '{traveler_type.replace(chr(39), chr(39)+chr(39))}'"
    if gender:
        extra_where += f" AND e.inferred_gender = '{gender.replace(chr(39), chr(39)+chr(39))}'"
    
    query = f"""
    SELECT 
        CAST({name_field} AS STRING) AS item_name,
        s.aspect_id,
        SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) AS positive_count,
        SUM(CASE WHEN LOWER(s.sentiment_type) = 'negative' THEN 1 ELSE 0 END) AS negative_count,
        COUNT(*) AS total_mentions,
        ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(*), 0), 0) AS satisfaction
        {select_extra}
    FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
    JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
    JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
    JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
    WHERE CAST({id_field} AS STRING) IN ('{items_sql}') {extra_where}
    GROUP BY {name_field}, s.aspect_id {group_extra}
    ORDER BY {name_field}, s.aspect_id
    """
    
    try:
        result = c.query(query).to_dataframe()
        print(f"[COMPARE] Query returned {len(result)} rows")
        if not result.empty:
            print(f"[COMPARE] Unique items in result: {result['item_name'].unique().tolist()}")
        else:
            print(f"[COMPARE] Query returned EMPTY result")
        
        # Convert aspect_id to int to match ASPECT_MAP keys
        if not result.empty:
            result['aspect_id'] = result['aspect_id'].astype(int)
            result['aspect_name'] = result['aspect_id'].map(ASPECT_MAP)
        
        comparison = {}
        for item in item_list:
            item_data = result[result['item_name'] == item]
            print(f"[COMPARE] Item '{item}' found {len(item_data)} rows")
            if not item_data.empty:
                total_pos = item_data['positive_count'].sum()
                total_neg = item_data['negative_count'].sum()
                # Get display_name for hotels (first row since all same)
                display_name = item_data['display_name'].iloc[0] if 'display_name' in item_data.columns else item
                comparison[item] = {
                    "display_name": display_name,
                    "aspects": item_data.to_dict(orient='records'),
                    "overall": {
                        "positive": int(total_pos),
                        "negative": int(total_neg),
                        "total_mentions": int(total_pos + total_neg),
                        "satisfaction": round(total_pos * 100 / max(total_pos + total_neg, 1))
                    }
                }
            else:
                comparison[item] = {"display_name": item, "aspects": [], "overall": {"positive": 0, "negative": 0, "total_mentions": 0, "satisfaction": 0}}
        
        return comparison
    except Exception as e:
        print(f"[COMPARE] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────
# AUTH ENDPOINT
# ─────────────────────────────────────────
class AuthRequest(BaseModel):
    key: str

@app.post("/api/auth")
async def authenticate(request: AuthRequest):
    """Validate admin key for full access"""
    admin_key = os.environ.get("SMAARTBRAND_ADMIN_KEY", "")
    
    if not admin_key:
        # No key configured - deny all
        return {"success": False, "message": "No admin key configured"}
    
    if request.key == admin_key:
        return {"success": True, "message": "Full access granted"}
    else:
        return {"success": False, "message": "Invalid key"}

# ─────────────────────────────────────────
# CHAT ENDPOINT - Data First + Agent Formatting
# Fetch accurate data from BigQuery, send to agent for intelligent formatting
# ─────────────────────────────────────────
import uuid

class ChatRequest(BaseModel):
    message: str
    hotel: Optional[str] = None
    brand: Optional[str] = None
    conversation_id: Optional[str] = None

# Data Agent Configuration
AGENT_ID = "agent_b9a402f4-9a19-40c7-849e-e1df4f3ad0b2"
LOCATION = "global"

# Agent System Prompt - emphasizes using provided data exactly
AGENT_SYSTEM_PROMPT = """You are SmaartAnalyst, a hotel decision intelligence assistant.

=== CRITICAL: USE PROVIDED DATA EXACTLY ===
I am providing you with EXACT data from our database. 
DO NOT query the database yourself.
DO NOT modify, round differently, or invent any numbers.
USE THE EXACT PERCENTAGES AND PHRASES PROVIDED BELOW.
If data is not provided, say "data not available" — do NOT make up numbers.

=== WHO YOU SERVE ===
Hotel operations teams: Brand Manager, SEO & Marketing, Housekeeping, Front Desk, Operations, F&B

=== ASPECT ICONS ===
Dining: 🍽️, Cleanliness: 🧹, Amenities: 🏊, Staff: 👨‍💼, Room: 🛏️, Location: 📍, Value for Money: 💰, General: ⭐

=== RESPONSE FORMAT ===

📊 **Insight**: [2-3 sentences using the EXACT % scores from the data provided]

👥 **Guest Mix**: [Use EXACT segment percentages from data]

🎯 **Actions by Department**:

👔 Brand Manager: [positioning based on data strengths + target top segment]

📢 SEO & Marketing: 
   ✓ PROMOTE: [keywords from top positive phrases]
   ✗ AVOID: [keywords from negative phrases or low scores]
   🎯 Target Audience: [top traveler segment from data]
   Ad copy: [use EXACT guest phrase from data]

🛏️ Housekeeping: [if Room/Cleanliness in data]
🛎️ Front Desk: [if Staff in data]
⚙️ Operations: [action for lowest scoring aspect]
🍽️ F&B: [if Dining in data]

Include 3-4 most relevant departments only.

=== QUERY-SPECIFIC FORMATS ===

**"SEO keywords"** → 
🔑 HIGH PRIORITY: [from top positive phrases]
⚠️ SECONDARY: [from medium aspects]
✗ AVOID: [from negative phrases]

**"Ad copy"** → Headlines (30 chars), Descriptions (90 chars) using EXACT guest phrases

**"FAQs"** → 5-8 Q&A pairs using data provided

**"Compare X vs Y"** → Use • for ≥80%, ○ for <80%

**"R&D" / "new hotel"** → Market analysis from competitor data

**Persona queries** → Filter insights to that segment's data

=== RULES ===
1. USE EXACT NUMBERS from data — no rounding, no inventing
2. USE EXACT PHRASES from the positive/negative lists
3. Be direct — max 250 words
4. Match format to query type
5. If data not provided, say "data not available" """


class ChatDataFetcher:
    """Fetch accurate data from BigQuery for chat context"""
    
    @staticmethod
    def get_satisfaction(c, hotel: str = None, brand: str = None) -> dict:
        where = []
        if hotel:
            where.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
        elif brand:
            where.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
        else:
            return {}
        
        query = f"""
        SELECT s.aspect_id,
            ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 0) as satisfaction
        FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
        JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {' AND '.join(where)}
        GROUP BY s.aspect_id ORDER BY satisfaction DESC
        """
        try:
            result = c.query(query).to_dataframe()
            return {ASPECT_MAP.get(row['aspect_id'], 'Other'): int(row['satisfaction']) for _, row in result.iterrows()}
        except:
            return {}
    
    @staticmethod
    def get_travelers(c, hotel: str = None, brand: str = None) -> dict:
        where = []
        if hotel:
            where.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
        elif brand:
            where.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
        else:
            return {}
        
        query = f"""
        SELECT e.traveler_type, COUNT(DISTINCT e.id) as count
        FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {' AND '.join(where)} AND e.traveler_type IS NOT NULL AND e.traveler_type != ''
        GROUP BY e.traveler_type ORDER BY count DESC
        """
        try:
            result = c.query(query).to_dataframe()
            total = result['count'].sum()
            return {row['traveler_type']: int(round(row['count'] * 100 / total)) for _, row in result.iterrows()}
        except:
            return {}
    
    @staticmethod
    def get_gender(c, hotel: str = None, brand: str = None) -> dict:
        where = []
        if hotel:
            where.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
        elif brand:
            where.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
        else:
            return {}
        
        query = f"""
        SELECT e.inferred_gender as gender, COUNT(DISTINCT e.id) as count
        FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {' AND '.join(where)} AND e.inferred_gender IS NOT NULL AND e.inferred_gender != ''
        GROUP BY e.inferred_gender ORDER BY count DESC
        """
        try:
            result = c.query(query).to_dataframe()
            total = result['count'].sum()
            data = {}
            for _, row in result.iterrows():
                label = 'Male' if row['gender'] == 'M' else 'Female' if row['gender'] == 'F' else row['gender']
                data[label] = int(round(row['count'] * 100 / total))
            return data
        except:
            return {}
    
    @staticmethod
    def get_stay_purpose(c, hotel: str = None, brand: str = None) -> dict:
        where = []
        if hotel:
            where.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
        elif brand:
            where.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
        else:
            return {}
        
        query = f"""
        SELECT e.stay_purpose, COUNT(DISTINCT e.id) as count
        FROM `{PROJECT}.{DATASET}.product_user_review_enriched` e
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {' AND '.join(where)} AND e.stay_purpose IS NOT NULL AND e.stay_purpose != ''
        GROUP BY e.stay_purpose ORDER BY count DESC
        """
        try:
            result = c.query(query).to_dataframe()
            total = result['count'].sum()
            return {row['stay_purpose']: int(round(row['count'] * 100 / total)) for _, row in result.iterrows()}
        except:
            return {}
    
    @staticmethod
    def get_phrases(c, hotel: str = None, brand: str = None, sentiment: str = 'positive', limit: int = 8) -> list:
        where = []
        if hotel:
            where.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
        elif brand:
            where.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
        else:
            return []
        
        where.append(f"LOWER(s.sentiment_type) = '{sentiment}'")
        
        query = f"""
        SELECT s.treemap_name as phrase, s.aspect_id
        FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
        JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {' AND '.join(where)} AND s.treemap_name IS NOT NULL
        GROUP BY s.treemap_name, s.aspect_id
        ORDER BY COUNT(*) DESC LIMIT {limit}
        """
        try:
            result = c.query(query).to_dataframe()
            return [{'phrase': row['phrase'], 'aspect': ASPECT_MAP.get(row['aspect_id'], 'General')} for _, row in result.iterrows()]
        except:
            return []
    
    @staticmethod
    def get_competitors(c, hotel: str = None, brand: str = None) -> list:
        if not hotel and not brand:
            return []
        
        try:
            if hotel:
                city_query = f"SELECT pl.City, pl.Star_Category FROM `{PROJECT}.{DATASET}.product_list` pl WHERE pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}' LIMIT 1"
            else:
                city_query = f"SELECT pl.City, MAX(pl.Star_Category) as Star_Category FROM `{PROJECT}.{DATASET}.product_list` pl JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id WHERE pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}' GROUP BY pl.City LIMIT 1"
            
            city_result = c.query(city_query).to_dataframe()
            if city_result.empty:
                return []
            
            city = city_result.iloc[0]['City']
            star = city_result.iloc[0]['Star_Category']
            
            query = f"""
            SELECT pd.Brand,
                ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 0) as satisfaction
            FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
            JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
            JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
            JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
            WHERE pl.City = '{city}' AND ABS(pl.Star_Category - {star}) <= 1
            GROUP BY pd.Brand ORDER BY satisfaction DESC LIMIT 5
            """
            result = c.query(query).to_dataframe()
            return [{'brand': row['Brand'], 'satisfaction': int(row['satisfaction']), 'city': city} for _, row in result.iterrows()]
        except:
            return []
    
    @staticmethod
    def get_segment_satisfaction(c, hotel: str = None, brand: str = None) -> dict:
        """Get satisfaction by aspect for each traveler type"""
        where = []
        if hotel:
            where.append(f"pl.Name = '{hotel.replace(chr(39), chr(39)+chr(39))}'")
        elif brand:
            where.append(f"pd.Brand = '{brand.replace(chr(39), chr(39)+chr(39))}'")
        else:
            return {}
        
        query = f"""
        SELECT e.traveler_type, s.aspect_id,
            ROUND(SUM(CASE WHEN LOWER(s.sentiment_type) = 'positive' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 0) as satisfaction
        FROM `{PROJECT}.{DATASET}.product_user_review_sentiment` s
        JOIN `{PROJECT}.{DATASET}.product_user_review_enriched` e ON s.user_review_id = e.id
        JOIN `{PROJECT}.{DATASET}.product_list` pl ON e.product_id = pl.product_id
        JOIN `{PROJECT}.{DATASET}.product_description` pd ON pl.product_id = pd.product_id
        WHERE {' AND '.join(where)} AND e.traveler_type IS NOT NULL AND e.traveler_type != ''
        GROUP BY e.traveler_type, s.aspect_id
        """
        try:
            result = c.query(query).to_dataframe()
            data = {}
            for _, row in result.iterrows():
                tt = row['traveler_type']
                aspect = ASPECT_MAP.get(row['aspect_id'], 'Other')
                if tt not in data:
                    data[tt] = {}
                data[tt][aspect] = int(row['satisfaction'])
            return data
        except:
            return {}


def format_data_for_agent(entity: str, entity_type: str, data: dict) -> str:
    """Format fetched data as clear text for the agent"""
    lines = [f"=== EXACT DATA FOR: {entity} ({entity_type}) ===", "USE THESE NUMBERS EXACTLY — DO NOT MODIFY OR INVENT", ""]
    
    # Satisfaction by Aspect
    sat = data.get('satisfaction', {})
    if sat:
        lines.append("SATISFACTION BY ASPECT (use these exact %):")
        for aspect, pct in sorted(sat.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"  • {aspect}: {pct}%")
        lines.append("")
    
    # Guest Mix
    travelers = data.get('travelers', {})
    if travelers:
        lines.append("GUEST MIX — TRAVELER TYPE (use these exact %):")
        for tt, pct in travelers.items():
            lines.append(f"  • {tt}: {pct}%")
        lines.append("")
    
    # Gender
    gender = data.get('gender', {})
    if gender:
        lines.append("GENDER SPLIT (use these exact %):")
        for g, pct in gender.items():
            lines.append(f"  • {g}: {pct}%")
        lines.append("")
    
    # Stay Purpose
    purpose = data.get('stay_purpose', {})
    if purpose:
        lines.append("STAY PURPOSE (use these exact %):")
        for p, pct in purpose.items():
            lines.append(f"  • {p}: {pct}%")
        lines.append("")
    
    # Positive Phrases
    positives = data.get('positives', [])
    if positives:
        lines.append("TOP POSITIVE PHRASES (use these exact phrases for SEO/Ad copy):")
        for p in positives:
            lines.append(f"  • \"{p['phrase']}\" ({p['aspect']})")
        lines.append("")
    
    # Negative Phrases
    negatives = data.get('negatives', [])
    if negatives:
        lines.append("TOP NEGATIVE PHRASES (these are complaints — use for AVOID keywords):")
        for p in negatives:
            lines.append(f"  • \"{p['phrase']}\" ({p['aspect']})")
        lines.append("")
    
    # Competitors
    competitors = data.get('competitors', [])
    if competitors:
        city = competitors[0].get('city', 'same city') if competitors else 'same city'
        lines.append(f"COMPETITOR RANKING in {city} (use these exact %):")
        for i, comp in enumerate(competitors, 1):
            marker = "👑" if i == 1 else f"{i}."
            you = " ← THIS ENTITY" if comp['brand'].lower() in entity.lower() or entity.lower() in comp['brand'].lower() else ""
            lines.append(f"  {marker} {comp['brand']}: {comp['satisfaction']}%{you}")
        lines.append("")
    
    # Segment Satisfaction
    seg_sat = data.get('segment_satisfaction', {})
    if seg_sat:
        lines.append("SATISFACTION BY TRAVELER TYPE (use for persona queries):")
        for tt, aspects in seg_sat.items():
            top3 = sorted(aspects.items(), key=lambda x: x[1], reverse=True)[:3]
            lines.append(f"  {tt}: " + ", ".join([f"{a} {v}%" for a, v in top3]))
        lines.append("")
    
    return "\n".join(lines)


def get_data_chat_client():
    """Get Gemini Data Analytics chat client"""
    try:
        from google.cloud import geminidataanalytics_v1alpha as gda
        from google.api_core import client_options
        
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
            
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            return gda.DataChatServiceClient(credentials=credentials, client_options=client_options.ClientOptions())
        except:
            return gda.DataChatServiceClient()
    except Exception as e:
        print(f"Data chat client error: {e}")
        return None


@app.post("/api/chat")
async def chat(request: ChatRequest):
    try:
        from google.cloud import geminidataanalytics_v1alpha as gda
        
        print(f"[CHAT] Request: hotel={request.hotel}, brand={request.brand}, message={request.message[:50]}...")
        
        # Step 1: Get BigQuery client and fetch accurate data
        bq = get_client()
        if not bq:
            print("[CHAT] BigQuery client unavailable")
            return {"response": "Database unavailable.", "conversation_id": None}
        
        entity = request.hotel or request.brand
        entity_type = 'hotel' if request.hotel else 'brand'
        
        print(f"[CHAT] Fetching data for {entity_type}: {entity}")
        
        # Fetch all data from BigQuery (accurate numbers)
        data = {
            'satisfaction': ChatDataFetcher.get_satisfaction(bq, request.hotel, request.brand),
            'travelers': ChatDataFetcher.get_travelers(bq, request.hotel, request.brand),
            'gender': ChatDataFetcher.get_gender(bq, request.hotel, request.brand),
            'stay_purpose': ChatDataFetcher.get_stay_purpose(bq, request.hotel, request.brand),
            'positives': ChatDataFetcher.get_phrases(bq, request.hotel, request.brand, 'positive', 8),
            'negatives': ChatDataFetcher.get_phrases(bq, request.hotel, request.brand, 'negative', 8),
            'competitors': ChatDataFetcher.get_competitors(bq, request.hotel, request.brand),
            'segment_satisfaction': ChatDataFetcher.get_segment_satisfaction(bq, request.hotel, request.brand)
        }
        
        print(f"[CHAT] Data fetched: satisfaction={len(data['satisfaction'])} aspects, travelers={len(data['travelers'])} types")
        
        # Format data for agent
        data_text = format_data_for_agent(entity, entity_type, data)
        
        # Step 2: Send to Data Agent for intelligent formatting
        cc = get_data_chat_client()
        if not cc:
            print("[CHAT] Data chat client unavailable")
            return {"response": "Chat service unavailable.", "conversation_id": None}
        
        print("[CHAT] Data chat client initialized")
        
        conv_id = request.conversation_id or f"smaart-{uuid.uuid4().hex[:8]}"
        
        # Build prompt with data + user query
        enhanced_prompt = f"""{AGENT_SYSTEM_PROMPT}

{data_text}

=== USER QUERY ===
{request.message}

Remember: Use the EXACT numbers and phrases from the data above. Do NOT query the database or invent any numbers."""
        
        print(f"[CHAT] Prompt length: {len(enhanced_prompt)} chars")
        
        # Setup agent paths
        parent = f"projects/{PROJECT}/locations/{LOCATION}"
        agent = f"{parent}/dataAgents/{AGENT_ID}"
        conv_path = cc.conversation_path(PROJECT, LOCATION, conv_id)
        
        print(f"[CHAT] Agent: {agent}")
        print(f"[CHAT] Conversation: {conv_path}")
        
        # Create conversation if needed
        try:
            cc.get_conversation(name=conv_path)
            print("[CHAT] Existing conversation found")
        except Exception as conv_err:
            print(f"[CHAT] Creating new conversation: {conv_err}")
            cc.create_conversation(request=gda.CreateConversationRequest(
                parent=parent,
                conversation_id=conv_id,
                conversation=gda.Conversation(agents=[agent])
            ))
            print("[CHAT] Conversation created")
        
        # Send to agent
        print("[CHAT] Sending to agent...")
        stream = cc.chat(request={
            "parent": parent,
            "conversation_reference": {
                "conversation": conv_path,
                "data_agent_context": {"data_agent": agent}
            },
            "messages": [{"user_message": {"text": enhanced_prompt}}]
        })
        
        response_text = ""
        chunk_count = 0
        for chunk in stream:
            chunk_count += 1
            print(f"[CHAT] Chunk {chunk_count}: {type(chunk).__name__}")
            
            # Capture from system_message (where the actual response comes)
            if hasattr(chunk, 'system_message') and hasattr(chunk.system_message, 'text'):
                for p in chunk.system_message.text.parts:
                    part_text = str(p)
                    print(f"[CHAT] System: {part_text[:100]}...")
                    # Skip meta messages, capture actual content
                    if part_text.startswith('📊') or part_text.startswith('🎯') or part_text.startswith('👔') or part_text.startswith('📢') or part_text.startswith('🛏') or part_text.startswith('🛎') or part_text.startswith('🍽') or part_text.startswith('⚙') or part_text.startswith('👥') or part_text.startswith('♂') or part_text.startswith('🔑') or part_text.startswith('⚠') or part_text.startswith('✓') or part_text.startswith('✗') or '**' in part_text:
                        response_text += part_text + "\n"
            
            # Also check agent_message
            if hasattr(chunk, 'agent_message') and hasattr(chunk.agent_message, 'text'):
                for p in chunk.agent_message.text.parts:
                    response_text += str(p)
            elif hasattr(chunk, 'message') and hasattr(chunk.message, 'content'):
                for p in chunk.message.content.parts:
                    response_text += p.text if hasattr(p, 'text') else str(p)
        
        print(f"[CHAT] Total chunks: {chunk_count}, Response length: {len(response_text)}")
        
        # Clean up
        if response_text:
            response_text = response_text.replace('💭 ', '')
        
        return {
            "response": response_text or "No response received.",
            "conversation_id": conv_id
        }
        
    except Exception as e:
        print(f"[CHAT] Error: {e}")
        traceback.print_exc()
        return {"response": f"Error: {str(e)}", "conversation_id": None}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
