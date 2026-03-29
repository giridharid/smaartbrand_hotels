# Smaartbrand Hotels - Complete Documentation (v46)

## Product Overview

### What is Smaartbrand Hotels?

Smaartbrand Hotels is a **B2B Decision Intelligence Platform** that transforms guest reviews from major hotel booking platforms into actionable insights for hotel operations teams. It provides:

1. **Visual Dashboard** — Interactive charts showing satisfaction scores, driver analysis, demographics, traveler type preferences, and stay purpose analysis across hotel brands
2. **AI Chat Analyst (SmaartAnalyst)** — Natural language interface to query the data, generate FAQs, marketing copy, and get department-specific recommendations

### Who is it for?

| Role | Use Case |
|------|----------|
| **Brand Manager** | Brand perception, competitive positioning, segment targeting |
| **SEO & Marketing** | Keywords to promote/avoid, ad copy, audience targeting |
| **Housekeeping** | Room cleanliness issues, maintenance priorities |
| **Front Desk** | Check-in experience, staff behavior insights |
| **Operations** | Service delivery, process improvements |
| **F&B** | Restaurant quality, dining experience feedback |
| **Product R&D** | New hotel planning, market gap analysis |

### Key Features

| Feature | Description |
|---------|-------------|
| **Multi-Aspect Sentiment** | 8 aspects tracked (Dining, Cleanliness, Amenities, Staff, Room, Location, Value for Money, General) |
| **Traveler Type Segmentation** | Family, Business, Couple, Solo, Group |
| **Gender Analysis** | Male vs Female preferences |
| **Stay Purpose Analysis** | Leisure, Business, Event, Transit, Honeymoon |
| **Brand/Hotel Comparison** | Side-by-side comparison of up to 3 brands or hotels |
| **City/Star Filtering** | Filter brand data by city and star category |
| **Natural Language Queries** | Ask questions in plain English (or Hindi/Telugu) |
| **SEO FAQ Generation** | Google-optimized Q&A for hotel websites |
| **Marketing Copy** | Ready-to-use ad copy with department-wise actions |
| **R&D Mode** | Market analysis for new hotel planning |
| **Rate Limiting** | 4 free requests, then admin key required |

---

## UI Layout (v46)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ [Acquink Logo] Smaartbrand              Requests: [====] 4/4  [🔐 Unlock]  │
│                Hotels Intelligence       (or) ✓ Full Access  Logout        │
├─────────────────────────────────────────────────────────────────────────────┤
│ [Brand View●] [Hotel View] [Compare Brands] [Compare Hotels]    [💬 Chat]  │
├─────────────────────────────────────────────────────────────────────────────┤
│ [Select Brand ▼] [All Cities ▼] [All Stars ▼] [Apply]                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ 🏢 Taj Hotels                                      Rating    Reviews   │ │
│ │    24 hotels in portfolio                          ★ 4.5     12,847    │ │
│ └─────────────────────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────────────┤
│ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │
│ │Overall Satis.│ │Top Strength  │ │Top Weakness  │ │Top Guest Type│        │
│ │   83%        │ │ Location     │ │Value for $   │ │ Family       │        │
│ │ 12,847 ment. │ │ 89% sat.     │ │ 41% sat.     │ │ 62% guests   │        │
│ └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘        │
├─────────────────────────────────────────────────────────────────────────────┤
│ [📊 Drivers●] [📈 Satisfaction] [👥 Demographics] [🎯 Stay] [✈️ Traveler]  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   [Share of Voice Chart]              [Satisfaction Radar Chart]           │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ [Acquink Logo] © 2026 Acquink. All rights reserved.    Powered by MASI     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Compare Hotels Layout (3-column grid)

```
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│    Hotel 1       │ │    Hotel 2       │ │    Hotel 3       │
│ [Select Brand ▼] │ │ [Select Brand ▼] │ │ [Select Brand ▼] │
│ [Select City  ▼] │ │ [Select City  ▼] │ │ [Select City  ▼] │
│ [Select Hotel ▼] │ │ [Select Hotel ▼] │ │ [Select Hotel ▼] │
└──────────────────┘ └──────────────────┘ └──────────────────┘
                    [Compare]
```

---

## Data Source

**BigQuery Project:** `gen-lang-client-0143536012`
**Dataset:** `analyst`

### Key Tables Used

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `product_list` | Hotel master data | product_id, Name, City, Star_Category, Rating |
| `product_description` | Brand and details | Brand, About_Us, Address |
| `product_detail` | Extended hotel info | review_count, positive_review_count |
| `product_user_review_enriched` | Enriched reviews | traveler_type, inferred_gender, stay_purpose |
| `product_user_review_sentiment` | Aspect sentiment | aspect_id, sentiment_type, treemap_name |

### Aspect Mapping

| aspect_id | Aspect Name | Icon |
|-----------|-------------|------|
| 1 | Dining | 🍽️ |
| 2 | Cleanliness | 🧹 |
| 3 | Amenities | 🏊 |
| 4 | Staff | 👨‍💼 |
| 5 | Room | 🛏️ |
| 6 | Location | 📍 |
| 7 | Value for Money | 💰 |
| 8 | General | ⭐ |

---

## API Endpoints

### Dashboard APIs

| Endpoint | Method | Parameters | Returns |
|----------|--------|------------|---------|
| `/api/brands` | GET | - | List of all brands |
| `/api/cities` | GET | brand | Cities for a brand |
| `/api/star_categories` | GET | brand, city | Star categories available |
| `/api/hotels` | GET | brand, city, star_category | Hotels matching filters |
| `/api/hotel_details` | GET | hotel OR brand, city?, star? | Hotel/brand details |
| `/api/satisfaction` | GET | hotel OR brand, city?, star? | Satisfaction by aspect |
| `/api/drivers` | GET | hotel OR brand, city?, star? | Driver analysis (SoV + Satisfaction) |
| `/api/demographics` | GET | hotel OR brand, city?, star? | Gender, traveler type, stay purpose |
| `/api/traveler_preferences` | GET | hotel OR brand, city?, star? | Satisfaction by aspect per traveler type |
| `/api/stay_purpose_preferences` | GET | hotel OR brand, city?, star? | Satisfaction by aspect per stay purpose |
| `/api/comparison` | GET | items, compare_by | Side-by-side comparison |

### Chat API

| Endpoint | Method | Body | Returns |
|----------|--------|------|---------|
| `/api/chat` | POST | `{message, hotel, brand, conversation_id}` | AI-generated response |

### Auth API

| Endpoint | Method | Body | Returns |
|----------|--------|------|---------|
| `/api/auth` | POST | `{key}` | Authentication status |

### Static Files

| Endpoint | Method | Returns |
|----------|--------|---------|
| `/acquink_logo.png` | GET | Acquink logo image (JPEG) |

---

## Files in Deployment Package (v46)

| File | Purpose |
|------|---------|
| `main.py` | FastAPI backend with all endpoints |
| `index.html` | Frontend dashboard |
| `acquink_logo.png` | Acquink logo (served via FileResponse) |
| `Dockerfile` | Container configuration |
| `railway.json` | Railway deployment config |
| `requirements.txt` | Python dependencies |
| `DOCUMENTATION.md` | API documentation |
| `SMAARTBRAND_HOTELS_DOCUMENTATION.md` | This file |

---

## Rate Limiting

| State | UI Display | Behavior |
|-------|------------|----------|
| Fresh user | `Requests: [====] 4/4` | 4 free API calls |
| After requests | `Requests: [==] 2/4` | Counter decreases |
| Exhausted | `Requests: [0/4]` + `🔐 Unlock` button | Login modal appears |
| Authenticated | `✓ Full Access` + `Logout` | Unlimited requests |

**Storage:** localStorage (browser-based, resets in incognito)

---

## Version History

| Version | Changes |
|---------|---------|
| v1-v22 | Initial development through clean production release |
| v23-v33 | Tab fixes, Compare button placement |
| v34 | Major 3-row UI restructure |
| v35-v38 | Acquink logo + footer branding |
| v39 | Compare Hotels 3-column grid layout |
| v40 | Correct layout order (Context → Stats → Tabs → Content) |
| v41 | Dropdown resets on mode switch |
| v42 | Dockerfile fix for logo |
| v43 | City/Star filters for all APIs |
| v44 | Footer, logo fallback, API null handling |
| v45 | Rate limit UI moved to header |
| **v46** | **Lock button hidden until exhausted, clean release** |

---

## Debug Queries

```sql
-- Find hotel by name
SELECT product_id, Name, City, Star_Category, Rating
FROM `gen-lang-client-0143536012.analyst.product_list`
WHERE LOWER(Name) LIKE '%novotel%bengaluru%'
LIMIT 10;

-- Aspect satisfaction for a hotel
SELECT aspect_id,
  CASE aspect_id WHEN 1 THEN 'Dining' WHEN 2 THEN 'Cleanliness' WHEN 3 THEN 'Amenities' 
  WHEN 4 THEN 'Staff' WHEN 5 THEN 'Room' WHEN 6 THEN 'Location' 
  WHEN 7 THEN 'Value for Money' WHEN 8 THEN 'General' END as aspect_name,
  ROUND(SUM(CASE WHEN sentiment_type='positive' THEN 1 ELSE 0 END)*100.0/COUNT(*),0) as satisfaction_pct,
  COUNT(*) as mentions
FROM `gen-lang-client-0143536012.analyst.product_user_review_sentiment`
WHERE product_id = <ID>
GROUP BY aspect_id ORDER BY aspect_id;

-- City rankings
SELECT p.Name, 
  ROUND(SUM(CASE WHEN s.sentiment_type='positive' THEN 1 ELSE 0 END)*100.0/COUNT(*),0) as sat_pct, 
  COUNT(*) as mentions
FROM `gen-lang-client-0143536012.analyst.product_user_review_sentiment` s
JOIN `gen-lang-client-0143536012.analyst.product_list` p ON s.product_id=p.product_id
WHERE LOWER(p.City)='bengaluru'
GROUP BY p.Name HAVING COUNT(*)>=100
ORDER BY sat_pct DESC LIMIT 20;

-- Top treemap phrases for an aspect
SELECT treemap_name, sentiment_type, COUNT(*) as cnt
FROM `gen-lang-client-0143536012.analyst.product_user_review_sentiment`
WHERE product_id = <ID> AND aspect_id = 7
GROUP BY treemap_name, sentiment_type
ORDER BY cnt DESC LIMIT 20;
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GCP_CREDENTIALS_JSON` | Base64-encoded GCP service account JSON |
| `SMAARTBRAND_ADMIN_KEY` | Admin key for full access |

---

## Production URL

```
https://smaartbrandhotels-production.up.railway.app
```

---

## Backlog / Future Features

| Feature | Description | Priority |
|---------|-------------|----------|
| **Feature Preference by Gender** | Extract which amenities/features males vs females prefer (e.g., spa, gym, pool) | High |
| **Stay Purpose Room Preferences** | What room types/features business vs leisure travelers want | High |
| **Room Preference Analysis** | Room size, bed type, view preferences by segment | High |
| **R&D Insights Dashboard** | Dedicated tab for new hotel planning insights | Medium |
| **Amenity Gap Analysis** | Which amenities are mentioned but missing | Medium |
| **Competitive Phrase Analysis** | What competitors are praised for that we lack | Medium |
| **IP-based Rate Limiting** | Server-side rate limiting by IP address | Low |
| **Export to PDF** | Download reports as PDF | Low |
| **Email Alerts** | Weekly digest of sentiment changes | Low |
| **Multi-language Support** | Hindi, Telugu, Tamil responses | Low |

---

## Known Limitations

1. **Rate Limiting** — Browser localStorage based, can be bypassed with incognito
2. **Value for Money** — Often has low mention count (may be unreliable with < 20 mentions)
3. **Traveler Type NULL** — ~75-90% of reviews don't have traveler type enrichment
4. **Stay Purpose NULL** — Similar NULL rate, excluded from calculations
5. **Logo** — File is JPEG despite .png extension (served with correct MIME type)
