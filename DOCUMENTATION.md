# Smaartbrand Hotels - Dashboard Documentation

## Overview

Smaartbrand Hotels is a **B2B Decision Intelligence Platform** that transforms guest reviews into actionable insights for hotel managers. Built with the same architecture as Smaartbrand Smartphones.

### Key Features

| Feature | Description |
|---------|-------------|
| **8 Aspect Analysis** | Dining, Cleanliness, Amenities, Staff, Room, Location, Value for Money, General |
| **Guest Segmentation** | Traveler Type (Business, Family, Couple, Solo, Group), Gender, Stay Purpose |
| **Brand & Hotel Views** | Switch between brand-level and individual hotel analysis |
| **Natural Language Chat** | Ask questions in plain English (or Hindi/Telugu) |
| **Spell Correction** | Handles typos like "marriot" → "Marriott" |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      USER INTERFACE                             │
│  ┌─────────────────────────┐  ┌─────────────────────────────┐  │
│  │    Visual Dashboard     │  │    SmaartAnalyst Chat       │  │
│  │  ┌─────────┬─────────┐  │  │                             │  │
│  │  │ Drivers │ Satisfy │  │  │  "What should we improve?"  │  │
│  │  ├─────────┼─────────┤  │  │  → Dining: 75%, Room: 68%   │  │
│  │  │ Demo    │ Compare │  │  │  → Actions by Department    │  │
│  │  └─────────┴─────────┘  │  │                             │  │
│  └─────────────────────────┘  └─────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FASTAPI BACKEND (main.py)                     │
│                                                                  │
│  /api/hotels      - List all hotels with metadata               │
│  /api/brands      - List all brands                             │
│  /api/satisfaction - Aspect satisfaction scores                 │
│  /api/drivers     - Share of Voice + Satisfaction               │
│  /api/demographics - Traveler type, gender, stay purpose        │
│  /api/phrases     - Top positive/negative phrases               │
│  /api/comparison  - Multi-hotel comparison                      │
│  /api/chat        - Data-first chat with Gemini                 │
└─────────────────────────────────────────────────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              ▼                                 ▼
┌─────────────────────────┐      ┌─────────────────────────────────┐
│       BigQuery          │      │      BigQuery ML (Gemini)       │
│                         │      │                                 │
│  analyst.product_list   │      │  ML.GENERATE_TEXT for chat      │
│  analyst.product_desc   │      │  Data-first approach:           │
│  analyst.review_enriched│      │  1. Query real data             │
│  analyst.review_sentiment│     │  2. Pass to Gemini              │
│                         │      │  3. Generate narrative          │
└─────────────────────────┘      └─────────────────────────────────┘
```

---

## Data Source

**BigQuery Dataset:** `gen-lang-client-0143536012.analyst`

### Tables

| Table | Purpose |
|-------|---------|
| `product_list` | Hotel name, city, star category |
| `product_description` | Brand, address, lat/long, rating, amenities |
| `product_user_review_enriched` | Review metadata: date, gender, traveler type, stay purpose |
| `product_user_review_sentiment` | Aspect-level sentiment: aspect_id, phrase, sentiment_type |

### Aspects (8 total)

| ID | Aspect | Icon |
|----|--------|------|
| 1 | Dining | 🍽️ |
| 2 | Cleanliness | 🧹 |
| 3 | Amenities | 🏊 |
| 4 | Staff | 👨‍💼 |
| 5 | Room | 🛏️ |
| 6 | Location | 📍 |
| 7 | Value for Money | 💰 |
| 8 | General | ⭐ |

### Guest Segments

**Traveler Types** (from review text keywords):
- Family - "with family", "family trip", "with kids", "family vacation"
- Couple - "honeymoon", "romantic getaway", "anniversary trip", "my wife and i"
- Business - "business trip", "for conference", "official visit", "corporate meeting"
- Solo - "solo trip", "traveling alone", "by myself"
- Group - "with friends", "with colleagues", "group trip"

**Note:** 75-90% of reviews are NULL (unclassified) because most reviewers don't explicitly state their trip type. Dashboard shows % of classified reviews only.

**Gender** (inferred from reviewer names):
- Male
- Female
- Unknown (excluded from charts)

**Stay Purpose** (from review text keywords):
- Leisure - "vacation", "holiday", "getaway", "staycation"
- Honeymoon - "honeymoon", "post wedding"
- Event - "wedding celebration", "birthday party", "family function"
- Business - "business trip", "for conference"
- Transit - "layover", "transit", "night halt", "before flight"

---

## API Endpoints

### GET /api/hotels
Returns list of all hotels with metadata.

### GET /api/brands
Returns list of all hotel brands.

### GET /api/satisfaction?hotel=X or ?brand=X
Returns satisfaction scores by aspect.

### GET /api/drivers?hotel=X or ?brand=X
Returns driver analysis (Share of Voice + Satisfaction).

### GET /api/demographics?hotel=X or ?brand=X
Returns:
- `traveler_type`: Distribution by traveler type
- `gender`: Gender distribution
- `stay_purpose`: Purpose of stay distribution

### GET /api/phrases?hotel=X&sentiment=positive
Returns top phrases with sentiment.

### GET /api/comparison?hotels=A,B,C
Returns comparison data for multiple hotels.

### POST /api/chat
Chat endpoint with data-first approach.

Request body:
```json
{
  "message": "What should we improve?",
  "hotel": "Hotel Name",  // optional
  "brand": "Brand Name"   // optional
}
```

---

## Chat System (Matching Smartphones Architecture)

### Data-First Flow

```
User Message
     │
     ▼
┌─────────────────┐
│ Spell Correction │ → "marriot" → "Marriott", "oberoy" → "Oberoi"
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  detect_intent  │ → Extract aspects, traveler type, gender, analysis type
└────────┬────────┘
         │
         ▼
┌──────────────────┐
│gather_context_data│ → Query BigQuery for REAL data (6 queries)
└────────┬─────────┘     • satisfaction by aspect
         │               • weakest aspects  
         │               • demographics
         │               • positive phrases
         │               • negative phrases
         │               • segment-specific (if traveler type specified)
         ▼
┌──────────────────┐
│format_data_for_llm│ → Format with hotel/brand name in each line
└────────┬─────────┘     "Taj Dining: 85%", "Taj Cleanliness: 72%"
         │
         ▼
┌──────────────────┐
│ BigQuery ML      │ → Generate narrative using ONLY real data
│ GENERATE_TEXT    │    Temperature: 0.3 (low creativity)
└────────┬─────────┘
         │
         ▼
    Response (with spell correction notice if applicable)
```

### Response Formats (Conditional)

**1. Simple Query** (general questions):
```
📊 **Insight**: [2-3 sentences with exact % from data]
👥 **Guest Mix**: [Family X%, Business Y%, Couple Z%]
```

**2. Strategic Query** (improvements/comparison/fix/should):
```
📊 **Insight**: [findings with %]
👥 **Guest Mix**: [breakdown]

🎯 **Actions by Department**:
📦 **Operations**: [specific action]
📢 **Marketing**: 
   ✓ PROMOTE: [top strengths]
   ✗ AVOID: [weaknesses]
🛏️ **Housekeeping**: [if Room/Cleanliness relevant]
🍽️ **F&B**: [if Dining relevant]
🛎️ **Front Desk**: [if Staff relevant]
```

**3. FAQ Format** (for website SEO):
```
❓ **Frequently Asked Questions**

1. **Is {hotel} good for families?**
   [Answer using Family data and aspect scores]

2. **How is the food at {hotel}?**
   [Answer using Dining satisfaction %]

3. **Is {hotel} clean and hygienic?**
   [Answer using Cleanliness %]

4. **What are the pros and cons of {hotel}?**
   [Top 3 strengths and weaknesses with %]

5. **Is {hotel} worth the price?**
   [Answer using Value for Money %]
```

**4. AdCopy Format** (marketing-ready):
```
📢 **Google Ads Headlines** (30 chars max):
1. [Strength-based]
2. [Guest phrase]
3. [Competitive angle]

📱 **Social Media Copy** (60-80 words):
[Conversational, emoji-friendly]

🎯 **Key Selling Points**:
✅ LEAD WITH: [strengths with %]
⚠️ AVOID: [weaknesses]

📝 **Product Page Copy** (100 words):
[Using guest phrases as social proof]

🌐 **Hindi Version**:
[Translated social copy]
```

### Multilingual Support

Responds in user's language:
- Hindi: "हमारी सर्विस कैसी है?"
- Tamil: "உணவு எப்படி?"
- Telugu: "Cleanliness రేటింగ్ ఎంత?"

---

## UI Features

### Tabs

1. **Drivers** - Share of Voice bar chart + Satisfaction radar
2. **Satisfaction** - Aspect satisfaction bar chart
3. **Demographics** - Traveler type, gender, stay purpose donuts
4. **Comparison** - Multi-hotel side-by-side table
5. **Phrases** - Top positive/negative guest phrases

### Theme

- Dark gradient background (teal/blue)
- Glass morphism styling
- Teal accent color (#0d9488)
- Inter font family
- ApexCharts for visualizations

---

## Deployment

### Railway

1. Set environment variable: `GCP_CREDENTIALS_JSON` (base64 encoded service account)
2. Deploy via Dockerfile

### Files

| File | Purpose |
|------|---------|
| `main.py` | FastAPI backend |
| `index.html` | Frontend dashboard |
| `Dockerfile` | Container config |
| `railway.json` | Railway deployment config |
| `requirements.txt` | Python dependencies |

---

## Test Queries for Chat

### Basic Queries
- "What is our dining satisfaction?"
- "How do guests rate cleanliness?"
- "Tell me about Taj"

### Strategic Queries (with department actions)
- "What should we improve?"
- "What are our weakest areas?"
- "What should Oberoi fix?"

### Segment-Specific
- "How do business travelers rate us?"
- "What do families complain about?"
- "How do couples rate the room?"

### SEO FAQs
- "Generate FAQs for our website"
- "Create FAQs for Marriott"

### Marketing AdCopy
- "Write marketing ad copy"
- "Create Google Ads for Taj"
- "Generate social media copy for dining"

### Spell Correction
- "marriot cleanliness" → "Marriott Cleanliness"
- "oberoy food" → "Oberoi Dining"
- "bangalore hotels" → "Bengaluru hotels"

### Multilingual
- "हमारी सर्विस कैसी है?" (Hindi)
- "Cleanliness రేటింగ్ ఎంత?" (Telugu)
- "உணவு எப்படி?" (Tamil)
