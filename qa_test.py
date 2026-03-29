#!/usr/bin/env python3
"""
QA Validation Script for SmaartBrand Hotels Dashboard
Run: python qa_test.py [BASE_URL]
Default: http://localhost:8080
"""

import sys
import json
import requests
from typing import Dict, List, Any

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"

# Test results
passed = 0
failed = 0
results = []

def log(status: str, test: str, details: str = ""):
    global passed, failed
    icon = "✅" if status == "PASS" else "❌"
    if status == "PASS":
        passed += 1
    else:
        failed += 1
    msg = f"{icon} {test}"
    if details:
        msg += f" — {details}"
    print(msg)
    results.append({"status": status, "test": test, "details": details})

def test_endpoint(name: str, url: str, checks: List[Dict] = None):
    """Test an endpoint and run optional checks on response"""
    try:
        resp = requests.get(f"{BASE_URL}{url}", timeout=30)
        if resp.status_code != 200:
            log("FAIL", name, f"Status {resp.status_code}")
            return None
        
        data = resp.json()
        
        # Run checks if provided
        if checks:
            for check in checks:
                check_type = check.get("type")
                if check_type == "not_empty":
                    if not data:
                        log("FAIL", name, "Empty response")
                        return None
                elif check_type == "min_length":
                    if len(data) < check.get("value", 1):
                        log("FAIL", name, f"Expected min {check['value']} items, got {len(data)}")
                        return None
                elif check_type == "has_key":
                    key = check.get("value")
                    if isinstance(data, list):
                        if not data or key not in data[0]:
                            log("FAIL", name, f"Missing key '{key}'")
                            return None
                    elif key not in data:
                        log("FAIL", name, f"Missing key '{key}'")
                        return None
                elif check_type == "aspect_names":
                    # Check that aspect_name is populated (not None/null)
                    if isinstance(data, list):
                        nulls = [d for d in data if d.get("aspect_name") is None]
                        if nulls:
                            log("FAIL", name, f"{len(nulls)} items have null aspect_name")
                            return None
        
        log("PASS", name)
        return data
    except requests.exceptions.Timeout:
        log("FAIL", name, "Timeout")
        return None
    except Exception as e:
        log("FAIL", name, str(e))
        return None

def test_post_endpoint(name: str, url: str, body: Dict, checks: List[Dict] = None):
    """Test a POST endpoint"""
    try:
        resp = requests.post(f"{BASE_URL}{url}", json=body, timeout=60)
        if resp.status_code != 200:
            log("FAIL", name, f"Status {resp.status_code}")
            return None
        
        data = resp.json()
        
        if checks:
            for check in checks:
                if check.get("type") == "has_key":
                    key = check.get("value")
                    if key not in data:
                        log("FAIL", name, f"Missing key '{key}'")
                        return None
        
        log("PASS", name)
        return data
    except requests.exceptions.Timeout:
        log("FAIL", name, "Timeout (expected for chat)")
        return None
    except Exception as e:
        log("FAIL", name, str(e))
        return None

def run_tests():
    print(f"\n{'='*60}")
    print(f"QA VALIDATION: {BASE_URL}")
    print(f"{'='*60}\n")
    
    # ─────────────────────────────────────────
    # 1. BASIC ENDPOINTS
    # ─────────────────────────────────────────
    print("📋 BASIC ENDPOINTS\n")
    
    # Homepage
    try:
        resp = requests.get(f"{BASE_URL}/", timeout=10)
        if resp.status_code == 200 and "SmaartBrand" in resp.text:
            log("PASS", "Homepage loads")
        else:
            log("FAIL", "Homepage loads", f"Status {resp.status_code}")
    except Exception as e:
        log("FAIL", "Homepage loads", str(e))
    
    # Brands
    brands = test_endpoint(
        "GET /api/brands",
        "/api/brands",
        [{"type": "not_empty"}, {"type": "min_length", "value": 5}]
    )
    
    if not brands:
        print("\n❌ Cannot continue without brands. Exiting.")
        return
    
    test_brand = brands[0]
    print(f"   Using test brand: {test_brand}")
    
    # Cities
    cities = test_endpoint(
        "GET /api/cities",
        f"/api/cities?brand={test_brand}",
        [{"type": "not_empty"}]
    )
    
    test_city = cities[0] if cities else None
    if test_city:
        print(f"   Using test city: {test_city}")
    
    # Star Categories
    if test_city:
        test_endpoint(
            "GET /api/star_categories",
            f"/api/star_categories?brand={test_brand}&city={test_city}",
            [{"type": "not_empty"}]
        )
    
    # Hotels
    hotels = test_endpoint(
        "GET /api/hotels",
        f"/api/hotels?brand={test_brand}",
        [{"type": "not_empty"}, {"type": "has_key", "value": "product_id"}]
    )
    
    test_hotel = hotels[0]["hotel_name"] if hotels else None
    test_product_id = str(hotels[0]["product_id"]) if hotels else None
    if test_hotel:
        print(f"   Using test hotel: {test_hotel}")
        print(f"   Using test product_id: {test_product_id}")
    
    # ─────────────────────────────────────────
    # 2. BRAND VIEW ENDPOINTS
    # ─────────────────────────────────────────
    print("\n📊 BRAND VIEW ENDPOINTS\n")
    
    test_endpoint(
        "GET /api/hotel_details (brand)",
        f"/api/hotel_details?brand={test_brand}",
        [{"type": "has_key", "value": "brand"}]
    )
    
    test_endpoint(
        "GET /api/satisfaction (brand)",
        f"/api/satisfaction?brand={test_brand}",
        [{"type": "not_empty"}, {"type": "aspect_names"}]
    )
    
    test_endpoint(
        "GET /api/drivers (brand)",
        f"/api/drivers?brand={test_brand}",
        [{"type": "not_empty"}, {"type": "aspect_names"}]
    )
    
    test_endpoint(
        "GET /api/demographics (brand)",
        f"/api/demographics?brand={test_brand}",
        [{"type": "has_key", "value": "gender"}]
    )
    
    test_endpoint(
        "GET /api/traveler_preferences (brand)",
        f"/api/traveler_preferences?brand={test_brand}",
        [{"type": "not_empty"}]
    )
    
    test_endpoint(
        "GET /api/stay_purpose_preferences (brand)",
        f"/api/stay_purpose_preferences?brand={test_brand}",
        [{"type": "not_empty"}]
    )
    
    # ─────────────────────────────────────────
    # 3. HOTEL VIEW ENDPOINTS
    # ─────────────────────────────────────────
    if test_hotel:
        print("\n🏨 HOTEL VIEW ENDPOINTS\n")
        
        test_endpoint(
            "GET /api/hotel_details (hotel)",
            f"/api/hotel_details?hotel={test_hotel}",
            [{"type": "has_key", "value": "hotel_name"}]
        )
        
        test_endpoint(
            "GET /api/satisfaction (hotel)",
            f"/api/satisfaction?hotel={test_hotel}",
            [{"type": "not_empty"}, {"type": "aspect_names"}]
        )
        
        test_endpoint(
            "GET /api/drivers (hotel)",
            f"/api/drivers?hotel={test_hotel}",
            [{"type": "not_empty"}, {"type": "aspect_names"}]
        )
        
        test_endpoint(
            "GET /api/demographics (hotel)",
            f"/api/demographics?hotel={test_hotel}",
            [{"type": "has_key", "value": "gender"}]
        )
    
    # ─────────────────────────────────────────
    # 4. COMPARISON ENDPOINTS
    # ─────────────────────────────────────────
    print("\n⚖️ COMPARISON ENDPOINTS\n")
    
    # Brand comparison
    if len(brands) >= 2:
        compare_brands = "|||".join(brands[:2])
        test_endpoint(
            "GET /api/comparison (brands)",
            f"/api/comparison?items={compare_brands}&compare_by=brand",
            [{"type": "not_empty"}]
        )
    
    # Hotel comparison (using product_ids)
    if hotels and len(hotels) >= 2:
        product_ids = [str(h["product_id"]) for h in hotels[:2]]
        compare_hotels = "|||".join(product_ids)
        test_endpoint(
            "GET /api/comparison (hotels by product_id)",
            f"/api/comparison?items={compare_hotels}&compare_by=hotel",
            [{"type": "not_empty"}]
        )
    
    # ─────────────────────────────────────────
    # 5. CHAT ENDPOINT
    # ─────────────────────────────────────────
    print("\n💬 CHAT ENDPOINT\n")
    
    chat_result = test_post_endpoint(
        "POST /api/chat (brand)",
        "/api/chat",
        {"message": "How am I doing?", "brand": test_brand},
        [{"type": "has_key", "value": "response"}]
    )
    
    if chat_result and chat_result.get("response"):
        response_len = len(chat_result["response"])
        print(f"   Chat response length: {response_len} chars")
        if response_len < 50:
            log("FAIL", "Chat response quality", "Response too short")
    
    # ─────────────────────────────────────────
    # 6. DATA QUALITY CHECKS
    # ─────────────────────────────────────────
    print("\n🔍 DATA QUALITY CHECKS\n")
    
    # Check satisfaction scores are reasonable (0-100)
    try:
        resp = requests.get(f"{BASE_URL}/api/satisfaction?brand={test_brand}", timeout=30)
        data = resp.json()
        bad_scores = [d for d in data if d.get("satisfaction", 0) < 0 or d.get("satisfaction", 0) > 100]
        if bad_scores:
            log("FAIL", "Satisfaction scores in range", f"{len(bad_scores)} out of range")
        else:
            log("PASS", "Satisfaction scores in range (0-100)")
    except:
        log("FAIL", "Satisfaction scores in range", "Could not check")
    
    # Check aspect mapping (no None values)
    try:
        resp = requests.get(f"{BASE_URL}/api/drivers?brand={test_brand}", timeout=30)
        data = resp.json()
        null_aspects = [d for d in data if d.get("aspect_name") is None]
        if null_aspects:
            log("FAIL", "Aspect names populated", f"{len(null_aspects)} null values")
        else:
            log("PASS", "Aspect names populated")
    except:
        log("FAIL", "Aspect names populated", "Could not check")
    
    # ─────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"RESULTS: {passed} passed, {failed} failed")
    print(f"{'='*60}\n")
    
    if failed == 0:
        print("🎉 All tests passed!")
    else:
        print("⚠️ Some tests failed. Review above for details.")
    
    return failed == 0

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
