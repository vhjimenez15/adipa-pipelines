"""
Simulates the WooCommerce REST API response for orders.
Structure mirrors the real WC /wp-json/wc/v3/orders endpoint
so swapping in real credentials later is just a URL + auth change.
"""
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

COURSES = {
    "CL": ["Psicología Clínica Avanzada", "Neuropsicología Infantil", "Terapia Cognitivo Conductual", "Mindfulness Terapéutico"],
    "MX": ["Psicoanálisis Contemporáneo", "Psicología Organizacional", "Terapia de Pareja", "Psicología Positiva"],
    "CO": ["Evaluación Psicológica", "Intervención en Crisis", "Psicología del Desarrollo", "Salud Mental Comunitaria"],
}

CURRENCIES = {"CL": "CLP", "MX": "MXN", "CO": "COP"}

PRICE_RANGES = {
    "CLP": (49_000, 299_000),
    "MXN": (1_500, 8_500),
    "COP": (180_000, 950_000),
}

STATUSES = ["completed", "completed", "completed", "processing", "on-hold"]  # weighted toward completed


def fetch_orders(country_code: str, since: datetime | None = None) -> list[dict]:
    """
    Returns a list of mock WC orders for the given country.
    `since` mimics the WC API `after` filter to only pull recent orders.
    """
    n = int(os.environ.get("MOCK_ORDERS_PER_RUN", 5))
    currency = CURRENCIES[country_code]
    price_min, price_max = PRICE_RANGES[currency]
    courses = COURSES[country_code]
    now = datetime.now(timezone.utc)

    orders = []
    for _ in range(n):
        order_date = now - timedelta(minutes=random.randint(0, 30))
        orders.append({
            "id": str(uuid.uuid4())[:8].upper(),  # short UUID mimics WC numeric IDs
            "status": random.choice(STATUSES),
            "currency": currency,
            "total": str(round(random.uniform(price_min, price_max), 2)),
            "date_created": order_date.isoformat(),
            "billing": {"email": f"user_{random.randint(1000, 9999)}@example.com"},
            "line_items": [
                {
                    "name": random.choice(courses),
                    "quantity": 1,
                }
            ],
        })
    return orders
