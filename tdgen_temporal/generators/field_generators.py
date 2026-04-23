"""
Field-level synthetic data generators for tdgen-temporal.
Standalone copy — no dependency on test-data-gen.
"""

import random
import string
from datetime import date, datetime, timedelta
from typing import Any

from faker import Faker


def make_faker(seed: int | None = None) -> Faker:
    fake = Faker("en_CA")
    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)
    return fake


# ── Helpers ────────────────────────────────────────────────────────────────

def random_decimal(mn: float, mx: float, decimals: int = 2) -> float:
    return round(random.uniform(mn, mx), decimals)


def luhn_checksum(number: str) -> int:
    digits = [int(d) for d in number]
    odd = digits[-1::-2]
    even = digits[-2::-2]
    return (sum(odd) + sum(sum(divmod(d * 2, 10)) for d in even)) % 10


def luhn_complete(partial: str) -> str:
    """Given a 15-digit partial card number, return 16-digit Luhn-valid number."""
    check = (10 - luhn_checksum(partial + "0")) % 10
    return partial + str(check)


def generate_card_number() -> str:
    prefix = random.choice(["4", "51", "52", "53", "54", "55"])
    length = 16
    partial = prefix + "".join(random.choices(string.digits, k=length - len(prefix) - 1))
    return luhn_complete(partial)


def generate_sin() -> str:
    """Synthetic Canadian SIN — format-compliant but not real."""
    return f"{random.randint(100,899)}-{random.randint(100,999)}-{random.randint(100,999)}"


def generate_account_number() -> str:
    return "".join(random.choices(string.digits, k=16))


def score_band(score: int) -> str:
    if score >= 750:
        return "excellent"
    if score >= 700:
        return "good"
    if score >= 650:
        return "fair"
    if score >= 600:
        return "medium"
    if score >= 500:
        return "low"
    return "very_high_risk"


def delinquency_bucket(days: int) -> str:
    if days <= 0:
        return "CURRENT"
    if days <= 30:
        return "B1"
    if days <= 60:
        return "B2"
    if days <= 90:
        return "B3"
    if days <= 120:
        return "B4"
    return "B5+"


_CA_PROVINCES = ["ON", "BC", "AB", "QC", "MB", "SK", "NS", "NB", "NL", "PE"]
_US_STATES    = ["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
_PROVINCE_STATE = _CA_PROVINCES + _US_STATES

_NETWORKS = ["VisaNet", "BankNet", "AmexNet", "Interac"]
_CHANNELS = ["online", "in_store", "mobile", "telephone", "atm"]
_POS_ENTRY = ["chip", "swipe", "contactless", "manual", "ecommerce"]
_AUTH_RESP_APPROVED = ["00", "10", "11"]
_AUTH_RESP_DECLINED = ["05", "14", "51", "54", "57", "61"]
_CARD_BRANDS = ["Visa", "Mastercard", "Amex", "Interac"]
_CURRENCIES  = ["CAD", "USD", "EUR", "GBP"]
_MCC_CODES   = [
    "5411", "5812", "5541", "5310", "5912", "5999", "4111",
    "4121", "5734", "7011", "5045", "5065", "5621", "5651",
]

_STATEMENT_NARRATIVES = [
    lambda f: f"AMZN MKTP CA*{random.randint(100000,999999)}",
    lambda f: f"TIM HORTONS #{random.randint(1000,9999)}",
    lambda f: f"WALMART SUPERCENTRE #{random.randint(1000,9999)}",
    lambda f: f"COSTCO WHOLESALE #{random.randint(1000,9999)}",
    lambda f: f"SHELL OIL {random.randint(10000,99999)}",
    lambda f: f"UBER* {random.choice(['TRIP','EATS'])} {f.lexify('???').upper()}{random.randint(10,99)}",
    lambda f: "NETFLIX.COM",
    lambda f: "APPLE.COM/BILL",
    lambda f: f"SHOPIFY* {f.last_name().upper()} STORE",
    lambda f: f"PAYPAL *{f.last_name().upper()}",
    lambda f: f"LOBLAWS #{random.randint(1000,9999)}",
    lambda f: f"METRO INC #{random.randint(1000,9999)}",
    lambda f: f"POS PURCHASE {f.bothify('??###').upper()}",
    lambda f: f"INTERAC E-TFR {f.first_name().upper()} {f.last_name().upper()}",
    lambda f: "FOREIGN TXN FEE",
    lambda f: "INTEREST CHARGE",
    lambda f: "ANNUAL FEE",
    lambda f: f"TD BANK PAYMENT THANK YOU",
]


def random_narrative(fake: Faker) -> str:
    return random.choice(_STATEMENT_NARRATIVES)(fake)


_CARD_PRODUCT_NAMES = [
    "TD Rewards Visa", "TD Aeroplan Visa Infinite", "TD Cash Back Mastercard",
    "TD Platinum Travel Visa", "TD First Class Travel Visa Infinite",
    "TD Rewards Mastercard", "TD Business Travel Visa", "TD Emerald Flex Rate Visa",
    "TD Low Rate Visa", "TD Aeroplan Visa",
]


def random_product_name() -> str:
    return random.choice(_CARD_PRODUCT_NAMES)


def expiry_date_from_today(years_ahead: int = 3) -> str:
    """Return YYYY-MM-DD expiry roughly years_ahead from today, end of month."""
    today = date.today()
    exp_year  = today.year + years_ahead + random.randint(0, 2)
    exp_month = random.randint(1, 12)
    # Last day of that month
    if exp_month == 12:
        last_day = date(exp_year, 12, 31)
    else:
        last_day = date(exp_year, exp_month + 1, 1) - timedelta(days=1)
    return last_day.isoformat()


def random_past_date(
    fake: Faker,
    years_back: int = 5,
    as_of: date | None = None,
    not_before: date | None = None,
) -> str:
    ceiling = (as_of or date.today()) - timedelta(days=1)
    start   = ceiling - timedelta(days=years_back * 365)
    if not_before and not_before > start:
        start = not_before
    if start > ceiling:
        start = ceiling
    return fake.date_between(start_date=start, end_date=ceiling).isoformat()
