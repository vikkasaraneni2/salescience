"""
Query Interpreter Module
-----------------------
Translates high-level, business-friendly user queries into technical job specs for the orchestrator API.
Contains mapping dictionaries and the QueryInterpreter class.
"""
import re
from typing import List, Optional, Dict, Any
import spacy
import pkg_resources

# Check spaCy and model version compatibility
spacy_version = pkg_resources.get_distribution('spacy').version
try:
    model_version = pkg_resources.get_distribution('en_core_web_sm').version
except Exception:
    model_version = None
if model_version and not spacy_version.startswith(model_version[:3]):
    raise RuntimeError(f"spaCy version ({spacy_version}) and en_core_web_sm version ({model_version}) do not match. Please install matching versions.")

# Load spaCy English model (make sure en_core_web_sm is installed)
nlp = spacy.load("en_core_web_sm")

# Mapping of business concepts to SEC form types and Yahoo data types
BUSINESS_TO_SEC_FORMS = {
    "financial reports": ["10-K", "10-Q", "20-F", "40-F"],
    "risk disclosures": ["8-K", "DEF 14A"],
    "proxy statements": ["DEF 14A"],
    "earnings": ["8-K", "10-Q", "10-K"],
    "stock transactions": ["4", "5", "3"],
    "insider trading": ["4", "5", "3"],
    "registration": ["S-1", "S-3", "S-4"],
    "mergers": ["8-K", "S-4"],
    "acquisitions": ["8-K", "S-4"],
    "press releases": ["8-K"],
    # Add more as needed
}
BUSINESS_TO_YAHOO_TYPES = {
    "financial reports": ["balance_sheet", "income_statement", "cash_flow"],
    "stock price": ["price_history", "market_cap"],
    "dividends": ["dividend_history"],
    "splits": ["split_history"],
    "analyst ratings": ["analyst_ratings"],
    # Add more as needed
}

# Synonyms and keyword variants for business concepts
BUSINESS_SYNONYMS = {
    "financial reports": ["financials", "annual report", "quarterly report", "statements", "10k", "10-q", "20-f", "40-f", "balance sheet", "income statement", "cash flow"],
    "risk disclosures": ["risks", "risk factors", "disclosure", "8k", "8-k", "def 14a", "proxy", "risk statement"],
    "proxy statements": ["proxy", "def 14a", "shareholder meeting", "proxy vote"],
    "earnings": ["earnings", "results", "profit", "loss", "income", "quarterly results", "eps", "net income"],
    "stock price": ["price", "stock", "share price", "market price", "stock value", "market cap"],
    "stock transactions": ["insider trades", "form 4", "form 5", "form 3", "ownership change"],
    "insider trading": ["insider trading", "insider transaction", "form 4", "form 5", "form 3"],
    "registration": ["registration", "s-1", "s-3", "s-4", "public offering"],
    "mergers": ["merger", "acquisition", "combine", "s-4", "8-k"],
    "acquisitions": ["acquisition", "acquire", "purchase", "s-4", "8-k"],
    "press releases": ["press release", "news", "announcement"],
    "dividends": ["dividend", "dividends", "dividend history"],
    "splits": ["split", "stock split", "split history"],
    "analyst ratings": ["analyst", "rating", "analyst rating", "buy", "sell", "hold"],
    # Add more as needed
}

class QueryInterpreter:
    """
    Translates high-level user queries into technical job specs for the orchestrator API.
    Uses spaCy NLP for entity/concept extraction, plus keyword/synonym/fuzzy matching.
    """
    def interpret(self, query: str, company: str = None, date_range: Optional[List[str]] = None, organization_id: Optional[str] = None, user_id: Optional[str] = None) -> Dict[str, Any]:
        query_lc = query.lower()
        sec_forms = set()
        yahoo_types = set()
        matched_concepts = set()
        nlp_doc = nlp(query)
        # 1. spaCy entity extraction for organizations, dates, and financial terms
        extracted_company = company
        extracted_dates = []
        for ent in nlp_doc.ents:
            if ent.label_ in ("ORG", "COMPANY") and not extracted_company:
                extracted_company = ent.text
            if ent.label_ in ("DATE",):
                extracted_dates.append(ent.text)
        # 2. Keyword and synonym matching (including partial/fuzzy)
        for concept, forms in BUSINESS_TO_SEC_FORMS.items():
            keywords = [concept] + BUSINESS_SYNONYMS.get(concept, [])
            for kw in keywords:
                if kw in query_lc:
                    sec_forms.update(forms)
                    matched_concepts.add(concept)
                    break
        for concept, types in BUSINESS_TO_YAHOO_TYPES.items():
            keywords = [concept] + BUSINESS_SYNONYMS.get(concept, [])
            for kw in keywords:
                if kw in query_lc:
                    yahoo_types.update(types)
                    matched_concepts.add(concept)
                    break
        # 3. Fuzzy/partial match (substring, e.g., "10k" matches "10-K")
        for concept, forms in BUSINESS_TO_SEC_FORMS.items():
            for kw in BUSINESS_SYNONYMS.get(concept, []):
                if any(kw.replace('-', '').replace(' ', '') in word.replace('-', '').replace(' ', '') for word in query_lc.split()):
                    sec_forms.update(forms)
                    matched_concepts.add(concept)
        for concept, types in BUSINESS_TO_YAHOO_TYPES.items():
            for kw in BUSINESS_SYNONYMS.get(concept, []):
                if any(kw.replace('-', '').replace(' ', '') in word.replace('-', '').replace(' ', '') for word in query_lc.split()):
                    yahoo_types.update(types)
                    matched_concepts.add(concept)
        # 4. Fallback: if nothing matched, default to financial reports
        if not sec_forms and not yahoo_types:
            sec_forms.update(BUSINESS_TO_SEC_FORMS["financial reports"])
            yahoo_types.update(BUSINESS_TO_YAHOO_TYPES["financial reports"])
            matched_concepts.add("financial reports")
        # 5. Parse date range if not provided, using spaCy and regex
        if not date_range:
            # Try spaCy-extracted dates first
            if extracted_dates:
                # Simple heuristic: use first and last date if two found, else single year
                years = [y for y in re.findall(r"(20\\d{2})", " ".join(extracted_dates))]
                if len(years) == 2:
                    date_range = [f"{years[0]}-01-01", f"{years[1]}-12-31"]
                elif len(years) == 1:
                    date_range = [f"{years[0]}-01-01", f"{years[0]}-12-31"]
            if not date_range:
                date_range = self._extract_date_range(query)
        return {
            "organization_id": organization_id,
            "user_id": user_id,
            "company": extracted_company,
            "date_range": date_range,
            "sec_form_types": list(sec_forms),
            "yahoo_data_types": list(yahoo_types),
            "matched_concepts": list(matched_concepts),
            "nlp_entities": [(ent.text, ent.label_) for ent in nlp_doc.ents],
        }

    def _extract_date_range(self, query: str) -> List[str]:
        # Simple regex to find years in the query
        years = re.findall(r"(20\\d{2})", query)
        if len(years) == 2:
            return [f"{years[0]}-01-01", f"{years[1]}-12-31"]
        elif len(years) == 1:
            return [f"{years[0]}-01-01", f"{years[0]}-12-31"]
        else:
            return [] 