"""
Query Interpreter Module for Natural Language Understanding
---------------------------------------------------------

This module serves as the natural language understanding (NLU) component
of the Salescience platform, translating high-level, business-friendly
user queries into technical job specifications that can be executed by
the orchestrator API.

System Architecture Context:
---------------------------
The Query Interpreter sits between the user interface and the technical API,
enabling a more intuitive and accessible way to interact with the system:

┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ User        │     │ Query       │     │ Orchestrator│     │ Data        │
│ Interface   │────▶│ Interpreter │────▶│ API         │────▶│ Acquisition │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │ NLP Models  │
                    │ & Mappings  │
                    └─────────────┘

Key Components:
-------------
1. Business Concept Mappings: Dictionaries that map business terminology
   to technical parameters (SEC form types, Yahoo data types, etc.)
   
2. Entity Recognition: Using spaCy NLP to identify named entities in queries,
   such as companies, dates, and financial concepts
   
3. Query Interpretation Logic: Algorithms to analyze queries and extract
   structured parameters and intents
   
4. Date Range Extraction: Specialized logic for identifying and normalizing
   date ranges in natural language
   
5. Fallback Strategies: Sensible defaults when queries are ambiguous or
   incomplete

Implementation Approach:
----------------------
The current implementation uses a hybrid approach combining:
- Rule-based mapping for known financial terminology
- spaCy NLP for entity recognition and parsing
- Regex pattern matching for specific formats (dates, form types)
- Fuzzy matching for handling typos and variants

This approach provides a balance of precision and flexibility while
minimizing dependencies on large language models or external NLP services.

Future enhancements could include:
- Fine-tuned domain-specific language models
- Semantic vector embeddings for more robust concept matching
- Feedback loops to improve accuracy based on user interactions
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
    Natural language query interpreter for financial data requests.
    
    This class is responsible for analyzing natural language queries about financial
    data and converting them into structured job specifications that can be executed
    by the orchestrator API. It combines multiple NLP techniques to understand
    user intent and extract relevant parameters.
    
    Key capabilities:
    1. Entity Recognition: Identifies companies, dates, and financial concepts
    2. Intent Classification: Determines what kind of data the user is requesting
    3. Parameter Mapping: Translates business concepts to technical parameters
    4. Ambiguity Resolution: Makes reasonable assumptions for unclear queries
    5. Date Range Extraction: Converts various date formats and ranges to standard format
    
    The interpreter uses a hybrid approach combining:
    - spaCy's named entity recognition for dates, organizations, etc.
    - Keyword/synonym matching for financial terminology
    - Regex pattern matching for structured formats
    - Fallback strategies for handling ambiguous queries
    
    This balance of techniques provides good accuracy while maintaining
    performance and minimizing dependencies on external services.
    
    Usage Example:
        interpreter = QueryInterpreter()
        job_spec = interpreter.interpret(
            query="Get me Apple's annual reports from 2020-2022",
            company="AAPL"
        )
        # Result would include SEC_form_types=["10-K"], date_range=["2020-01-01", "2022-12-31"]
    """
    def interpret(self, query: str, company: str = None, date_range: Optional[List[str]] = None, 
                organization_id: Optional[str] = None, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Interpret a natural language query into a structured job specification.
        
        This method analyzes the given query string using multiple NLP techniques
        to extract relevant parameters and intents, then constructs a structured
        job specification that can be submitted to the orchestrator API.
        
        The interpretation process follows these steps:
        1. Convert query to lowercase for case-insensitive matching
        2. Parse with spaCy NLP for named entity recognition
        3. Perform keyword and synonym matching for financial concepts
        4. Apply fuzzy matching for handling typos and variants
        5. Implement fallback strategies for ambiguous queries
        6. Extract and normalize date ranges
        7. Construct the final job specification
        
        Args:
            query: The natural language query string to interpret
            company: Optional company identifier (overrides any extracted company)
            date_range: Optional explicit date range (overrides any extracted dates)
            organization_id: Optional organization identifier for multi-tenant isolation
            user_id: Optional user identifier for personalization and tracking
            
        Returns:
            Dict containing the structured job specification with the following keys:
            - organization_id: From input parameter
            - user_id: From input parameter
            - company: Company identifier (from input or extracted)
            - date_range: List with start and end dates in ISO format
            - sec_form_types: List of SEC form types to retrieve
            - yahoo_data_types: List of Yahoo Finance data types to retrieve
            - matched_concepts: List of business concepts identified in the query
            - nlp_entities: List of named entities extracted by spaCy
            
        Example:
            Query: "Get Apple's annual reports and earnings for 2020-2022"
            Result: {
                "company": "AAPL",
                "date_range": ["2020-01-01", "2022-12-31"],
                "sec_form_types": ["10-K", "10-Q", "8-K"],
                "yahoo_data_types": ["income_statement"],
                "matched_concepts": ["financial reports", "earnings"]
            }
        """
        # Convert query to lowercase for case-insensitive matching
        query_lc = query.lower()
        
        # Initialize sets to collect matched form types and data sources
        sec_forms = set()
        yahoo_types = set()
        matched_concepts = set()
        
        # Process the query with spaCy NLP for entity extraction
        nlp_doc = nlp(query)
        
        # 1. Named Entity Recognition: Extract organizations, dates, and financial terms
        # -----------------------------------------------------------
        # Use the company parameter if provided, otherwise try to extract from query
        extracted_company = company
        extracted_dates = []
        
        # Iterate through entities identified by spaCy
        for ent in nlp_doc.ents:
            # Extract company if not already provided
            if ent.label_ in ("ORG", "COMPANY") and not extracted_company:
                extracted_company = ent.text
            
            # Collect date entities for later processing
            if ent.label_ in ("DATE",):
                extracted_dates.append(ent.text)
        
        # 2. Keyword and Synonym Matching: Map business terms to technical parameters
        # -----------------------------------------------------------
        # Match SEC form types using business concepts and their synonyms
        for concept, forms in BUSINESS_TO_SEC_FORMS.items():
            # Get all possible keywords for this concept
            keywords = [concept] + BUSINESS_SYNONYMS.get(concept, [])
            
            # Check if any keyword appears in the query
            for kw in keywords:
                if kw in query_lc:
                    # Add all corresponding form types to our results
                    sec_forms.update(forms)
                    matched_concepts.add(concept)
                    break  # Move to next concept once we've found a match
        
        # Similarly match Yahoo data types
        for concept, types in BUSINESS_TO_YAHOO_TYPES.items():
            keywords = [concept] + BUSINESS_SYNONYMS.get(concept, [])
            for kw in keywords:
                if kw in query_lc:
                    yahoo_types.update(types)
                    matched_concepts.add(concept)
                    break
        
        # 3. Fuzzy/Partial Matching: Handle typos and word variants
        # -----------------------------------------------------------
        # This helps match things like "10k" to "10-K" or "annual report" to "annual-report"
        for concept, forms in BUSINESS_TO_SEC_FORMS.items():
            for kw in BUSINESS_SYNONYMS.get(concept, []):
                # Normalize by removing spaces and hyphens, then check if any word contains the keyword
                normalized_kw = kw.replace('-', '').replace(' ', '')
                if any(normalized_kw in word.replace('-', '').replace(' ', '') 
                       for word in query_lc.split()):
                    sec_forms.update(forms)
                    matched_concepts.add(concept)
                    break
        
        # Similarly apply fuzzy matching for Yahoo data types
        for concept, types in BUSINESS_TO_YAHOO_TYPES.items():
            for kw in BUSINESS_SYNONYMS.get(concept, []):
                normalized_kw = kw.replace('-', '').replace(' ', '')
                if any(normalized_kw in word.replace('-', '').replace(' ', '') 
                       for word in query_lc.split()):
                    yahoo_types.update(types)
                    matched_concepts.add(concept)
                    break
        
        # 4. Fallback Strategy: Use reasonable defaults if no matches found
        # -----------------------------------------------------------
        # If no specific data types were identified, default to financial reports
        if not sec_forms and not yahoo_types:
            sec_forms.update(BUSINESS_TO_SEC_FORMS["financial reports"])
            yahoo_types.update(BUSINESS_TO_YAHOO_TYPES["financial reports"])
            matched_concepts.add("financial reports")
        
        # 5. Date Range Processing: Extract and normalize dates
        # -----------------------------------------------------------
        # Use provided date_range parameter if available
        if not date_range:
            # Try to use dates extracted by spaCy
            if extracted_dates:
                # Look for year patterns in the extracted dates
                years = [y for y in re.findall(r"(20\d{2})", " ".join(extracted_dates))]
                
                # Create ISO format date range based on found years
                if len(years) == 2:
                    # Start from beginning of first year to end of second year
                    date_range = [f"{years[0]}-01-01", f"{years[1]}-12-31"]
                elif len(years) == 1:
                    # Use the single year as both start and end
                    date_range = [f"{years[0]}-01-01", f"{years[0]}-12-31"]
            
            # If no dates found yet, try regex-based extraction
            if not date_range:
                date_range = self._extract_date_range(query)
        
        # 6. Construct the Final Job Specification
        # -----------------------------------------------------------
        # Assemble all extracted information into the structured job spec
        return {
            # Context information for multi-tenancy and personalization
            "organization_id": organization_id,
            "user_id": user_id,
            
            # Primary data request parameters
            "company": extracted_company,
            "date_range": date_range,
            "sec_form_types": list(sec_forms),
            "yahoo_data_types": list(yahoo_types),
            
            # Metadata about the interpretation process
            "matched_concepts": list(matched_concepts),
            "nlp_entities": [(ent.text, ent.label_) for ent in nlp_doc.ents],
            
            # Add query string for reference and debugging
            "original_query": query
        }

    def _extract_date_range(self, query: str) -> List[str]:
        """
        Extract date range from a query string using regular expressions.
        
        This helper method uses regex patterns to identify year mentions in the
        query text and convert them to a standardized ISO date range format.
        It serves as a fallback for when spaCy's entity recognition doesn't
        identify date entities.
        
        The method looks for:
        - Four-digit years (e.g., "2020")
        - Year ranges with hyphens (e.g., "2020-2022")
        - Year ranges with "to" or "through" (e.g., "2020 to 2022")
        - Relative time references ("last 3 years", "past decade", etc.)
          would be handled in future enhancements
        
        Args:
            query: The natural language query string to analyze
            
        Returns:
            List containing two ISO date strings [start_date, end_date],
            or an empty list if no date range could be extracted
            
        Example:
            "Get annual reports from 2020 to 2022" -> ["2020-01-01", "2022-12-31"]
            "Show me 2021 financials" -> ["2021-01-01", "2021-12-31"]
        """
        # Regex pattern for finding year references (focus on 2000s for modern filings)
        # Corrected pattern (removed the escaping of the backslash as it was causing issues)
        years = re.findall(r"(20\d{2})", query)
        
        # If we found exactly two years, assume they form a range
        if len(years) == 2:
            # Convert to ISO format: start of first year to end of second year
            return [f"{years[0]}-01-01", f"{years[1]}-12-31"]
            
        # If we found just one year, use it for both start and end
        elif len(years) == 1:
            # Convert to ISO format: full year range
            return [f"{years[0]}-01-01", f"{years[0]}-12-31"]
            
        # No years found, return empty list
        # In a more advanced implementation, we would handle relative dates here
        # (e.g., "last 3 years", "previous quarter", etc.)
        else:
            # Default to empty list - the calling code can apply further defaults if needed
            return [] 