"""
XBRL Data Handler
----------------

Specialized handler for XBRL (eXtensible Business Reporting Language) data extraction
from SEC filings. This module focuses on retrieving structured financial data in
standardized XBRL format, enabling consistent extraction of financial metrics
across companies and time periods.

XBRL provides standardized financial reporting elements that allow for quantitative
analysis and financial modeling with consistent data structure across different
companies and reporting periods.

Key responsibilities:
1. XBRL fact retrieval for specific financial concepts
2. Company concept mapping and validation
3. Structured financial data extraction
4. Standardized XBRL data envelope creation

This handler uses the delegation pattern to leverage the core SECClient
for API communication while providing specialized functionality for
XBRL data processing workflows.
"""

import httpx
import datetime
import uuid
from typing import Dict, Any, Optional

from config import settings
from .client import SECClient
from data_acquisition.errors import (
    SalescienceError, ErrorContext,
    SECError, SECAuthenticationError, SECRateLimitError, SECNotFoundError, SECParsingError,
    MissingConfigurationError, DataSourceConnectionError, DataSourceTimeoutError,
    ValidationError, log_error
)

# Get centralized logging configuration
from data_acquisition.utils import configure_logging

# Get logger specific to this module
logger = configure_logging("sec_xbrl")

# Configuration from centralized settings
SEC_API_KEY = settings.api_keys.sec
SEC_API_BASE_URL = settings.service_urls.sec_api_base


class XBRLHandler:
    """
    Specialized handler for XBRL (eXtensible Business Reporting Language) data extraction.
    
    This class provides focused functionality for retrieving structured financial data
    from SEC filings in XBRL format. XBRL standardizes financial reporting elements,
    allowing for consistent extraction of financial metrics across companies and
    time periods.
    
    The handler uses the delegation pattern, relying on SECClient for core API 
    communication while implementing specialized business logic for XBRL data
    processing and structured financial data extraction.
    
    Common XBRL concepts that can be retrieved:
    - us-gaap:Revenues - Total revenue
    - us-gaap:NetIncomeLoss - Net income/loss  
    - us-gaap:Assets - Total assets
    - us-gaap:Liabilities - Total liabilities
    - us-gaap:StockholdersEquity - Shareholders' equity
    - us-gaap:EarningsPerShareBasic - Basic EPS
    - us-gaap:OperatingIncomeLoss - Operating income
    - us-gaap:CashAndCashEquivalentsAtCarryingValue - Cash and equivalents
    - us-gaap:PropertyPlantAndEquipmentNet - PP&E (net)
    - us-gaap:RetainedEarningsAccumulatedDeficit - Retained earnings
    
    Usage:
        client = SECClient()
        xbrl_handler = XBRLHandler(client)
        
        # Get revenue data for a company
        revenue_data = xbrl_handler.fetch_xbrl_fact({
            'ticker': 'AAPL'
        }, 'us-gaap:Revenues')
        
        # Get net income data
        income_data = xbrl_handler.fetch_xbrl_fact({
            'cik': '0000320193'
        }, 'us-gaap:NetIncomeLoss')
    """
    
    def __init__(self, sec_client: Optional[SECClient] = None):
        """
        Initialize the XBRL handler.
        
        Args:
            sec_client: Optional SECClient instance. If not provided, creates a new one.
        """
        self.client = sec_client or SECClient()
        
        # Verify API key is available
        if not SEC_API_KEY:
            logger.warning("SEC_API_KEY not set in settings.api_keys.sec. XBRL handler will not function.")
    
    def fetch_xbrl_fact(self, params: Dict[str, Any], concept: str, 
                       request_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetches a specific XBRL fact (e.g., us-gaap:Revenues) for a company using sec-api.io.
        
        XBRL (eXtensible Business Reporting Language) is a structured data format that 
        standardizes financial reporting elements. This method accesses specific financial 
        data points in XBRL format, allowing for consistent extraction of standardized 
        financial metrics across companies and time periods. This is particularly valuable 
        for quantitative analysis and financial modeling.
        
        The method retrieves historical data for the specified concept across multiple
        reporting periods, providing a time series of financial data that can be used
        for trend analysis, financial modeling, and comparative analysis.
        
        Args:
            params: Dictionary that must include 'cik' or 'ticker' for company identification
            concept: XBRL concept/tag specifying the financial metric to retrieve.
                    Format is typically "us-gaap:ConceptName" where ConceptName is the
                    specific financial element (e.g., 'us-gaap:Revenues', 'us-gaap:NetIncomeLoss')
            request_id: Optional request ID for tracing and logging
            
        Returns:
            Dict[str, Any]: Envelope with XBRL fact data and metadata.
            The returned envelope contains:
                - content: A dictionary with the XBRL fact data, typically including
                  values across multiple reporting periods with metadata about each value
                - content_type: 'json' (XBRL data is returned in JSON format)
                - source: 'sec-xbrl' (identifies this as XBRL data from SEC)
                - status: 'success' for successful retrieval, or error status
                - metadata: Information about the request including CIK, ticker,
                  concept name, retrieval timestamp, and source URL
            
        Raises:
            MissingConfigurationError: If SEC_API_KEY is not configured
            ValidationError: If required parameters are missing or invalid
            SECError: If SEC API call fails
            SECNotFoundError: If no XBRL data is found for the specified concept
            SECAuthenticationError: If authentication fails
            SECRateLimitError: If rate limit is exceeded
            
        Example usage:
            xbrl_handler = XBRLHandler()
            
            # Get revenue data
            result = xbrl_handler.fetch_xbrl_fact({'ticker': 'AAPL'}, 'us-gaap:Revenues')
            
            # Get balance sheet data
            assets = xbrl_handler.fetch_xbrl_fact({'ticker': 'MSFT'}, 'us-gaap:Assets')
            
            # Access the data
            revenue_facts = result['content']
            for period, value in revenue_facts.items():
                print(f"Period: {period}, Revenue: {value}")
            
        Note:
            The XBRL data returned includes multiple reporting periods and may contain
            both annual and quarterly data points. Each data point includes metadata
            about the reporting period, fiscal year/quarter, and submission details.
            
            Common XBRL concepts include:
            - us-gaap:Revenues - Total revenue/sales
            - us-gaap:NetIncomeLoss - Net income or loss
            - us-gaap:Assets - Total assets
            - us-gaap:Liabilities - Total liabilities  
            - us-gaap:StockholdersEquity - Shareholders' equity
            - us-gaap:EarningsPerShareBasic - Basic earnings per share
            - us-gaap:OperatingIncomeLoss - Operating income or loss
            - us-gaap:CashAndCashEquivalentsAtCarryingValue - Cash and cash equivalents
            - us-gaap:PropertyPlantAndEquipmentNet - Property, plant & equipment (net)
            - us-gaap:RetainedEarningsAccumulatedDeficit - Retained earnings
        """
        # Generate request_id if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
        
        logger.info(f"[{request_id}] Fetching XBRL fact for concept: {concept}")
        
        # Create error context for XBRL-specific operations
        context = ErrorContext(
            request_id=request_id,
            source_type="SEC-XBRL",
            xbrl_concept=concept
        )
        
        # Validate XBRL concept format
        if not concept or not isinstance(concept, str):
            error_msg = "XBRL concept must be a non-empty string"
            logger.error(f"[{request_id}] {error_msg}")
            raise ValidationError(error_msg, context=context)
        
        # Basic concept format validation (should contain a colon for namespace)
        if ':' not in concept:
            logger.warning(f"[{request_id}] XBRL concept '{concept}' does not appear to have a namespace prefix")
        
        # Check SEC API key availability
        if not SEC_API_KEY:
            error_msg = "SEC_API_KEY is not set in the centralized configuration (settings.api_keys.sec)."
            logger.error(f"[{request_id}] {error_msg}")
            raise MissingConfigurationError(error_msg, context=context)
        
        # Validate and resolve company parameters using the client
        try:
            cik, ticker = self.client._validate_and_resolve_company_params(params, request_id)
            context.company = {"cik": cik, "ticker": ticker}
        except (ValidationError, SECError) as e:
            logger.error(f"[{request_id}] Failed to validate company parameters for XBRL concept {concept}: {e}")
            raise
        
        # Construct the XBRL API URL
        # SEC API endpoint for company concept data: /xbrl/companyconcept
        xbrl_url = f"{SEC_API_BASE_URL}/xbrl/companyconcept?cik={cik.lstrip('0')}&concept={concept}&token={SEC_API_KEY}"
        
        # Set up request headers
        headers = {
            "User-Agent": settings.sec_user_agent,
            "Accept": "application/json",
            "X-Request-ID": request_id
        }
        
        logger.debug(f"[{request_id}] XBRL API URL: {xbrl_url}")
        
        try:
            logger.info(f"[{request_id}] Fetching XBRL fact {concept} for CIK {cik}")
            
            # Make the API request with timeout
            resp = httpx.get(xbrl_url, headers=headers, timeout=settings.sec_request_timeout_sec)
            
            # Log response details
            logger.debug(f"[{request_id}] XBRL API status code: {resp.status_code}")
            
            # Handle different response scenarios
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    logger.debug(f"[{request_id}] XBRL API response received")
                    
                    # Validate response structure
                    if not data:
                        error_msg = f"Empty response from XBRL API for concept {concept}"
                        logger.error(f"[{request_id}] {error_msg}")
                        raise SECNotFoundError(error_msg, context=context)
                    
                    # Check for facts data in response
                    if 'facts' not in data:
                        error_msg = f"No XBRL facts found for CIK {cik}, concept {concept}"
                        logger.warning(f"[{request_id}] {error_msg}")
                        raise SECNotFoundError(error_msg, context=context)
                    
                    facts_data = data['facts']
                    
                    # Additional validation - ensure facts data is not empty
                    if not facts_data:
                        error_msg = f"Empty XBRL facts data for CIK {cik}, concept {concept}"
                        logger.warning(f"[{request_id}] {error_msg}")
                        raise SECNotFoundError(error_msg, context=context)
                    
                    logger.info(f"[{request_id}] Successfully retrieved XBRL data for concept {concept}")
                    
                    # Create standardized response envelope
                    envelope = {
                        'content': facts_data,
                        'content_type': 'json',
                        'source': 'sec-xbrl',
                        'status': 'success',
                        'metadata': {
                            'cik': cik,
                            'ticker': ticker,
                            'concept': concept,
                            'retrieved_at': datetime.datetime.utcnow().isoformat() + 'Z',
                            'xbrl_url': xbrl_url,
                            'request_id': request_id
                        }
                    }
                    
                    # Add additional metadata if available from the response
                    if 'label' in data:
                        envelope['metadata']['concept_label'] = data['label']
                    if 'description' in data:
                        envelope['metadata']['concept_description'] = data['description']
                    if 'entityName' in data:
                        envelope['metadata']['entity_name'] = data['entityName']
                    
                    return envelope
                    
                except ValueError as e:
                    error_msg = f"Failed to parse XBRL API response for concept {concept}"
                    logger.error(f"[{request_id}] {error_msg}: {e}")
                    raise SECParsingError(error_msg, context=context, cause=e)
                    
            elif resp.status_code == 400:
                error_msg = f"Bad request to XBRL API - invalid concept or parameters: {resp.text}"
                logger.error(f"[{request_id}] {error_msg}")
                raise ValidationError(error_msg, context=context)
                
            elif resp.status_code == 401 or resp.status_code == 403:
                error_msg = f"XBRL API authentication failed: {resp.text}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECAuthenticationError(error_msg, context=context)
                
            elif resp.status_code == 404:
                error_msg = f"XBRL concept '{concept}' not found for CIK {cik}"
                logger.warning(f"[{request_id}] {error_msg}")
                raise SECNotFoundError(error_msg, context=context)
                
            elif resp.status_code == 429:
                error_msg = f"XBRL API rate limit exceeded: {resp.text}"
                logger.warning(f"[{request_id}] {error_msg}")
                raise SECRateLimitError(error_msg, context=context)
                
            else:
                error_msg = f"SEC API XBRL endpoint returned status {resp.status_code}: {resp.text}"
                logger.error(f"[{request_id}] {error_msg}")
                raise SECError(error_msg, context=context)
                
        except httpx.TimeoutException as e:
            error_msg = f"Timeout while fetching XBRL data for concept {concept}"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise DataSourceTimeoutError(error_msg, context=context, cause=e)
            
        except httpx.NetworkError as e:
            error_msg = f"Network error while fetching XBRL data for concept {concept}"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise DataSourceConnectionError(error_msg, context=context, cause=e)
            
        except (SECError, SECAuthenticationError, SECRateLimitError, SECNotFoundError, 
                SECParsingError, ValidationError, MissingConfigurationError):
            # Re-raise SEC-specific and framework errors without wrapping
            raise
            
        except Exception as e:
            error_msg = f"Unexpected error fetching XBRL fact for concept {concept}"
            logger.error(f"[{request_id}] {error_msg}: {e}")
            raise SECError(error_msg, context=context, cause=e)
    
    def _validate_xbrl_concept(self, concept: str) -> bool:
        """
        Validate XBRL concept format and common patterns.
        
        Args:
            concept: XBRL concept string to validate
            
        Returns:
            bool: True if concept appears to be valid format
            
        Note:
            This is a basic format validation. The SEC API will provide
            the definitive validation of whether a concept exists.
        """
        if not concept or not isinstance(concept, str):
            return False
        
        # Basic format check - should have namespace prefix
        if ':' not in concept:
            return False
        
        # Check for common XBRL namespaces
        common_namespaces = [
            'us-gaap:',
            'dei:',  # Document and Entity Information
            'ifrs:',  # International Financial Reporting Standards
            'srt:',   # SEC Reporting Taxonomy
        ]
        
        has_known_namespace = any(concept.startswith(ns) for ns in common_namespaces)
        
        # Log warning for unknown namespaces but don't reject
        if not has_known_namespace:
            logger.debug(f"XBRL concept '{concept}' uses uncommon namespace - validation will be performed by SEC API")
        
        return True
    
    def get_common_concepts(self) -> Dict[str, str]:
        """
        Get a dictionary of common XBRL concepts with descriptions.
        
        Returns:
            Dict mapping concept names to human-readable descriptions
            
        Note:
            This is a convenience method to help users discover commonly
            used XBRL concepts for financial data extraction.
        """
        return {
            'us-gaap:Revenues': 'Total revenues/sales',
            'us-gaap:NetIncomeLoss': 'Net income or loss',
            'us-gaap:Assets': 'Total assets',
            'us-gaap:Liabilities': 'Total liabilities',
            'us-gaap:StockholdersEquity': 'Shareholders\' equity',
            'us-gaap:EarningsPerShareBasic': 'Basic earnings per share',
            'us-gaap:EarningsPerShareDiluted': 'Diluted earnings per share',
            'us-gaap:OperatingIncomeLoss': 'Operating income or loss',
            'us-gaap:GrossProfit': 'Gross profit',
            'us-gaap:CostOfRevenue': 'Cost of revenue/sales',
            'us-gaap:CashAndCashEquivalentsAtCarryingValue': 'Cash and cash equivalents',
            'us-gaap:PropertyPlantAndEquipmentNet': 'Property, plant & equipment (net)',
            'us-gaap:RetainedEarningsAccumulatedDeficit': 'Retained earnings',
            'us-gaap:CommonStockSharesOutstanding': 'Common stock shares outstanding',
            'us-gaap:CommonStockSharesIssued': 'Common stock shares issued',
            'us-gaap:ResearchAndDevelopmentExpense': 'Research and development expense',
            'us-gaap:InterestExpense': 'Interest expense',
            'us-gaap:IncomeTaxExpenseBenefit': 'Income tax expense/benefit',
            'us-gaap:DepreciationDepletionAndAmortization': 'Depreciation and amortization',
            'us-gaap:AccountsReceivableNetCurrent': 'Accounts receivable (net, current)',
            'us-gaap:InventoryNet': 'Inventory (net)',
            'us-gaap:AccountsPayableCurrent': 'Accounts payable (current)',
            'us-gaap:DebtCurrent': 'Current portion of debt',
            'us-gaap:DebtNoncurrent': 'Non-current debt',
            'us-gaap:DeferredRevenueCurrent': 'Deferred revenue (current)',
            'us-gaap:MarketableSecuritiesCurrent': 'Marketable securities (current)',
        }