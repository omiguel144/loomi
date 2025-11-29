
"""
Collection Expander - Automatically discover all pages in a collection
======================================================================
Handles pagination and multi-page collections efficiently.
"""

from typing import List, Set
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse
import httpx
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

class CollectionExpander:
    """Expands collection URLs to include all pagination pages."""
    
    @staticmethod
    def expand_collection(client: httpx.Client, collection_url: str, 
                          max_pages: int = 10) -> List[str]:
        """
        Discover all paginated URLs for a collection.
        Returns list of URLs including the original.
        """
        urls = [collection_url]
        
        # Try common pagination patterns
        for page in range(2, max_pages + 1):
            # Shopify style: ?page=2
            if 'buddhapants.com' in collection_url:
                paginated = f"{collection_url.split('?')[0]}?page={page}"
            # Query param style
            elif '?' in collection_url:
                parsed = urlparse(collection_url)
                params = parse_qs(parsed.query)
                params['page'] = [str(page)]
                new_query = urlencode(params, doseq=True)
                paginated = urlunparse(parsed._replace(query=new_query))
            else:
                # Path style: /products/page/2
                paginated = f"{collection_url.rstrip('/')}/page/{page}"
            
            try:
                resp = client.get(paginated, timeout=10)
                if resp.status_code == 200 and len(resp.text) > 500:
                    # Check if it has products
                    soup = BeautifulSoup(resp.text, 'lxml')
                    if soup.find('a', href=lambda x: x and '/products/' in str(x)):
                        urls.append(paginated)
                        logger.debug(f"Found page {page}: {paginated}")
                    else:
                        break  # No more products
                else:
                    break  # No more pages
            except:
                break
        
        return urls
