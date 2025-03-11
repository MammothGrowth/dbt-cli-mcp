# Task T2: Web Scraper Module Implementation

## Objective

Create a robust, reusable module that fetches HTML content from API documentation websites with error handling and retry logic.

## Specifications

### Requirements

- Create a `fetch_html()` function that retrieves HTML content from a URL
- Implement error handling for HTTP status codes (403, 404, 429, 500, etc.)
- Implement user-agent rotation to avoid blocking
- Add configurable timeout handling with exponential backoff
- Include proper logging at appropriate levels

### Implementation Details

```python
def fetch_html(url: str, max_retries: int = 3, timeout: int = 10, 
               backoff_factor: float = 1.5) -> str:
    """Fetch HTML content from a URL with retry and error handling."""
    # User-agent rotation implementation
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15...",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36..."
    ]
    
    # URL validation
    if not url.startswith(('http://', 'https://')):
        raise ValueError(f"Invalid URL: {url}")
    
    # Retry loop with exponential backoff
    for attempt in range(max_retries + 1):
        try:
            # Select a random user agent
            user_agent = random.choice(user_agents)
            headers = {"User-Agent": user_agent}
            
            # Make the request with timeout
            response = requests.get(url, headers=headers, timeout=timeout)
            
            # Handle response based on status code
            if response.status_code == 200:
                return response.text
            elif response.status_code == 403:
                # On 403, retry with a different user agent
                continue
            elif response.status_code == 404:
                raise RuntimeError(f"Page not found (404): {url}")
            elif response.status_code == 429:
                # On rate limit, use a longer backoff
                wait_time = backoff_factor * (2 ** attempt) * 2
                time.sleep(wait_time)
                continue
        except requests.RequestException as e:
            if attempt < max_retries:
                wait_time = backoff_factor * (2 ** attempt)
                time.sleep(wait_time)
            else:
                raise RuntimeError(f"Failed to fetch {url} after {max_retries+1} attempts")
```

### Error Handling

- HTTP 403: Retry with different user agent
- HTTP 404: Raise error immediately
- HTTP 429: Retry with longer backoff
- HTTP 5xx: Retry with standard backoff
- Connection timeouts: Retry with standard backoff

## Acceptance Criteria

- [ ] Retrieves HTML content from common API documentation sites
- [ ] User agent rotation works correctly for 403 errors
- [ ] Exponential backoff implemented for retries
- [ ] All errors handled gracefully with appropriate logging
- [ ] Raises clear exceptions when retrieval fails

## Testing

### Key Test Cases

- Success case with mock response
- 403 response with user agent rotation
- 404 response (should raise error)
- 429 response with longer backoff
- Max retry behavior
- Invalid URL handling

### Example Test

```python
@responses.activate
def test_fetch_html_403_retry():
    """Test retry with user agent rotation on 403."""
    # Setup mock responses - first 403, then 200
    responses.add(
        responses.GET,
        "https://example.com/docs",
        body="Forbidden",
        status=403
    )
    responses.add(
        responses.GET,
        "https://example.com/docs",
        body="<html><body>Success after retry</body></html>",
        status=200
    )
    
    # Call the function
    html = fetch_html("https://example.com/docs")
    
    # Verify the result
    assert "<body>Success after retry</body>" in html
```

## Dependencies

- Task T1: Project Setup

## Developer Workflow

1. Review project structure set up in T1
2. Write tests first
3. Implement the fetch_html() function
4. Verify all tests pass
5. Update work progress log