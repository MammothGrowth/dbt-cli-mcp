# Weather MCP Server Example

This example demonstrates a production-ready MCP server that fetches weather data from the OpenWeather API.

## Capabilities

- Tool: `get_weather` - Fetch forecast for a specified city and number of days
- Resource: `weather://{city}/current` - Get current weather for a specified city
- Dual operation as both MCP server and CLI tool

## Implementation

```python
#!/usr/bin/env python3
# /// script
# requires-python = ">=3.8"
# dependencies = [
#   "mcp[cli]>=0.1.0",
#   "requests>=2.31.0",
#   "pydantic>=2.0.0",
# ]
# ///

import os
import sys
import logging
import argparse
from typing import Annotated, Dict, Any, Optional
import requests
from pydantic import BaseModel, Field
from mcp.server.fastmcp import FastMCP
from mcp.shared.exceptions import McpError
from mcp.types import ErrorData, INTERNAL_ERROR, INVALID_PARAMS

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("weather_mcp")

def configure_logging(debug=False):
    if debug:
        logger.setLevel(logging.DEBUG)

# Create FastMCP server
mcp = FastMCP("Weather Service")

# Define parameter models with validation
class WeatherParams(BaseModel):
    """Parameters for weather forecast."""
    city: Annotated[str, Field(description="City name")]
    days: Annotated[
        Optional[int],
        Field(default=1, ge=1, le=5, description="Number of days (1-5)"),
    ]

def fetch_weather(city: str, days: int = 1) -> Dict[str, Any]:
    """Fetch weather data from OpenWeather API."""
    logger.debug(f"Fetching weather for {city}, {days} days")
    
    # Get API key from environment
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        raise McpError(ErrorData(
            code=INTERNAL_ERROR,
            message="OPENWEATHER_API_KEY environment variable is required"
        ))
    
    try:
        # Make API request
        response = requests.get(
            "https://api.openweathermap.org/data/2.5/forecast",
            params={
                "q": city,
                "cnt": days * 8,  # API returns data in 3-hour steps
                "appid": api_key,
                "units": "metric"
            },
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Weather API request failed: {str(e)}", exc_info=True)
        raise McpError(ErrorData(
            code=INTERNAL_ERROR,
            message=f"Weather API error: {str(e)}"
        ))

@mcp.tool()
def get_weather(city: str, days: int = 1) -> str:
    """Get weather forecast for a city."""
    try:
        # Validate and fetch data
        if days < 1 or days > 5:
            raise ValueError("Days must be between 1 and 5")
        weather_data = fetch_weather(city, days)
        
        # Format the response
        forecast_items = weather_data.get("list", [])
        if not forecast_items:
            return f"No forecast data available for {city}"
        
        result = f"Weather forecast for {city}:\n\n"
        current_date = None
        
        for item in forecast_items:
            # Group by date
            dt_txt = item.get("dt_txt", "")
            date_part = dt_txt.split(" ")[0] if dt_txt else ""
            
            if date_part and date_part != current_date:
                current_date = date_part
                result += f"## {current_date}\n\n"
            
            # Add forecast details
            time_part = dt_txt.split(" ")[1].split(":")[0] + ":00" if dt_txt else ""
            temp = item.get("main", {}).get("temp", "N/A")
            weather_desc = item.get("weather", [{}])[0].get("description", "N/A")
            result += f"- **{time_part}**: {temp}°C, {weather_desc}\n"
        
        return result
    except ValueError as e:
        raise McpError(ErrorData(code=INVALID_PARAMS, message=str(e)))
    except McpError:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise McpError(ErrorData(
            code=INTERNAL_ERROR,
            message=f"Error getting weather forecast: {str(e)}"
        ))

@mcp.resource("weather://{city}/current")
def get_current_weather_resource(city: str) -> str:
    """Get current weather for a city."""
    try:
        # Get API key
        api_key = os.environ.get("OPENWEATHER_API_KEY")
        if not api_key:
            raise ValueError("OPENWEATHER_API_KEY environment variable is required")
        
        # Fetch current weather
        response = requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={"q": city, "appid": api_key, "units": "metric"},
            timeout=10
        )
        response.raise_for_status()
        
        # Format response
        data = response.json()
        return {
            "temperature": data.get("main", {}).get("temp", "N/A"),
            "conditions": data.get("weather", [{}])[0].get("description", "N/A"),
            "humidity": data.get("main", {}).get("humidity", "N/A"),
            "wind_speed": data.get("wind", {}).get("speed", "N/A"),
            "timestamp": data.get("dt", 0)
        }
    except Exception as e:
        logger.error(f"Error getting current weather: {str(e)}", exc_info=True)
        raise McpError(ErrorData(
            code=INTERNAL_ERROR,
            message=f"Error getting current weather: {str(e)}"
        ))

# Dual mode operation (MCP server or CLI tool)
if __name__ == "__main__":
    # Test mode
    if "--test" in sys.argv:
        logger.info("Testing Weather MCP server initialization...")
        try:
            api_key = os.environ.get("OPENWEATHER_API_KEY")
            if not api_key:
                raise ValueError("OPENWEATHER_API_KEY environment variable is required")
            logger.info("Weather MCP server initialization test successful")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Weather MCP server initialization test failed: {str(e)}")
            sys.exit(1)
    # MCP server mode
    elif len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] == "--debug"):
        if "--debug" in sys.argv:
            configure_logging(debug=True)
        logger.info("Starting Weather MCP server")
        mcp.run()
    # CLI tool mode
    else:
        args = argparse.ArgumentParser().parse_args()
        if hasattr(args, 'city') and args.city:
            print(get_weather(args.city, getattr(args, 'days', 1)))
        else:
            print("Error: --city is required for CLI mode")
            sys.exit(1)
```

## Key Design Patterns

### 1. Structured Error Handling

```python
try:
    # Operation that might fail
except ValueError as e:
    # Client errors - invalid input
    raise McpError(ErrorData(code=INVALID_PARAMS, message=str(e)))
except requests.RequestException as e:
    # External service errors
    raise McpError(ErrorData(code=INTERNAL_ERROR, message=f"API error: {str(e)}"))
except Exception as e:
    # Unexpected errors
    raise McpError(ErrorData(code=INTERNAL_ERROR, message=f"Unexpected error: {str(e)}"))
```

### 2. Parameter Validation

```python
# Schema-based validation with Pydantic
class WeatherParams(BaseModel):
    city: Annotated[str, Field(description="City name")]
    days: Annotated[Optional[int], Field(default=1, ge=1, le=5)]

# Runtime validation
if days < 1 or days > 5:
    raise ValueError("Days must be between 1 and 5")
```

### 3. Environment Variable Management

```python
api_key = os.environ.get("OPENWEATHER_API_KEY")
if not api_key:
    raise McpError(ErrorData(
        code=INTERNAL_ERROR,
        message="OPENWEATHER_API_KEY environment variable is required"
    ))
```

### 4. Resource URI Templates

```python
@mcp.resource("weather://{city}/current")
def get_current_weather_resource(city: str) -> str:
    """Get current weather for a city."""
    # Implementation...
```

### 5. Configurable Logging

```python
def configure_logging(debug=False):
    if debug:
        logger.setLevel(logging.DEBUG)

# Usage
logger.debug("Detailed operation information")
logger.info("Normal operational messages")
logger.warning("Something concerning but not critical")
logger.error("Something went wrong", exc_info=True)
```

## Testing and Usage

### Unit Testing

```python
@patch('weather_mcp.fetch_weather')
def test_get_weather_formatting(mock_fetch):
    # Setup test data
    mock_fetch.return_value = {"list": [{"dt_txt": "2023-04-15 12:00:00", 
                                        "main": {"temp": 15.2}, 
                                        "weather": [{"description": "clear sky"}]}]}
    
    # Call function
    result = get_weather("London", 1)
    
    # Verify results
    assert "Weather forecast for London" in result
    assert "**12:00**: 15.2°C, clear sky" in result
```

### Running Tests

```bash
# Run all tests
uv run -m pytest

# Run with coverage
uv run -m pytest --cov=weather_mcp
```

### Registering with Claude/RooCode

Add to MCP settings (`~/Library/Application Support/Code/User/globalStorage/rooveterinaryinc.roo-cline/settings/cline_mcp_settings.json`):

```json
{
  "mcpServers": {
    "weather": {
      "command": "uv",
      "args": ["run", "-s", "/path/to/weather_mcp.py"],
      "env": {
        "OPENWEATHER_API_KEY": "your_api_key_here"
      },
      "disabled": false
    }
  }
}