# MCP Server Requirements Gathering Guide

This guide helps structure the requirements gathering process for a new MCP server by defining key datapoints needed and providing sample questions to elicit that information.

## Required Datapoints and Sample Questions

### 1. Core Purpose

**Datapoints needed:**
- Primary problem being solved
- Key capabilities required
- Intended users and use cases
- Success criteria

**Sample questions:**
- "What specific problem or need is this MCP server intended to solve?"
- "How will users (AI assistants or humans) benefit from this MCP server?"

### 2. Functional Requirements

**Datapoints needed:**
- Essential features (MVP)
- Future/optional features
- Expected behavior
- Input parameters and validation rules
- Output format and structure
- Error handling approach

**Sample questions:**
- "What specific actions or tools should this MCP server provide?"
- "What format (Markdown, JSON, etc.) should responses use, and what specific data should be included?"
- "How should different types of errors be handled and communicated?"

### 3. External Dependencies

**Datapoints needed:**
- External APIs/services required
- Authentication methods
- Rate limits and quotas
- Data sources
- Required libraries

**Sample questions:**
- "What external APIs or services will this MCP server need to interact with?"
- "What are the authentication requirements, rate limits, or other constraints for these external services?"

### 4. Performance Requirements

**Datapoints needed:**
- Response time expectations
- Throughput requirements
- Data volume considerations
- Resource constraints

**Sample questions:**
- "What are the maximum acceptable response times for this service?"
- "What volume of data might be processed in a typical request?"

### 5. Security Requirements

**Datapoints needed:**
- Sensitive data handling
- Authentication needs
- Authorization rules
- Data privacy considerations

**Sample questions:**
- "What sensitive data will this MCP server handle?"
- "Are there any data privacy requirements to consider?"

### 6. Deployment Context

**Datapoints needed:**
- Target deployment environment
- Required environment variables
- Installation requirements
- Integration points

**Sample questions:**
- "Where will this MCP server be deployed?"
- "What environment variables or configuration will be needed?"

### 7. Edge Cases and Limitations

**Datapoints needed:**
- Known edge cases
- Error scenarios
- Fallback mechanisms
- Timeout handling

**Sample questions:**
- "What happens if external services are unavailable?"
- "How should the server handle unexpected input or data formats?"

### 8. Testing Requirements

**Datapoints needed:**
- Critical test scenarios
- Test coverage expectations
- Performance testing needs
- Test environment requirements

**Sample questions:**
- "What are the critical test cases for this MCP server?"
- "What level of test coverage is required?"

## Domain-Specific Datapoints

### For Data Retrieval MCP Servers
- Data sources to access
- Filtering/pagination requirements
- Data freshness requirements

### For API Integration MCP Servers
- Specific endpoints needed
- Credential management approach
- Response handling requirements

### For Processing/Transformation MCP Servers
- Input formats supported
- Transformation logic
- Processing error handling

### For Search MCP Servers
- Content to be searchable
- Search algorithm requirements
- Result ranking/presentation needs

## Gathering Technique Tips

1. **Start broad, then narrow:** Begin with general questions about purpose and goals, then drill down into specifics.

2. **Use examples:** Ask for examples of expected inputs and outputs to clarify requirements.

3. **Explore boundaries:** Ask about edge cases, exceptional conditions, and what should happen when things go wrong.

4. **Validate understanding:** Paraphrase requirements back to ensure accurate understanding.

5. **Consider the future:** Ask about potential future needs to design for extensibility.

6. **Document assumptions:** Note any assumptions made during the requirements gathering process.

7. **Identify constraints:** Determine any technical, time, or resource constraints that will impact implementation.