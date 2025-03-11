# dbt CLI MCP Server Implementation Plan

## Project Overview

**Project Name**: DBT CLI MCP Server

**Purpose**: Create an MCP server that wraps the functionality of the dbt CLI to enable a Coding Agent to make decisions on what files need to be added to scope, testing, building, etc

**Core Capabilities**:
- provide standard interface to core CLI commands: build, run, compile, test, ls, show, debug, help, init, retry, run-operation, seed, deps, snapshot
- always output structured JSON when command supports `--output json`
- be extremely detailed in the MCP tool descritions and the parameter descriptions.  It should guide the client to WHY it should use the command.  The MCP server should compete to be used against other tools/options the Agent may have

## docs

dbt command reference: docs/dbt_cheat_sheet.md
Guide for Coding Agents to build their own MCP:  docs/llm_guide_to_mcp.md
details about python MCP library: docs/python_fastMCP.md



## architecture

1. always use 'uv' 
2. never run python directly
3. use latest python version
4. always output 

## Getting Started

load in all /docs/ into context
use mcp_architecture_instructions/GETTING_STARTED.md as the guide for getting started