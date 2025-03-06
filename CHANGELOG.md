# Changelog

All notable changes to the Apache Beam MCP Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2023-03-06

### Added
- Comprehensive API documentation in `docs/API.md`
- Standardized response format across all API endpoints
- Dummy objects for error responses to ensure consistent structure

### Changed
- **BREAKING**: Changed resource identifier from `id` to `mcp_resource_id`
- **BREAKING**: Changed Tool type field from `type` to `tool_type`
- **BREAKING**: Error responses now always include a dummy object in the `data` field
- Updated API responses to exclude unset and null fields
- Added response validation options to API route decorators
- Improved error handling consistency across all endpoints

### Fixed
- Fixed inconsistent response structures in error cases
- Fixed validation errors when returning error responses
- Fixed tool creation tests using incorrect field names

## Previous Versions

For changes in previous versions, please contact the development team. 