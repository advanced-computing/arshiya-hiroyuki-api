## Lab4 Project

### Team: Arshiya and Hiroyuki

### Name: NYC School Bus Delay API

### Overview
The Bus Delay API provides historical data on school bus delays. This API helps analyze delay trends, identify causes, and improve transit planning by offering various endpoints to query and filter data based on different criteria.

### Features
- Date Analysis: Obtain the number of delays by date.
- Reason Analysis: Retrieve the number of delays based on a specific reason.
- Borough Analysis: Retrieve the number of delays based on the borough.
- Custom Queries: Filter data based on specific columns, limits, and offsets.
- ID Research: Research data based on ID.
- Format Flexibility: Retrieve data in JSON or CSV format.

### Endpoints
- GET / Echoes: the request details for debugging purposes
- GET /date?date=<YYYY-MM-DD>: Obtain the number of delays for a specific date. This data includs dates from September 3, 2024 to January 28, 2025.
- GET /reason?reason=<REASON>: Retrieve the number of delays for a specific reason. REASON includes Accident, Delayed by School, Flat Tire, Heavy Traffic, Late return from Field Trip, Mechanical Problem, Problem Run, Weather Conditions, Won`t Start, and Other.
- GET /boro?boro=<BOROUGH>: Retrieve the number of delays for a specific borough. BOROUGH includes All Boroughs, Brooklyn, Bronx, Connecticut, Manhattan, Nassau County, New Jersey, Queens, Rockland County, Staten Island and Westchester
- GET /date_records?date=<YYYY-MM-DD>&format=<json|csv>: Fetch all records for a specific date in the specified format.
- GET /records?format=<json|csv>&column=<COLUMN>&value=<VALUE>&limit=<LIMIT>&offset=<OFFSET>: List records with optional filtering based on a specific column, value, limit, and offset.
- GET /record/<ID>?format=<json|csv>: Retrieve a single record by identifier in the specified format. The Busbreakdown_ID starts from 1933904 to 1972650, while the numbers jump around in the middle.

### Example Usage

Count records by date:
- GET /date?date=2024-09-26

Count records by reason:
- GET /reason?reason=Heavy%20Traffic

Count records by borough:
- GET /boro?boro=Manhattan

Fetch all delays for a specific date (JSON format):
- GET /date_records?date=2024-09-27&format=json

Fetch all delays for a specific date (CSV format):
- GET /date_records?date=2024-09-27&format=csv

Fetch records with pagination:
- GET /records?format=jason&column=Reason&value=Heavy%20Traffic&limit=5&offset=0

Fetch a specific record by ID:
- GET /record/1933906?format=json