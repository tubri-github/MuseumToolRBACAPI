# Fisheries Database API with Elasticsearch

This FastAPI application provides an API for the fisheries collection database with Elasticsearch integration for powerful search capabilities.

## Features

- FastAPI backend for efficient API development
- Elasticsearch integration for advanced search capabilities
- Compatibility with the existing PostgreSQL database
- Automatic data synchronization between PostgreSQL and Elasticsearch

## Getting Started

### Prerequisites

- Python 3.9+
- PostgreSQL database
- Elasticsearch 7.x or 8.x

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd fisheries-api
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   ```bash
   cp .env.template .env
   # Edit the .env file with your database and Elasticsearch settings
   ```

### Running the Application

Start the FastAPI application:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be accessible at: http://localhost:8000

### Initial Data Synchronization

To populate Elasticsearch with your existing PostgreSQL data:

```bash
curl -X POST "http://localhost:8000/search/sync" -H "Content-Type: application/json" -d '{"force_full_sync": true}'
```

## API Documentation

- Interactive API documentation: http://localhost:8000/docs
- ReDoc alternative documentation: http://localhost:8000/redoc

## API Endpoints

The API endpoints mirror the original Node.js implementation:

### Statistics
- GET `/speciesStats` - Get species statistics
- GET `/familyList` - Get family list
- GET `/genusList` - Get genus list for a specific family
- GET `/speciesList` - Get species list for a specific genus
- GET `/timeline` - Get monthly collection timeline

### Lots
- POST `/lot` - Create a new lot
- PUT `/lot` - Update a lot
- POST `/updatelot` - Update a lot (alternate endpoint)
- GET `/lot/:ids/:limit` - Get lots by IDs
- GET `/lotString/:catid` - Get lot string by catalog ID
- GET `/jarsizes` - Get jar sizes
- GET `/preparation` - Get preparation types
- POST `/deaccession` - Deaccession a lot
- GET `/deaccession/:priid` - Get deaccession information
- GET `/lots` - Get lots with advanced filtering
- GET `/lotcount/:year` - Get lot count by year
- GET `/determinations/:primaryID` - Get determinations by primary ID
- GET `/preparations/:primaryID` - Get preparations by primary ID

### ULM
- GET `/ulm` - Get ULM record
- GET `/ulmrandom` - Get a random ULM record
- GET `/ulmlotlist` - Get ULM records with filtering
- POST `/updateulmrandom` - Update a ULM record
- GET `/reportulm` - Report a ULM record
- GET `/ulmstatsu` - Get ULM statistics by user
- GET `/ulmstatsreview` - Get ULM statistics by review status
- GET `/ulmreportdata` - Generate Excel report for ULM data
- GET `/ulmnotfoundreportdata` - Generate Excel report for not found ULM data

### OST
- GET `/ost` - Get OST record
- GET `/ostlist` - Get OST records with filtering
- POST `/updateost` - Update an OST record
- GET `/reportost` - Report an OST record

### Locality
- GET `/locality/:keyword` - Get locality by keyword
- POST `/locality` - Create a new locality
- GET `/localitycount/:year` - Get locality count by year
- GET `/localityAdvanced/` - Get localities with advanced filtering
- GET `/country` - Get countries
- GET `/state` - Get states
- GET `/county` - Get counties
- GET `/continent` - Get continents

### Taxon
- GET `/taxons/:keyword` - Get taxa by keyword
- GET `/familysearch/:keyword` - Get families by keyword
- GET `/determination` - Get determinations
- POST `/taxon` - Create a new taxon
- GET `/determiners` - Get determiners

### Authentication
- POST `/login` - Authenticate user
- GET `/getInfo` - Get user information
- POST `/logout` - Log out user

### Admin
- GET `/recentadded` - Get recently added items

### Search
- GET `/search/ulm` - Search ULM data
- GET `/search/ost` - Search OST data
- GET `/search/locality` - Search locality data
- GET `/search/taxon` - Search taxon data
- GET `/search/lots` - Search lots data
- GET `/search/loans` - Search loans data
- POST `/search/sync` - Synchronize data between PostgreSQL and Elasticsearch

## Project Structure

```
app/
├── main.py                # FastAPI application entry point
├── api/
│   ├── __init__.py
│   └── endpoints/         # API route definitions
├── core/
│   ├── __init__.py
│   └── config.py          # Configuration settings
├── db/
│   ├── __init__.py
│   ├── database.py        # PostgreSQL database connection
│   └── elasticsearch.py   # Elasticsearch connection
└── services/
    ├── __init__.py
    ├── es_sync.py         # Elasticsearch synchronization
    └── search.py          # Search service
```

## License

[Your license here]