### Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Node.js 16+ (for frontend development)
  
### Set up a virtual environment and install dependencies
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

### Set up environment variables:
DATABASE_URL=postgresql://username:password@localhost:5432/ecfr
OPENAI_API_KEY=your_openai_api_key  # Required for AI features


### Database Setup
Create a new PostgreSQL database:
sql
CREATE DATABASE ecfr;
Run the database migrations or import initial data using the scripts in the ingestion-script directory.

## Running the API
bash
Start the FastAPI server
./start_api.sh
or
uvicorn agency_api:app --reload --host 0.0.0.0 --port 8000
The API will be available at http://localhost:8000 with interactive documentation at http://localhost:8000/docs.

## Running the Web Interface
bash
cd UI
python -m http.server 3000
Then open http://localhost:3000 in your browser.

## API Endpoints

GET /agencies - List all agencies with optional filtering
GET /word-counts/{title} - Get word counts for a specific CFR title
POST /ai/agency-summary - Generate AI-powered agency summary
