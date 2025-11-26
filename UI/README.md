# eCFR Agencies Directory UI

A professional React-based web application for browsing and searching U.S. government agencies from the Electronic Code of Federal Regulations (eCFR).

## Features

- **Professional Design**: Clean, modern interface built with Tailwind CSS
- **Agency Search**: Search agencies by slug or name with real-time filtering
- **Detailed Views**: Expandable agency cards showing CFR references and metadata
- **Responsive Layout**: Works seamlessly on desktop, tablet, and mobile devices
- **Loading States**: Smooth loading indicators and error handling
- **No Build Required**: Uses CDN libraries - just open and run!

## Quick Start

1. Make sure your FastAPI server is running on `http://0.0.0.0:8000`
2. Navigate to the UI directory: `cd UI`
3. Start a local server: `python -m http.server 3000` or `npm start`
4. Open your browser to: `http://localhost:3000`

## Alternative: Direct File Access

You can also open `index.html` directly in your browser, but you may need to configure CORS settings on your API server.

## Technology Stack

- **React 18**: Modern React with hooks
- **Tailwind CSS**: Utility-first CSS framework
- **Axios**: HTTP client for API requests
- **Babel Standalone**: In-browser JSX transformation

## API Integration

The UI connects to the FastAPI backend at `http://0.0.0.0:8000/agencies` with optional query parameters:
- `?slug=<agency-slug>` - Filter by agency slug
- `?name=<agency-name>` - Filter by agency name (case-insensitive)
- `?slug=<slug>&name=<name>` - Combined filtering

## Components

### App Component
Main application component with state management for agencies, loading, and search.

### SearchBar Component
Search interface with slug and name inputs, plus search and clear buttons.

### AgencyCard Component
Display individual agency information with expandable CFR references.

### LoadingSpinner & ErrorMessage
Reusable components for loading and error states.

## Styling

- Professional gradient badges for CFR references
- Smooth hover animations and transitions
- Clean card-based layout
- Responsive grid system
- Modern color scheme with proper contrast

## Development

This project uses CDN-based libraries to eliminate build complexity. All React components are defined within the main HTML file using Babel's in-browser transformation.

To modify:
1. Edit `index.html` directly
2. Changes are reflected immediately on browser refresh
3. No compilation or build steps required