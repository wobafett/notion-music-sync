# Notion Music Project Plan

## Overview
A Python project that synchronizes music data from MusicBrainz API to three Notion databases: Artists, Albums, and Songs. Following the same pattern as Notion Movies, Notion Games, and Notion Books projects.

## API Choice: MusicBrainz

**Why MusicBrainz?**
- ✅ Free and open-source
- ✅ Comprehensive metadata (artists, releases, recordings)
- ✅ Well-documented API
- ✅ Community-maintained database
- ✅ No API key required (just user agent)
- ✅ Supports relationships between entities

**Rate Limits:**
- 1 request per second (can be increased with proper user agent)
- No daily limit

**Alternative APIs Considered:**
- **Spotify API**: Requires OAuth, rate limits, commercial use restrictions
- **Last.fm API**: Good for scrobbling data, less comprehensive metadata
- **Discogs API**: Good for vinyl/collectors, less comprehensive for digital music

## Database Structure

### 1. Artists Database
**Core Fields:**
- `Name` (Title) - Artist name
- `MusicBrainz ID` (Number) - MBID for artist
- `Sort Name` (Rich Text) - Sortable name
- `Type` (Select) - Person, Group, Orchestra, Choir, Character, Other
- `Country` (Select) - Country of origin
- `Begin Date` (Date) - When artist started
- `End Date` (Date) - When artist ended (if applicable)
- `Disambiguation` (Rich Text) - Clarification text
- `Description` (Rich Text) - Biography/description
- `Tags` (Multi-select) - Genre tags
- `Rating` (Number) - Community rating
- `Last Updated` (Date) - Sync timestamp

**Relations:**
- `Albums` (Relation) → Albums database
- `Songs` (Relation) → Songs database

### 2. Albums Database
**Core Fields:**
- `Title` (Title) - Album/release title
- `MusicBrainz ID` (Number) - MBID for release
- `Artist` (Relation) → Artists database
- `Release Date` (Date) - Release date
- `Country` (Select) - Country of release
- `Label` (Multi-select) - Record labels
- `Status` (Select) - Official, Promotion, Bootleg, Pseudo-Release
- `Packaging` (Select) - None, Jewel Case, etc.
- `Barcode` (Rich Text) - Barcode/UPC
- `Format` (Multi-select) - CD, Digital, Vinyl, etc.
- `Track Count` (Number) - Number of tracks
- `Description` (Rich Text) - Album description
- `Tags` (Multi-select) - Genre tags
- `Rating` (Number) - Community rating
- `Cover Image` (URL) - Album artwork
- `MusicBrainz URL` (URL) - Link to MusicBrainz page
- `Last Updated` (Date) - Sync timestamp

**Relations:**
- `Artist` (Relation) → Artists database
- `Songs` (Relation) → Songs database

### 3. Songs Database
**Core Fields:**
- `Title` (Title) - Song/recording title
- `MusicBrainz ID` (Number) - MBID for recording
- `Artist` (Relation) → Artists database
- `Album` (Relation) → Albums database
- `Track Number` (Number) - Position on album
- `Length` (Number) - Duration in seconds
- `ISRC` (Rich Text) - International Standard Recording Code
- `Disambiguation` (Rich Text) - Clarification text
- `Description` (Rich Text) - Song description
- `Tags` (Multi-select) - Genre tags
- `Rating` (Number) - Community rating
- `MusicBrainz URL` (URL) - Link to MusicBrainz page
- `Last Updated` (Date) - Sync timestamp

**Relations:**
- `Artist` (Relation) → Artists database
- `Album` (Relation) → Albums database

## Project Structure

```
Notion Music/
├── notion_musicbrainz_sync.py    # Main sync script
├── property_config.py             # Property ID mappings
├── find_property_ids.py           # Helper to find property IDs
├── requirements.txt                # Python dependencies
├── env.example                     # Environment variable template
├── README.md                       # Project documentation
└── PLAN.md                        # This file
```

## Implementation Plan

### Phase 1: Core Infrastructure
1. **MusicBrainzAPI Class**
   - Search artists by name
   - Search releases (albums) by title/artist
   - Search recordings (songs) by title/artist
   - Get detailed artist information
   - Get detailed release information
   - Get detailed recording information
   - Rate limiting (1 req/sec)
   - Caching for frequently accessed data

2. **NotionAPI Class** (reusable from other projects)
   - Database operations
   - Page creation/updates
   - Relation management

3. **Property Configuration**
   - Separate configs for each database
   - Or unified config with database-specific sections

### Phase 2: Sync Logic
1. **Artist Sync**
   - Search by name
   - Match existing artists by MBID
   - Update artist properties
   - Handle artist types and aliases

2. **Album Sync**
   - Search by title + artist
   - Link to artist via relation
   - Update album properties
   - Handle multiple releases of same album

3. **Song Sync**
   - Search by title + artist
   - Link to artist and album via relations
   - Update song properties
   - Handle track numbers

### Phase 3: Advanced Features
1. **Relation Management**
   - Auto-link albums to artists
   - Auto-link songs to albums and artists
   - Handle orphaned records

2. **Smart Matching**
   - Fuzzy matching for artist/album names
   - Handle variations (feat., vs., etc.)
   - Disambiguation logic

3. **Batch Processing**
   - Parallel processing for multiple items
   - Efficient API usage
   - Progress tracking

### Phase 4: Optimization
1. **Caching**
   - Cache API responses
   - Cache property mappings
   - Reduce redundant API calls

2. **Change Detection**
   - Only update changed fields
   - Skip unchanged records
   - Track last update time

3. **Error Handling**
   - Retry logic for failed requests
   - Graceful degradation
   - Comprehensive logging

## Sync Workflow

### Single Database Sync
```python
# Sync artists
python notion_musicbrainz_sync.py --database artists

# Sync albums
python notion_musicbrainz_sync.py --database albums

# Sync songs
python notion_musicbrainz_sync.py --database songs
```

### Full Sync
```python
# Sync all databases
python notion_musicbrainz_sync.py --all
```

### Last Page Sync (for iOS shortcuts)
```python
# Sync only the most recently edited page
python notion_musicbrainz_sync.py --last-page --database artists
```

## Environment Variables

```env
# Notion API Configuration
NOTION_TOKEN=secret_...
NOTION_ARTISTS_DATABASE_ID=...
NOTION_ALBUMS_DATABASE_ID=...
NOTION_SONGS_DATABASE_ID=...

# MusicBrainz API Configuration
MUSICBRAINZ_USER_AGENT=YourAppName/1.0 (contact@example.com)

# Optional: Logging level
LOG_LEVEL=INFO
```

## MusicBrainz API Endpoints

### Artists
- `GET /ws/2/artist/?query=...` - Search artists
- `GET /ws/2/artist/{mbid}?inc=...` - Get artist details

### Releases (Albums)
- `GET /ws/2/release/?query=...` - Search releases
- `GET /ws/2/release/{mbid}?inc=...` - Get release details

### Recordings (Songs)
- `GET /ws/2/recording/?query=...` - Search recordings
- `GET /ws/2/recording/{mbid}?inc=...` - Get recording details

## Field Behavior Configuration

Similar to other projects, support different behaviors:
- `default` - Always overwrite with MusicBrainz data
- `merge` - Merge with existing data (for tags, etc.)
- `preserve` - Only update if MusicBrainz has data
- `skip` - Never update this field

## Challenges & Solutions

### Challenge 1: Multiple Artists per Song
**Solution:** Use multi-relation or primary artist + featured artists

### Challenge 2: Album Variations
**Solution:** Use disambiguation field and match by artist + title + year

### Challenge 3: Missing Metadata
**Solution:** Graceful handling, preserve existing data when API lacks info

### Challenge 4: Rate Limiting
**Solution:** Implement 1-second delay, use caching, batch requests when possible

### Challenge 5: Relation Management
**Solution:** Update relations after all entities are synced, handle orphaned records

## Next Steps

1. ✅ Create project plan (this document)
2. ⏳ Set up project structure
3. ⏳ Implement MusicBrainzAPI class
4. ⏳ Implement NotionAPI wrapper
5. ⏳ Create property config system
6. ⏳ Implement artist sync
7. ⏳ Implement album sync
8. ⏳ Implement song sync
9. ⏳ Add relation management
10. ⏳ Add error handling and logging
11. ⏳ Create documentation
12. ⏳ Test with real data

## Questions to Consider

1. **Single script vs. separate scripts?**
   - Recommendation: Single script with `--database` flag for flexibility

2. **How to handle compilation albums?**
   - Multiple artists per album - use multi-relation or separate handling

3. **Cover art source?**
   - MusicBrainz has cover art via Cover Art Archive
   - Alternative: Last.fm or Spotify for better quality

4. **How to handle live albums vs. studio albums?**
   - Use tags or separate field for album type

5. **Should we support multiple releases of same album?**
   - Yes, use disambiguation and release date to differentiate

