# Notion MusicBrainz Sync

A Python script that synchronizes music data from the MusicBrainz API to your Notion databases. Perfect for maintaining an up-to-date music collection with rich metadata for Artists, Albums, Songs, and Labels.

## ✨ Features

- **Four Database Support**: Syncs Artists, Albums, Songs, and Labels separately or all together
- **Automatic Data Sync**: Keeps your Notion databases updated with latest MusicBrainz information
- **Rich Metadata**: Includes genres, tags, ratings, release dates, labels, and more
- **Cover Art**: Automatically fetches album artwork from Cover Art Archive
- **Smart Matching**: Searches MusicBrainz by name and selects best matches
- **Efficient Updates**: Only updates changed data to minimize API calls
- **Rate Limiting**: Respects MusicBrainz's 1 request/second limit
- **Last Page Mode**: Sync only the most recently edited page (perfect for iOS shortcuts)

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- MusicBrainz user agent (app name and contact email)
- Notion integration token ([Create one here](https://www.notion.so/my-integrations))
- Four Notion databases (Artists, Albums, Songs, Labels)

### Installation

1. **Clone or navigate to the project**
   ```bash
   cd "Notion Music"
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp env.example .env
   # Edit .env with your API keys and database IDs
   ```

4. **Configure your Notion databases**
   - Run `python find_property_ids.py` to get your property IDs
   - Update `property_config.py` with your property IDs

5. **Run the sync**
   ```bash
   python notion_musicbrainz_sync.py --database all
   ```

## 📋 Setup Guide

### 1. Environment Variables

Create a `.env` file with the following variables:

```env
# Notion API Configuration
NOTION_TOKEN=secret_...
NOTION_ARTISTS_DATABASE_ID=...
NOTION_ALBUMS_DATABASE_ID=...
NOTION_SONGS_DATABASE_ID=...
NOTION_LABELS_DATABASE_ID=...

# MusicBrainz API Configuration
# Required: Set a proper user agent (app name and contact email)
MUSICBRAINZ_USER_AGENT=NotionMusicSync/1.0 (your-email@example.com)

# Optional: Logging level (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL=INFO
```

### 2. MusicBrainz API Setup

MusicBrainz is free and open-source. You just need to set a proper user agent:

1. Format: `AppName/Version (contact@example.com)`
2. Include your contact email so MusicBrainz can reach you if needed
3. No API key required!

### 3. Notion Database Setup

You need four databases in Notion. Here are the recommended properties:

#### Artists Database

| Property Name | Type | Description |
|---------------|------|-------------|
| Name | Title | Artist name |
| MusicBrainz ID | Rich Text | MusicBrainz UUID |
| Sort Name | Rich Text | Sortable name |
| Type | Select | Person, Group, Orchestra, etc. |
| Country | Select | Country code |
| Begin Date | Date | When artist started |
| End Date | Date | When artist ended (if applicable) |
| Disambiguation | Rich Text | Clarification text |
| Description | Rich Text | Biography |
| Tags | Multi-select | Genre tags |
| Rating | Number | Community rating |
| MusicBrainz URL | URL | Link to MusicBrainz page |
| Last Updated | Date | Sync timestamp |
| Albums | Relation | → Albums database |
| Songs | Relation | → Songs database |

#### Albums Database

| Property Name | Type | Description |
|---------------|------|-------------|
| Title | Title | Album/release title |
| MusicBrainz ID | Rich Text | MusicBrainz UUID |
| Artist | Relation | → Artists database |
| Release Date | Date | Release date |
| Country | Select | Country of release |
| Label | Multi-select | Record labels |
| Status | Select | Official, Promotion, Bootleg, etc. |
| Packaging | Select | None, Jewel Case, etc. |
| Barcode | Rich Text | Barcode/UPC |
| Format | Multi-select | CD, Digital, Vinyl, etc. |
| Track Count | Number | Number of tracks |
| Description | Rich Text | Album description |
| Tags | Multi-select | Genre tags |
| Rating | Number | Community rating |
| Cover Image | URL | Album artwork |
| MusicBrainz URL | URL | Link to MusicBrainz page |
| Last Updated | Date | Sync timestamp |
| Songs | Relation | → Songs database |

#### Songs Database

| Property Name | Type | Description |
|---------------|------|-------------|
| Title | Title | Song/recording title |
| MusicBrainz ID | Rich Text | MusicBrainz UUID |
| Artist | Relation | → Artists database |
| Album | Relation | → Albums database |
| Track Number | Number | Position on album |
| Length | Number | Duration in seconds |
| ISRC | Rich Text | International Standard Recording Code |
| Disambiguation | Rich Text | Clarification text |
| Description | Rich Text | Song description |
| Tags | Multi-select | Genre tags |
| Rating | Number | Community rating |
| MusicBrainz URL | URL | Link to MusicBrainz page |
| Last Updated | Date | Sync timestamp |

#### Labels Database

| Property Name | Type | Description |
|---------------|------|-------------|
| Name | Title | Label name |
| MusicBrainz ID | Rich Text | MusicBrainz UUID |
| Type | Select | Label, Distributor, etc. |
| Country | Select | Country code |
| Begin Date | Date | When label started |
| End Date | Date | When label ended (if applicable) |
| Disambiguation | Rich Text | Clarification text |
| Description | Rich Text | Label description |
| Tags | Multi-select | Genre tags |
| Rating | Number | Community rating |
| MusicBrainz URL | URL | Link to MusicBrainz page |
| Last Updated | Date | Sync timestamp |
| Albums | Relation | → Albums database |

### 4. Property Configuration

The repository includes a `property_config.py` file with example property IDs. You need to update these with your own database's property IDs.

**Step 1: Get your property IDs**
```bash
python find_property_ids.py
```

**Step 2: Update the configuration**
Edit `property_config.py` and replace the `None` values with your actual property IDs from the script output.

**Note:** Property IDs are not sensitive data - they're just identifiers for your database fields. You can safely commit your `property_config.py` file to your repository.

#### Field Behavior Configuration

The script supports four different behaviors for how multi-select fields are handled during sync:

```python
FIELD_BEHAVIOR = {
    # Fields that merge MusicBrainz data with existing data
    'artists_tags_property_id': 'merge',
    'albums_tags_property_id': 'merge',
    'albums_label_property_id': 'merge',
    'albums_format_property_id': 'merge',
    'songs_tags_property_id': 'merge',
    'labels_tags_property_id': 'merge',
}
```

**Behavior Options:**
- **`'default'`**: Always overwrite with MusicBrainz data (even if empty)
- **`'merge'`**: Merge MusicBrainz data with existing data (remove duplicates)
- **`'preserve'`**: Only update if MusicBrainz has data (preserve existing if empty)
- **`'skip'`**: Never update this field

## 📊 Usage Examples

### Sync All Databases
```bash
python notion_musicbrainz_sync.py --database all
```

### Sync Specific Database
```bash
# Sync only artists
python notion_musicbrainz_sync.py --database artists

# Sync only albums
python notion_musicbrainz_sync.py --database albums

# Sync only songs
python notion_musicbrainz_sync.py --database songs

# Sync only labels
python notion_musicbrainz_sync.py --database labels
```

### Last Page Mode (for iOS Shortcuts)
```bash
# Sync only the most recently edited page
python notion_musicbrainz_sync.py --last-page --database artists
```

### Force Update All Pages
```bash
# Process all pages including already synced content
python notion_musicbrainz_sync.py --force-all
```

## 🔧 Troubleshooting

### Common Issues

1. **401 Unauthorized**: Check your `NOTION_TOKEN` in `.env`
2. **Property not found**: Run `find_property_ids.py` to get correct IDs
3. **Rate limiting**: The script includes built-in rate limiting (1 req/sec)
4. **Missing data**: Some artists/albums/songs may not have complete MusicBrainz data
5. **Cover art not found**: Not all releases have cover art in Cover Art Archive

### Debug Mode

Enable debug logging by setting in your `.env` file:
```env
LOG_LEVEL=DEBUG
```

## 📈 Performance

- **Rate Limited**: 1 request per second (MusicBrainz requirement)
- **Caching**: Comprehensive caching reduces redundant API calls
- **Efficient**: Only updates changed data
- **Error Handling**: Robust error recovery with retry logic
- **Single-threaded**: Due to rate limiting, processing is sequential

## 🎵 MusicBrainz API

This project uses the [MusicBrainz API](https://musicbrainz.org/doc/MusicBrainz_API), which provides:

- **Free and open-source**: No API key required
- **Comprehensive metadata**: Artists, releases, recordings, and more
- **Community-maintained**: Constantly updated by music enthusiasts
- **Cover Art Archive**: Integration with cover art database

**Rate Limits:**
- 1 request per second (can be increased with proper user agent)
- No daily limit

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

- [MusicBrainz](https://musicbrainz.org/) for the comprehensive music database
- [Cover Art Archive](https://coverartarchive.org/) for album artwork
- [Notion](https://www.notion.so/) for the flexible database platform
- The Python community for excellent libraries

## 📞 Support

If you encounter issues:

1. Check the troubleshooting section
2. Review the configuration guide
3. Check the logs for error details
4. Open an issue on GitHub

---

**Happy listening! 🎵**

