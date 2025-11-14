#!/usr/bin/env python3
"""
Notion MusicBrainz Sync Script
Synchronizes music information from MusicBrainz API to Notion database pages.
Supports three databases: Artists, Albums, and Songs.
"""

import os
import sys
import logging
import time
import argparse
import re
from typing import Dict, List, Optional, Union
from datetime import datetime
import requests
from notion_client import Client
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# Configure logging
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('notion_musicbrainz_sync.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Try to import custom property configuration
try:
    from property_config import (
        # Artists
        ARTISTS_TITLE_PROPERTY_ID, ARTISTS_MUSICBRAINZ_ID_PROPERTY_ID,
        ARTISTS_SORT_NAME_PROPERTY_ID, ARTISTS_TYPE_PROPERTY_ID,
        ARTISTS_GENDER_PROPERTY_ID, ARTISTS_AREA_PROPERTY_ID, ARTISTS_BORN_IN_PROPERTY_ID,
        ARTISTS_IG_LINK_PROPERTY_ID, ARTISTS_WEBSITE_LINK_PROPERTY_ID,
        ARTISTS_YOUTUBE_LINK_PROPERTY_ID, ARTISTS_BANDCAMP_LINK_PROPERTY_ID,
        ARTISTS_STREAMING_LINK_PROPERTY_ID,
        ARTISTS_COUNTRY_PROPERTY_ID, ARTISTS_BEGIN_DATE_PROPERTY_ID,
        ARTISTS_END_DATE_PROPERTY_ID, ARTISTS_DISAMBIGUATION_PROPERTY_ID,
        ARTISTS_DESCRIPTION_PROPERTY_ID, ARTISTS_GENRES_PROPERTY_ID, ARTISTS_TAGS_PROPERTY_ID,
        ARTISTS_RATING_PROPERTY_ID, ARTISTS_LAST_UPDATED_PROPERTY_ID,
        ARTISTS_MUSICBRAINZ_URL_PROPERTY_ID, ARTISTS_ALBUMS_PROPERTY_ID,
        ARTISTS_SONGS_PROPERTY_ID,
        # Albums
        ALBUMS_TITLE_PROPERTY_ID, ALBUMS_MUSICBRAINZ_ID_PROPERTY_ID,
        ALBUMS_ARTIST_PROPERTY_ID, ALBUMS_RELEASE_DATE_PROPERTY_ID,
        ALBUMS_COUNTRY_PROPERTY_ID, ALBUMS_LABEL_PROPERTY_ID,
        ALBUMS_TYPE_PROPERTY_ID, ALBUMS_LISTEN_PROPERTY_ID,
        ALBUMS_STATUS_PROPERTY_ID, ALBUMS_PACKAGING_PROPERTY_ID,
        ALBUMS_BARCODE_PROPERTY_ID, ALBUMS_FORMAT_PROPERTY_ID,
        ALBUMS_TRACK_COUNT_PROPERTY_ID, ALBUMS_DESCRIPTION_PROPERTY_ID,
        ALBUMS_GENRES_PROPERTY_ID, ALBUMS_TAGS_PROPERTY_ID, ALBUMS_RATING_PROPERTY_ID,
        ALBUMS_COVER_IMAGE_PROPERTY_ID, ALBUMS_MUSICBRAINZ_URL_PROPERTY_ID,
        ALBUMS_LAST_UPDATED_PROPERTY_ID, ALBUMS_SONGS_PROPERTY_ID,
        # Songs
        SONGS_TITLE_PROPERTY_ID, SONGS_MUSICBRAINZ_ID_PROPERTY_ID,
        SONGS_ARTIST_PROPERTY_ID, SONGS_ALBUM_PROPERTY_ID,
        SONGS_TRACK_NUMBER_PROPERTY_ID, SONGS_LENGTH_PROPERTY_ID,
        SONGS_ISRC_PROPERTY_ID, SONGS_DISAMBIGUATION_PROPERTY_ID,
        SONGS_DESCRIPTION_PROPERTY_ID, SONGS_GENRES_PROPERTY_ID,
        SONGS_TAGS_PROPERTY_ID, SONGS_LISTEN_PROPERTY_ID,
        SONGS_RATING_PROPERTY_ID, SONGS_MUSICBRAINZ_URL_PROPERTY_ID,
        SONGS_LAST_UPDATED_PROPERTY_ID,
        # Labels
        LABELS_TITLE_PROPERTY_ID, LABELS_MUSICBRAINZ_ID_PROPERTY_ID,
        LABELS_TYPE_PROPERTY_ID, LABELS_COUNTRY_PROPERTY_ID,
        LABELS_BEGIN_DATE_PROPERTY_ID, LABELS_END_DATE_PROPERTY_ID,
        LABELS_DISAMBIGUATION_PROPERTY_ID, LABELS_DESCRIPTION_PROPERTY_ID,
        LABELS_GENRES_PROPERTY_ID, LABELS_TAGS_PROPERTY_ID, LABELS_RATING_PROPERTY_ID,
        LABELS_MUSICBRAINZ_URL_PROPERTY_ID, LABELS_OFFICIAL_WEBSITE_PROPERTY_ID,
        LABELS_IG_PROPERTY_ID, LABELS_BANDCAMP_PROPERTY_ID, LABELS_FOUNDED_PROPERTY_ID,
        LABELS_LAST_UPDATED_PROPERTY_ID, LABELS_ALBUMS_PROPERTY_ID, LABELS_AREA_PROPERTY_ID
    )
except ImportError:
    logger.error("property_config.py not found. Please create this file with your property IDs.")
    logger.error("Copy property_config.example.py to property_config.py and update with your property IDs.")
    sys.exit(1)


class MusicBrainzAPI:
    """MusicBrainz API client for fetching music data."""
    
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self.base_url = "https://musicbrainz.org/ws/2"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'application/json'
        })
        
        # Rate limiting - MusicBrainz allows 1 request per second
        self.request_delay = 1.0
        self.last_request_time = 0
        
        # Caching to reduce API calls
        self._cache = {
            'artists': {},
            'releases': {},
            'recordings': {},
            'labels': {},
            'cover_art': {}
        }
    
    def _rate_limit(self):
        """Apply rate limiting between requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.request_delay:
            sleep_time = self.request_delay - time_since_last_request
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _make_api_request(self, url: str, params: Dict = None, max_retries: int = 3) -> requests.Response:
        """Make an API request with rate limiting and retry logic."""
        if params is None:
            params = {}
        
        for attempt in range(max_retries + 1):
            try:
                # Apply rate limiting before each request
                self._rate_limit()
                
                # Make the request
                response = self.session.get(url, params=params)
                
                # Check for rate limiting (429)
                if response.status_code == 429:
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) + 1  # Exponential backoff
                        logger.warning(f"Rate limited (429). Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Rate limit exceeded after {max_retries} retries")
                        response.raise_for_status()
                
                # Check for other HTTP errors
                if response.status_code >= 400:
                    if attempt < max_retries:
                        wait_time = 1 + (attempt * 0.5)
                        logger.warning(f"HTTP {response.status_code} error. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        response.raise_for_status()
                
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    wait_time = 1 + (attempt * 0.5)
                    logger.warning(f"Request failed: {e}. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Request failed after {max_retries} retries: {e}")
                    raise
        
        raise Exception("Max retries exceeded")
    
    def search_artists(self, name: str, limit: int = 5) -> List[Dict]:
        """Search for artists by name."""
        try:
            url = f"{self.base_url}/artist"
            params = {
                'query': name,
                'limit': limit,
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('artists', [])
            
        except Exception as e:
            logger.error(f"Error searching for artist '{name}': {e}")
            return []
    
    def get_artist(self, mbid: str) -> Optional[Dict]:
        """Get detailed artist information by MBID."""
        try:
            # Check cache first
            if mbid in self._cache['artists']:
                logger.debug(f"Using cached artist data for MBID {mbid}")
                return self._cache['artists'][mbid]
            
            url = f"{self.base_url}/artist/{mbid}"
            params = {
                'inc': 'aliases+tags+ratings+release-groups+genres+url-rels+area-rels',
                'fmt': 'json'
            }
            # Note: 'genres' in inc will include genres on both artist and release-groups
            
            response = self._make_api_request(url, params)
            artist = response.json()
            
            # Cache the result
            self._cache['artists'][mbid] = artist
            return artist
            
        except Exception as e:
            logger.error(f"Error getting artist {mbid}: {e}")
            return None
    
    def search_releases(self, title: str, artist: str = None, limit: int = 5) -> List[Dict]:
        """Search for releases (albums) by title and optionally artist."""
        try:
            url = f"{self.base_url}/release"
            
            # Build query
            query_parts = [f'release:"{title}"']
            if artist:
                query_parts.append(f'artist:"{artist}"')
            
            params = {
                'query': ' AND '.join(query_parts),
                'limit': limit,
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('releases', [])
            
        except Exception as e:
            logger.error(f"Error searching for release '{title}': {e}")
            return []
    
    def search_releases_by_recording(self, recording_id: str, limit: int = 50) -> List[Dict]:
        """Search for releases that contain a specific recording."""
        try:
            url = f"{self.base_url}/release"
            
            # Search for releases containing this recording
            params = {
                'query': f'reid:{recording_id}',
                'limit': limit,
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('releases', [])
            
        except Exception as e:
            logger.debug(f"Error searching for releases by recording {recording_id}: {e}")
            return []
    
    def get_release(self, mbid: str) -> Optional[Dict]:
        """Get detailed release information by MBID."""
        try:
            # Check cache first
            if mbid in self._cache['releases']:
                logger.debug(f"Using cached release data for MBID {mbid}")
                return self._cache['releases'][mbid]
            
            url = f"{self.base_url}/release/{mbid}"
            params = {
                'inc': 'artists+labels+recordings+release-groups+tags+ratings+genres+url-rels',
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            release = response.json()
            
            # Cache the result
            self._cache['releases'][mbid] = release
            return release
            
        except Exception as e:
            logger.error(f"Error getting release {mbid}: {e}")
            return None
    
    def search_recordings(self, title: str, artist: str = None, album: str = None, limit: int = 5) -> List[Dict]:
        """Search for recordings (songs) by title and optionally artist and album."""
        try:
            url = f"{self.base_url}/recording"
            
            # Build query
            query_parts = [f'recording:"{title}"']
            if artist:
                query_parts.append(f'artist:"{artist}"')
            if album:
                query_parts.append(f'release:"{album}"')
            
            params = {
                'query': ' AND '.join(query_parts),
                'limit': limit,
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('recordings', [])
            
        except Exception as e:
            logger.error(f"Error searching for recording '{title}': {e}")
            return []
    
    def get_recording(self, mbid: str) -> Optional[Dict]:
        """Get detailed recording information by MBID."""
        try:
            # Check cache first
            if mbid in self._cache['recordings']:
                logger.debug(f"Using cached recording data for MBID {mbid}")
                return self._cache['recordings'][mbid]
            
            url = f"{self.base_url}/recording/{mbid}"
            params = {
                'inc': 'artists+releases+release-groups+tags+ratings+isrcs+url-rels+genres',
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            recording = response.json()
            
            # Cache the result
            self._cache['recordings'][mbid] = recording
            return recording
            
        except Exception as e:
            logger.error(f"Error getting recording {mbid}: {e}")
            return None
    
    def get_cover_art_url(self, release_mbid: str) -> Optional[str]:
        """Get cover art URL from Cover Art Archive."""
        try:
            # Check cache first
            if release_mbid in self._cache['cover_art']:
                logger.debug(f"Using cached cover art URL for release {release_mbid}")
                return self._cache['cover_art'][release_mbid]
            
            # Cover Art Archive API
            url = f"https://coverartarchive.org/release/{release_mbid}"
            
            response = self._make_api_request(url)
            data = response.json()
            
            # Get front cover image
            images = data.get('images', [])
            front_cover = next((img for img in images if img.get('front', False)), None)
            
            if front_cover:
                cover_url = front_cover.get('image')
                # Cache the result
                self._cache['cover_art'][release_mbid] = cover_url
                return cover_url
            
            return None
            
        except Exception as e:
            logger.debug(f"No cover art found for release {release_mbid}: {e}")
            return None
    
    def _get_spotify_access_token(self) -> Optional[str]:
        """Get Spotify access token using client credentials flow."""
        try:
            client_id = os.getenv('SPOTIFY_CLIENT_ID')
            client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
            
            if not client_id or not client_secret:
                logger.debug("SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET not set")
                return None
            
            # Spotify token endpoint
            url = "https://accounts.spotify.com/api/token"
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            data = {
                'grant_type': 'client_credentials'
            }
            
            response = requests.post(
                url, 
                headers=headers, 
                data=data,
                auth=(client_id, client_secret),
                timeout=10
            )
            
            if response.status_code == 200:
                token_data = response.json()
                return token_data.get('access_token')
            else:
                logger.debug(f"Spotify token request failed with status {response.status_code}")
                return None
                
        except Exception as e:
            logger.debug(f"Error getting Spotify access token: {e}")
            return None
    
    def _get_spotify_album_url(self, album_title: str, artist_name: str = None) -> Optional[str]:
        """Get Spotify album URL by searching Spotify API."""
        try:
            # Get access token
            access_token = self._get_spotify_access_token()
            if not access_token:
                return None
            
            # Rate limit: Spotify allows many requests, but we'll be conservative
            time.sleep(0.1)  # 100ms delay
            
            # Search for album
            url = "https://api.spotify.com/v1/search"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            # Build query: album title and optionally artist name
            if artist_name:
                query = f'album:"{album_title}" artist:"{artist_name}"'
            else:
                query = f'album:"{album_title}"'
            
            params = {
                'q': query,
                'type': 'album',
                'limit': 1
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if (data.get('albums') and 
                    data['albums'].get('items') and 
                    len(data['albums']['items']) > 0):
                    album = data['albums']['items'][0]
                    
                    # Get the Spotify external URL
                    if album.get('external_urls') and album['external_urls'].get('spotify'):
                        return album['external_urls']['spotify']
                    else:
                        logger.debug(f"Spotify album {album_title} has no external URL")
                else:
                    logger.debug(f"Spotify search returned no results for album {album_title}")
            elif response.status_code == 401:
                logger.debug("Spotify access token expired or invalid")
            else:
                logger.debug(f"Spotify API returned status {response.status_code} for album {album_title}")
            
            return None
            
        except Exception as e:
            logger.debug(f"Error fetching Spotify album URL: {e}")
            return None
    
    def _get_spotify_album_image(self, album_title: str, artist_name: str = None) -> Optional[str]:
        """Get album cover image URL from Spotify API."""
        try:
            # Get access token
            access_token = self._get_spotify_access_token()
            if not access_token:
                return None
            
            # Rate limit: Spotify allows many requests, but we'll be conservative
            time.sleep(0.1)  # 100ms delay
            
            # Search for album
            url = "https://api.spotify.com/v1/search"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            # Build query: album title and optionally artist name
            if artist_name:
                query = f'album:"{album_title}" artist:"{artist_name}"'
            else:
                query = f'album:"{album_title}"'
            
            params = {
                'q': query,
                'type': 'album',
                'limit': 1
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if (data.get('albums') and 
                    data['albums'].get('items') and 
                    len(data['albums']['items']) > 0):
                    album = data['albums']['items'][0]
                    
                    # Spotify returns images in an array, sorted by size (largest first)
                    if album.get('images') and len(album['images']) > 0:
                        # Get the first (largest) image
                        image = album['images'][0]
                        if image.get('url'):
                            return image['url']
                    else:
                        logger.debug(f"Spotify album {album_title} has no images")
                else:
                    logger.debug(f"Spotify search returned no results for album {album_title}")
            elif response.status_code == 401:
                logger.debug("Spotify access token expired or invalid")
            else:
                logger.debug(f"Spotify API returned status {response.status_code} for album {album_title}")
            
            return None
            
        except Exception as e:
            logger.debug(f"Error fetching Spotify album image: {e}")
            return None
    
    def _get_spotify_track_url(self, track_title: str, artist_name: str = None) -> Optional[str]:
        """Get Spotify track URL by searching Spotify API."""
        try:
            # Get access token
            access_token = self._get_spotify_access_token()
            if not access_token:
                return None
            
            # Rate limit: Spotify allows many requests, but we'll be conservative
            time.sleep(0.1)  # 100ms delay
            
            # Search for track
            url = "https://api.spotify.com/v1/search"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            # Build query: track title and optionally artist name
            if artist_name:
                query = f'track:"{track_title}" artist:"{artist_name}"'
            else:
                query = f'track:"{track_title}"'
            
            params = {
                'q': query,
                'type': 'track',
                'limit': 1
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if (data.get('tracks') and 
                    data['tracks'].get('items') and 
                    len(data['tracks']['items']) > 0):
                    track = data['tracks']['items'][0]
                    
                    # Get the Spotify external URL
                    if track.get('external_urls') and track['external_urls'].get('spotify'):
                        return track['external_urls']['spotify']
                    else:
                        logger.debug(f"Spotify track {track_title} has no external URL")
                else:
                    logger.debug(f"Spotify search returned no results for track {track_title}")
            elif response.status_code == 401:
                logger.debug("Spotify access token expired or invalid")
            else:
                logger.debug(f"Spotify API returned status {response.status_code} for track {track_title}")
            
            return None
            
        except Exception as e:
            logger.debug(f"Error fetching Spotify track URL: {e}")
            return None
    
    def _get_spotify_artist_image(self, artist_name: str, artist_mbid: str = None) -> Optional[str]:
        """Get artist image URL from Spotify API."""
        try:
            # Get access token
            access_token = self._get_spotify_access_token()
            if not access_token:
                return None
            
            # Rate limit: Spotify allows many requests, but we'll be conservative
            time.sleep(0.1)  # 100ms delay
            
            # Search for artist
            url = "https://api.spotify.com/v1/search"
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            # Use MBID if available for more accurate results, otherwise use artist name
            if artist_mbid:
                # Spotify doesn't directly support MBID, so we'll search by name
                # but we could potentially use external_ids in the future
                query = f'artist:"{artist_name}"'
            else:
                query = f'artist:"{artist_name}"'
            
            params = {
                'q': query,
                'type': 'artist',
                'limit': 1
            }
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if (data.get('artists') and 
                    data['artists'].get('items') and 
                    len(data['artists']['items']) > 0):
                    artist = data['artists']['items'][0]
                    
                    # Spotify returns images in an array, sorted by size (largest first)
                    if artist.get('images') and len(artist['images']) > 0:
                        # Get the first (largest) image
                        image = artist['images'][0]
                        if image.get('url'):
                            return image['url']
                    else:
                        logger.debug(f"Spotify artist {artist_name} has no images")
                else:
                    logger.debug(f"Spotify search returned no results for {artist_name}")
            elif response.status_code == 401:
                logger.debug("Spotify access token expired or invalid")
            else:
                logger.debug(f"Spotify API returned status {response.status_code} for artist {artist_name}")
            
            return None
            
        except Exception as e:
            logger.debug(f"Error fetching Spotify image: {e}")
            return None
    
    def get_artist_image_url(self, artist_mbid: str, artist_name: str = None, artist_data: Dict = None) -> Optional[str]:
        """Get artist image URL from Spotify API."""
        try:
            if not artist_name:
                return None
            
            # Get image from Spotify
            spotify_image = self._get_spotify_artist_image(artist_name, artist_mbid)
            if spotify_image:
                logger.debug(f"Found Spotify image for {artist_name}")
                return spotify_image
            
            return None
            
        except Exception as e:
            logger.debug(f"No artist image found for {artist_mbid}: {e}")
            return None
    
    def search_labels(self, name: str, limit: int = 5) -> List[Dict]:
        """Search for labels by name."""
        try:
            url = f"{self.base_url}/label"
            params = {
                'query': name,
                'limit': limit,
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            data = response.json()
            
            return data.get('labels', [])
            
        except Exception as e:
            logger.error(f"Error searching for label '{name}': {e}")
            return []
    
    def get_label(self, mbid: str) -> Optional[Dict]:
        """Get detailed label information by MBID."""
        try:
            # Check cache first
            if mbid in self._cache['labels']:
                logger.debug(f"Using cached label data for MBID {mbid}")
                return self._cache['labels'][mbid]
            
            url = f"{self.base_url}/label/{mbid}"
            params = {
                'inc': 'aliases+tags+ratings+url-rels+area-rels+genres',
                'fmt': 'json'
            }
            
            response = self._make_api_request(url, params)
            label = response.json()
            
            # Cache the result
            self._cache['labels'][mbid] = label
            return label
            
        except Exception as e:
            logger.error(f"Error getting label {mbid}: {e}")
            return None


class NotionAPI:
    """Notion API client for database operations."""
    
    def __init__(self, token: str):
        self.client = Client(auth=token)
    
    def get_database(self, database_id: str) -> Optional[Dict]:
        """Get database information."""
        try:
            return self.client.databases.retrieve(database_id)
        except Exception as e:
            logger.error(f"Error retrieving database {database_id}: {e}")
            return None
    
    def query_database(self, database_id: str, filter_params: Optional[Dict] = None) -> List[Dict]:
        """Query database for pages."""
        try:
            pages = []
            has_more = True
            start_cursor = None
            
            while has_more:
                params = {}
                if start_cursor:
                    params['start_cursor'] = start_cursor
                if filter_params:
                    params['filter'] = filter_params
                
                response = self.client.databases.query(database_id, **params)
                pages.extend(response['results'])
                has_more = response['has_more']
                start_cursor = response.get('next_cursor')
            
            return pages
        except Exception as e:
            logger.error(f"Error querying database {database_id}: {e}")
            return []
    
    def get_page(self, page_id: str) -> Optional[Dict]:
        """Get a single page by ID."""
        try:
            return self.client.pages.retrieve(page_id)
        except Exception as e:
            logger.error(f"Error retrieving page {page_id}: {e}")
            return None
    
    def create_page(self, database_id: str, properties: Dict, cover_url: Optional[str] = None, icon: Optional[Union[str, Dict]] = None) -> Optional[str]:
        """Create a new page in a database."""
        try:
            page_data = {
                'parent': {'database_id': database_id},
                'properties': properties
            }
            
            if cover_url:
                page_data['cover'] = {
                    'type': 'external',
                    'external': {'url': cover_url}
                }
            
            if icon:
                if isinstance(icon, str):
                    page_data['icon'] = {'type': 'emoji', 'emoji': icon}
                elif isinstance(icon, dict):
                    page_data['icon'] = icon
            
            page = self.client.pages.create(**page_data)
            return page['id']
        except Exception as e:
            logger.error(f"Error creating page: {e}")
            return None
    
    def update_page(self, page_id: str, properties: Dict, cover_url: Optional[str] = None, icon: Optional[Union[str, Dict]] = None) -> bool:
        """Update a page with new properties and optionally set the cover image and icon."""
        try:
            update_data = {'properties': properties}
            
            if cover_url:
                update_data['cover'] = {
                    'type': 'external',
                    'external': {'url': cover_url}
                }
            
            if icon:
                if isinstance(icon, str):
                    update_data['icon'] = {'type': 'emoji', 'emoji': icon}
                elif isinstance(icon, dict):
                    update_data['icon'] = icon
            
            self.client.pages.update(page_id, **update_data)
            return True
        except Exception as e:
            logger.error(f"Error updating page {page_id}: {e}")
            return False


class NotionMusicBrainzSync:
    """Main class for synchronizing Notion databases with MusicBrainz data."""
    
    def __init__(self, notion_token: str, musicbrainz_user_agent: str,
                 artists_db_id: Optional[str] = None,
                 albums_db_id: Optional[str] = None,
                 songs_db_id: Optional[str] = None,
                 labels_db_id: Optional[str] = None):
        self.notion = NotionAPI(notion_token)
        self.mb = MusicBrainzAPI(musicbrainz_user_agent)
        
        self.artists_db_id = artists_db_id
        self.albums_db_id = albums_db_id
        self.songs_db_id = songs_db_id
        self.labels_db_id = labels_db_id
        self.locations_db_id = os.getenv('NOTION_LOCATIONS_DATABASE_ID')
        
        # Property mappings for each database
        self.artists_properties = {}
        self.albums_properties = {}
        self.songs_properties = {}
        self.labels_properties = {}
        
        # Property ID to key mappings
        self.artists_property_id_to_key = {}
        self.albums_property_id_to_key = {}
        self.songs_property_id_to_key = {}
        self.labels_property_id_to_key = {}
        
        # Caches for performance optimization
        self._location_cache = None  # Cache location name -> page_id (None = not loaded, {} = loaded empty)
        self._locations_title_key = None  # Cache title property key for locations
        self._database_pages_cache = {}  # Cache full database queries
        
        # Load database schemas
        if self.artists_db_id:
            self._load_artists_schema()
        if self.albums_db_id:
            self._load_albums_schema()
        if self.songs_db_id:
            self._load_songs_schema()
        if self.labels_db_id:
            self._load_labels_schema()
    
    def _load_artists_schema(self):
        """Load and analyze the Artists database schema."""
        try:
            database = self.notion.get_database(self.artists_db_id)
            if not database:
                logger.error("Could not retrieve Artists database schema")
                return
            
            properties = database.get('properties', {})
            
            # Create property ID to key mapping
            for prop_key, prop_data in properties.items():
                prop_id = prop_data.get('id')
                if prop_id:
                    self.artists_property_id_to_key[prop_id] = prop_key
            
            # Map property IDs
            self.artists_properties = {
                'title': ARTISTS_TITLE_PROPERTY_ID,
                'musicbrainz_id': ARTISTS_MUSICBRAINZ_ID_PROPERTY_ID,
                'sort_name': ARTISTS_SORT_NAME_PROPERTY_ID,
                'type': ARTISTS_TYPE_PROPERTY_ID,
                'gender': ARTISTS_GENDER_PROPERTY_ID,
                'area': ARTISTS_AREA_PROPERTY_ID,
                'born_in': ARTISTS_BORN_IN_PROPERTY_ID,
                'ig_link': ARTISTS_IG_LINK_PROPERTY_ID,
                'website_link': ARTISTS_WEBSITE_LINK_PROPERTY_ID,
                'youtube_link': ARTISTS_YOUTUBE_LINK_PROPERTY_ID,
                'bandcamp_link': ARTISTS_BANDCAMP_LINK_PROPERTY_ID,
                'streaming_link': ARTISTS_STREAMING_LINK_PROPERTY_ID,
                'country': ARTISTS_COUNTRY_PROPERTY_ID,
                'begin_date': ARTISTS_BEGIN_DATE_PROPERTY_ID,
                'end_date': ARTISTS_END_DATE_PROPERTY_ID,
                'disambiguation': ARTISTS_DISAMBIGUATION_PROPERTY_ID,
                'description': ARTISTS_DESCRIPTION_PROPERTY_ID,
                'genres': ARTISTS_GENRES_PROPERTY_ID,
                'tags': ARTISTS_TAGS_PROPERTY_ID,
                'rating': ARTISTS_RATING_PROPERTY_ID,
                'last_updated': ARTISTS_LAST_UPDATED_PROPERTY_ID,
                'musicbrainz_url': ARTISTS_MUSICBRAINZ_URL_PROPERTY_ID,
            }
            
            logger.info("✓ Artists database schema loaded")
            
        except Exception as e:
            logger.error(f"Error loading Artists database schema: {e}")
    
    def _load_albums_schema(self):
        """Load and analyze the Albums database schema."""
        try:
            database = self.notion.get_database(self.albums_db_id)
            if not database:
                logger.error("Could not retrieve Albums database schema")
                return
            
            properties = database.get('properties', {})
            
            # Create property ID to key mapping
            for prop_key, prop_data in properties.items():
                prop_id = prop_data.get('id')
                if prop_id:
                    self.albums_property_id_to_key[prop_id] = prop_key
            
            # Map property IDs
            self.albums_properties = {
                'title': ALBUMS_TITLE_PROPERTY_ID,
                'musicbrainz_id': ALBUMS_MUSICBRAINZ_ID_PROPERTY_ID,
                'artist': ALBUMS_ARTIST_PROPERTY_ID,
                'release_date': ALBUMS_RELEASE_DATE_PROPERTY_ID,
                'country': ALBUMS_COUNTRY_PROPERTY_ID,
                'label': ALBUMS_LABEL_PROPERTY_ID,
                'type': ALBUMS_TYPE_PROPERTY_ID,
                'listen': ALBUMS_LISTEN_PROPERTY_ID,
                'status': ALBUMS_STATUS_PROPERTY_ID,
                'packaging': ALBUMS_PACKAGING_PROPERTY_ID,
                'barcode': ALBUMS_BARCODE_PROPERTY_ID,
                'format': ALBUMS_FORMAT_PROPERTY_ID,
                'track_count': ALBUMS_TRACK_COUNT_PROPERTY_ID,
                'description': ALBUMS_DESCRIPTION_PROPERTY_ID,
                'genres': ALBUMS_GENRES_PROPERTY_ID,
                'tags': ALBUMS_TAGS_PROPERTY_ID,
                'rating': ALBUMS_RATING_PROPERTY_ID,
                'cover_image': ALBUMS_COVER_IMAGE_PROPERTY_ID,
                'musicbrainz_url': ALBUMS_MUSICBRAINZ_URL_PROPERTY_ID,
                'last_updated': ALBUMS_LAST_UPDATED_PROPERTY_ID,
                'songs': ALBUMS_SONGS_PROPERTY_ID,
            }
            
            logger.info("✓ Albums database schema loaded")
            
        except Exception as e:
            logger.error(f"Error loading Albums database schema: {e}")
    
    def _load_songs_schema(self):
        """Load and analyze the Songs database schema."""
        try:
            database = self.notion.get_database(self.songs_db_id)
            if not database:
                logger.error("Could not retrieve Songs database schema")
                return
            
            properties = database.get('properties', {})
            
            # Create property ID to key mapping
            for prop_key, prop_data in properties.items():
                prop_id = prop_data.get('id')
                if prop_id:
                    self.songs_property_id_to_key[prop_id] = prop_key
            
            # Map property IDs
            self.songs_properties = {
                'title': SONGS_TITLE_PROPERTY_ID,
                'musicbrainz_id': SONGS_MUSICBRAINZ_ID_PROPERTY_ID,
                'artist': SONGS_ARTIST_PROPERTY_ID,
                'album': SONGS_ALBUM_PROPERTY_ID,
                'track_number': SONGS_TRACK_NUMBER_PROPERTY_ID,
                'length': SONGS_LENGTH_PROPERTY_ID,
                'isrc': SONGS_ISRC_PROPERTY_ID,
                'disambiguation': SONGS_DISAMBIGUATION_PROPERTY_ID,
                'description': SONGS_DESCRIPTION_PROPERTY_ID,
                'genres': SONGS_GENRES_PROPERTY_ID,
                'tags': SONGS_TAGS_PROPERTY_ID,
                'listen': SONGS_LISTEN_PROPERTY_ID,
                'rating': SONGS_RATING_PROPERTY_ID,
                'musicbrainz_url': SONGS_MUSICBRAINZ_URL_PROPERTY_ID,
                'last_updated': SONGS_LAST_UPDATED_PROPERTY_ID,
            }
            
            logger.info("✓ Songs database schema loaded")
            
        except Exception as e:
            logger.error(f"Error loading Songs database schema: {e}")
    
    def _load_labels_schema(self):
        """Load and analyze the Labels database schema."""
        try:
            database = self.notion.get_database(self.labels_db_id)
            if not database:
                logger.error("Could not retrieve Labels database schema")
                return
            
            properties = database.get('properties', {})
            
            # Create property ID to key mapping
            for prop_key, prop_data in properties.items():
                prop_id = prop_data.get('id')
                if prop_id:
                    self.labels_property_id_to_key[prop_id] = prop_key
            
            # Map property IDs
            self.labels_properties = {
                'title': LABELS_TITLE_PROPERTY_ID,
                'musicbrainz_id': LABELS_MUSICBRAINZ_ID_PROPERTY_ID,
                'type': LABELS_TYPE_PROPERTY_ID,
                'country': LABELS_COUNTRY_PROPERTY_ID,
                'begin_date': LABELS_BEGIN_DATE_PROPERTY_ID,
                'end_date': LABELS_END_DATE_PROPERTY_ID,
                'disambiguation': LABELS_DISAMBIGUATION_PROPERTY_ID,
                'description': LABELS_DESCRIPTION_PROPERTY_ID,
                'genres': LABELS_GENRES_PROPERTY_ID,
                'tags': LABELS_TAGS_PROPERTY_ID,
                'rating': LABELS_RATING_PROPERTY_ID,
                'last_updated': LABELS_LAST_UPDATED_PROPERTY_ID,
                'musicbrainz_url': LABELS_MUSICBRAINZ_URL_PROPERTY_ID,
                'official_website': LABELS_OFFICIAL_WEBSITE_PROPERTY_ID,
                'ig': LABELS_IG_PROPERTY_ID,
                'bandcamp': LABELS_BANDCAMP_PROPERTY_ID,
                'founded': LABELS_FOUNDED_PROPERTY_ID,
                'albums': LABELS_ALBUMS_PROPERTY_ID,
                'area': LABELS_AREA_PROPERTY_ID,
            }
            
            logger.info("✓ Labels database schema loaded")
            
        except Exception as e:
            logger.error(f"Error loading Labels database schema: {e}")
    
    def _get_property_key(self, property_id: Optional[str], database: str) -> Optional[str]:
        """Get the property key for a given property ID in a specific database."""
        if not property_id:
            return None
        
        if database == 'artists':
            return self.artists_property_id_to_key.get(property_id)
        elif database == 'albums':
            return self.albums_property_id_to_key.get(property_id)
        elif database == 'songs':
            return self.songs_property_id_to_key.get(property_id)
        elif database == 'labels':
            return self.labels_property_id_to_key.get(property_id)
        return None
    
    def sync_artist_page(self, page: Dict, force_all: bool = False) -> Optional[bool]:
        """Sync a single artist page with MusicBrainz data."""
        try:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # Extract title
            title_prop_id = self.artists_properties.get('title')
            if not title_prop_id:
                logger.warning(f"Missing title property for Artists database")
                return None
            
            title_key = self._get_property_key(title_prop_id, 'artists')
            if not title_key:
                logger.warning(f"Could not find title property key")
                return None
            
            title_prop = properties.get(title_key, {})
            if title_prop.get('type') != 'title' or not title_prop.get('title'):
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            title = title_prop['title'][0]['plain_text']
            logger.info(f"Processing artist: {title}")
            
            # Check for existing MBID
            mb_id_prop_id = self.artists_properties.get('musicbrainz_id')
            existing_mbid = None
            if mb_id_prop_id:
                mb_id_key = self._get_property_key(mb_id_prop_id, 'artists')
                if mb_id_key:
                    mb_id_prop = properties.get(mb_id_key, {})
                    # MBID is stored as rich_text (UUID string)
                    if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                        existing_mbid = mb_id_prop['rich_text'][0]['plain_text']
            
            # Search or get artist data
            artist_data = None
            if existing_mbid:
                artist_data = self.mb.get_artist(existing_mbid)
                if not artist_data:
                    logger.warning(f"Could not find artist with MBID {existing_mbid}, searching by name")
                    existing_mbid = None
                elif not force_all:
                    # Skip pages with existing MBIDs unless force_all is True
                    logger.info(f"Skipping artist '{title}' - already has MBID {existing_mbid} (use --force-all to update)")
                    return None
            
            if not artist_data:
                search_results = self.mb.search_artists(title, limit=5)
                if not search_results:
                    logger.warning(f"Could not find artist: {title}")
                    return False
                
                # Select best match (first result for now)
                best_match = search_results[0]
                artist_data = self.mb.get_artist(best_match['id'])
            
            if not artist_data:
                logger.warning(f"Could not get artist data for: {title}")
                return False
            
            # Format properties
            notion_props = self._format_artist_properties(artist_data)
            
            # Preserve existing relations (merge instead of replace)
            notion_props = self._merge_relations(page, notion_props, 'artists')
            
            # Get artist image from Spotify
            artist_image_url = None
            if artist_data.get('id'):
                artist_name = artist_data.get('name')
                artist_image_url = self.mb.get_artist_image_url(artist_data['id'], artist_name, artist_data)
                if artist_image_url:
                    logger.info(f"Found artist image for {title} from Spotify: {artist_image_url[:50]}...")
                else:
                    logger.debug(f"No artist image found for {title}")
            
            # Set icon (use emoji if no image, otherwise image will be cover)
            icon = '🎤'  # Microphone emoji for artists
            
            # Update the page (use artist image as cover if available)
            if self.notion.update_page(page_id, notion_props, artist_image_url, icon):
                logger.info(f"Successfully updated artist: {title}")
                return True
            else:
                logger.error(f"Failed to update artist: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing artist page {page.get('id')}: {e}")
            return False
    
    def _format_artist_properties(self, artist_data: Dict) -> Dict:
        """Format MusicBrainz artist data for Notion properties."""
        properties = {}
        
        try:
            # Title (name)
            if artist_data.get('name') and self.artists_properties.get('title'):
                prop_key = self._get_property_key(self.artists_properties['title'], 'artists')
                if prop_key:
                    properties[prop_key] = {
                        'title': [{'text': {'content': artist_data['name']}}]
                    }
            
            # MusicBrainz ID (store as string in rich_text since MBIDs are UUIDs)
            if artist_data.get('id') and self.artists_properties.get('musicbrainz_id'):
                prop_key = self._get_property_key(self.artists_properties['musicbrainz_id'], 'artists')
                if prop_key:
                    # Store MBID as string - it's a UUID, not a number
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': artist_data['id']}}]
                    }
            
            # Sort name
            if artist_data.get('sort-name') and self.artists_properties.get('sort_name'):
                prop_key = self._get_property_key(self.artists_properties['sort_name'], 'artists')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': artist_data['sort-name']}}]
                    }
            
            # Type
            if artist_data.get('type') and self.artists_properties.get('type'):
                prop_key = self._get_property_key(self.artists_properties['type'], 'artists')
                if prop_key:
                    properties[prop_key] = {'select': {'name': artist_data['type']}}
            
            # Gender
            if artist_data.get('gender') and self.artists_properties.get('gender'):
                prop_key = self._get_property_key(self.artists_properties['gender'], 'artists')
                if prop_key:
                    properties[prop_key] = {'select': {'name': artist_data['gender']}}
            
            # Area (relation to Locations database)
            if artist_data.get('area') and artist_data['area'].get('name') and self.artists_properties.get('area') and self.locations_db_id:
                area_name = artist_data['area']['name']
                location_page_id = self._find_or_create_location_page(area_name)
                if location_page_id:
                    prop_key = self._get_property_key(self.artists_properties['area'], 'artists')
                    if prop_key:
                        properties[prop_key] = {
                            'relation': [{'id': location_page_id}]
                        }
            
            # Born In (relation to Locations database)
            if self.artists_properties.get('born_in') and self.locations_db_id:
                born_in_location = None
                # Try to get from begin-area
                if artist_data.get('begin-area') and artist_data['begin-area'].get('name'):
                    born_in_location = artist_data['begin-area']['name']
                
                prop_key = self._get_property_key(self.artists_properties['born_in'], 'artists')
                if prop_key:
                    if born_in_location:
                        # Only set relation if we have data from MusicBrainz
                        location_page_id = self._find_or_create_location_page(born_in_location)
                        if location_page_id:
                            properties[prop_key] = {
                                'relation': [{'id': location_page_id}]
                            }
                    # If no data from MusicBrainz, explicitly clear the relation
                    else:
                        properties[prop_key] = {
                            'relation': []
                        }
            
            # Extract URLs from relationships
            ig_url = None
            website_url = None
            youtube_url = None
            bandcamp_url = None
            spotify_url = None
            
            if artist_data.get('relations'):
                for relation in artist_data['relations']:
                    relation_type = relation.get('type', '').lower()
                    url_resource = relation.get('url', {}).get('resource', '').lower()
                    
                    # Instagram
                    if relation_type == 'instagram' or (relation_type == 'social network' and 'instagram' in url_resource):
                        ig_url = relation.get('url', {}).get('resource')
                    # Official homepage/website
                    elif relation_type == 'official homepage' or relation_type == 'official website':
                        website_url = relation.get('url', {}).get('resource')
                    # YouTube (exclude YouTube Music)
                    elif ('youtube' in url_resource or 'youtu.be' in url_resource) and 'music.youtube.com' not in url_resource:
                        youtube_url = relation.get('url', {}).get('resource')
                    # Bandcamp
                    elif 'bandcamp' in url_resource:
                        bandcamp_url = relation.get('url', {}).get('resource')
                    # Spotify
                    elif 'spotify' in url_resource:
                        spotify_url = relation.get('url', {}).get('resource')
            
            # IG Link
            if ig_url and self.artists_properties.get('ig_link'):
                prop_key = self._get_property_key(self.artists_properties['ig_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': ig_url}
            
            # Official Website Link
            if website_url and self.artists_properties.get('website_link'):
                prop_key = self._get_property_key(self.artists_properties['website_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': website_url}
            
            # YouTube Link
            if youtube_url and self.artists_properties.get('youtube_link'):
                prop_key = self._get_property_key(self.artists_properties['youtube_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': youtube_url}
            
            # Bandcamp Link
            if bandcamp_url and self.artists_properties.get('bandcamp_link'):
                prop_key = self._get_property_key(self.artists_properties['bandcamp_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': bandcamp_url}
            
            # Streaming Link (Spotify)
            if spotify_url and self.artists_properties.get('streaming_link'):
                prop_key = self._get_property_key(self.artists_properties['streaming_link'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': spotify_url}
            
            # Country
            if artist_data.get('area') and artist_data['area'].get('iso-3166-1-code-list'):
                country_code = artist_data['area']['iso-3166-1-code-list'][0]
                if self.artists_properties.get('country'):
                    prop_key = self._get_property_key(self.artists_properties['country'], 'artists')
                    if prop_key:
                        properties[prop_key] = {'select': {'name': country_code}}
            
            # Begin date and End date - based on first and latest release dates
            # Using a single date property with start (first release) and end (latest release)
            if artist_data.get('id'):
                # Fetch releases for this artist to get release dates
                release_dates = self._get_artist_release_dates(artist_data['id'])
                
                if release_dates:
                    # Begin date = earliest release date (start of range)
                    earliest_date = min(release_dates)
                    # End date = latest release date (end of range)
                    latest_date = max(release_dates)
                    
                    if self.artists_properties.get('begin_date'):
                        prop_key = self._get_property_key(self.artists_properties['begin_date'], 'artists')
                        if prop_key:
                            # Set both start and end dates in the same date property
                            properties[prop_key] = {
                                'date': {
                                    'start': earliest_date[:10],  # First release date
                                    'end': latest_date[:10]       # Latest release date
                                }
                            }
            
            # Disambiguation
            if artist_data.get('disambiguation') and self.artists_properties.get('disambiguation'):
                prop_key = self._get_property_key(self.artists_properties['disambiguation'], 'artists')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': artist_data['disambiguation']}}]
                    }
            
            # Genres - use only genres directly from the artist (not from release-groups)
            # This matches what MusicBrainz shows on the artist page
            if artist_data.get('genres') and self.artists_properties.get('genres'):
                genres = [genre['name'] for genre in artist_data['genres'] if genre.get('name')]
                if genres:
                    prop_key = self._get_property_key(self.artists_properties['genres'], 'artists')
                    if prop_key:
                        properties[prop_key] = {
                            'multi_select': [{'name': genre} for genre in genres[:10]]  # Limit to 10 genres
                        }
            
            # Tags - these are separate from genres and come directly from the artist
            # Only include tags that are different from genres (genres have priority)
            if artist_data.get('tags') and self.artists_properties.get('tags'):
                # Get genre names for comparison
                genre_names = set()
                if artist_data.get('genres'):
                    genre_names = {genre['name'] for genre in artist_data['genres'] if genre.get('name')}
                
                # Filter tags: exclude tags that match genres (genres have priority)
                tags = []
                for tag in artist_data['tags']:
                    tag_name = tag.get('name')
                    # Only include tags that:
                    # 1. Have a name
                    # 2. Are different from genres (genres take priority)
                    if tag_name and tag_name not in genre_names:
                        tags.append(tag_name)
                
                if tags:
                    prop_key = self._get_property_key(self.artists_properties['tags'], 'artists')
                    if prop_key:
                        properties[prop_key] = {
                            'multi_select': [{'name': tag} for tag in tags[:10]]  # Limit to 10 tags
                        }
            
            # MusicBrainz URL
            if artist_data.get('id') and self.artists_properties.get('musicbrainz_url'):
                mb_url = f"https://musicbrainz.org/artist/{artist_data['id']}"
                prop_key = self._get_property_key(self.artists_properties['musicbrainz_url'], 'artists')
                if prop_key:
                    properties[prop_key] = {'url': mb_url}
            
            # Last updated
            if self.artists_properties.get('last_updated'):
                prop_key = self._get_property_key(self.artists_properties['last_updated'], 'artists')
                if prop_key:
                    properties[prop_key] = {'date': {'start': datetime.now().isoformat()}}
            
        except Exception as e:
            logger.error(f"Error formatting artist properties: {e}")
        
        return properties
    
    def _get_artist_release_dates(self, artist_mbid: str) -> List[str]:
        """Get all release dates for an artist from MusicBrainz."""
        release_dates = []
        
        try:
            # Search for releases by this artist
            url = f"{self.mb.base_url}/release"
            params = {
                'query': f'arid:{artist_mbid}',
                'limit': 100,  # Get up to 100 releases
                'fmt': 'json'
            }
            
            response = self.mb._make_api_request(url, params)
            data = response.json()
            
            releases = data.get('releases', [])
            
            for release in releases:
                if release.get('date'):
                    release_date = release['date']
                    # Only add valid dates (YYYY-MM-DD format or partial)
                    # Normalize partial dates: YYYY -> YYYY-01-01, YYYY-MM -> YYYY-MM-01
                    if release_date and len(release_date) >= 4:  # At least YYYY
                        # Normalize to YYYY-MM-DD format for proper comparison
                        normalized_date = self._normalize_date(release_date)
                        if normalized_date:
                            release_dates.append(normalized_date)
            
            logger.debug(f"Found {len(release_dates)} release dates for artist {artist_mbid}")
            
        except Exception as e:
            logger.warning(f"Error fetching release dates for artist {artist_mbid}: {e}")
        
        return release_dates
    
    def _get_mbid_from_related_page(self, page_id: str, database_type: str) -> Optional[str]:
        """Get MusicBrainz ID from a related page.
        
        Args:
            page_id: The Notion page ID
            database_type: 'artists', 'albums', 'songs', or 'labels'
            
        Returns:
            The MusicBrainz ID if found, None otherwise
        """
        try:
            page = self.notion.get_page(page_id)
            if not page:
                return None
            
            properties = page.get('properties', {})
            
            # Get the MBID property ID based on database type
            mb_id_prop_id = None
            if database_type == 'artists':
                mb_id_prop_id = self.artists_properties.get('musicbrainz_id')
            elif database_type == 'albums':
                mb_id_prop_id = self.albums_properties.get('musicbrainz_id')
            elif database_type == 'songs':
                mb_id_prop_id = self.songs_properties.get('musicbrainz_id')
            elif database_type == 'labels':
                mb_id_prop_id = self.labels_properties.get('musicbrainz_id')
            
            if not mb_id_prop_id:
                return None
            
            # Get the property key
            prop_key = self._get_property_key(mb_id_prop_id, database_type)
            if not prop_key:
                return None
            
            # Extract MBID from rich_text
            mb_id_prop = properties.get(prop_key, {})
            if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                return mb_id_prop['rich_text'][0]['plain_text']
            
            return None
        except Exception as e:
            logger.debug(f"Error getting MBID from related page {page_id}: {e}")
            return None
    
    def _recording_appears_on_album(self, recording_id: str, album_mbid: str) -> bool:
        """Check if a recording appears on a specific album.
        
        Args:
            recording_id: The recording MBID
            album_mbid: The album (release) MBID
            
        Returns:
            True if the recording appears on the album, False otherwise
        """
        try:
            # Get the album/release data
            release_data = self.mb.get_release(album_mbid)
            if not release_data:
                return False
            
            # Check if any medium contains this recording
            for medium in release_data.get('media', []):
                for track in medium.get('tracks', []):
                    if track.get('recording') and track['recording'].get('id') == recording_id:
                        return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking if recording {recording_id} appears on album {album_mbid}: {e}")
            return False
    
    def _release_is_by_artist(self, release_data: Dict, artist_mbid: str) -> bool:
        """Check if a release is by a specific artist.
        
        Args:
            release_data: The release data from MusicBrainz
            artist_mbid: The artist MBID
            
        Returns:
            True if the release is by the artist, False otherwise
        """
        try:
            # Check artist-credit
            if release_data.get('artist-credit'):
                for ac in release_data['artist-credit']:
                    if ac.get('artist') and ac['artist'].get('id') == artist_mbid:
                        return True
            
            # Check release-group artist-credit
            if release_data.get('release-group') and release_data['release-group'].get('artist-credit'):
                for ac in release_data['release-group']['artist-credit']:
                    if ac.get('artist') and ac['artist'].get('id') == artist_mbid:
                        return True
            
            return False
        except Exception as e:
            logger.debug(f"Error checking if release is by artist {artist_mbid}: {e}")
            return False
    
    def _release_contains_recordings(self, release_data: Dict, recording_mbids: List[str], recording_titles: List[str] = None) -> bool:
        """Check if a release contains all specified recordings.
        
        Args:
            release_data: The release data from MusicBrainz
            recording_mbids: List of recording MBIDs that must appear on the release
            recording_titles: Optional list of recording titles to check if MBIDs aren't available
            
        Returns:
            True if the release contains all recordings, False otherwise
        """
        if not recording_mbids and not recording_titles:
            return True
        
        try:
            # Collect all recording IDs and titles from the release
            release_recording_ids = set()
            release_recording_titles = set()
            for medium in release_data.get('media', []):
                for track in medium.get('tracks', []):
                    recording = track.get('recording', {})
                    if recording.get('id'):
                        release_recording_ids.add(recording['id'])
                    if recording.get('title'):
                        # Normalize title for comparison
                        normalized_title = ' '.join(self._normalize_title_for_matching(recording['title']))
                        release_recording_titles.add(normalized_title)
            
            # Check by MBID first (most reliable)
            if recording_mbids:
                required_set = set(recording_mbids)
                if not required_set.issubset(release_recording_ids):
                    return False
            
            # Check by title if MBIDs weren't available or as additional verification
            if recording_titles:
                required_titles = {' '.join(self._normalize_title_for_matching(title)) for title in recording_titles}
                if not required_titles.issubset(release_recording_titles):
                    return False
            
            return True
        except Exception as e:
            logger.debug(f"Error checking if release contains recordings: {e}")
            return False
    
    def _merge_relations(self, page: Dict, new_properties: Dict, database_type: str) -> Dict:
        """Merge new relation properties with existing relations to preserve user-added connections.
        
        Args:
            page: The existing Notion page
            new_properties: New properties to be set
            database_type: 'artists', 'albums', or 'songs'
            
        Returns:
            Updated properties dict with merged relations
        """
        try:
            existing_properties = page.get('properties', {})
            merged_properties = new_properties.copy()
            
            # Get relation property IDs for this database type
            relation_property_ids = []
            if database_type == 'artists':
                # Artists don't typically have relations to other artists/albums/songs in our schema
                # But we should preserve any relations they might have
                pass
            elif database_type == 'albums':
                relation_property_ids = [
                    ('artist', self.albums_properties.get('artist')),
                    ('songs', self.albums_properties.get('songs')),
                    ('label', self.albums_properties.get('label')),
                ]
            elif database_type == 'songs':
                relation_property_ids = [
                    ('artist', self.songs_properties.get('artist')),
                    ('album', self.songs_properties.get('album')),
                ]
            
            # Merge each relation property
            for relation_name, relation_prop_id in relation_property_ids:
                if not relation_prop_id:
                    continue
                
                # Get property keys
                new_prop_key = self._get_property_key(relation_prop_id, database_type)
                if not new_prop_key:
                    continue
                
                # Get existing relations
                existing_relation_prop = existing_properties.get(new_prop_key, {})
                existing_relations = existing_relation_prop.get('relation', [])
                existing_relation_ids = {rel.get('id') for rel in existing_relations if rel.get('id')}
                
                # Get new relations (if the property exists in new_properties)
                new_relation_prop = new_properties.get(new_prop_key, {})
                new_relations = new_relation_prop.get('relation', [])
                new_relation_ids = {rel.get('id') for rel in new_relations if rel.get('id')}
                
                # If new_properties has this relation property, merge with existing
                # If it doesn't have it, preserve existing relations by not updating
                if new_prop_key in new_properties:
                    # Merge: combine existing and new, avoiding duplicates
                    merged_relation_ids = existing_relation_ids | new_relation_ids
                    merged_relations = [{'id': rel_id} for rel_id in merged_relation_ids]
                    
                    # Always set the merged relations (even if empty, to preserve existing if new is empty)
                    merged_properties[new_prop_key] = {'relation': merged_relations}
                    logger.debug(f"Merged {relation_name} relations: {len(existing_relations)} existing + {len(new_relations)} new = {len(merged_relations)} total")
                elif existing_relations:
                    # If new_properties doesn't have this relation, preserve existing by not updating
                    # (existing relations will remain unchanged)
                    logger.debug(f"Preserving existing {relation_name} relations: {len(existing_relations)} (not in new properties)")
            
            return merged_properties
            
        except Exception as e:
            logger.warning(f"Error merging relations: {e}")
            # Return new properties if merge fails
            return new_properties
    
    def _normalize_title_for_matching(self, title: str) -> List[str]:
        """Normalize a title for exact word matching.
        
        Removes special characters, converts to lowercase, and splits into words.
        Used to compare titles word-for-word (not fuzzy matching).
        
        Args:
            title: The title to normalize
            
        Returns:
            List of normalized words
        """
        if not title:
            return []
        
        # Remove special characters, keep only alphanumeric and spaces
        normalized = re.sub(r'[^a-zA-Z0-9\s]', ' ', title)
        # Convert to lowercase and split into words
        words = [word for word in normalized.lower().split() if word]
        return words
    
    def _titles_match_exactly(self, title1: str, title2: str) -> bool:
        """Check if two titles match exactly (word-for-word, case-insensitive, ignoring special chars).
        
        Args:
            title1: First title
            title2: Second title
            
        Returns:
            True if titles match word-for-word, False otherwise
        """
        words1 = self._normalize_title_for_matching(title1)
        words2 = self._normalize_title_for_matching(title2)
        return words1 == words2
    
    def _normalize_date(self, date_str: str) -> Optional[str]:
        """Normalize a date string to YYYY-MM-DD format for comparison.
        
        Handles partial dates:
        - YYYY -> YYYY-01-01
        - YYYY-MM -> YYYY-MM-01
        - YYYY-MM-DD -> YYYY-MM-DD (unchanged)
        """
        if not date_str or len(date_str) < 4:
            return None
        
        try:
            parts = date_str.split('-')
            year = parts[0]
            
            if len(parts) == 1:
                # Just YYYY
                return f"{year}-01-01"
            elif len(parts) == 2:
                # YYYY-MM
                month = parts[1]
                return f"{year}-{month}-01"
            else:
                # YYYY-MM-DD (or more)
                return date_str[:10]  # Take first 10 chars (YYYY-MM-DD)
        except Exception:
            return None
    
    def sync_album_page(self, page: Dict, force_all: bool = False) -> Optional[bool]:
        """Sync a single album page with MusicBrainz data."""
        try:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # Extract title
            title_prop_id = self.albums_properties.get('title')
            if not title_prop_id:
                logger.warning(f"Missing title property for Albums database")
                return None
            
            title_key = self._get_property_key(title_prop_id, 'albums')
            if not title_key:
                logger.warning(f"Could not find title property key")
                return None
            
            title_prop = properties.get(title_key, {})
            if title_prop.get('type') != 'title' or not title_prop.get('title'):
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            title = title_prop['title'][0]['plain_text']
            logger.info(f"Processing album: {title}")
            
            # Try to extract artist name and MBID from relation
            artist_name = None
            artist_mbid = None
            artist_prop_id = self.albums_properties.get('artist')
            if artist_prop_id:
                artist_key = self._get_property_key(artist_prop_id, 'albums')
                if artist_key:
                    artist_prop = properties.get(artist_key, {})
                    if artist_prop.get('relation'):
                        # Get first related artist
                        relation = artist_prop['relation']
                        if relation:
                            # Fetch the artist page to get the name and MBID
                            artist_page_id = relation[0]['id']
                            artist_page = self.notion.get_page(artist_page_id)
                            if artist_page:
                                artist_props = artist_page.get('properties', {})
                                artist_title_key = self._get_property_key(self.artists_properties.get('title'), 'artists')
                                if artist_title_key and artist_props.get(artist_title_key):
                                    artist_title_prop = artist_props[artist_title_key]
                                    if artist_title_prop.get('title') and artist_title_prop['title']:
                                        artist_name = artist_title_prop['title'][0]['plain_text']
                                        logger.debug(f"Found artist from relation: {artist_name}")
                                
                                # Get artist MBID for verification
                                artist_mbid = self._get_mbid_from_related_page(artist_page_id, 'artists')
                                if artist_mbid:
                                    logger.debug(f"Found artist MBID from relation: {artist_mbid}")
            
            # Try to extract related song MBIDs and titles from relation
            song_mbids = []
            song_titles = []
            songs_prop_id = self.albums_properties.get('songs')
            if songs_prop_id:
                songs_key = self._get_property_key(songs_prop_id, 'albums')
                if songs_key:
                    songs_prop = properties.get(songs_key, {})
                    if songs_prop.get('relation'):
                        logger.info(f"Found {len(songs_prop['relation'])} related song(s) for album")
                        # Get MBIDs and titles from all related song pages
                        for song_relation in songs_prop['relation']:
                            song_page_id = song_relation.get('id')
                            if song_page_id:
                                song_mbid = self._get_mbid_from_related_page(song_page_id, 'songs')
                                if song_mbid:
                                    song_mbids.append(song_mbid)
                                    logger.debug(f"Found song MBID from relation: {song_mbid}")
                                
                                # Also get song title as fallback
                                song_page = self.notion.get_page(song_page_id)
                                if song_page:
                                    song_props = song_page.get('properties', {})
                                    song_title_key = self._get_property_key(self.songs_properties.get('title'), 'songs')
                                    if song_title_key and song_props.get(song_title_key):
                                        song_title_prop = song_props[song_title_key]
                                        if song_title_prop.get('title') and song_title_prop['title']:
                                            song_title = song_title_prop['title'][0]['plain_text']
                                            song_titles.append(song_title)
                                            logger.info(f"Found song title from relation: {song_title}")
                                else:
                                    logger.warning(f"Could not fetch song page {song_page_id} to get title")
            
            # Check for existing MBID
            mb_id_prop_id = self.albums_properties.get('musicbrainz_id')
            existing_mbid = None
            if mb_id_prop_id:
                mb_id_key = self._get_property_key(mb_id_prop_id, 'albums')
                if mb_id_key:
                    mb_id_prop = properties.get(mb_id_key, {})
                    # MBID is stored as rich_text (UUID string)
                    if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                        existing_mbid = mb_id_prop['rich_text'][0]['plain_text']
            
            # Search or get release data
            release_data = None
            if existing_mbid:
                release_data = self.mb.get_release(existing_mbid)
                if not release_data:
                    logger.warning(f"Could not find release with MBID {existing_mbid}, searching by title")
                    existing_mbid = None
                else:
                    # Verify the existing release contains all related songs
                    if song_mbids or song_titles:
                        logger.info(f"Verifying existing release {existing_mbid} contains {len(song_mbids)} song MBIDs and {len(song_titles)} song titles")
                        if not self._release_contains_recordings(release_data, song_mbids, song_titles):
                            logger.warning(f"Existing release {existing_mbid} does not contain all related songs, searching for a new match")
                            release_data = None
                            existing_mbid = None
                            # Keep artist filter - we'll use it to narrow search and verify songs match
                            # Only clear if we're absolutely sure the artist is wrong (which we can't know yet)
                        else:
                            logger.info(f"Existing release {existing_mbid} contains all related songs, using it")
                            # Skip pages with existing MBIDs unless force_all is True
                            if not force_all:
                                logger.info(f"Skipping album '{title}' - already has MBID {existing_mbid} (use --force-all to update)")
                                return None
                    elif not force_all:
                        # No related songs to verify, skip if force_all is False
                        logger.info(f"Skipping album '{title}' - already has MBID {existing_mbid} (use --force-all to update)")
                        return None
            
            if not release_data:
                # New approach: Use related artist or song MBID to get candidate releases
                # Priority: artist MBID > song MBID > song title
                search_results = []
                
                if artist_mbid:
                    # Get all releases by this artist
                    logger.info(f"Searching for releases by artist MBID: {artist_mbid}")
                    url = f"{self.mb.base_url}/release"
                    params = {
                        'query': f'arid:{artist_mbid}',
                        'limit': 100,  # Get up to 100 releases
                        'fmt': 'json'
                    }
                    response = self.mb._make_api_request(url, params)
                    data = response.json()
                    search_results = data.get('releases', [])
                    logger.info(f"Found {len(search_results)} releases by artist")
                
                elif song_mbids:
                    # Get all releases containing this song
                    logger.info(f"Searching for releases containing song MBID: {song_mbids[0]}")
                    search_results = self.mb.search_releases_by_recording(song_mbids[0], limit=100)
                    logger.info(f"Found {len(search_results)} releases containing song")
                
                elif song_titles:
                    # Find the song MBID first, then get releases
                    logger.info(f"Searching for song by title: {song_titles[0]}")
                    recording_search = self.mb.search_recordings(song_titles[0], limit=5)
                    if recording_search:
                        # Take the first exact match
                        for rec in recording_search:
                            if self._titles_match_exactly(song_titles[0], rec.get('title', '')):
                                recording_id = rec.get('id')
                                logger.info(f"Found song MBID: {recording_id}, searching for releases")
                                search_results = self.mb.search_releases_by_recording(recording_id, limit=100)
                                logger.info(f"Found {len(search_results)} releases containing song")
                                break
                
                # If we still don't have results, fall back to regular search
                if not search_results:
                    logger.info(f"Falling back to regular search for: {title}")
                    search_results = self.mb.search_releases(title, artist_name, limit=50)
                
                if not search_results:
                    logger.warning(f"Could not find album: {title}")
                    return False
                
                # Filter releases by exact title match
                matching_releases = []
                for result in search_results:
                    result_title = result.get('title', '')
                    if self._titles_match_exactly(title, result_title):
                        matching_releases.append(result)
                
                if not matching_releases:
                    logger.warning(f"No releases found with exact title match for '{title}'")
                    return False
                
                logger.info(f"Found {len(matching_releases)} releases with matching title")
                
                # Use existing scoring logic to find the best release
                # Optimization: Score with available data first, then fetch full data for top candidates
                scored_releases = []
                for release in matching_releases:
                    # Use the same scoring logic as songs (US country, album type, earliest date)
                    score, date = self._score_release_for_song(release)
                    
                    # Boost score if release contains all related songs (check with available data first)
                    contains_songs = False
                    if song_mbids or song_titles:
                        contains_songs = self._release_contains_recordings(release, song_mbids, song_titles)
                        if contains_songs:
                            score += 1000  # Large boost for containing required songs
                    
                    scored_releases.append((score, date, release, contains_songs))
                
                # Sort by score (descending), then by date (ascending - earlier is better)
                scored_releases.sort(key=lambda x: (-x[0], x[1]))
                
                # Only fetch full release data for top 10 candidates (or all if < 10)
                top_candidates = min(10, len(scored_releases))
                top_releases = []
                
                for i in range(top_candidates):
                    score, date, release, contains_songs = scored_releases[i]
                    release_mbid = release.get('id')
                    
                    # Fetch full release data for accurate final scoring
                    if release_mbid:
                        full_release = self.mb.get_release(release_mbid)
                        if full_release:
                            # Re-score with full data
                            score, date = self._score_release_for_song(full_release)
                            
                            # Re-check if release contains all related songs with full data
                            if song_mbids or song_titles:
                                contains_songs = self._release_contains_recordings(full_release, song_mbids, song_titles)
                                if contains_songs:
                                    score += 1000
                            
                            release = full_release
                    
                    top_releases.append((score, date, release, contains_songs))
                
                # Re-sort top candidates with full data
                top_releases.sort(key=lambda x: (-x[0], x[1]))
                
                if not top_releases:
                    logger.warning(f"Could not fetch release data for scoring")
                    return False
                
                # If we have required songs, filter to only releases that contain them
                if song_mbids or song_titles:
                    releases_with_songs = [r for r in top_releases if r[3]]  # r[3] is contains_songs
                    if releases_with_songs:
                        # Only consider releases that contain all required songs
                        top_releases = releases_with_songs
                        logger.info(f"Filtered to {len(top_releases)} releases that contain all required songs")
                    else:
                        logger.warning(f"No releases found that contain all required songs, but continuing anyway")
                
                # Get the best match
                best_release = top_releases[0][2]
                contains_songs = top_releases[0][3]
                
                if contains_songs:
                    logger.info(f"Best release contains all related songs")
                elif song_mbids or song_titles:
                    logger.warning(f"Best release does not contain all related songs")
                
                release_data = best_release
                logger.info(f"Selected best release: {best_release.get('title')} (ID: {best_release.get('id')})")
            
            if not release_data:
                logger.warning(f"Could not get album data for: {title}")
                return False
            
            # Format properties
            notion_props = self._format_album_properties(release_data)
            
            # Preserve existing relations (merge instead of replace)
            notion_props = self._merge_relations(page, notion_props, 'albums')
            
            # Get cover art - try Cover Art Archive first, then Spotify as fallback
            cover_url = None
            if release_data.get('id'):
                cover_url = self.mb.get_cover_art_url(release_data['id'])
                if not cover_url:
                    # Fallback to Spotify if Cover Art Archive doesn't have it
                    album_title = release_data.get('title', title)
                    artist_name = None
                    # Get artist name from artist-credit
                    if release_data.get('artist-credit') and release_data['artist-credit']:
                        first_artist = release_data['artist-credit'][0].get('artist', {})
                        artist_name = first_artist.get('name')
                    
                    if album_title:
                        cover_url = self.mb._get_spotify_album_image(album_title, artist_name)
                        if cover_url:
                            logger.info(f"Found album cover image from Spotify for {title}")
            
            # Set icon
            icon = '💿'  # CD emoji for albums
            
            # Update the page
            if self.notion.update_page(page_id, notion_props, cover_url, icon):
                logger.info(f"Successfully updated album: {title}")
                return True
            else:
                logger.error(f"Failed to update album: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing album page {page.get('id')}: {e}")
            return False
    
    def _format_album_properties(self, release_data: Dict) -> Dict:
        """Format MusicBrainz release data for Notion properties."""
        properties = {}
        
        try:
            # Title
            if release_data.get('title') and self.albums_properties.get('title'):
                prop_key = self._get_property_key(self.albums_properties['title'], 'albums')
                if prop_key:
                    properties[prop_key] = {
                        'title': [{'text': {'content': release_data['title']}}]
                    }
            
            # MusicBrainz ID (store as string in rich_text since MBIDs are UUIDs)
            if release_data.get('id') and self.albums_properties.get('musicbrainz_id'):
                prop_key = self._get_property_key(self.albums_properties['musicbrainz_id'], 'albums')
                if prop_key:
                    # Store MBID as string - it's a UUID, not a number
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': release_data['id']}}]
                    }
            
            # Release date
            if release_data.get('date') and self.albums_properties.get('release_date'):
                release_date = release_data['date']
                prop_key = self._get_property_key(self.albums_properties['release_date'], 'albums')
                if prop_key:
                    properties[prop_key] = {'date': {'start': release_date[:10]}}
            
            # Artists (as relations)
            if release_data.get('artist-credit') and self.albums_properties.get('artist') and self.artists_db_id:
                # Extract artist names and MBIDs from artist-credit
                artist_names = []
                artist_mbids = []
                
                for ac in release_data.get('artist-credit', []):
                    if ac.get('artist'):
                        artist = ac['artist']
                        artist_name = artist.get('name')
                        artist_mbid = artist.get('id')
                        if artist_name:
                            artist_names.append(artist_name)
                            if artist_mbid:
                                artist_mbids.append(artist_mbid)
                            else:
                                artist_mbids.append(None)
                
                if artist_names:
                    # Find or create artist pages and get their IDs
                    artist_page_ids = []
                    for i, artist_name in enumerate(artist_names[:5]):  # Limit to 5 artists
                        artist_mbid = artist_mbids[i] if i < len(artist_mbids) else None
                        artist_page_id = self._find_or_create_artist_page(artist_name, artist_mbid)
                        if artist_page_id:
                            artist_page_ids.append(artist_page_id)
                    
                    if artist_page_ids:
                        prop_key = self._get_property_key(self.albums_properties['artist'], 'albums')
                        if prop_key:
                            properties[prop_key] = {
                                'relation': [{'id': page_id} for page_id in artist_page_ids]
                            }
            
            # Country
            if release_data.get('country') and self.albums_properties.get('country'):
                prop_key = self._get_property_key(self.albums_properties['country'], 'albums')
                if prop_key:
                    properties[prop_key] = {'select': {'name': release_data['country']}}
            
            # Labels (as relations)
            if release_data.get('label-info') and self.albums_properties.get('label') and self.labels_db_id:
                label_names = [li['label']['name'] for li in release_data['label-info'] if li.get('label', {}).get('name')]
                label_mbids = [li['label']['id'] for li in release_data['label-info'] if li.get('label', {}).get('id')]
                
                if label_names:
                    # Find or create label pages and get their IDs
                    label_page_ids = []
                    for i, label_name in enumerate(label_names[:5]):  # Limit to 5 labels
                        label_mbid = label_mbids[i] if i < len(label_mbids) else None
                        label_page_id = self._find_or_create_label_page(label_name, label_mbid)
                        if label_page_id:
                            label_page_ids.append(label_page_id)
                    
                    if label_page_ids:
                        prop_key = self._get_property_key(self.albums_properties['label'], 'albums')
                        if prop_key:
                            properties[prop_key] = {
                                'relation': [{'id': page_id} for page_id in label_page_ids]
                            }
            
            # Status
            if release_data.get('status') and self.albums_properties.get('status'):
                prop_key = self._get_property_key(self.albums_properties['status'], 'albums')
                if prop_key:
                    properties[prop_key] = {'select': {'name': release_data['status']}}
            
            # Packaging
            if release_data.get('packaging') and self.albums_properties.get('packaging'):
                prop_key = self._get_property_key(self.albums_properties['packaging'], 'albums')
                if prop_key:
                    properties[prop_key] = {'select': {'name': release_data['packaging']}}
            
            # Barcode
            if release_data.get('barcode') and self.albums_properties.get('barcode'):
                prop_key = self._get_property_key(self.albums_properties['barcode'], 'albums')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': release_data['barcode']}}]
                    }
            
            # Format
            if release_data.get('media') and self.albums_properties.get('format'):
                formats = []
                for medium in release_data['media']:
                    if medium.get('format'):
                        formats.append(medium['format'])
                if formats:
                    prop_key = self._get_property_key(self.albums_properties['format'], 'albums')
                    if prop_key:
                        properties[prop_key] = {
                            'multi_select': [{'name': fmt} for fmt in set(formats)]
                        }
            
            # Track count
            if release_data.get('media') and self.albums_properties.get('track_count'):
                total_tracks = sum(medium.get('track-count', 0) for medium in release_data['media'])
                if total_tracks > 0:
                    prop_key = self._get_property_key(self.albums_properties['track_count'], 'albums')
                    if prop_key:
                        properties[prop_key] = {'number': total_tracks}
            
            # Genres - use only genres directly from the release-group (not aggregated)
            # This matches what MusicBrainz shows on the release page
            if release_data.get('release-group') and release_data['release-group'].get('genres'):
                genres = [genre['name'] for genre in release_data['release-group']['genres'] if genre.get('name')]
                if genres and self.albums_properties.get('genres'):
                    prop_key = self._get_property_key(self.albums_properties['genres'], 'albums')
                    if prop_key:
                        properties[prop_key] = {
                            'multi_select': [{'name': genre} for genre in genres[:10]]  # Limit to 10 genres
                        }
            
            # Tags - these are separate from genres
            # Only include tags that are different from genres (genres have priority)
            if release_data.get('tags') and self.albums_properties.get('tags'):
                # Get genre names for comparison
                genre_names = set()
                if release_data.get('release-group') and release_data['release-group'].get('genres'):
                    genre_names = {genre['name'] for genre in release_data['release-group']['genres'] if genre.get('name')}
                
                # Filter tags: exclude tags that match genres (genres have priority)
                tags = []
                for tag in release_data['tags']:
                    tag_name = tag.get('name')
                    # Only include tags that:
                    # 1. Have a name
                    # 2. Are different from genres (genres take priority)
                    if tag_name and tag_name not in genre_names:
                        tags.append(tag_name)
                
                if tags:
                    prop_key = self._get_property_key(self.albums_properties['tags'], 'albums')
                    if prop_key:
                        properties[prop_key] = {
                            'multi_select': [{'name': tag} for tag in tags[:10]]  # Limit to 10 tags
                        }
            
            # Album Type (from release-group primary-type)
            if release_data.get('release-group') and release_data['release-group'].get('primary-type') and self.albums_properties.get('type'):
                album_type = release_data['release-group']['primary-type']
                prop_key = self._get_property_key(self.albums_properties['type'], 'albums')
                if prop_key:
                    properties[prop_key] = {'select': {'name': album_type}}
            
            # Spotify link (from url-rels) - check for both "streaming" and "free streaming"
            spotify_url = None
            if release_data.get('relations') and self.albums_properties.get('listen'):
                for relation in release_data.get('relations', []):
                    relation_type = relation.get('type', '').lower()
                    # Check for both "streaming" and "free streaming" relation types
                    if relation_type in ['streaming', 'free streaming']:
                        url_resource = relation.get('url', {})
                        if isinstance(url_resource, dict):
                            url_str = url_resource.get('resource', '')
                        else:
                            url_str = str(url_resource)
                        
                        # Check if it's a Spotify URL
                        if url_str and 'spotify' in url_str.lower() and 'spotify.com' in url_str.lower():
                            spotify_url = url_str
                            break
            
            # If no Spotify link found in MusicBrainz, try searching Spotify directly
            if not spotify_url and self.albums_properties.get('listen'):
                album_title = release_data.get('title', '')
                artist_name = None
                # Get artist name from artist-credit
                if release_data.get('artist-credit') and release_data['artist-credit']:
                    first_artist = release_data['artist-credit'][0].get('artist', {})
                    artist_name = first_artist.get('name')
                
                if album_title:
                    spotify_url = self.mb._get_spotify_album_url(album_title, artist_name)
                    if spotify_url:
                        logger.debug(f"Found Spotify URL via API search: {spotify_url}")
            
            if spotify_url:
                prop_key = self._get_property_key(self.albums_properties['listen'], 'albums')
                if prop_key:
                    properties[prop_key] = {'url': spotify_url}
            
            # MusicBrainz URL
            if release_data.get('id') and self.albums_properties.get('musicbrainz_url'):
                mb_url = f"https://musicbrainz.org/release/{release_data['id']}"
                prop_key = self._get_property_key(self.albums_properties['musicbrainz_url'], 'albums')
                if prop_key:
                    properties[prop_key] = {'url': mb_url}
            
            # Last updated
            if self.albums_properties.get('last_updated'):
                prop_key = self._get_property_key(self.albums_properties['last_updated'], 'albums')
                if prop_key:
                    properties[prop_key] = {'date': {'start': datetime.now().isoformat()}}
            
        except Exception as e:
            logger.error(f"Error formatting album properties: {e}")
        
        return properties
    
    def _find_or_create_artist_page(self, artist_name: str, artist_mbid: Optional[str] = None) -> Optional[str]:
        """Find or create an artist page in the Artists database and return its page ID."""
        if not self.artists_db_id:
            return None
        
        try:
            # First, try to find existing artist by name
            title_prop_id = self.artists_properties.get('title')
            if not title_prop_id:
                return None
            
            title_key = self._get_property_key(title_prop_id, 'artists')
            if not title_key:
                return None
            
            # Search for existing artist page by title
            filter_params = {
                'property': title_key,
                'title': {
                    'equals': artist_name
                }
            }
            
            existing_pages = self.notion.query_database(self.artists_db_id, filter_params)
            
            if existing_pages:
                # Found existing artist
                return existing_pages[0]['id']
            
            # If no exact match, try searching all pages and match by name (case-insensitive)
            all_pages = self.notion.query_database(self.artists_db_id)
            for page in all_pages:
                page_props = page.get('properties', {})
                page_title_prop = page_props.get(title_key, {})
                if page_title_prop.get('title') and page_title_prop['title']:
                    page_title = page_title_prop['title'][0]['plain_text']
                    if page_title.lower() == artist_name.lower():
                        return page['id']
            
            # Artist doesn't exist - create it
            logger.info(f"Creating new artist page: {artist_name}")
            
            # Get artist data from MusicBrainz if we have MBID
            artist_data = None
            if artist_mbid:
                artist_data = self.mb.get_artist(artist_mbid)
            
            # Format properties for new artist
            artist_props = {}
            artist_props[title_key] = {
                'title': [{'text': {'content': artist_name}}]
            }
            
            # Add MusicBrainz ID if available
            if artist_data and artist_data.get('id') and self.artists_properties.get('musicbrainz_id'):
                mb_id_key = self._get_property_key(self.artists_properties['musicbrainz_id'], 'artists')
                if mb_id_key:
                    artist_props[mb_id_key] = {
                        'rich_text': [{'text': {'content': artist_data['id']}}]
                    }
            
            # Create the artist page
            artist_page_id = self.notion.create_page(
                self.artists_db_id,
                artist_props,
                None,
                '🎤'  # Microphone emoji
            )
            
            if artist_page_id:
                logger.info(f"Created artist page: {artist_name} (ID: {artist_page_id})")
                # If we have full artist data, update the page with it
                if artist_data:
                    full_props = self._format_artist_properties(artist_data)
                    self.notion.update_page(artist_page_id, full_props)
            
            return artist_page_id
            
        except Exception as e:
            logger.error(f"Error finding/creating artist page for '{artist_name}': {e}")
            return None
    
    def _score_release_for_song(self, release: Dict) -> tuple:
        """Score a release for song-to-album matching.
        
        Returns a tuple (score, date) where:
        - Higher score = better match
        - Date is used for tie-breaking (earlier is better)
        
        Criteria (in priority order):
        1. US country (prefer US)
        2. Album type (prefer "Album" over other types)
        3. Earliest release date
        """
        score = 0
        release_date = None
        
        # Get release-group for album type
        release_group = release.get('release-group', {})
        release_group_type = release_group.get('type', '').lower() if release_group else ''
        
        # Get country - check release-events if not in release directly
        country = release.get('country') or ''
        country = country.upper() if country else ''
        if not country and release.get('release-events'):
            # Get country from first release event
            first_event = release['release-events'][0]
            country = first_event.get('area', {}).get('iso-3166-1-codes', [''])[0] if first_event.get('area') else ''
            if country:
                country = country.upper()
        
        # Get release date - check multiple sources
        date_str = release.get('date', '')
        
        # If no date in release, check release-events (first event date)
        if not date_str and release.get('release-events'):
            first_event = release['release-events'][0]
            date_str = first_event.get('date', '')
        
        # If still no date, check release-group first-release-date
        if not date_str:
            release_group = release.get('release-group', {})
            if release_group:
                date_str = release_group.get('first-release-date', '')
        
        if date_str:
            # Normalize date to YYYY-MM-DD for comparison
            # Prefer full dates over partial dates (year-only or year-month)
            try:
                parts = date_str.split('-')
                if len(parts) >= 1:
                    year = parts[0]
                    if len(parts) == 1:
                        # Just year - set to end of year so it sorts after all full dates in that year
                        month = '12'
                        day = '31'
                    elif len(parts) == 2:
                        # Year and month - set to end of month so it sorts after all full dates in that month
                        month = parts[1]
                        # Get last day of month (approximate - use 28 to be safe, or 31 for most months)
                        if month in ['01', '03', '05', '07', '08', '10', '12']:
                            day = '31'
                        elif month in ['04', '06', '09', '11']:
                            day = '30'
                        else:
                            day = '28'  # February
                    else:
                        # Full date - use as is
                        month = parts[1]
                        day = parts[2]
                    release_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except Exception:
                pass
        
        # Scoring:
        # 1. Country: US = 100 points, others = 0
        if country == 'US':
            score += 100
        
        # 2. Album type: Album = 50 points, others = 0
        if release_group_type == 'album':
            score += 50
        
        # Date will be used for sorting (earlier is better)
        # We'll return it as a string for comparison
        
        return (score, release_date or '9999-12-31')  # Use far future date if no date
    
    def _find_best_release_for_song(self, releases: List[Dict]) -> Optional[Dict]:
        """Find the best release for a song based on scoring criteria.
        
        Criteria:
        - US country (prefer US)
        - Album type (prefer "Album")
        - Earliest release date
        
        Optimization: Score releases with available data first, then only fetch full data
        for top candidates to minimize API calls.
        """
        if not releases:
            return None
        
        # First pass: Score all releases with available data (no API calls)
        # This allows us to identify top candidates before fetching full data
        scored_releases = []
        for release in releases:
            score, date = self._score_release_for_song(release)
            scored_releases.append((score, date, release))
        
        # Sort by score (descending), then by date (ascending - earlier is better)
        scored_releases.sort(key=lambda x: (-x[0], x[1]))
        
        # Only fetch full release data for top 10 candidates (or all if < 10)
        # This dramatically reduces API calls when there are many releases
        top_candidates = min(10, len(scored_releases))
        top_releases = []
        
        for i in range(top_candidates):
            score, date, release = scored_releases[i]
            release_mbid = release.get('id')
            
            # Fetch full release data for accurate final scoring
            if release_mbid:
                full_release = self.mb.get_release(release_mbid)
                if full_release:
                    # Merge full release data with basic release data
                    release = {**release, **full_release}
                    # Re-score with full data
                    score, date = self._score_release_for_song(release)
            
            top_releases.append((score, date, release))
        
        # Re-sort top candidates with full data
        top_releases.sort(key=lambda x: (-x[0], x[1]))
        
        # Return the best match
        return top_releases[0][2] if top_releases else None
    
    def _find_or_create_album_page(self, album_title: str, album_mbid: Optional[str] = None) -> Optional[str]:
        """Find or create an album page in the Albums database and return its page ID."""
        if not self.albums_db_id:
            return None
        
        try:
            # First, try to find existing album by title
            title_prop_id = self.albums_properties.get('title')
            if not title_prop_id:
                return None
            
            title_key = self._get_property_key(title_prop_id, 'albums')
            if not title_key:
                return None
            
            # Search for existing album page by title
            filter_params = {
                'property': title_key,
                'title': {
                    'equals': album_title
                }
            }
            
            existing_pages = self.notion.query_database(self.albums_db_id, filter_params)
            
            if existing_pages:
                # Found existing album
                return existing_pages[0]['id']
            
            # If no exact match, try searching all pages and match by name (case-insensitive)
            all_pages = self.notion.query_database(self.albums_db_id)
            for page in all_pages:
                page_props = page.get('properties', {})
                page_title_prop = page_props.get(title_key, {})
                if page_title_prop.get('title') and page_title_prop['title']:
                    page_title = page_title_prop['title'][0]['plain_text']
                    if page_title.lower() == album_title.lower():
                        return page['id']
            
            # Album doesn't exist - create it
            logger.info(f"Creating new album page: {album_title}")
            
            # Get album data from MusicBrainz if we have MBID
            album_data = None
            if album_mbid:
                album_data = self.mb.get_release(album_mbid)
            
            # Format properties for new album
            album_props = {}
            album_props[title_key] = {
                'title': [{'text': {'content': album_title}}]
            }
            
            # Add MusicBrainz ID if available
            if album_data and album_data.get('id') and self.albums_properties.get('musicbrainz_id'):
                mb_id_key = self._get_property_key(self.albums_properties['musicbrainz_id'], 'albums')
                if mb_id_key:
                    album_props[mb_id_key] = {
                        'rich_text': [{'text': {'content': album_data['id']}}]
                    }
            
            # Create the album page
            album_page_id = self.notion.create_page(
                self.albums_db_id,
                album_props,
                None,
                '💿'  # CD emoji
            )
            
            if album_page_id:
                logger.info(f"Created album page: {album_title} (ID: {album_page_id})")
                # If we have full album data, update the page with it
                if album_data:
                    full_props = self._format_album_properties(album_data)
                    self.notion.update_page(album_page_id, full_props)
            
            return album_page_id
            
        except Exception as e:
            logger.error(f"Error finding/creating album page for '{album_title}': {e}")
            return None
    
    def _find_or_create_label_page(self, label_name: str, label_mbid: Optional[str] = None) -> Optional[str]:
        """Find or create a label page in the Labels database and return its page ID."""
        if not self.labels_db_id:
            return None
        
        try:
            # First, try to find existing label by name
            title_prop_id = self.labels_properties.get('title')
            if not title_prop_id:
                return None
            
            title_key = self._get_property_key(title_prop_id, 'labels')
            if not title_key:
                return None
            
            # Search for existing label page by title
            filter_params = {
                'property': title_key,
                'title': {
                    'equals': label_name
                }
            }
            
            existing_pages = self.notion.query_database(self.labels_db_id, filter_params)
            
            if existing_pages:
                # Found existing label
                return existing_pages[0]['id']
            
            # If no exact match, try searching all pages and match by name (case-insensitive)
            all_pages = self.notion.query_database(self.labels_db_id)
            for page in all_pages:
                page_props = page.get('properties', {})
                page_title_prop = page_props.get(title_key, {})
                if page_title_prop.get('title') and page_title_prop['title']:
                    page_title = page_title_prop['title'][0]['plain_text']
                    if page_title.lower() == label_name.lower():
                        return page['id']
            
            # Label doesn't exist - create it
            logger.info(f"Creating new label page: {label_name}")
            
            # Get label data from MusicBrainz if we have MBID
            label_data = None
            if label_mbid:
                label_data = self.mb.get_label(label_mbid)
            
            # Format properties for new label
            label_props = {}
            label_props[title_key] = {
                'title': [{'text': {'content': label_name}}]
            }
            
            # Add MusicBrainz ID if available
            if label_data and label_data.get('id') and self.labels_properties.get('musicbrainz_id'):
                mb_id_key = self._get_property_key(self.labels_properties['musicbrainz_id'], 'labels')
                if mb_id_key:
                    label_props[mb_id_key] = {
                        'rich_text': [{'text': {'content': label_data['id']}}]
                    }
            
            # Create the label page
            label_page_id = self.notion.create_page(
                self.labels_db_id,
                label_props,
                None,
                '🏷️'  # Label emoji
            )
            
            if label_page_id:
                logger.info(f"Created label page: {label_name} (ID: {label_page_id})")
                # If we have full label data, update the page with it
                if label_data:
                    full_props = self._format_label_properties(label_data)
                    self.notion.update_page(label_page_id, full_props)
            
            return label_page_id
            
        except Exception as e:
            logger.error(f"Error finding/creating label page for '{label_name}': {e}")
            return None
    
    def _load_locations_cache(self):
        """Load all locations into cache to avoid repeated database queries."""
        if not self.locations_db_id or self._location_cache is not None:
            return
        
        try:
            # Query all location pages once
            all_pages = self.notion.query_database(self.locations_db_id)
            
            # Find the title property key
            if all_pages:
                first_page_props = all_pages[0].get('properties', {})
                for prop_key, prop_data in first_page_props.items():
                    if prop_data.get('type') == 'title':
                        self._locations_title_key = prop_key
                        break
            
            if not self._locations_title_key:
                logger.warning("Could not find title property in Locations database")
                self._location_cache = {}  # Mark as loaded (empty)
                return
            
            # Build cache: location name (lowercase) -> page_id
            self._location_cache = {}
            for page in all_pages:
                page_props = page.get('properties', {})
                page_title_prop = page_props.get(self._locations_title_key, {})
                if page_title_prop.get('title') and page_title_prop['title']:
                    page_title = page_title_prop['title'][0]['plain_text']
                    self._location_cache[page_title.lower()] = page['id']
            
            logger.debug(f"Loaded {len(self._location_cache)} locations into cache")
            
        except Exception as e:
            logger.error(f"Error loading locations cache: {e}")
            self._location_cache = {}  # Mark as loaded (empty)
    
    def _find_or_create_location_page(self, location_name: str) -> Optional[str]:
        """Find or create a location page in the Locations database and return its page ID."""
        if not self.locations_db_id:
            return None
        
        try:
            # Load cache if not already loaded
            if self._location_cache is None:
                self._load_locations_cache()
            
            # Check cache first
            location_name_lower = location_name.lower()
            if location_name_lower in self._location_cache:
                return self._location_cache[location_name_lower]
            
            # Location doesn't exist - create it
            if not self._locations_title_key:
                # Need to get title key if we don't have it
                all_pages = self.notion.query_database(self.locations_db_id)
                if all_pages:
                    first_page_props = all_pages[0].get('properties', {})
                    for prop_key, prop_data in first_page_props.items():
                        if prop_data.get('type') == 'title':
                            self._locations_title_key = prop_key
                            break
            
            if not self._locations_title_key:
                logger.warning("Could not find title property in Locations database")
                return None
            
            logger.info(f"Creating new location page: {location_name}")
            
            # Format properties for new location
            location_props = {}
            location_props[self._locations_title_key] = {
                'title': [{'text': {'content': location_name}}]
            }
            
            # Create the location page
            location_page_id = self.notion.create_page(
                self.locations_db_id,
                location_props,
                None,
                '📍'  # Location pin emoji
            )
            
            if location_page_id:
                logger.info(f"Created location page: {location_name} (ID: {location_page_id})")
                # Add to cache
                self._location_cache[location_name_lower] = location_page_id
            
            return location_page_id
            
        except Exception as e:
            logger.error(f"Error finding/creating location page for '{location_name}': {e}")
            return None
    
    def sync_song_page(self, page: Dict, force_all: bool = False) -> Optional[bool]:
        """Sync a single song page with MusicBrainz data."""
        try:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # Extract title
            title_prop_id = self.songs_properties.get('title')
            if not title_prop_id:
                logger.warning(f"Missing title property for Songs database")
                return None
            
            title_key = self._get_property_key(title_prop_id, 'songs')
            if not title_key:
                logger.warning(f"Could not find title property key")
                return None
            
            title_prop = properties.get(title_key, {})
            if title_prop.get('type') != 'title' or not title_prop.get('title'):
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            title = title_prop['title'][0]['plain_text']
            logger.info(f"Processing song: {title}")
            
            # Try to extract artist name from relation
            artist_name = None
            artist_prop_id = self.songs_properties.get('artist')
            if artist_prop_id:
                artist_key = self._get_property_key(artist_prop_id, 'songs')
                if artist_key:
                    artist_prop = properties.get(artist_key, {})
                    if artist_prop.get('relation'):
                        # Fetch the artist page to get the name
                        artist_page_id = artist_prop['relation'][0]['id'] if artist_prop['relation'] else None
                        if artist_page_id:
                            artist_page = self.notion.get_page(artist_page_id)
                            if artist_page:
                                artist_props = artist_page.get('properties', {})
                                artist_title_key = self._get_property_key(self.artists_properties.get('title'), 'artists')
                                if artist_title_key and artist_props.get(artist_title_key):
                                    artist_title_prop = artist_props[artist_title_key]
                                    if artist_title_prop.get('title') and artist_title_prop['title']:
                                        artist_name = artist_title_prop['title'][0]['plain_text']
                                        logger.debug(f"Found artist from relation: {artist_name}")
            
            # Try to extract album name and MBID from relation
            album_name = None
            album_mbid = None
            album_prop_id = self.songs_properties.get('album')
            if album_prop_id:
                album_key = self._get_property_key(album_prop_id, 'songs')
                if album_key:
                    album_prop = properties.get(album_key, {})
                    if album_prop.get('relation'):
                        # Fetch the album page to get the name and MBID
                        album_page_id = album_prop['relation'][0]['id'] if album_prop['relation'] else None
                        if album_page_id:
                            album_page = self.notion.get_page(album_page_id)
                            if album_page:
                                album_props = album_page.get('properties', {})
                                album_title_key = self._get_property_key(self.albums_properties.get('title'), 'albums')
                                if album_title_key and album_props.get(album_title_key):
                                    album_title_prop = album_props[album_title_key]
                                    if album_title_prop.get('title') and album_title_prop['title']:
                                        album_name = album_title_prop['title'][0]['plain_text']
                                        logger.debug(f"Found album from relation: {album_name}")
                                
                                # Get album MBID for verification
                                album_mbid = self._get_mbid_from_related_page(album_page_id, 'albums')
                                if album_mbid:
                                    logger.debug(f"Found album MBID from relation: {album_mbid}")
            
            # Check for existing MBID
            mb_id_prop_id = self.songs_properties.get('musicbrainz_id')
            existing_mbid = None
            if mb_id_prop_id:
                mb_id_key = self._get_property_key(mb_id_prop_id, 'songs')
                if mb_id_key:
                    mb_id_prop = properties.get(mb_id_key, {})
                    # MBID is stored as rich_text (UUID string)
                    if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                        existing_mbid = mb_id_prop['rich_text'][0]['plain_text']
            
            # Search or get recording data
            recording_data = None
            if existing_mbid:
                recording_data = self.mb.get_recording(existing_mbid)
                if not recording_data:
                    logger.warning(f"Could not find recording with MBID {existing_mbid}, searching by title")
                    existing_mbid = None
                elif not force_all:
                    # Skip pages with existing MBIDs unless force_all is True
                    logger.info(f"Skipping song '{title}' - already has MBID {existing_mbid} (use --force-all to update)")
                    return None
            
            if not recording_data:
                search_results = self.mb.search_recordings(title, artist_name, album_name, limit=20)
                if not search_results:
                    logger.warning(f"Could not find song: {title}")
                    return False
                
                # Find exact match (word-for-word, case-insensitive, ignoring special characters)
                # Also verify it appears on the related album if album_mbid is provided
                best_match = None
                for result in search_results:
                    result_title = result.get('title', '')
                    if not self._titles_match_exactly(title, result_title):
                        continue
                    
                    # If we have an album MBID, verify the recording appears on that album
                    if album_mbid:
                        recording_id = result.get('id')
                        if recording_id and not self._recording_appears_on_album(recording_id, album_mbid):
                            logger.debug(f"Recording '{result_title}' does not appear on album {album_mbid}, skipping")
                            continue
                    
                    best_match = result
                    logger.debug(f"Found exact match: '{result_title}' for '{title}'")
                    break
                
                # If no exact match found, warn and skip
                if not best_match:
                    logger.warning(f"No exact match found for '{title}'. Closest match: '{search_results[0].get('title', 'Unknown')}'. Skipping to avoid incorrect match.")
                    return False
                
                recording_data = self.mb.get_recording(best_match['id'])
            
            if not recording_data:
                logger.warning(f"Could not get song data for: {title}")
                return False
            
            # Format properties
            notion_props = self._format_song_properties(recording_data)
            
            # Preserve existing relations (merge instead of replace)
            notion_props = self._merge_relations(page, notion_props, 'songs')
            
            # Set icon
            icon = '🎵'  # Musical note emoji for songs
            
            # Update the page
            if self.notion.update_page(page_id, notion_props, None, icon):
                logger.info(f"Successfully updated song: {title}")
                return True
            else:
                logger.error(f"Failed to update song: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing song page {page.get('id')}: {e}")
            return False
    
    def _format_song_properties(self, recording_data: Dict) -> Dict:
        """Format MusicBrainz recording data for Notion properties."""
        properties = {}
        
        try:
            # Title
            if recording_data.get('title') and self.songs_properties.get('title'):
                prop_key = self._get_property_key(self.songs_properties['title'], 'songs')
                if prop_key:
                    properties[prop_key] = {
                        'title': [{'text': {'content': recording_data['title']}}]
                    }
            
            # MusicBrainz ID (store as string in rich_text since MBIDs are UUIDs)
            if recording_data.get('id') and self.songs_properties.get('musicbrainz_id'):
                prop_key = self._get_property_key(self.songs_properties['musicbrainz_id'], 'songs')
                if prop_key:
                    # Store MBID as string - it's a UUID, not a number
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': recording_data['id']}}]
                    }
            
            # Artists (as relations)
            if recording_data.get('artist-credit') and self.songs_properties.get('artist') and self.artists_db_id:
                # Extract artist names and MBIDs from artist-credit
                artist_names = []
                artist_mbids = []
                
                for ac in recording_data.get('artist-credit', []):
                    if ac.get('artist'):
                        artist = ac['artist']
                        artist_name = artist.get('name')
                        artist_mbid = artist.get('id')
                        if artist_name:
                            artist_names.append(artist_name)
                            if artist_mbid:
                                artist_mbids.append(artist_mbid)
                            else:
                                artist_mbids.append(None)
                
                if artist_names:
                    # Find or create artist pages and get their IDs
                    artist_page_ids = []
                    for i, artist_name in enumerate(artist_names[:5]):  # Limit to 5 artists
                        artist_mbid = artist_mbids[i] if i < len(artist_mbids) else None
                        artist_page_id = self._find_or_create_artist_page(artist_name, artist_mbid)
                        if artist_page_id:
                            artist_page_ids.append(artist_page_id)
                    
                    if artist_page_ids:
                        prop_key = self._get_property_key(self.songs_properties['artist'], 'songs')
                        if prop_key:
                            properties[prop_key] = {
                                'relation': [{'id': page_id} for page_id in artist_page_ids]
                            }
            
            # Album (as relation) - get best release based on criteria
            best_release = None  # Initialize for use in genres extraction
            if recording_data.get('id') and self.songs_properties.get('album') and self.albums_db_id:
                # Get releases from recording data
                releases = recording_data.get('releases', [])
                
                # If we have few releases or they don't have complete data, search for more releases
                # by searching for releases with the same title as the song (and same artist)
                if len(releases) < 5 or not any(r.get('country') for r in releases):
                    # Get artist name for search
                    artist_name = None
                    if recording_data.get('artist-credit') and recording_data['artist-credit']:
                        first_artist = recording_data['artist-credit'][0].get('artist', {})
                        artist_name = first_artist.get('name')
                    
                    # Search for releases with the same title as the song
                    song_title = recording_data.get('title')
                    if song_title and artist_name:
                        search_results = self.mb.search_releases(song_title, artist_name, limit=50)
                        if search_results:
                            # Merge with existing releases (avoid duplicates)
                            existing_ids = {r.get('id') for r in releases if r.get('id')}
                            for result in search_results:
                                if result.get('id') and result['id'] not in existing_ids:
                                    releases.append(result)
                
                if releases:
                    # Find the best release based on criteria:
                    # - US country (prefer US)
                    # - Album type (prefer "Album")
                    # - Earliest release date
                    try:
                        best_release = self._find_best_release_for_song(releases)
                    except Exception as e:
                        logger.warning(f"Error finding best release for song: {e}")
                        best_release = None
                    
                    if best_release:
                        release_title = best_release.get('title')
                        release_mbid = best_release.get('id')
                        
                        if release_title:
                            # Find or create album page
                            album_page_id = self._find_or_create_album_page(release_title, release_mbid)
                            if album_page_id:
                                prop_key = self._get_property_key(self.songs_properties['album'], 'songs')
                                if prop_key:
                                    properties[prop_key] = {
                                        'relation': [{'id': album_page_id}]
                                    }
                        
                        # Extract track number from the best release
                        # Track number is in media -> tracks -> position (for the matching recording)
                        recording_id = recording_data.get('id')
                        if recording_id and best_release.get('media'):
                            track_number = None
                            for medium in best_release.get('media', []):
                                if medium.get('tracks'):
                                    for track in medium.get('tracks', []):
                                        if track.get('recording') and track['recording'].get('id') == recording_id:
                                            # Found the track - get its position
                                            track_number = track.get('position')
                                            if track_number:
                                                break
                                    if track_number:
                                        break
                            
                            if track_number and self.songs_properties.get('track_number'):
                                prop_key = self._get_property_key(self.songs_properties['track_number'], 'songs')
                                if prop_key:
                                    properties[prop_key] = {'number': int(track_number)}
            
            # Length
            if recording_data.get('length') and self.songs_properties.get('length'):
                length_seconds = recording_data['length'] / 1000  # Convert from milliseconds
                prop_key = self._get_property_key(self.songs_properties['length'], 'songs')
                if prop_key:
                    properties[prop_key] = {'number': int(length_seconds)}
            
            # ISRC
            if recording_data.get('isrc-list') and self.songs_properties.get('isrc'):
                isrc = recording_data['isrc-list'][0] if recording_data['isrc-list'] else None
                if isrc:
                    prop_key = self._get_property_key(self.songs_properties['isrc'], 'songs')
                    if prop_key:
                        properties[prop_key] = {
                            'rich_text': [{'text': {'content': isrc}}]
                        }
            
            # Disambiguation
            if recording_data.get('disambiguation') and self.songs_properties.get('disambiguation'):
                prop_key = self._get_property_key(self.songs_properties['disambiguation'], 'songs')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': recording_data['disambiguation']}}]
                    }
            
            # Genres - from the best release's release-group (same as albums)
            if best_release and best_release.get('release-group') and best_release['release-group'].get('genres') and self.songs_properties.get('genres'):
                genres = [genre['name'] for genre in best_release['release-group']['genres'] if genre.get('name')]
                if genres:
                    prop_key = self._get_property_key(self.songs_properties['genres'], 'songs')
                    if prop_key:
                        properties[prop_key] = {
                            'multi_select': [{'name': genre} for genre in genres[:10]]  # Limit to 10 genres
                        }
            
            # Tags - filter out tags that match genres (genres have priority)
            if recording_data.get('tags') and self.songs_properties.get('tags'):
                # Get genre names for comparison (from best_release if available)
                genre_names = set()
                if best_release and best_release.get('release-group') and best_release['release-group'].get('genres'):
                    genre_names = {genre['name'] for genre in best_release['release-group']['genres'] if genre.get('name')}
                
                # Filter tags: exclude tags that match genres (genres have priority)
                tags = []
                for tag in recording_data['tags']:
                    tag_name = tag.get('name')
                    # Only include tags that:
                    # 1. Have a name
                    # 2. Are different from genres (genres take priority)
                    if tag_name and tag_name not in genre_names:
                        tags.append(tag_name)
                
                if tags:
                    prop_key = self._get_property_key(self.songs_properties['tags'], 'songs')
                    if prop_key:
                        properties[prop_key] = {
                            'multi_select': [{'name': tag} for tag in tags[:10]]
                        }
            
            # Spotify link (from url-rels) - check for both "streaming" and "free streaming"
            spotify_url = None
            if recording_data.get('relations') and self.songs_properties.get('listen'):
                for relation in recording_data.get('relations', []):
                    relation_type = relation.get('type', '').lower()
                    # Check for both "streaming" and "free streaming" relation types
                    if relation_type in ['streaming', 'free streaming']:
                        url_resource = relation.get('url', {})
                        if isinstance(url_resource, dict):
                            url_str = url_resource.get('resource', '')
                        else:
                            url_str = str(url_resource)
                        
                        # Check if it's a Spotify URL
                        if url_str and 'spotify' in url_str.lower() and 'spotify.com' in url_str.lower():
                            spotify_url = url_str
                            break
            
            # If no Spotify link found in MusicBrainz, try searching Spotify directly
            if not spotify_url and self.songs_properties.get('listen'):
                song_title = recording_data.get('title', '')
                artist_name = None
                # Get artist name from artist-credit
                if recording_data.get('artist-credit') and recording_data['artist-credit']:
                    first_artist = recording_data['artist-credit'][0].get('artist', {})
                    artist_name = first_artist.get('name')
                
                if song_title:
                    spotify_url = self.mb._get_spotify_track_url(song_title, artist_name)
                    if spotify_url:
                        logger.debug(f"Found Spotify URL via API search: {spotify_url}")
            
            if spotify_url:
                prop_key = self._get_property_key(self.songs_properties['listen'], 'songs')
                if prop_key:
                    properties[prop_key] = {'url': spotify_url}
            
            # MusicBrainz URL
            if recording_data.get('id') and self.songs_properties.get('musicbrainz_url'):
                mb_url = f"https://musicbrainz.org/recording/{recording_data['id']}"
                prop_key = self._get_property_key(self.songs_properties['musicbrainz_url'], 'songs')
                if prop_key:
                    properties[prop_key] = {'url': mb_url}
            
            # Last updated
            if self.songs_properties.get('last_updated'):
                prop_key = self._get_property_key(self.songs_properties['last_updated'], 'songs')
                if prop_key:
                    properties[prop_key] = {'date': {'start': datetime.now().isoformat()}}
            
        except Exception as e:
            logger.error(f"Error formatting song properties: {e}")
        
        return properties
    
    def sync_label_page(self, page: Dict, force_all: bool = False) -> Optional[bool]:
        """Sync a single label page with MusicBrainz data."""
        try:
            page_id = page['id']
            properties = page.get('properties', {})
            
            # Extract title
            title_prop_id = self.labels_properties.get('title')
            if not title_prop_id:
                logger.warning(f"Missing title property for Labels database")
                return None
            
            title_key = self._get_property_key(title_prop_id, 'labels')
            if not title_key:
                logger.warning(f"Could not find title property key")
                return None
            
            title_prop = properties.get(title_key, {})
            if title_prop.get('type') != 'title' or not title_prop.get('title'):
                logger.warning(f"Missing title for page {page_id}")
                return None
            
            title = title_prop['title'][0]['plain_text']
            logger.info(f"Processing label: {title}")
            
            # Check for existing MBID
            mb_id_prop_id = self.labels_properties.get('musicbrainz_id')
            existing_mbid = None
            if mb_id_prop_id:
                mb_id_key = self._get_property_key(mb_id_prop_id, 'labels')
                if mb_id_key:
                    mb_id_prop = properties.get(mb_id_key, {})
                    # MBID is stored as rich_text (UUID string)
                    if mb_id_prop.get('rich_text') and mb_id_prop['rich_text']:
                        existing_mbid = mb_id_prop['rich_text'][0]['plain_text']
            
            # Search or get label data
            label_data = None
            if existing_mbid:
                label_data = self.mb.get_label(existing_mbid)
                if not label_data:
                    logger.warning(f"Could not find label with MBID {existing_mbid}, searching by name")
                    existing_mbid = None
                elif not force_all:
                    # Skip pages with existing MBIDs unless force_all is True
                    logger.info(f"Skipping label '{title}' - already has MBID {existing_mbid} (use --force-all to update)")
                    return None
            
            if not label_data:
                search_results = self.mb.search_labels(title, limit=5)
                if not search_results:
                    logger.warning(f"Could not find label: {title}")
                    return False
                
                # Select best match (first result for now)
                best_match = search_results[0]
                label_data = self.mb.get_label(best_match['id'])
            
            if not label_data:
                logger.warning(f"Could not get label data for: {title}")
                return False
            
            # Format properties
            notion_props = self._format_label_properties(label_data)
            
            # Set icon
            icon = '🏷️'  # Label emoji for labels
            
            # Update the page
            if self.notion.update_page(page_id, notion_props, None, icon):
                logger.info(f"Successfully updated label: {title}")
                return True
            else:
                logger.error(f"Failed to update label: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error syncing label page {page.get('id')}: {e}")
            return False
    
    def _format_label_properties(self, label_data: Dict) -> Dict:
        """Format MusicBrainz label data for Notion properties."""
        properties = {}
        
        try:
            # Title (name)
            if label_data.get('name') and self.labels_properties.get('title'):
                prop_key = self._get_property_key(self.labels_properties['title'], 'labels')
                if prop_key:
                    properties[prop_key] = {
                        'title': [{'text': {'content': label_data['name']}}]
                    }
            
            # MusicBrainz ID (store as string in rich_text since MBIDs are UUIDs)
            if label_data.get('id') and self.labels_properties.get('musicbrainz_id'):
                prop_key = self._get_property_key(self.labels_properties['musicbrainz_id'], 'labels')
                if prop_key:
                    # Store MBID as string - it's a UUID, not a number
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': label_data['id']}}]
                    }
            
            # Type
            if label_data.get('type') and self.labels_properties.get('type'):
                prop_key = self._get_property_key(self.labels_properties['type'], 'labels')
                if prop_key:
                    properties[prop_key] = {'select': {'name': label_data['type']}}
            
            # Country
            if label_data.get('area') and label_data['area'].get('iso-3166-1-code-list'):
                country_code = label_data['area']['iso-3166-1-code-list'][0]
                if self.labels_properties.get('country'):
                    prop_key = self._get_property_key(self.labels_properties['country'], 'labels')
                    if prop_key:
                        properties[prop_key] = {'select': {'name': country_code}}
            
            # Begin date
            if label_data.get('life-span') and label_data['life-span'].get('begin'):
                begin_date = label_data['life-span']['begin']
                if self.labels_properties.get('begin_date'):
                    prop_key = self._get_property_key(self.labels_properties['begin_date'], 'labels')
                    if prop_key:
                        properties[prop_key] = {'date': {'start': begin_date[:10]}}  # YYYY-MM-DD
            
            # End date
            if label_data.get('life-span') and label_data['life-span'].get('end'):
                end_date = label_data['life-span']['end']
                if self.labels_properties.get('end_date'):
                    prop_key = self._get_property_key(self.labels_properties['end_date'], 'labels')
                    if prop_key:
                        properties[prop_key] = {'date': {'start': end_date[:10]}}
            
            # Disambiguation
            if label_data.get('disambiguation') and self.labels_properties.get('disambiguation'):
                prop_key = self._get_property_key(self.labels_properties['disambiguation'], 'labels')
                if prop_key:
                    properties[prop_key] = {
                        'rich_text': [{'text': {'content': label_data['disambiguation']}}]
                    }
            
            # Genres
            if label_data.get('genres') and self.labels_properties.get('genres'):
                genres = [genre['name'] for genre in label_data['genres'] if genre.get('name')]
                if genres:
                    prop_key = self._get_property_key(self.labels_properties['genres'], 'labels')
                    if prop_key:
                        properties[prop_key] = {
                            'multi_select': [{'name': genre} for genre in genres[:10]]  # Limit to 10 genres
                        }
            
            # Tags - filter out tags that match genres (genres have priority)
            if label_data.get('tags') and self.labels_properties.get('tags'):
                # Get genre names for comparison
                genre_names = set()
                if label_data.get('genres'):
                    genre_names = {genre['name'] for genre in label_data['genres'] if genre.get('name')}
                
                # Filter tags: exclude tags that match genres (genres have priority)
                tags = []
                for tag in label_data['tags']:
                    tag_name = tag.get('name')
                    # Only include tags that:
                    # 1. Have a name
                    # 2. Are different from genres (genres take priority)
                    if tag_name and tag_name not in genre_names:
                        tags.append(tag_name)
                
                if tags:
                    prop_key = self._get_property_key(self.labels_properties['tags'], 'labels')
                    if prop_key:
                        properties[prop_key] = {
                            'multi_select': [{'name': tag} for tag in tags[:10]]  # Limit to 10 tags
                        }
            
            # MusicBrainz URL
            if label_data.get('id') and self.labels_properties.get('musicbrainz_url'):
                mb_url = f"https://musicbrainz.org/label/{label_data['id']}"
                prop_key = self._get_property_key(self.labels_properties['musicbrainz_url'], 'labels')
                if prop_key:
                    properties[prop_key] = {'url': mb_url}
            
            # Extract URLs from url-rels
            ig_url = None
            website_url = None
            bandcamp_url = None
            
            if label_data.get('relations'):
                for relation in label_data['relations']:
                    relation_type = relation.get('type', '').lower()
                    url_resource = relation.get('url', {}).get('resource', '').lower()
                    
                    # Instagram
                    if relation_type == 'instagram' or (relation_type == 'social network' and 'instagram' in url_resource):
                        ig_url = relation.get('url', {}).get('resource')
                    # Official homepage/website/site
                    elif (relation_type == 'official homepage' or 
                          relation_type == 'official website' or 
                          relation_type == 'official site'):
                        website_url = relation.get('url', {}).get('resource')
                    # Bandcamp
                    elif 'bandcamp' in url_resource:
                        bandcamp_url = relation.get('url', {}).get('resource')
            
            # Official Website Link
            if website_url and self.labels_properties.get('official_website'):
                prop_key = self._get_property_key(self.labels_properties['official_website'], 'labels')
                if prop_key:
                    properties[prop_key] = {'url': website_url}
            
            # IG Link
            if ig_url and self.labels_properties.get('ig'):
                prop_key = self._get_property_key(self.labels_properties['ig'], 'labels')
                if prop_key:
                    properties[prop_key] = {'url': ig_url}
            
            # Bandcamp Link
            if bandcamp_url and self.labels_properties.get('bandcamp'):
                prop_key = self._get_property_key(self.labels_properties['bandcamp'], 'labels')
                if prop_key:
                    properties[prop_key] = {'url': bandcamp_url}
            
            # Founded (date from begin date)
            if label_data.get('life-span') and label_data['life-span'].get('begin') and self.labels_properties.get('founded'):
                begin_date = label_data['life-span']['begin']
                # Format as date (YYYY-MM-DD, truncate to 10 chars if longer)
                prop_key = self._get_property_key(self.labels_properties['founded'], 'labels')
                if prop_key:
                    properties[prop_key] = {'date': {'start': begin_date[:10]}}  # YYYY-MM-DD
            
            # Area (relation to Locations database)
            if label_data.get('area') and label_data['area'].get('name') and self.labels_properties.get('area') and self.locations_db_id:
                area_name = label_data['area']['name']
                location_page_id = self._find_or_create_location_page(area_name)
                if location_page_id:
                    prop_key = self._get_property_key(self.labels_properties['area'], 'labels')
                    if prop_key:
                        properties[prop_key] = {
                            'relation': [{'id': location_page_id}]
                        }
            
            # Last updated
            if self.labels_properties.get('last_updated'):
                prop_key = self._get_property_key(self.labels_properties['last_updated'], 'labels')
                if prop_key:
                    properties[prop_key] = {'date': {'start': datetime.now().isoformat()}}
            
        except Exception as e:
            logger.error(f"Error formatting label properties: {e}")
        
        return properties
    
    def run_sync(self, database: str = 'all', force_all: bool = False, last_page: bool = False) -> Dict:
        """Run the synchronization process for specified database(s)."""
        logger.info(f"Starting Notion-MusicBrainz synchronization (database: {database})")
        
        if database not in ['artists', 'albums', 'songs', 'labels', 'all']:
            logger.error(f"Invalid database: {database}. Must be 'artists', 'albums', 'songs', 'labels', or 'all'")
            return {'success': False, 'message': f'Invalid database: {database}'}
        
        start_time = time.time()
        results = {
            'success': True,
            'total_pages': 0,
            'successful_updates': 0,
            'failed_updates': 0,
            'skipped_updates': 0,
            'duration': 0
        }
        
        databases_to_sync = []
        if database == 'all':
            databases_to_sync = ['artists', 'albums', 'songs', 'labels']
        else:
            databases_to_sync = [database]
        
        for db_name in databases_to_sync:
            if db_name == 'artists' and not self.artists_db_id:
                logger.warning("Artists database ID not configured, skipping")
                continue
            elif db_name == 'albums' and not self.albums_db_id:
                logger.warning("Albums database ID not configured, skipping")
                continue
            elif db_name == 'songs' and not self.songs_db_id:
                logger.warning("Songs database ID not configured, skipping")
                continue
            elif db_name == 'labels' and not self.labels_db_id:
                logger.warning("Labels database ID not configured, skipping")
                continue
            
            logger.info(f"Syncing {db_name} database...")
            
            # Initialize location cache if needed (for artists and labels)
            if db_name in ['artists', 'labels'] and self.locations_db_id:
                self._load_locations_cache()
            
            db_id = getattr(self, f'{db_name}_db_id')
            pages = self.notion.query_database(db_id)
            
            if not pages:
                logger.warning(f"No pages found in {db_name} database")
                continue
            
            # Handle last-page mode
            if last_page:
                logger.info(f"Last-page mode: Processing only the most recently edited page in {db_name}")
                pages.sort(key=lambda page: page.get('last_edited_time', ''), reverse=True)
                pages = pages[:1]
            
            logger.info(f"Found {len(pages)} pages to process in {db_name}")
            results['total_pages'] += len(pages)
            
            successful = 0
            failed = 0
            skipped = 0
            
            # Process pages (single-threaded due to rate limiting)
            for i, page in enumerate(pages, 1):
                try:
                    if db_name == 'artists':
                        result = self.sync_artist_page(page, force_all)
                    elif db_name == 'albums':
                        result = self.sync_album_page(page, force_all)
                    elif db_name == 'songs':
                        result = self.sync_song_page(page, force_all)
                    elif db_name == 'labels':
                        result = self.sync_label_page(page, force_all)
                    else:
                        result = None
                    
                    if result is True:
                        successful += 1
                    elif result is False:
                        failed += 1
                    else:
                        skipped += 1
                    
                    logger.info(f"Completed {db_name} page {i}/{len(pages)}")
                    
                except Exception as e:
                    logger.error(f"Error processing {db_name} page {page.get('id')}: {e}")
                    failed += 1
            
            results['successful_updates'] += successful
            results['failed_updates'] += failed
            results['skipped_updates'] += skipped
        
        end_time = time.time()
        results['duration'] = end_time - start_time
        
        logger.info(f"Sync completed in {results['duration']:.2f} seconds")
        logger.info(f"Successful updates: {results['successful_updates']}")
        logger.info(f"Failed updates: {results['failed_updates']}")
        if results['skipped_updates'] > 0:
            logger.info(f"Skipped updates: {results['skipped_updates']}")
        
        return results


def validate_environment():
    """Validate environment variables and configuration."""
    errors = []
    
    notion_token = os.getenv('NOTION_TOKEN')
    musicbrainz_user_agent = os.getenv('MUSICBRAINZ_USER_AGENT')
    artists_db_id = os.getenv('NOTION_ARTISTS_DATABASE_ID')
    albums_db_id = os.getenv('NOTION_ALBUMS_DATABASE_ID')
    songs_db_id = os.getenv('NOTION_SONGS_DATABASE_ID')
    
    if not notion_token:
        errors.append("NOTION_TOKEN: Your Notion integration token")
    if not musicbrainz_user_agent:
        errors.append("MUSICBRAINZ_USER_AGENT: Your app name and contact email (e.g., 'MyApp/1.0 (email@example.com)')")
    
    labels_db_id = os.getenv('NOTION_LABELS_DATABASE_ID')
    
    if not artists_db_id and not albums_db_id and not songs_db_id and not labels_db_id:
        errors.append("At least one database ID must be configured (NOTION_ARTISTS_DATABASE_ID, NOTION_ALBUMS_DATABASE_ID, NOTION_SONGS_DATABASE_ID, or NOTION_LABELS_DATABASE_ID)")
    
    if errors:
        logger.error("Missing required environment variables:")
        for error in errors:
            logger.error(f"  - {error}")
        logger.error("\nPlease check your .env file or environment variables.")
        return False
    
    if not notion_token.startswith('secret_'):
        logger.warning("NOTION_TOKEN should start with 'secret_'")
    
    return True


def main():
    """Main function to run the sync script."""
    try:
        parser = argparse.ArgumentParser(description='Synchronize Notion databases with MusicBrainz data')
        parser.add_argument('--database', type=str, choices=['artists', 'albums', 'songs', 'labels', 'all'],
                           default='all', help='Database to sync (default: all)')
        parser.add_argument('--force-all', action='store_true',
                           help='Force update all pages, even if they already have an MBID (default: only update pages without MBIDs)')
        parser.add_argument('--last-page', action='store_true',
                           help='Sync only the most recently edited page')
        args = parser.parse_args()
        
        logger.info("Starting Notion MusicBrainz Sync")
        
        if not validate_environment():
            sys.exit(1)
        
        notion_token = os.getenv('NOTION_TOKEN')
        musicbrainz_user_agent = os.getenv('MUSICBRAINZ_USER_AGENT')
        artists_db_id = os.getenv('NOTION_ARTISTS_DATABASE_ID')
        albums_db_id = os.getenv('NOTION_ALBUMS_DATABASE_ID')
        songs_db_id = os.getenv('NOTION_SONGS_DATABASE_ID')
        labels_db_id = os.getenv('NOTION_LABELS_DATABASE_ID')
        
        sync = NotionMusicBrainzSync(
            notion_token,
            musicbrainz_user_agent,
            artists_db_id,
            albums_db_id,
            songs_db_id,
            labels_db_id
        )
        
        result = sync.run_sync(
            database=args.database,
            force_all=args.force_all,
            last_page=args.last_page
        )
        
        if result['success']:
            logger.info("Synchronization completed successfully")
            logger.info(f"Updated: {result['successful_updates']} pages")
            logger.info(f"Failed: {result['failed_updates']} pages")
            if args.last_page:
                logger.info("Last page mode completed - only the most recently edited page was processed")
            sys.exit(0)
        else:
            logger.error("Synchronization failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Synchronization interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()

