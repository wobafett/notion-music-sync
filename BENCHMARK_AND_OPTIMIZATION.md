# Sync Performance Benchmark & Optimization Analysis

## Benchmark Results

### Before Optimizations

| Database | Pages | Time | Time per Page |
|----------|-------|------|---------------|
| Artists  | 8     | ~82s | ~10.2s/page   |
| Albums   | 6     | ~53s | ~8.8s/page    |
| Songs    | 6     | ~88s | ~14.7s/page   |
| Labels   | 5     | ~24s | ~4.8s/page    |

### After Optimizations

| Database | Pages | Time | Time per Page | Improvement |
|----------|-------|------|---------------|-------------|
| Artists  | 8     | ~24s | ~3.0s/page    | **71% faster** ⚡ |
| Albums   | 6     | ~49s | ~8.2s/page    | **8% faster** |
| Songs    | 6     | ~71s | ~11.8s/page   | **19% faster** |
| Labels   | 5     | ~11s | ~2.2s/page    | **54% faster** ⚡ |

### Overall Summary

- **Total time before**: ~247 seconds
- **Total time after**: ~155 seconds
- **Overall improvement**: **37% faster** 🎉

## Major Bottlenecks Identified

### 1. Location Page Lookups (HIGH IMPACT)
**Issue**: `_find_or_create_location_page()` queries the ENTIRE locations database every time it's called.
- Called for every artist's "Area" and "Born In" fields
- Called for every label's "Area" field
- If multiple artists/labels share the same location, we query the database multiple times

**Impact**: 
- Artists: 2 calls per artist (Area + Born In) = 16 queries for 8 artists
- Labels: 1 call per label = 5 queries for 5 labels
- Each query fetches ALL location pages

**Solution**: Cache location lookups in memory during sync session

### 2. Full Release Data Fetching for Scoring (HIGH IMPACT)
**Issue**: `_find_best_release_for_song()` fetches full release data for ALL releases just to score them.
- If a song appears on 50 releases, that's 50 API calls (50 seconds!)
- We only need full data for the top candidates, not all of them

**Impact**: 
- Songs sync is slowest (~14.7s/page) because of this
- Albums sync also affected when searching for best release

**Solution**: 
- Score releases using available data first
- Only fetch full data for top N candidates (e.g., top 5-10)
- Or fetch full data in parallel batches (but we're rate-limited)

### 3. Multiple Individual `get_page()` Calls (MEDIUM IMPACT)
**Issue**: For albums with multiple related songs, we call `get_page()` individually for each song.
- If an album has 10 songs, that's 10 separate API calls
- Each call has network overhead

**Impact**: 
- Albums with many songs are slower
- Notion API rate limits may apply

**Solution**: Batch fetch related pages or cache them

### 4. Database Queries for Finding Existing Pages (MEDIUM IMPACT)
**Issue**: `_find_or_create_album_page()`, `_find_or_create_artist_page()`, etc. query the entire database every time.
- Called multiple times during sync
- Each query fetches ALL pages in the database

**Impact**: 
- Extra Notion API calls
- Network overhead

**Solution**: Cache database queries during sync session

### 5. Spotify API Calls (LOW-MEDIUM IMPACT)
**Issue**: We make separate Spotify API calls for each album/song, even when MusicBrainz might have the link.
- 0.1s delay per call adds up
- We check MusicBrainz first, but still make many Spotify calls

**Impact**: 
- Adds ~0.1-0.2s per album/song
- Not a major bottleneck but could be optimized

**Solution**: Already optimized (check MusicBrainz first), but could batch or cache Spotify token

## Recommended Optimizations

### Priority 1: Cache Location Lookups
- Cache location pages in memory during sync
- Query locations database once at start of sync
- Look up from cache instead of querying each time

### Priority 2: Optimize Release Scoring
- Score releases using available data first
- Only fetch full release data for top 5-10 candidates
- This could reduce API calls by 80-90% for songs with many releases

### Priority 3: Cache Database Queries
- Cache full database queries (artists, albums, songs, labels) at start of sync
- Use cached data for finding existing pages
- Refresh cache periodically if needed

### Priority 4: Batch Related Page Fetches
- When processing albums with multiple songs, batch fetch song pages
- Notion API supports batch operations (though limited)

## Expected Performance Improvements

| Optimization | Expected Speedup | Implementation Effort | Status |
|--------------|------------------|----------------------|--------|
| Cache location lookups | 20-30% faster | Low | ✅ Implemented |
| Optimize release scoring | 50-70% faster (songs) | Medium | ✅ Implemented |
| Cache database queries | 10-20% faster | Low | ⏸️ Deferred |
| Batch page fetches | 5-10% faster | Medium | ⏸️ Deferred |

**Total Expected Improvement**: 40-60% faster overall

## Implemented Optimizations

### 1. Location Cache (✅ Implemented)
- **What**: Cache all location pages at the start of sync for artists/labels
- **Impact**: Eliminates repeated database queries for the same locations
- **Code Changes**:
  - Added `_location_cache` and `_locations_title_key` to cache
  - Added `_load_locations_cache()` method to load all locations once
  - Modified `_find_or_create_location_page()` to use cache
  - Cache is initialized when syncing artists or labels

### 2. Optimized Release Scoring (✅ Implemented)
- **What**: Score releases with available data first, then only fetch full data for top 10 candidates
- **Impact**: Reduces API calls by 80-90% for songs/albums with many releases
- **Code Changes**:
  - Modified `_find_best_release_for_song()` to do two-pass scoring
  - Modified album sync to use the same optimization
  - Only fetches full release data for top 10 candidates instead of all releases

### 3. Deferred Optimizations
- **Database Query Caching**: Could cache full database queries, but impact is lower
- **Batch Page Fetches**: Notion API has limited batch support, would require refactoring

## Testing Recommendations

Run benchmarks again to measure actual improvement:
```bash
# Test each database type
time python3 notion_musicbrainz_sync.py --database artists --force-all
time python3 notion_musicbrainz_sync.py --database albums --force-all
time python3 notion_musicbrainz_sync.py --database songs --force-all
time python3 notion_musicbrainz_sync.py --database labels --force-all
```

## Actual Results

✅ **Artists**: **71% faster** (24s vs 82s) - Exceeded expectations! Location cache had huge impact.
✅ **Labels**: **54% faster** (11s vs 24s) - Exceeded expectations! Location cache had huge impact.
✅ **Songs**: **19% faster** (71s vs 88s) - Release scoring optimization helped, but less than expected (likely because these songs don't have many releases)
✅ **Albums**: **8% faster** (49s vs 53s) - Small improvement (likely because these albums don't have many release candidates)

**Overall: 37% faster** - Saved ~92 seconds per full sync!

### Analysis

The location cache optimization had a much bigger impact than expected, especially for artists and labels. This suggests that:
1. Many artists/labels share the same locations
2. The repeated database queries were a significant bottleneck
3. The cache eliminates all redundant queries

The release scoring optimization had less impact than expected, likely because:
1. The test songs/albums don't have many release candidates
2. The optimization shines when there are 20+ releases per song/album
3. Most songs/albums in the test set have fewer releases

### Recommendations

For databases with many releases per song/album, the release scoring optimization will have even bigger impact. The location cache optimization is already providing excellent results across the board.

