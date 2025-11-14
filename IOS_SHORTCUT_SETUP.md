# iOS Shortcut Setup for GitHub Actions

This guide will help you set up an iOS Shortcut to trigger the Notion MusicBrainz Sync workflow.

## Prerequisites

1. **GitHub Personal Access Token** with `repo` scope
   - Go to: https://github.com/settings/tokens/new
   - Name: "iOS Shortcut - Notion Music Sync"
   - Select scope: `repo` (full control of private repositories)
   - Generate and copy the token

2. **GitHub Repository** (already set up ✅)

## Setting Up GitHub Secrets

Before using the shortcut, you need to add your secrets to GitHub:

1. Go to: https://github.com/wobafett/notion-music-sync/settings/secrets/actions
2. Click "New repository secret" for each of these:

   - `NOTION_TOKEN` - Your Notion integration token
   - `NOTION_ARTISTS_DATABASE_ID` - Your Artists database ID
   - `NOTION_ALBUMS_DATABASE_ID` - Your Albums database ID
   - `NOTION_SONGS_DATABASE_ID` - Your Songs database ID
   - `NOTION_LABELS_DATABASE_ID` - Your Labels database ID
   - `NOTION_LOCATIONS_DATABASE_ID` - Your Locations database ID
   - `MUSICBRAINZ_USER_AGENT` - Your MusicBrainz user agent (e.g., "NotionMusicSync/1.0 (your-email@example.com)")
   - `SPOTIFY_CLIENT_ID` - Your Spotify client ID
   - `SPOTIFY_CLIENT_SECRET` - Your Spotify client secret

## Creating the iOS Shortcut

### Option 1: Simple Shortcut (Last Page Sync)

This shortcut syncs the most recently edited page:

1. Open **Shortcuts** app on iOS
2. Tap **+** to create a new shortcut
3. Add these actions:

   **Action 1: Get Contents of URL**
   - URL: `https://api.github.com/repos/wobafett/notion-music-sync/dispatches`
   - Method: `POST`
   - Headers:
     - `Accept`: `application/vnd.github+json`
     - `Authorization`: `Bearer YOUR_GITHUB_TOKEN`
     - `X-GitHub-Api-Version`: `2022-11-28`
     - `Content-Type`: `application/json`
   - Request Body: `{"event_type": "sync", "client_payload": {"database": "artists", "last_page": true}}`

4. **Action 2: Show Notification** (optional)
   - Title: "Sync Started"
   - Body: "Syncing latest artist page..."

5. Name your shortcut: "Sync Latest Artist"

### Option 2: Advanced Shortcut (With Menu)

This shortcut lets you choose what to sync:

1. Open **Shortcuts** app on iOS
2. Tap **+** to create a new shortcut
3. Add these actions:

   **Action 1: Choose from Menu**
   - Options:
     - "Sync Latest Artist"
     - "Sync Latest Album"
     - "Sync Latest Song"
     - "Sync Latest Label"
     - "Sync All Artists"
     - "Sync All Albums"
     - "Sync All Songs"
     - "Sync All Labels"
     - "Sync Everything"

   **Action 2: If** (for each menu option)
   - Condition: Menu Result equals "Sync Latest Artist"
   - Then: Set variable `database` = "artists", `last_page` = true

   **Action 3: Get Contents of URL**
   - URL: `https://api.github.com/repos/wobafett/notion-music-sync/dispatches`
   - Method: `POST`
   - Headers:
     - `Accept`: `application/vnd.github+json`
     - `Authorization`: `Bearer YOUR_GITHUB_TOKEN`
     - `X-GitHub-Api-Version`: `2022-11-28`
     - `Content-Type`: `application/json`
   - Request Body: `{"event_type": "sync", "client_payload": {"database": "[database variable]", "last_page": "[last_page variable]"}}`

### Option 3: Quick Actions (Recommended)

Create separate shortcuts for common actions:

#### Shortcut 1: "Sync Latest Artist"
```json
{
  "event_type": "sync",
  "client_payload": {
    "database": "artists",
    "last_page": true
  }
}
```

#### Shortcut 2: "Sync Latest Album"
```json
{
  "event_type": "sync",
  "client_payload": {
    "database": "albums",
    "last_page": true
  }
}
```

#### Shortcut 3: "Sync Latest Song"
```json
{
  "event_type": "sync",
  "client_payload": {
    "database": "songs",
    "last_page": true
  }
}
```

## Request Body Examples

### Sync Latest Artist
```json
{
  "event_type": "sync",
  "client_payload": {
    "database": "artists",
    "last_page": true
  }
}
```

### Sync All Albums
```json
{
  "event_type": "sync",
  "client_payload": {
    "database": "albums",
    "force_all": false
  }
}
```

### Force Sync All Songs
```json
{
  "event_type": "sync",
  "client_payload": {
    "database": "songs",
    "force_all": true
  }
}
```

### Sync Everything
```json
{
  "event_type": "sync",
  "client_payload": {
    "database": "all"
  }
}
```

## Testing

1. Run your shortcut
2. Check GitHub Actions: https://github.com/wobafett/notion-music-sync/actions
3. You should see a workflow run start within a few seconds

## Troubleshooting

### "Resource not accessible by integration"
- Make sure your GitHub token has `repo` scope
- Make sure the repository is accessible (not private if using a free token)

### "Workflow not found"
- Make sure the workflow file is committed to the `main` branch
- Check that the workflow file is in `.github/workflows/sync.yml`

### "Secret not found"
- Make sure all required secrets are added in GitHub repository settings
- Check secret names match exactly (case-sensitive)

## Security Note

⚠️ **Important**: Your GitHub token is stored in the Shortcut. Consider:
- Using a token with minimal permissions
- Regularly rotating your token
- Not sharing your shortcut with others

## Alternative: Using Workflow Dispatch (Manual Trigger)

You can also trigger workflows manually from GitHub:
1. Go to: https://github.com/wobafett/notion-music-sync/actions
2. Click "Notion MusicBrainz Sync"
3. Click "Run workflow"
4. Select your options and run

