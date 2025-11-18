# How to Get Google OAuth Token for Testing

## Option 1: Google OAuth Playground (Recommended)

1. **Visit**: https://developers.google.com/oauthplayground/

2. **Configure Settings** (Click the gear icon ⚙️ in top right):
   - Check "Use your own OAuth credentials"
   - OAuth Client ID: `1054491250355-4fjsqd2lqv8ddvrctfd360b1m6dr5d5n.apps.googleusercontent.com`
   - OAuth Client secret: (You'll need this from the Google Cloud Console)
   - Click "Close"

3. **Authorize APIs**:
   - In Step 1, select: **Google OAuth2 API v2** → **https://www.googleapis.com/auth/userinfo.email**
   - Click "Authorize APIs"
   - Sign in with your Google account
   - Click "Allow"

4. **Exchange Authorization Code**:
   - In Step 2, click "Exchange authorization code for tokens"

5. **Copy the Token**:
   - Look for `"id_token"` in the response (it's a long string starting with `eyJ...`)
   - Copy this entire token

6. **Use the Token**:
   - Open `test-api.ps1`
   - Replace `YOUR_GOOGLE_OAUTH_TOKEN` with your copied token
   - Run: `.\test-api.ps1`

## Option 2: Use Postman

1. Open Postman
2. Create a new request
3. Go to Authorization tab → Type: OAuth 2.0
4. Get New Access Token with your client ID
5. Copy the `id_token` from the response

## Quick Test Without OAuth Setup

If you just want to test the API flow without OAuth:
- Run: `.\test-api-no-auth.ps1` (simplified version for local testing)

---

**Note**: The token expires after 1 hour. Generate a new one if it expires.
