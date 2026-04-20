# Play Store Data Safety Worksheet

Use this worksheet to complete the Play Console Data Safety form accurately.

## Data Types Potentially Processed
- Personal info:
  - Email address (account auth)
  - Name fields from scanned documents (when user scans)
- Sensitive personal info:
  - Government ID numbers and licence numbers (if scanned)
- Financial info:
  - Not collected directly by default
- Photos/media/files:
  - Embedded licence photo (if present in barcode payload)
- App activity/diagnostics:
  - Crash/error telemetry

## Collection and Sharing Matrix
- Collected:
  - Yes, when user signs in/scans data
- Shared:
  - Yes, with configured business systems (CRM/DMS) and service providers
- Required vs optional:
  - Authentication required for normal operation
  - Some fields optional by workflow

## Data Handling Notes
- Transport:
  - Release build should use HTTPS endpoints only.
- Storage:
  - Auth/connector backend stores operational data; connector now has persistent command store.
- Retention:
  - Define retention policy in backend ops docs and privacy policy.

## User Controls
- Account/data deletion request channel defined in privacy policy.
- Logout supported in app.

## Actions Before Submission
- Verify production endpoints are HTTPS.
- Confirm privacy policy URL is public and final.
- Validate form answers against actual runtime behavior and logs.
- Re-check after each major feature change.
