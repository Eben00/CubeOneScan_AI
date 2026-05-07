# Pilot Tracking Plan (Day 1)

This is a minimal, low-friction tracking setup for pilot usage after sharing the QR code.

## Goal

Track this funnel daily:

1. QR opened
2. App login success
3. Consent link sent
4. Consent approved
5. Score shown in app

## What You Already Have

- Backend audit logging in `cubeone-auth/server.js` via `appendAudit(...)`.
- Login audit events already captured (for example `login_success`).
- Admin audit endpoint available at `/api/v1/admin/audit-events`.
- Firebase plugins already configured in app build (`google-services`, Crashlytics), but no explicit app analytics events yet.

## Day-1 Setup (No Big Refactor)

### 1) Track QR opens

- Create a short redirect link (Bitly/Rebrandly) that points to the direct arm64 APK URL.
- Generate the QR from that short link (not from the raw APK URL).
- Keep one stable short link for pilot.

Use this metric daily:

- `qr_clicks_total`

### 2) Track backend login + user activity from audit log

Use existing audit events in auth service:

- `login_success`
- `login_failed`

Daily metrics:

- `unique_active_users` (distinct users with `login_success`)
- `logins_total`
- `failed_logins_total`

### 3) Add 4 app events (smallest useful set)

Add an app analytics helper and emit only these events:

- `app_open`
- `consent_send_tapped`
- `consent_status_approved`
- `credit_score_rendered`

This gives you install/use confidence beyond QR clicks.

## Exact Instrumentation Points

Add events in these files:

- `app/src/main/java/com/cubeone/scan/ui/LoginActivity.kt`
  - On successful login (`AuthResult.Success`) -> log `app_open` once user reaches app.
- `app/src/main/java/com/cubeone/scan/ui/LicenseResultActivity.kt`
  - When user taps send approval email -> log `consent_send_tapped`.
  - In `tryUpdateConsentStatus(...)` when status becomes `approved` -> log `consent_status_approved`.
  - In `renderCreditScoreUi(score, band)` when score is non-null -> log `credit_score_rendered`.

Recommended event params (keep tiny):

- `dealer_id`
- `user_id`
- `lead_correlation_id` (where available)
- `brand` (`cubeone` / `evolvesa`)

## Reporting Cadence (Simple)

Share one pilot update per day with:

- QR clicks
- Unique active users
- Consent sends
- Consent approvals
- Scores rendered
- Top blocker notes (2-3 bullets)

## Optional Next Step (After Day 1)

Add a tiny `/api/v1/admin/pilot-metrics` endpoint in `cubeone-auth/server.js` that aggregates audit + app event counts for a selected date range. This removes manual counting.
