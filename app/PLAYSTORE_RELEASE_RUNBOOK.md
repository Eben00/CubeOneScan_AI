# Play Store Release Runbook

## Goal
Ship a safe release of CubeOneScan as an Android App Bundle (`.aab`) using staged rollout.

## Prerequisites
- `keystore.properties` exists at repo root and points to a valid release keystore.
- Release backend endpoints are HTTPS and production-ready.
- Connector/auth health checks pass in production environment.

## 1) Versioning
- Update `versionCode` and `versionName` in `app/build.gradle.kts`.
- Keep `versionCode` strictly increasing for every upload.

## 2) Build Release Bundle
- Windows:
  - `gradlew.bat :app:bundleRelease`
- Output path:
  - `app/build/outputs/bundle/release/app-release.aab`

## 3) Pre-Upload Smoke Checks
- Install release APK/AAB equivalent in internal test track.
- Verify:
  - login/register
  - scanner open/close and permissions
  - driver licence decode
  - vehicle decode
  - lead/command submission to backend
- Confirm no debug endpoints or test keys remain.

## 4) Play Console Upload
- Go to Play Console -> Internal testing.
- Create new release and upload `app-release.aab`.
- Add concise release notes (user-visible changes only).
- Roll out to internal testers first.

## 5) Promotion Strategy
- Internal testing -> Closed testing -> Production.
- Production staged rollout:
  - 5% (monitor 24h)
  - 25% (monitor 24-48h)
  - 100%

## 6) Monitoring and Rollback
- Watch:
  - Crash-free sessions/users
  - ANR rate
  - Auth failure rate
  - Command failure/dead-letter rate on connector
- If regression appears:
  - halt rollout in Play Console
  - fix and publish a higher `versionCode` hotfix

## 7) Release Artifacts to Keep
- Signed `.aab`
- Mapping file (if minify enabled later)
- Release notes
- Checklist sign-off (QA + backend + product)
