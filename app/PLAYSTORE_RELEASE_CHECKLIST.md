# Play Store Release Checklist

## 1) Release Signing
- Copy `keystore.properties.example` to `../keystore.properties` (repo root).
- Fill real keystore values and ensure the keystore file exists.
- Keep `keystore.properties` and `.jks` out of source control.

## 2) Build Configuration
- Confirm release build blocks cleartext traffic.
- Confirm app version is updated in `app/build.gradle.kts`:
  - `versionCode` incremented
  - `versionName` updated
- Verify app starts and login/scan flow works on a physical device.

## 3) Backend Readiness
- `connector` running with persistent store and retry settings.
- `cubeone-auth` reachable from device network.
- Health checks pass:
  - `/healthz`
  - `/readyz`
- Production secrets configured in environment (no defaults).

## 4) Security and Privacy
- Use HTTPS endpoints for release app configuration.
- Remove test keys and non-production endpoints.
- Prepare Privacy Policy URL and support contact email.
- Validate Data Safety answers against actual collected/stored data.

## 5) Quality Gate
- Run smoke tests: login, register, scan DL, scan vehicle, submit lead.
- Validate error handling for backend offline scenarios.
- Check Crashlytics receives non-fatal and fatal events.
- Confirm no debug-only UI/messages in release build.

## 6) Build and Upload
- Build app bundle:
  - `./gradlew :app:bundleRelease` (or `gradlew.bat` on Windows)
- Upload `app-release.aab` to Play Console Internal Testing.
- Add release notes and start staged rollout.

## 7) Post-Release Monitoring
- Watch crash-free users, ANR rate, auth error rate, command failure rate.
- Review dead-letter queue in connector and replay/fix failures.
- Track lead creation success in EvolveSA adapter logs.
