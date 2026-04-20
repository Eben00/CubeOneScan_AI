# CubeOneScan Connector (v1)

Minimal backend for CubeOneScan mobile app.

## Endpoints

### Health check
- `GET /healthz`

### Standard commands API
- `POST /api/v1/commands`
  - Header: `Authorization: Bearer <API_KEY>`
  - JSON body:
    - `commandType` (string): e.g. `CREATE_LEAD`, `CREATE_STOCK_UNIT`, `SEND_STOCK_TO_LEAD`, `TRADE_IN`, `STOCK_TAKE`, `MARKET_VALUE_REPORT`
    - `correlationId` (optional string)
    - `payload` (object): normalized scan data
    - `meta` (optional object)
  - Returns:
    - Most commands: `{ correlationId, status, commandType }` with HTTP **202** (processed in background; poll `GET` for result).
    - **`MARKET_VALUE_REPORT` only:** HTTP **200** with `{ correlationId, status: "done", commandType, result }` (immediate response for faster valuation in the app).

- `GET /api/v1/commands/:correlationId`
  - Header: `Authorization: Bearer <API_KEY>`
  - Returns command status + result (in-memory for now)

### Trade-in capture payload (required for `TRADE_IN`)
- `payload.photos`: object with `front`, `left`, `right`, `back` photo URLs
- `payload.damageWireframe.markers`: array of wireframe marker points
- `payload.damages`: array of items with `zone`, `description`, `reconCost`

### Webhooks (placeholder)
- `POST /api/v1/webhooks/:type`
  - Header: `Authorization: Bearer <API_KEY>`

## Local run

```powershell
cd C:\AI\connector
npm install
```

1. **Create environment file** (copy the example, then edit secrets):

```powershell
copy .env.example .env
notepad .env
```

2. **Minimum variables** in `.env`:

| Variable | Purpose |
|----------|---------|
| `PORT` | Listen port (default `8080`) |
| `API_KEY` | Secret the mobile app sends as `Authorization: Bearer <API_KEY>` |
| `TRUTRADE_API_KEY` | Valuation provider API key |
| `TRUTRADE_API_SECRET` | Valuation provider API secret |
| `TRUTRADE_BASE_URL` | Optional; defaults to `https://api.yourvehiclevalue.co.za/api` |

Optional (WhatsApp via Twilio): `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_FROM`.

3. **Start the server:**

```powershell
npm start
```

You should see: `CubeOneScan connector listening on port 8080`.

4. **Smoke test** (PowerShell):

```powershell
Invoke-RestMethod -Uri "http://localhost:8080/healthz" -Method GET
```

5. **Expose with Cloudflare Tunnel** (optional, for phone on another network):

- In one window: `npm start` (connector on `localhost:8080`).
- In another: `cloudflared tunnel --url http://localhost:8080`
- Use the printed `https://....trycloudflare.com` URL as **Base URL** in the CubeOneScan app **Settings** (same `API_KEY` as in `.env`).

6. **Android app** (CubeOneScan → Settings):

- **Base URL**: `http://10.0.2.2:8080` if using emulator; or your PC LAN IP / Cloudflare URL for a real device.
- **API Key**: must match `API_KEY` in `.env` exactly.

## Example command

```bash
curl -X POST "http://localhost:8080/api/v1/commands" ^
  -H "Authorization: Bearer change-me" ^
  -H "Content-Type: application/json" ^
  -d "{\"commandType\":\"CREATE_LEAD\",\"correlationId\":\"scan_123\",\"payload\":{\"driverLicense\":{\"idNumber\":\"7902...\"}}}"
```

