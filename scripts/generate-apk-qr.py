#!/usr/bin/env python3
"""
Generate a QR code image that encodes a download URL for your Android APK.

Prerequisites:
  pip install -r scripts/requirements-qr.txt

Example:
  python scripts/generate-apk-qr.py ^
    --url "https://downloads.example.com/app-evolvesa-release.apk" ^
    --out EvolveSA-install-qr.png

Hosting notes:
  - Use HTTPS. Serve the .apk with a stable URL (e.g. R2/S3, or a file on your site).
  - Prefer short links for smaller, easier-to-scan QR codes (optional URL shortener).
  - For Play Store instead of APK, pass the Play link; the QR is the same idea.
"""
from __future__ import annotations

import argparse
import sys


def main() -> int:
    p = argparse.ArgumentParser(description="Generate QR PNG for an APK (or any) download URL.")
    p.add_argument("--url", required=True, help="HTTPS URL that opens/downloads the APK when scanned.")
    p.add_argument(
        "--out",
        default="apk-install-qr.png",
        help="Output PNG path (default: apk-install-qr.png).",
    )
    p.add_argument(
        "--box-size",
        type=int,
        default=12,
        help="Pixels per QR module (default: 12). Larger = easier to scan, bigger image.",
    )
    p.add_argument(
        "--border",
        type=int,
        default=2,
        help="Quiet zone in modules (default: 2; spec recommends >=4 for difficult cameras).",
    )
    args = p.parse_args()

    url = args.url.strip()
    if not url.lower().startswith("https://"):
        print("Warning: use HTTPS for mobile browsers; URL does not start with https://", file=sys.stderr)

    try:
        import qrcode
    except ImportError:
        print("Missing dependency. Run: pip install -r scripts/requirements-qr.txt", file=sys.stderr)
        return 1

    border = max(4, args.border)  # improve scan reliability on phone cameras
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=args.box_size,
        border=border,
    )
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    img.save(args.out)
    print(f"Wrote {args.out} ({img.size[0]}x{img.size[1]} px) for URL:\n  {url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
