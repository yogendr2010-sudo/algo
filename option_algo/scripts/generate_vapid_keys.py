# scripts/generate_vapid_keys.py
# ================================================================
# Generates a VAPID key pair for Web Push notifications.
#
# Run ONCE per deployment:
#   python scripts/generate_vapid_keys.py
#
# Copy the printed VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY into your
# .env file. The public key is also sent to browsers (it's safe to
# expose); the private key must stay secret on the server.
# ================================================================

from py_vapid import Vapid01


def main():
    vapid = Vapid01()
    vapid.generate_keys()

    # py_vapid stores keys as cryptography EC key objects — export
    # to the raw base64url format browsers/pywebpush expect.
    from cryptography.hazmat.primitives import serialization
    import base64

    private_raw = vapid.private_key.private_numbers().private_value.to_bytes(32, "big")
    private_b64 = base64.urlsafe_b64encode(private_raw).rstrip(b"=").decode()

    public_numbers = vapid.public_key.public_numbers()
    x = public_numbers.x.to_bytes(32, "big")
    y = public_numbers.y.to_bytes(32, "big")
    public_raw = b"\x04" + x + y   # uncompressed point format
    public_b64 = base64.urlsafe_b64encode(public_raw).rstrip(b"=").decode()

    print("Add these to your .env file:\n")
    print(f"VAPID_PUBLIC_KEY={public_b64}")
    print(f"VAPID_PRIVATE_KEY={private_b64}")
    print(f"VAPID_CLAIM_EMAIL=admin@yourdomain.com")


if __name__ == "__main__":
    main()
