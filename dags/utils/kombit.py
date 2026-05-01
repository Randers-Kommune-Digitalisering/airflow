import tempfile
import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.backends import default_backend
from cryptography import x509


class TempClientCert:
    def __init__(self):
        self.cert_path = None

    def __enter__(self):
        private_pem = os.environ.get("CLIENT_CERT_PRIVATE_KEY")
        public_pem = os.environ.get("CLIENT_CERT_PUBLIC_KEY")
        if not private_pem or not public_pem:
            raise ValueError("Both CLIENT_CERT_PRIVATE_KEY and CLIENT_CERT_PUBLIC_KEY environment variables must be set.")

        private_key = serialization.load_pem_private_key(
            private_pem.encode("utf-8"),
            password=None,
            backend=default_backend()
        )

        cert = x509.load_pem_x509_certificate(
            public_pem.encode("utf-8"),
            backend=default_backend()
        )

        p12_bytes = pkcs12.serialize_key_and_certificates(
            name=b"client-cert",
            key=private_key,
            cert=cert,
            cas=[],
            encryption_algorithm=serialization.NoEncryption()
        )
        with tempfile.NamedTemporaryFile("wb", delete=False) as p12_file:
            p12_file.write(p12_bytes)
            p12_file.flush()
            self.cert_path = p12_file.name
        return self.cert_path

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cert_path and os.path.exists(self.cert_path):
            try:
                os.unlink(self.cert_path)
            except Exception:
                pass
