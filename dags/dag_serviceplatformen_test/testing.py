def test():
    client_cert_path = None
    try:
        import os
        from pathlib import Path
        import tempfile
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.serialization import pkcs12
        from cryptography.hazmat.backends import default_backend
        from cryptography import x509

        DAG_DIR = Path(__file__).parent
        certs_dir = DAG_DIR / "certs"
        certs_dir.mkdir(parents=True, exist_ok=True)
        paths = [
            certs_dir / "ADG_PROD_Adgangsstyring_2.cer",
            certs_dir / "YDI_PROD_Ydelsesindeks_2.cer"
        ]

        private_pem = os.environ["CLIENT_CERT_PRIVATE_KEY"]
        public_pem = os.environ["CLIENT_CERT_PUBLIC_KEY"]

        client_cert_path = None
        if private_pem and public_pem:
            # Load private key
            private_key = serialization.load_pem_private_key(
                private_pem.encode("utf-8"),
                password=None,
                backend=default_backend()
            )
            # Load public certificate
            cert = x509.load_pem_x509_certificate(
                public_pem.encode("utf-8"),
                backend=default_backend()
            )
            # No additional cert chain
            additional_certs = []

            # Create PKCS#12
            # pfx_password = None  # No password
            p12_bytes = pkcs12.serialize_key_and_certificates(
                name=b"client-cert",
                key=private_key,
                cert=cert,
                cas=additional_certs,
                encryption_algorithm=serialization.NoEncryption()
            )
            with tempfile.NamedTemporaryFile("wb", delete=False) as p12_file:
                p12_file.write(p12_bytes)
                p12_file.flush()
                client_cert_path = p12_file.name
            print(f"Wrote {client_cert_path} from PEM environment variables using cryptography.")
        else:
            print("CLIENT_CERT_PRIVATE_KEY or CLIENT_CERT_PUBLIC_KEY environment variable not set. Skipping clientCertPROD.p12 write.")

        for fp in paths:
            if fp.exists():
                print(f"Found certificate file: {fp}")
            else:
                print(f"!!! Certificate file not found: {fp}")

        if client_cert_path:
            if Path(client_cert_path).exists():
                print(f"Client certificate .p12 file created at: {client_cert_path}")
            else:
                raise FileNotFoundError(f"Expected client certificate .p12 file not found at: {client_cert_path}")
        else:
            raise FileNotFoundError("Client certificate .p12 file was not created due to missing environment variables.")

        print("Importing ...")
        from kombit_client.integrations.sf1491 import YdelseListeHentClient
        print("Initializing ...")
        service = YdelseListeHentClient(
            cvr="29189668",
            sts_certificate_file_path=str(paths[0]),
            service_certificate_file_path=str(paths[1]),
            client_certificate_file_path=client_cert_path
        )
        print("Calling service ...")
        response = service.effektuering_hent(cpr="1234567890")
        print(response)

    finally:
        if client_cert_path and Path(client_cert_path).exists():
            try:
                os.unlink(client_cert_path)
                print(f"Deleted temporary client certificate: {client_cert_path}")
            except Exception as e:
                print(f"Failed to delete temporary client certificate: {e}")


if __name__ == "__main__":
    test()
