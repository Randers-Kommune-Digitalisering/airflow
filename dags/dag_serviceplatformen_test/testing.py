def test():
    client_cert_path = None
    try:
        import os
        from pathlib import Path
        import tempfile
        import subprocess

        print(os.environ["TEST_ENV_VAR"])
        return

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
            with tempfile.NamedTemporaryFile("w", delete=False) as pub_file, tempfile.NamedTemporaryFile("w", delete=False) as priv_file, tempfile.NamedTemporaryFile("wb", delete=False) as p12_file:
                pub_file.write(public_pem)
                pub_file.flush()
                priv_file.write(private_pem)
                priv_file.flush()
                pub_path = pub_file.name
                priv_path = priv_file.name
                client_cert_path = p12_file.name

            # Use openssl to create .p12 file
            pfx_password = ""  # No password
            cmd = [
                "openssl", "pkcs12", "-export",
                "-out", client_cert_path,
                "-inkey", priv_path,
                "-in", pub_path,
                "-passout", f"pass:{pfx_password}"
            ]
            try:
                subprocess.check_call(cmd)
                print(f"Wrote {client_cert_path} from PEM environment variables using openssl.")
            except Exception as e:
                print(f"Failed to create .p12 file: {e}")
                client_cert_path = None
            finally:
                os.unlink(pub_path)
                os.unlink(priv_path)
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
