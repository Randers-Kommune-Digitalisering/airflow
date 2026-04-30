def test():
    from pathlib import Path
    from utils.kombit import TempClientCert

    DAG_DIR = Path(__file__).parent
    certs_dir = DAG_DIR / "certs"
    certs_dir.mkdir(parents=True, exist_ok=True)
    paths = [
        certs_dir / "ADG_PROD_Adgangsstyring_2.cer",
        certs_dir / "YDI_PROD_Ydelsesindeks_2.cer"
    ]

    for fp in paths:
        if fp.exists():
            print(f"Found certificate file: {fp}")
        else:
            print(f"!!! Certificate file not found: {fp}")

    with TempClientCert() as client_cert_path:
        print(f"Client certificate .p12 file created at: {client_cert_path}")
        print("Importing ...")
        from kombit_client.integrations.sf1491 import YdelseListeHentClient
        print("Initializing ...")
        service = YdelseListeHentClient(
            # cvr="29189668",
            # sts_certificate_file_path=str(paths[0]),
            # service_certificate_file_path=str(paths[1]),
            client_certificate_file_path=client_cert_path
        )
        print("Calling service ...")
        response = service.effektuering_hent(cpr="1234567890")
        print(response)


if __name__ == "__main__":
    test()
