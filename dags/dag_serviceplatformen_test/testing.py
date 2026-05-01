def test():
    from utils.kombit import TempClientCert
    from kombit_client.integrations.sf1491 import YdelseListeHentClient
    with TempClientCert() as client_cert_path:
        print(f"Client certificate .p12 file created at: {client_cert_path}")
        print("Importing ...")
        print("Initializing ...")
        service = YdelseListeHentClient(
            client_certificate_file_path=client_cert_path
        )
        print("Calling service ...")
        response = service.effektuering_hent(cpr="1234567890")
        print(response)


if __name__ == "__main__":
    test()
