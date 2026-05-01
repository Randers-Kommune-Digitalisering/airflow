def test():
    from utils.kombit import TempClientCert

    import os

    cert_dirs = [
        "/opt/airflow/dags/certs",
        "/opt/airflow/certs",
        "/opt/airflow/dags/dag_serviceplatformen_test/certs",
        "/tmp/certs"
    ]
    found = False
    for cert_dir in cert_dirs:
        print(f"[CHECK] Listing files in: {cert_dir}")
        if os.path.isdir(cert_dir):
            found = True
            files = os.listdir(cert_dir)
            for f in files:
                print(f"  - {f}")
        else:
            print("(directory does not exist)")
    if not found:
        print("[CHECK] No certs directory found in common locations.\n"
              "Try placing your certs in one of these paths or check your DAGs mount configuration.")

    with TempClientCert() as client_cert_path:
        print(f"Client certificate .p12 file created at: {client_cert_path}")
        print("Importing ...")
        from kombit_client.integrations.sf1491 import YdelseListeHentClient
        print("Initializing ...")
        service = YdelseListeHentClient(
            # cvr="29189668",
            # sts_certificate_file_path=cert_paths[0],
            # service_certificate_file_path=cert_paths[1],
            client_certificate_file_path=client_cert_path
        )
        print("Calling service ...")
        response = service.effektuering_hent(cpr="1234567890")
        print(response)


if __name__ == "__main__":
    test()
