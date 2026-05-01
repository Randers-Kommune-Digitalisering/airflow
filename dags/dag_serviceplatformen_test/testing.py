def test():
    from utils.kombit import TempClientCert

    import os

    # List current working directory
    cwd = os.getcwd()
    print(f"[CHECK] Current working directory: {cwd}")
    try:
        files = os.listdir(cwd)
        print("[CHECK] Files in current directory:")
        for f in files:
            print(f"  - {f}")
    except Exception as e:
        print(f"[CHECK] Could not list files in current directory: {e}")

    # Go to parent and list directories
    parent = os.path.dirname(cwd)
    print(f"[CHECK] Parent directory: {parent}")
    try:
        entries = os.listdir(parent)
        print("[CHECK] Directories in parent directory:")
        for entry in entries:
            full_path = os.path.join(parent, entry)
            if os.path.isdir(full_path):
                print(f"  - {entry}/")
    except Exception as e:
        print(f"[CHECK] Could not list parent directory: {e}")

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
