# from utils.api_requests import APIClient
# from utils.config import CPR_SERVICE_URL, CPR_CLIENT_ID, CPR_CLIENT_SECRET


# class CPRClient(APIClient):
#     def __init__(self):
#         super().__init__(base_url=CPR_SERVICE_URL, client_id=CPR_CLIENT_ID, client_secret=CPR_CLIENT_SECRET, realm='randers-kommune')

#     def lookup_address(self, cpr_number):
#         endpoint = f'/PersonBaseDataExtendedService/lookup/address/{cpr_number}'
#         return self.make_request(method='GET', path=endpoint)
