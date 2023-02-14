import json
import pandas as pd

class WatsonDataAPI:

    def __init__(self):
        print("WatsonDataAPI init() call")
        self.cpd_cluster_host = 'https://zen-cpd-zen.apps.idp.lghnh.com'
        self.project = '12345'#Project(project_id=os.environ['PROJECT_ID'])    

    def get_cpd_cluster_host(self):
        return self.cpd_cluster_host

    def get_project(self):
        return self.project



class MapTerm_JSON(WatsonDataAPI):
    def __init__(self, info_json, max_retries_api_call=30, timeout=0.1, max_retries_map=5):
        super().__init__()

        def get_token(info_json):
            # load_file(info_json)
            f = open(info_json)
            info = json.load(f)

            headers = {
                'cache-control': 'no-cache',
                'content-type': 'application/json'
            }

            payload = json.dumps({"username":info["username"], "password":info["password"]})
            try:
                # authresponse = self.s.post(f'{self.cpd_cluster_host}/icp4d-api/v1/authorize', headers=headers, data=payload, verify=False)
                # token=json.loads(authresponse.text)['token']
                # return token

                print(payload)
            except:
                print("Fail to get token")

        self.timeout = timeout
        self.max_retries_map = max_retries_map

        print("1) timeout: ", self.timeout)
        print("2) max_retries_map: ", max_retries_map)
        print("3) max_retries_api_call: ", max_retries_api_call)

        print(super().get_cpd_cluster_host())

        # session = requests.session()
        # session.mount('https://',HTTPAdapter(max_retries=max_retries_api_call))

        # self.s = session
        self.token = get_token(info_json)   


from getpass import getpass

def user_INPUT():
    username = input("1) ID를 입력하세요:")
    password = getpass("2) 비밀번호: ")

    return username, password


class MapTerm_INPUT(WatsonDataAPI):
    def __init__(self, max_retries_api_call=30, timeout=0.1, max_retries_map=5):
        super().__init__()

        def get_token():
            username, password = user_INPUT()

            headers = {
                'cache-control': 'no-cache',
                'content-type': 'application/json'
            }

            payload = json.dumps({"username":username, "password":password})
            try:
                # authresponse = self.s.post(f'{self.cpd_cluster_host}/icp4d-api/v1/authorize', headers=headers, data=payload, verify=False)
                # token=json.loads(authresponse.text)['token']
                # return token

                print(payload)
            except:
                print("Fail to get token")

        self.timeout = timeout
        self.max_retries_map = max_retries_map

        print("1) timeout: ", self.timeout)
        print("2) max_retries_map: ", max_retries_map)
        print("3) max_retries_api_call: ", max_retries_api_call)

        print(super().get_cpd_cluster_host())

        # session = requests.session()
        # session.mount('https://',HTTPAdapter(max_retries=max_retries_api_call))

        # self.s = session
        self.token = get_token()   


class MapTerm_JOB(WatsonDataAPI):
    def __init__(self, USERNAME, PASSWORD, FILENAME, max_retries_api_call=30, timeout=0.1, max_retries_map=5):
        super().__init__()

        def get_token():
            headers = {
                'cache-control': 'no-cache',
                'content-type': 'application/json'
            }

            payload = json.dumps({"username":USERNAME, "password":PASSWORD})
            try:
                # authresponse = self.s.post(f'{self.cpd_cluster_host}/icp4d-api/v1/authorize', headers=headers, data=payload, verify=False)
                # token=json.loads(authresponse.text)['token']
                # return token

                print(payload)
            except:
                print("Fail to get token")

        self.timeout = timeout
        self.max_retries_map = max_retries_map

        print("1) timeout: ", self.timeout)
        print("2) max_retries_map: ", max_retries_map)
        print("3) max_retries_api_call: ", max_retries_api_call)

        print(super().get_cpd_cluster_host())

        # session = requests.session()
        # session.mount('https://',HTTPAdapter(max_retries=max_retries_api_call))

        # self.s = session
        self.token = get_token()

        df = pd.read_csv(FILENAME)
        print(df.shape)
