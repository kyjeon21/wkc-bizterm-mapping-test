import warnings
import pandas as pd
from pandas.core.common import SettingWithCopyWarning

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

from ibm_watson_studio_lib import access_project_or_space
wslib = access_project_or_space()
from project_lib import Project
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import json
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
from requests.adapters import HTTPAdapter
from pandas.io.json import json_normalize
import pandas as pd
import os
import time

def load_file(file):
    project = Project(project_id=os.environ['PROJECT_ID'])
    f = open(file, 'w+b')
    f.write(project.get_file(file).getbuffer())
    f.close()
    
def save_file(df, file_name):
    df.to_csv(file_name, index=False)
    # file_path = "/home/wsuser/work/" + file_name
    # with open(file_path, 'rb') as f:
    #     wslib.save_data(asset_name_or_item=file_name, data=f.read(), overwrite=True)
    # wslib.save_data(file_name, df.to_csv(index=False).encode(),overwrite=True)

# https://cloud.ibm.com/apidocs/watson-data-api-cpd

class WatsonDataAPI:
    def __init__(self):
        self.cpd_cluster_host = 'https://cpd-zen.apps.infra.cp4dex.com'
        # self.project = Project(project_id=os.environ['PROJECT_ID'])       
        
    def get_artifact_info(self, metadata_name, artifact_type):
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token,
            'Cache-Control': "no-cache",
            'Connection': "keep-alive"
        }

        search_body = {
            "size": 1000,
            "_source": ["artifact_id","metadata.name","entity.artifacts.version_id","entity.artifacts.artifact_id"],
            "query": {
                "match": {"metadata.artifact_type": artifact_type}
            }
        }
        parent_cat = self.s.post(f'{self.cpd_cluster_host}/v3/search', verify=False,  json=search_body, headers=headers)
        
        exist = False
        if parent_cat.status_code == 200:
            category=json.loads(parent_cat.text)
            for i in category['rows']:
                if i['metadata']['name']== metadata_name:
                    exist = True
                    print(artifact_type, metadata_name, "exists")
                    exists_category=True
                    artifact_id = i['entity']['artifacts']['artifact_id']
                    version_id = i['entity']['artifacts']['version_id']
                    break
        if not exist: 
            return False, False
        return artifact_id, version_id     
    
    def get_catalog_id(self, catalog_name):

        # Create new header for the requests
        headers = {
        'Content-Type': "application/json",
        'Authorization': "Bearer "+self.token

        }

        # endpoint to get all the catalogs 
        get_catalog=self.s.get(f"{self.cpd_cluster_host}/v2/catalogs",verify=False, headers=headers)

        ## Find the catalog created with specific name and store name and id of it into catalog_name and catalog_id respectively
        try:
            get_catalog_json=json.loads(get_catalog.text)['catalogs']
        except:
            print("The below error has occurred. Please ensure that catalog, '" + catalog_name + "', exists")
            raise

        catalog_id = ''
        for metadata in get_catalog_json:
            if metadata['entity']['name']==catalog_name:
                catalog_id=metadata['metadata']['guid']
                return catalog_id

        if catalog_id == '':
            print("The provided catalog name cannot be found. Please ensure that catalog, '" + catalog_name + "', exists")
            return None
    
    def get_category_id(self, category_name):
        try:
            headers = {
                'Content-Type': "application/json",
                'Authorization': "Bearer "+self.token,
                'Cache-Control': "no-cache",
                'Connection': "keep-alive"
                }

            search_body = {
                "size": 1000,
                "_source": ["artifact_id","metadata.name","categories.primary_category_name"],
               "query": {    
                       "match": {"metadata.artifact_type": "category"}
               }
            }
            parent_cat = self.s.post(f"{self.cpd_cluster_host}/v3/search", verify=False,  json=search_body, headers=headers)
            category_ids = []
            if parent_cat.status_code == 200:
                category=json.loads(parent_cat.text)
                for i in category['rows']:
                    if i['metadata']['name']== category_name:
                        exists_category=True
                        cat_id=i['artifact_id'] 
                        category_id = cat_id
                        if 'primary_category_name' in i['categories']:
                            parent_category_name = i['categories']['primary_category_name']
                        else:
                            parent_category_name = None
                        category_ids.append({"category_name":category_name,"category_id":category_id, "parent_category_name":parent_category_name})
                return category_ids
            else:
                raise

        except:
            print("The below error has occurred. " + "Please ensure that category, '" + category_name + "', exists.")
            raise ValueError(parent_cat.text)
    
    def get_asset_info(self,asset_id,catalog_id):
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        get_asset=self.s.get(f"{self.cpd_cluster_host}/v2/assets/{asset_id}?catalog_id="+catalog_id,verify=False, headers=headers)
        get_asset_json=json.loads(get_asset.text)
        return get_asset_json
        
        
        
    
    def get_asset_id_in_project(self):
        payload={"query":"*:*","limit":200}
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        get_asset=self.s.post(f"{self.cpd_cluster_host}/v2/asset_types/asset/search?project_id="+os.environ['PROJECT_ID'],json=payload,verify=False, headers=headers)
        get_asset_json=json.loads(get_asset.text)
        ret = []
        for each in get_asset_json['results']:
            ret.append((each['metadata']['name'],each['metadata']['asset_id']))
        df = pd.DataFrame(ret, columns=['name','asset_id'])
        return df
    
    def get_asset_id_in_catalog(self, catalog_name):
        catalog_id = self.get_catalog_id(catalog_name)
        payload={"query":"*:*","limit":200}
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        get_asset=self.s.post(f"{self.cpd_cluster_host}/v2/asset_types/asset/search?catalog_id="+catalog_id,json=payload,verify=False, headers=headers)
        get_asset_json=json.loads(get_asset.text)
        ret = []
        for each in get_asset_json['results']:
            ret.append((each['metadata']['name'],each['metadata']['asset_id']))
        df = pd.DataFrame(ret, columns=['name','asset_id'])
        return df
            
    def get_terms_id(self, category_name):
        category_ids = self.get_category_id(category_name)
        ret = []
        for each in category_ids:
            payload={
                "size":300,"from":0,"_source":["artifact_id","metadata.artifact_type","metadata.name","metadata.description","categories","entity.artifacts"],
                "query":{"bool":{"filter":{"bool":{"minimum_should_match":1,
                                                   "should":[{"term":{"categories.primary_category_id":each["category_id"]}},{"term":{"categories.secondary_category_ids":each["category_id"]}}],
                                                   "must_not":{"terms":{"metadata.artifact_type":["category"]}}}}}}}
            headers = {
                'Content-Type': "application/json",
                'Authorization': "Bearer "+self.token,
                'Cache-Control': "no-cache",
                'Connection': "keep-alive"
            }
            wf=self.s.post(f"{self.cpd_cluster_host}/v3/search",headers=headers,json=payload,verify=False)
            wf_json=json.loads(wf.text)['rows']
            df_terms=pd.json_normalize(wf_json)
            if not df_terms.empty: 
                df_terms=df_terms[['entity.artifacts.global_id','metadata.name','categories.primary_category_name',"metadata.artifact_type"]]
                df_terms['parent_category_name']=each['parent_category_name']
            ret.append(df_terms)
        ret = pd.concat(ret,axis=0, ignore_index=True)
        return ret      
    
    
    def map_terms_to_asset(self, map_terms_csv):
        catalog2id =dict()
        category2id = dict()
        terms2id = dict()
        catalogasset2id = dict()
        
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        # load_file(map_terms_csv)
        map_terms = pd.read_csv(map_terms_csv)
        map_terms= map_terms.sort_values(by=['DataAsset']).reset_index(drop=True)


        for each in set(map_terms.DataAsset.values):

            df = map_terms[map_terms['DataAsset']==each]
            start =time.time()
            cnt = 0
            print(f"[DataAsset:{each}]")
            start = time.time()
            num_bizterm = len(df[df['Done']=='N'])
            while set(df['Done'].values)!={'Y'}:
                if cnt==self.max_retries_map: break
                cnt+=1
                print(f"try {cnt}")
                print('*'*120)
                for _index, rows in df.iterrows():
                    if rows.Catalog not in catalog2id.keys():
                        catalog2id[rows.Catalog] = self.get_catalog_id(rows.Catalog)
                    catalog_id = catalog2id[rows.Catalog]
                    if catalog_id is not None and rows.Done =='N':

                        print(f"{_index}-[BizTerm:{rows.BusinessTerm}][Category:{rows.Category}] =>[Column:{rows.ColumnHeader}][DataAsset:{rows.DataAsset}][Catalog:{rows.Catalog}]")

                        catalog_name = rows.Catalog
                        category = [each.strip() for each in rows.Category.split('>>')][-1]
                        if category not in category2id.keys():
                            category2id[category] = self.get_terms_id(category)
                        df_terms = category2id[category]
                        df_terms = df_terms[(df_terms['metadata.name']==rows.BusinessTerm) & (df_terms['metadata.artifact_type']=='glossary_term')].copy()
                        df_terms['ColumnHeader']=rows.ColumnHeader

                        if catalog_name not in catalogasset2id.keys():
                            catalogasset2id[catalog_name]=self.get_asset_id_in_catalog(catalog_name)
                        catalog_asset_ids = catalogasset2id[catalog_name]
                        name = rows.DataAsset
                        catalog_asset_id = catalog_asset_ids[catalog_asset_ids['name']==name].asset_id.values[0]
                        payload={
                            "name": "column_info",
                            "entity":{
                            }
                        }
                        try: 
                            t=self.s.post(f"{self.cpd_cluster_host}/v2/assets/{catalog_asset_id}/attributes?catalog_id={catalog_id}",json=payload,headers=headers,verify=False,timeout=self.timeout)
                        except:
                            print('Fail to generate attribute')
                        try: 
                            t = self.s.get(f"{self.cpd_cluster_host}/v2/assets/{catalog_asset_id}?catalog_id={catalog_id}",json=payload,headers=headers,verify=False,timeout=self.timeout)

                            t = json.loads(t.text)
                            if 'data_asset' in t['entity'].keys():
                                i=0
                                for index, rows in df_terms.iterrows(): 
                                    i+=1
                                    print(rows["metadata.name"],"is mapped to",rows.ColumnHeader.strip(), 'in' ,name,'of',catalog_name)
                                    payload=[{"op":"add",
                                              "path":"/"+rows.ColumnHeader.strip(),
                                              "value":{
                                                  "column_terms":[{
                                                      "term_display_name":rows['metadata.name'],
                                                      "term_id":rows["entity.artifacts.global_id"]}
                                                  ]},
                                              "attribute":"column_info"}]
                                    url=f"{self.cpd_cluster_host}/v2/assets/"+catalog_asset_id+"/attributes/column_info?catalog_id="+catalog_id
                                    try:
                                        patch_attribute=self.s.patch(url,json=payload,headers=headers,verify=False,timeout=self.timeout)
                                        json.loads(patch_attribute.text)
                                        df.loc[_index,'Done']='Y'
                                    except:
                                        print('Fail to patch attribute')
                                    print("="*120)
                        except:
                            print('Fail to get asset info')
            map_terms.loc[df.index[df['Done']=='Y'].tolist(),'Done']='Y'
            end = time.time()
            elapsed_time = end - start
            print(f"[DataAsset:{each}] mapping is done!!")
            print(f"{num_bizterm} Business term mapping is done and elapsed time is {elapsed_time}s")
            print("\n")
        save_file(map_terms,map_terms_csv)

    def get_cpd_cluster_host(self):
        return self.cpd_cluster_host

    # def get_project(self):
    #     return self.project



from getpass import getpass

def user_INPUT():
    username = input("1) ID를 입력하세요:")
    password = getpass("2) 비밀번호: ")

    return username, password


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
                authresponse = self.s.post(f'{self.cpd_cluster_host}/icp4d-api/v1/authorize', headers=headers, data=payload, verify=False)
                token=json.loads(authresponse.text)['token']
                return token
            except:
                print("Fail to get token")

        self.timeout = timeout
        self.max_retries_map = max_retries_map

        session = requests.session()
        session.mount('https://',HTTPAdapter(max_retries=max_retries_api_call))

        self.s = session
        self.token = get_token(info_json)   




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
                authresponse = self.s.post(f'{self.cpd_cluster_host}/icp4d-api/v1/authorize', headers=headers, data=payload, verify=False)
                token=json.loads(authresponse.text)['token']
                return token
            except:
                print("Fail to get token")

        self.timeout = timeout
        self.max_retries_map = max_retries_map

        session = requests.session()
        session.mount('https://',HTTPAdapter(max_retries=max_retries_api_call))

        self.s = session
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
                authresponse = self.s.post(f'{self.cpd_cluster_host}/icp4d-api/v1/authorize', headers=headers, data=payload, verify=False)
                token=json.loads(authresponse.text)['token']
                return token
            except:
                print("Fail to get token")

        self.timeout = timeout
        self.max_retries_map = max_retries_map

        session = requests.session()
        session.mount('https://',HTTPAdapter(max_retries=max_retries_api_call))

        self.s = session
        self.token = get_token()

