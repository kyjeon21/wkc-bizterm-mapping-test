#!/usr/bin/env python3
__author__ = "Kiyeon Jeon"
__copyright__ = 'Copyright 2023, Watson Knowledge Catalog'
__date__ = "02/16/2023"
__version__ = "1.1"
__email__ = "kiyeon.jeon@ibm.com"


import requests
from requests.adapters import HTTPAdapter, Retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json
import pandas as pd
from pandas import json_normalize
import time
import logging
from abc import *
import logging
logger = logging.getLogger('API Error Log')
logger.setLevel(logging.ERROR)
handler = logging.FileHandler('error.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

DEFAULT_TIMEOUT = 0.5


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)
    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


class WatsonKnowledgeCatalog(metaclass=ABCMeta):
    def __init__(self, cpd_cluster_host, logger=logger):
        self.cpd_cluster_host = cpd_cluster_host
        self.logger=logger
        self.token = None
        self.metadata = {
            "catalog2id":{},
            "category2id":{},
            "categorypath2biztermdict":{},
        }
    @abstractmethod
    def get_token(self):
        pass

    def get_catalog_id(self, catalog_name):
        if catalog_name in self.metadata['catalog2id'].keys():
            return self.metadata['catalog2id'][catalog_name]
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry))
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        print(f"getting catalog id of {catalog_name}.. ")
        try:
            r = s.get(
                f"{self.cpd_cluster_host}/v2/catalogs", 
                verify=False, 
                headers=headers
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
        catalogs = json.loads(r.text)['catalogs']

        catalog_id = None
        for catalog in catalogs:
            if catalog['entity']['name'] == catalog_name:
                catalog_id = catalog['metadata']['guid']
                self.metadata['catalog2id'][catalog_name] = catalog_id
                break
        if catalog_id is None:
            print(f"The provided catalog name ({catalog_name}) does not exist!")
        
        return catalog_id
    
    def get_category_id(self, category_path):
        if category_path in self.metadata['category2id'].keys():
            return self.metadata['category2id'][category_path]
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry,timeout=10))
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token,
            'Cache-Control': "no-cache",
            # 'Connection': "keep-alive"
        }
        payload = {
            "_source": ["artifact_id","metadata.name","categories"],
            "query": {
                "bool": {
                    "must":[
                        {"match": {"metadata.name":category_path}},
                        {"match": {"metadata.artifact_type": "category"}}
                    ]

                }
            }
        }
        print(f"searching category id of {category_path}.. ")
        try:
            r = s.post(
                f"{self.cpd_cluster_host}/v3/search",
                verify=False,
                json=payload,
                headers=headers
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
        rows = json.loads(r.text)['rows']

        category_hierarchy = [each.strip() for each in category_path.split('>>')]
        category_name= category_hierarchy[-1]
        is_root = True if len(category_hierarchy)==1 else False
        category_parent_name = category_hierarchy[-2] if not is_root else None

        category_id = None
        for row in rows:
            if row['metadata']['name']== category_name:
                if is_root or (not is_root and row["categories"]["primary_category_name"]==category_parent_name):
                    category_id = row["artifact_id"]
                    self.metadata['category2id'][category_path] = category_id
                    break
        if category_id is None:
            print(f"The provided catalog name ({category_path}) does not exist!")
        return category_id
    
    def get_asset_id(self, asset_name, catalog_name):
        catalog_id = self.get_catalog_id(catalog_name)
        if catalog_id is None:
            return None
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry,timeout=10))
        search_body={
            "query": f"asset.name:{asset_name}"
        }
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        print(f"searching asset id of {asset_name} in {catalog_name}.. ")
        try:
            r = s.post(
                f"{self.cpd_cluster_host}/v2/asset_types/asset/search?catalog_id="+catalog_id,
                json=search_body,
                verify=False, 
                headers=headers
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
        results = json.loads(r.text)['results']
        asset_id = None
        for result in results:
            if result['metadata']['name']==asset_name:
                asset_id= result['metadata']['asset_id']
                break
        if asset_id is None:
            print(f"The provided asset name ({asset_name}) does not exist in catalog name ({catalog_name})!")
        return asset_id
    def view_asset_info(self, asset_name, catalog_name):
        catalog_id = self.get_catalog_id(catalog_name)
        asset_id = self.get_asset_id(asset_name, catalog_name)
        
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry))
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        print(f"getting asset info of {asset_name} in {catalog_name}.. ")
        try:
            r = s.get(
                f"{self.cpd_cluster_host}/v2/assets/{asset_id}?catalog_id={catalog_id}", 
                verify=False, 
                headers=headers
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
        r_json = json.loads(r.text)
        print(json.dumps(r_json, indent=4))

    def create_attribute(self, asset_name, catalog_name):
        catalog_id = self.get_catalog_id(catalog_name)
        asset_id = self.get_asset_id(asset_name, catalog_name)
        
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry))
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        payload = {
            "name": "column_info",
            "entity":{
            }
        }
        print(f"creating column_info attribute of {asset_name} in {catalog_name}.. ")
        try: 
            r=s.post(
                f"{self.cpd_cluster_host}/v2/assets/{asset_id}/attributes?catalog_id={catalog_id}",
                json=payload,
                headers=headers,
                verify=False
            )
        except requests.exceptions.RequestException as e:
            print('Fail to create attribute')
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
    def view_attribute(self, asset_name, catalog_name):
        catalog_id = self.get_catalog_id(catalog_name)
        asset_id = self.get_asset_id(asset_name, catalog_name)
        
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry))
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        print(f"getting column_info attribute of {asset_name} in {catalog_name}.. ")
        try: 
            r=s.get(
                f"{self.cpd_cluster_host}/v2/assets/{asset_id}/attributes/column_info?catalog_id={catalog_id}",
                headers=headers,
                verify=False
            )
        except requests.exceptions.RequestException as e:
            print('Fail to get attribute')
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
        r_json = json.loads(r.text)
        print(json.dumps(r_json, indent=4))
            
    def delete_attribute(self, asset_name, catalog_name):
        catalog_id = self.get_catalog_id(catalog_name)
        asset_id = self.get_asset_id(asset_name, catalog_name)
        
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry))
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        print(f"deleting column_info attribute of {asset_name} in {catalog_name}.. ")
        try: 
            r=s.delete(
                f"{self.cpd_cluster_host}/v2/assets/{asset_id}/attributes/column_info?catalog_id={catalog_id}",
                headers=headers,
                verify=False
            )
        except requests.exceptions.RequestException as e:
            print('Fail to delete attribute')
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
    def get_bizterm_id(self, bizterm, category_path):
        if category_path in self.metadata['categorypath2biztermdict'].keys():
            if bizterm not in self.metadata['categorypath2biztermdict'][category_path].keys():
                print(f"The provided business term ({bizterm}) does not exist!")
                return None
            return self.metadata['categorypath2biztermdict'][category_path][bizterm]

        category_id = self.get_category_id(category_path)
        if category_id is None:
            return None
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry, timeout=10))
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token,
            'Cache-Control': "no-cache",
            # 'Connection': "keep-alive"
        }

        payload={
            "size": 300, 
            "from": 0, 
            "_source": [
                "artifact_id",
                "metadata.artifact_type",
                "metadata.name",
                "metadata.description",
                "categories",
                "entity.artifacts"],
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            # "minimum_should_match":1,
                            "should": [
                                # {"match": {"metadata.name": bizterm}},
                                {"term":{"categories.primary_category_id":category_id}}
                            ],
                            "must_not": {
                                "terms": {
                                    "metadata.artifact_type": ["category"]
                                }
                            }
                        }
                    }
                }
            }
        }
        print(f"searching business terms in {category_path}.. ")
        try:
            r = s.post(
                f"{self.cpd_cluster_host}/v3/search",
                headers=headers,
                json=payload,
                verify=False
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
        df = json_normalize(json.loads(r.text)["rows"])
        bizterm2id = dict(zip(df['metadata.name'], df['artifact_id']))
        self.metadata['categorypath2biztermdict'][category_path] = bizterm2id

        bizterm_id = None
        if len(bizterm2id)>0:
            for key, val in bizterm2id.items():
                if key==bizterm:
                    bizterm_id = val
                    break
        if bizterm_id is None:
            print(f"The provided business term ({bizterm}) does not exist!")
        return bizterm_id

        # if len(json.loads(r.text)["rows"])>0:
        #     for row in json.loads(r.text)["rows"]:
        #         if row['metadata']['name']==bizterm: 
        #             bizterm_id = row["artifact_id"]
        #             self.metadata['categorypath2biztermsdf'][category_path] = bizterm_id
        #             break
        # if bizterm_id is None:
        #     print(f"The provided business term ({bizterm}) does not exist!")
        # return bizterm_id
        
    def update_attribute(self, asset_name, catalog_name,column_name, bizterm, category_path):
        catalog_id = self.get_catalog_id(catalog_name)
        asset_id = self.get_asset_id(asset_name, catalog_name)
        bizterm_id = self.get_bizterm_id(bizterm, category_path)
        if None in set([catalog_id, asset_id, bizterm_id]):
            return
        
        s = requests.session()
        retry = Retry(total=15,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry,timeout=15))
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        payload=[
            {
                "op":"add",
                "path":"/"+column_name,
                "value":{
                    "column_terms":[
                        {
                            "term_display_name":bizterm,
                            "term_id":bizterm_id
                        }
                    ]
                }
            }
        ]
        print(f"updating column_info attribute of {column_name} of {asset_name} in {catalog_name} with {bizterm} in {category_path}.. ")
        try: 
            r=s.patch(
                f"{self.cpd_cluster_host}/v2/assets/{asset_id}/attributes/column_info?catalog_id={catalog_id}",
                json=payload,
                headers=headers,
                verify=False
            )
        except requests.exceptions.RequestException as e:
            print('Fail to update attribute')
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
    
    def map_bizterm(self, map_bizterm_csv='map-bizterm-glossary.csv'):
        df = pd.read_csv(map_bizterm_csv)
        start = time.time()

        df_attribute = df[['Catalog', 'DataAsset']].drop_duplicates(ignore_index=True)
        print('='*100)
        print(f"1. Creating attribute..")
        for idx, row in df_attribute.iterrows():
            print('-'*100)
            print(f"{idx}-{row.DataAsset} of {row.Catalog}..")
            self.create_attribute(row.DataAsset, row.Catalog)
        print('='*100)            
        print(f"2. Patching column info attribute into data asset in catalogs..")

        for idx, row in df.iterrows():
            print('-'*100)
            print(f"{idx}-{row.BusinessTerm} is mapped to {row.ColumnHeader} in {row.DataAsset} of {row.Catalog}..")
            self.update_attribute(row.DataAsset, row.Catalog, row.ColumnHeader, row.BusinessTerm, row.Category)
        end = time.time()
        elapsed_time = end-start
        print('='*100)
        print(f"it takes {elapsed_time} seconds / ({elapsed_time/len(df)} sec per column)")
        print('='*100)

    def map_bizterm_allatonce(self, map_bizterm_csv='map-bizterm-glossary.csv'):
        
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer "+self.token
        }
        
        df = pd.read_csv(map_bizterm_csv)
        start = time.time()
        for idx,((catalog_name, asset_name), rows) in enumerate(df.groupby(['Catalog','DataAsset'])):
            print('='*100)
            print(f"{idx}-Creating and patching attribute on {asset_name} of {catalog_name}..")
            print('='*100)
            s = requests.session()
            retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
            s.mount('https://',TimeoutHTTPAdapter(max_retries=retry))
            catalog_id = self.get_catalog_id(catalog_name)
            asset_id = self.get_asset_id(asset_name, catalog_name)
            payload = dict()
            payload['name'] = "column_info"
            payload['entity'] = dict()
            for idx, row in rows.iterrows():
                payload['entity'][row.ColumnHeader]=dict()
                bizterm_id = self.get_bizterm_id(row.BusinessTerm, row.Category)
                payload['entity'][row.ColumnHeader]['column_terms'] = [
                    {
                        'term_display_name': row.BusinessTerm, 'term_id': bizterm_id
                    }
                ]            
            try: 
                r=s.post(
                    f"{self.cpd_cluster_host}/v2/assets/{asset_id}/attributes?catalog_id={catalog_id}",
                    json=payload,
                    headers=headers,
                    verify=False
                )
            except requests.exceptions.RequestException as e:
                print('Fail to create attribute')
                self.logger.error(str(e))
                raise SystemExit(e)
            finally:
                s.close()
        end = time.time()
        elapsed_time = end - start
        print('='*100)
        print(f"it takes {elapsed_time} seconds / ({elapsed_time/len(df)} sec per asset)")
        

class MapTermsJSON(WatsonKnowledgeCatalog):
    def __init__(self, cpd_cluster_host, info_json):
        super().__init__(cpd_cluster_host)
        self.token = self.get_token(info_json)
    
                
    def get_token(self, info_json):
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry))
        info = json.load(open(info_json))
        headers = {
            'cache-control': 'no-cache',
            'content-type': 'application/json'
        }

        payload = json.dumps({"username":info["username"], "password":info["password"]})
        try:
            r = s.post(f'{self.cpd_cluster_host}/icp4d-api/v1/authorize', headers=headers, data=payload, verify=False)
            token=json.loads(r.text)['token']
        except requests.exceptions.RequestException as e:  # This is the correct syntax
            raise SystemExit(e)
        finally:
            self.logger.error(str(e))
            s.close()
        return token
    
        
class MapTermsInput(WatsonKnowledgeCatalog):
    def __init__(self, cpd_cluster_host,logger=logger):
        super().__init__(cpd_cluster_host,logger)
        self.token = self.get_token()
    def get_token(self):
        def user_input():
            from getpass import getpass
            username = input("1) Please enter your ID:")
            password = getpass("2) Password: ")
            return username, password
        username, password = user_input()

        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry))
        headers = {
            'cache-control': 'no-cache',
            'content-type': 'application/json'
        }

        payload = json.dumps({"username":username, "password":password})
        try:
            r = s.post(f'{self.cpd_cluster_host}/icp4d-api/v1/authorize', headers=headers, data=payload, verify=False)
            token=json.loads(r.text)['token']
        except requests.exceptions.RequestException as e:
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
        return token
    

class MapTermsJob(WatsonKnowledgeCatalog):
    def __init__(self, cpd_cluster_host, username, password, filename, logger=logger):
        super().__init__(cpd_cluster_host,logger)
        self.token = self.get_token(username, pasword)
    def get_token(self, username, password):
        s = requests.session()
        retry = Retry(total=10,backoff_factor=0.5,status_forcelist=[500,504])
        s.mount('https://',TimeoutHTTPAdapter(max_retries=retry))
        headers = {
            'cache-control': 'no-cache',
            'content-type': 'application/json'
        }

        payload = json.dumps({"username":username, "password":password})
        try:
            r = s.post(f'{self.cpd_cluster_host}/icp4d-api/v1/authorize', headers=headers, data=payload, verify=False)
            token=json.loads(r.text)['token']
        except requests.exceptions.RequestException as e:
            self.logger.error(str(e))
            raise SystemExit(e)
        finally:
            s.close()
        return token