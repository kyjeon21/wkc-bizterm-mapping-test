# 비지니스 용어 일괄 매핑 (Watson Knowledge Governance)

- 개괄

초기셋업으로써 Cloud Pak for Data에 미리 만들어둔 거버넌스 체계를 입힐 때, Metadata Enrichment를 통해 비지니스 용어들을 모든 데이터 에셋에 붙이는 작업은 반복적이고 시간이 많이 든다. 이러한 한계점을 해결하기 위해 [Watson Data API](https://cloud.ibm.com/apidocs/watson-data-api-cpd)를 이용하여 카탈로그의 에셋들에 비지니스 용어를 붙이는 커스텀 코드를 만들었다. 


- 데모

    [See Notebook](./assets/data_asset/test_class_TypeA.ipynb)
    
- 결과

    ![image1](./assets/docs/term_map_activity.png)
  
    ![image2](./assets/docs/term_map_activity_detail.png)
  
    ![image3](./assets/docs/term_map_res1.png)
  
    ![image4](./assets/docs/term_map_res2.png)
    
- 성능

    It was tested in an environment with about 1,000 business terms, and it was confirmed that it took about 1 second for each asset. Considering the 50 assets per catalog for 20 catalogs, it is expected to take approximately 1000 seconds (16.6 minutes) to map business term to all the catalogs.
    
- 디버깅

    + There is a process of finding the category ids and business term ids through elasticsearch in the code, so the time required for API requests may vary depending on the size and system of the governance. Depending on the situation, you may need to change the timeout and retry parameters in the [code](./assets/data_asset/wkcapi_v1.py).
    + An error log is created for each exception situation, so you can see the [error.log](./assets/data_asset/error.log) and understand what the problem is.
    
  
- 한계점
    1. It is not possible to distinguish between assets with the same name in the catalog.
    
    2. It is not possible to distinguish between categories with somewhat identical cateogy paths. This is because the cateogry metadata contains only the parent category information. More work is needed to distinguish them. I will force customers not to use this category structure. ex) ParentA >> SubCategory1 >> SubCategory 2 vs. ParentB >> SubCategory1 >> SubCategory2) 
    

- 참조 

    The following are the functions implemented [source code](./assets/data_asset/wkcapi_v1.py)

    1. get_catalog_id(catalog_name) : get catalog id of a given catalog name
    
    2. get_category_id(category_path) : get category id of a given category path (ex. Parent Category >> Sub Category A)
    
    3. get_asset_id(asset_name, catalog_name) : get asset id of a given asset name in a given catalog name
    
    4. view_asset_info(asset_name, catalog_name) : print metadata information of a given asset name in a given catalog name
    
    5. create_attribute(asset_name, catalog_name) : create column_info attribute in a given asset name of a given catalog name 
    
    6. view_attribute(asset_name, catalog_name) : print column_info attribute in a given asset name of a given catalog name
    
    7. delete_attribute(asset_name, catalog_name): delete column_info attribute in a given asset name of a given catalog name
    
    8. update_attribute(asset_name, catalog_name,column_name,bizterm_name, category_path) : patch column_info attribute with business term name and id corresponding to a given column
    
    9. get_bizterm_id(bizterm_name, category_path) : get business term id of a given business term name in a given category path 
    
    10. map_bizterm(map_bizterm_csv): patch column info attribute with a business term on each column in all the assets given in a given csv file
    
    11. map_bizterm_allatonce(map_bizterm_csv): create column info attribute including all the business terms of each asset name in a given csv file
