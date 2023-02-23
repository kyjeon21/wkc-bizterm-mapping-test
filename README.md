# WKCOpsAsset

When customers apply their governance system to Cloud Pak for Data, there is a limit to performing Metadata Enrichment(MDE) for all of the numerous business terms in the initial setup. To address these limitations, we used the Watson Data API to create custom code for mapping business terms to each asset in the catalogs.
    
- Demo

    [See Notebook](./assets/data_asset/test_class_TypeA.ipynb)
    
- Result

    ![image1](./assets/docs/term_map_activity.png)
  
    ![image2](./assets/docs/term_map_activity_detail.png)
  
    ![image3](./assets/docs/term_map_res1.png)
  
    ![image4](./assets/docs/term_map_res2.png)
  
- Limitation
1. It is not possible to distinguish between assets with the same name in the catalog.
    
2. It is not possible to distinguish between categories with somewhat identical cateogy paths. This is because the cateogry metadata contains only the parent category information. More work is needed to distinguish them. I will force customers not to use this category structure. ex) ParentA >> SubCategory1 >> SubCategory 2 vs. ParentB >> SubCategory1 >> SubCategory2) 
    

- Reference 

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