import os
from deltalake import DeltaTable
from unitycatalog import Unitycatalog

if __name__ == "__main__":
    # Example usage
    uc_base_url=f"{os.getenv('UNITY_ENDPOINT').rstrip('/')}/api/2.1/unity-catalog/"
    uc_default_headers={"Authorization": f"Bearer {os.getenv('UNITY_TOKEN')}"}
    uc = Unitycatalog(base_url=uc_base_url,default_headers=uc_default_headers)

    table_info = uc.tables.retrieve("samples.bakehouse.sales_suppliers")
    print(f"Table location: {table_info.storage_location}")
    table = DeltaTable(table_info.storage_location)
    df = table.to_pandas()
    print(df.head())