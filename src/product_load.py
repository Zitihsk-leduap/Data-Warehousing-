from lib.Config import Config
from lib.Logger import Logger
from lib.Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "PRODUCT_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_PRODUCT")
v.set("TMP_TABLE", "TMP_D_PRODUCT")
v.set("TGT_TABLE", "TGT_D_PRODUCT")
sf = Config(v)

truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

# Load to temporary table
temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (PRODUCT_ID, PRODUCT_NAME, SUBCATEGORY_NAME)
                SELECT DISTINCT PRODUCT_ID, PRODUCT_NAME, SUBCATEGORY_NAME
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
            """
sf.execute_query(temp_query)

# Expire current records where attributes have changed
expire_query = f"""
                UPDATE {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
                SET TGT.EFF_END_DATE = CURRENT_DATE(),
                    TGT.IS_CURRENT   = FALSE
                WHERE TGT.IS_CURRENT = TRUE
                  AND EXISTS (
                    SELECT 1
                    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_SUBCATEGORY SC
                        ON SC.SUBCATEGORY_NAME = TMP.SUBCATEGORY_NAME AND SC.IS_CURRENT = TRUE
                    WHERE TMP.PRODUCT_ID = TGT.PRODUCT_ID
                      AND (TMP.PRODUCT_NAME != TGT.PRODUCT_NAME
                           OR SC.SUBCATEGORY_KEY != TGT.SUBCATEGORY_KEY)
                );
            """
sf.execute_query(expire_query)

# Insert new versions of changed records + brand new records
insert_query = f"""
                INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')}
                (PRODUCT_ID, PRODUCT_NAME, SUBCATEGORY_KEY, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
                SELECT TMP.PRODUCT_ID, TMP.PRODUCT_NAME, SC.SUBCATEGORY_KEY,
                       CURRENT_DATE(), '9999-12-31'::DATE, TRUE
                FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_SUBCATEGORY SC
                    ON SC.SUBCATEGORY_NAME = TMP.SUBCATEGORY_NAME AND SC.IS_CURRENT = TRUE
                WHERE NOT EXISTS (
                    SELECT 1 FROM {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} TGT
                    WHERE TGT.PRODUCT_ID      = TMP.PRODUCT_ID
                      AND TGT.PRODUCT_NAME    = TMP.PRODUCT_NAME
                      AND TGT.SUBCATEGORY_KEY = SC.SUBCATEGORY_KEY
                      AND TGT.IS_CURRENT      = TRUE
                );
            """
sf.execute_query(insert_query)

v.get("LOG").close()
