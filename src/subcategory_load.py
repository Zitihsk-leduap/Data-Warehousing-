from lib.Config import Config
from lib.Logger import Logger
from lib.Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "SUBCATEGORY_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_SUBCATEGORY")
v.set("TMP_TABLE", "TMP_D_SUBCATEGORY")
v.set("TGT_TABLE", "TGT_D_SUBCATEGORY")
sf = Config(v)

truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (SUBCATEGORY_NAME, CATEGORY_NAME)
                SELECT DISTINCT SUBCATEGORY_NAME, CATEGORY_NAME
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
            """
sf.execute_query(temp_query)

expire_query = f"""
                UPDATE {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
                SET TGT.EFF_END_DATE = CURRENT_DATE(),
                    TGT.IS_CURRENT   = FALSE
                WHERE TGT.IS_CURRENT = TRUE
                  AND EXISTS (
                    SELECT 1
                    FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_CATEGORY C
                        ON C.CATEGORY_NAME = TMP.CATEGORY_NAME AND C.IS_CURRENT = TRUE
                    WHERE TMP.SUBCATEGORY_NAME = TGT.SUBCATEGORY_NAME
                      AND C.CATEGORY_KEY != TGT.CATEGORY_KEY
                );
            """
sf.execute_query(expire_query)

insert_query = f"""
                INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')}
                (SUBCATEGORY_NAME, CATEGORY_KEY, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
                SELECT TMP.SUBCATEGORY_NAME, C.CATEGORY_KEY,
                       CURRENT_DATE(), '9999-12-31'::DATE, TRUE
                FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_CATEGORY C
                    ON C.CATEGORY_NAME = TMP.CATEGORY_NAME AND C.IS_CURRENT = TRUE
                WHERE NOT EXISTS (
                    SELECT 1 FROM {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} TGT
                    WHERE TGT.SUBCATEGORY_NAME = TMP.SUBCATEGORY_NAME
                      AND TGT.CATEGORY_KEY = C.CATEGORY_KEY
                      AND TGT.IS_CURRENT = TRUE
                );
            """
sf.execute_query(insert_query)

v.get("LOG").close()
