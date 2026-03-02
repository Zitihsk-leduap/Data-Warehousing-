from lib.Config import Config
from lib.Logger import Logger
from lib.Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "REGION_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_REGION")
v.set("TMP_TABLE", "TMP_D_REGION")
v.set("TGT_TABLE", "TGT_D_REGION")
sf = Config(v)

truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (REGION_NAME, COUNTRY_NAME)
                SELECT DISTINCT REGION_NAME, COUNTRY_NAME
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
                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_COUNTRY C
                        ON C.COUNTRY_NAME = TMP.COUNTRY_NAME AND C.IS_CURRENT = TRUE
                    WHERE TMP.REGION_NAME = TGT.REGION_NAME
                      AND C.COUNTRY_KEY != TGT.COUNTRY_KEY
                );
            """
sf.execute_query(expire_query)

insert_query = f"""
                INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')}
                (REGION_NAME, COUNTRY_KEY, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
                SELECT TMP.REGION_NAME, C.COUNTRY_KEY,
                       CURRENT_DATE(), '9999-12-31'::DATE, TRUE
                FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_COUNTRY C
                    ON C.COUNTRY_NAME = TMP.COUNTRY_NAME AND C.IS_CURRENT = TRUE
                WHERE NOT EXISTS (
                    SELECT 1 FROM {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} TGT
                    WHERE TGT.REGION_NAME = TMP.REGION_NAME
                      AND TGT.COUNTRY_KEY = C.COUNTRY_KEY
                      AND TGT.IS_CURRENT = TRUE
                );
            """
sf.execute_query(insert_query)

v.get("LOG").close()
