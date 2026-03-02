from lib.Config import Config
from lib.Logger import Logger
from lib.Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "STATE_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_STATE")
v.set("TMP_TABLE", "TMP_D_STATE")
v.set("TGT_TABLE", "TGT_D_STATE")
sf = Config(v)

truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (STATE_NAME, REGION_NAME)
                SELECT DISTINCT STATE_NAME, REGION_NAME
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
                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_REGION R
                        ON R.REGION_NAME = TMP.REGION_NAME AND R.IS_CURRENT = TRUE
                    WHERE TMP.STATE_NAME = TGT.STATE_NAME
                      AND R.REGION_KEY != TGT.REGION_KEY
                );
            """
sf.execute_query(expire_query)

insert_query = f"""
                INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')}
                (STATE_NAME, REGION_KEY, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
                SELECT TMP.STATE_NAME, R.REGION_KEY,
                       CURRENT_DATE(), '9999-12-31'::DATE, TRUE
                FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_REGION R
                    ON R.REGION_NAME = TMP.REGION_NAME AND R.IS_CURRENT = TRUE
                WHERE NOT EXISTS (
                    SELECT 1 FROM {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} TGT
                    WHERE TGT.STATE_NAME = TMP.STATE_NAME
                      AND TGT.REGION_KEY = R.REGION_KEY
                      AND TGT.IS_CURRENT = TRUE
                );
            """
sf.execute_query(insert_query)

v.get("LOG").close()
