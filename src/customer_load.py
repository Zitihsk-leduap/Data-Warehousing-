from lib.Config import Config
from lib.Logger import Logger
from lib.Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "CUSTOMER_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_CUSTOMER")
v.set("TMP_TABLE", "TMP_D_CUSTOMER")
v.set("TGT_TABLE", "TGT_D_CUSTOMER")
sf = Config(v)

truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_NAME)
                SELECT DISTINCT CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_NAME
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
                    INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_SEGMENT S
                        ON S.SEGMENT_NAME = TMP.SEGMENT_NAME AND S.IS_CURRENT = TRUE
                    WHERE TMP.CUSTOMER_ID = TGT.CUSTOMER_ID
                      AND (TMP.CUSTOMER_NAME != TGT.CUSTOMER_NAME
                           OR S.SEGMENT_KEY != TGT.SEGMENT_KEY)
                );
            """
sf.execute_query(expire_query)

insert_query = f"""
                INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')}
                (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_KEY, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
                SELECT TMP.CUSTOMER_ID, TMP.CUSTOMER_NAME, S.SEGMENT_KEY,
                       CURRENT_DATE(), '9999-12-31'::DATE, TRUE
                FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_SEGMENT S
                    ON S.SEGMENT_NAME = TMP.SEGMENT_NAME AND S.IS_CURRENT = TRUE
                WHERE NOT EXISTS (
                    SELECT 1 FROM {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} TGT
                    WHERE TGT.CUSTOMER_ID   = TMP.CUSTOMER_ID
                      AND TGT.CUSTOMER_NAME = TMP.CUSTOMER_NAME
                      AND TGT.SEGMENT_KEY   = S.SEGMENT_KEY
                      AND TGT.IS_CURRENT    = TRUE
                );
            """
sf.execute_query(insert_query)

v.get("LOG").close()
