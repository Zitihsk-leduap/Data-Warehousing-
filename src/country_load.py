from lib.Config import Config
from lib.Logger import Logger
from lib.Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "COUNTRY_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_D_COUNTRY")
v.set("TMP_TABLE", "TMP_D_COUNTRY")
v.set("TGT_TABLE", "TGT_D_COUNTRY")
sf = Config(v)

truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (COUNTRY_NAME)
                SELECT DISTINCT COUNTRY_NAME
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')}
            """
sf.execute_query(temp_query)

# SCD2: Insert only truly new records
insert_query = f"""
                INSERT INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')}
                (COUNTRY_NAME, EFF_START_DATE, EFF_END_DATE, IS_CURRENT)
                SELECT TMP.COUNTRY_NAME, CURRENT_DATE(), '9999-12-31'::DATE, TRUE
                FROM {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} TMP
                WHERE NOT EXISTS (
                    SELECT 1 FROM {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} TGT
                    WHERE TGT.COUNTRY_NAME = TMP.COUNTRY_NAME
                      AND TGT.IS_CURRENT = TRUE
                );
            """
sf.execute_query(insert_query)

v.get("LOG").close()
