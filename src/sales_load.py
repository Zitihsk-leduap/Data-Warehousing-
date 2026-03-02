from lib.Config import Config
from lib.Logger import Logger
from lib.Variable import Variables

v = Variables()
v.set("SCRIPT_NAME", "SALES_LOAD")
v.set("LOG", Logger(v))
v.set("STG_VIEW", "STG_F_SALES")
v.set("TMP_TABLE", "TMP_F_SALES")
v.set("TGT_TABLE", "TGT_F_SALES")
sf = Config(v)

truncate_query = f"TRUNCATE TABLE {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}"
sf.execute_query(truncate_query)

temp_query = f"""
                INSERT INTO {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')}
                (ROW_ID, ORDER_ID, ORDER_DATE_KEY, SHIP_DATE_KEY,
                 CUSTOMER_KEY, PRODUCT_KEY, CITY_KEY, SHIP_MODE_KEY,
                 QUANTITY, SALES, DISCOUNT, DISCOUNT_AMOUNT, REVENUE, PROFIT)
                SELECT
                     SRC.ROW_ID
                    ,SRC.ORDER_ID
                    ,ORD_DT.DATE_KEY
                    ,SHP_DT.DATE_KEY
                    ,CUS.CUSTOMER_KEY
                    ,PRD.PRODUCT_KEY
                    ,CTY.CITY_KEY
                    ,SHIP.SHIP_MODE_KEY
                    ,SRC.QUANTITY
                    ,SRC.SALES
                    ,SRC.DISCOUNT
                    ,SRC.DISCOUNT_AMOUNT
                    ,SRC.REVENUE
                    ,SRC.PROFIT
                FROM {v.get('STG_SCHEMA')}.{v.get('STG_VIEW')} SRC
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_CUSTOMER CUS
                    ON CUS.CUSTOMER_ID = SRC.CUSTOMER_ID AND CUS.IS_CURRENT = TRUE
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_PRODUCT PRD
                    ON PRD.PRODUCT_ID = SRC.PRODUCT_ID AND PRD.IS_CURRENT = TRUE
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_CITY CTY
                    ON CTY.POSTAL_CODE = SRC.POSTAL_CODE
                    AND CTY.CITY_NAME = SRC.CITY AND CTY.IS_CURRENT = TRUE
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_DATE ORD_DT
                    ON ORD_DT.FULL_DATE = SRC.ORDER_DATE
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_DATE SHP_DT
                    ON SHP_DT.FULL_DATE = SRC.SHIP_DATE
                INNER JOIN {v.get('TGT_SCHEMA')}.TGT_D_SHIP_MODE SHIP
                    ON SHIP.SHIP_MODE = SRC.SHIP_MODE AND SHIP.IS_CURRENT = TRUE
            """
sf.execute_query(temp_query)

merge_query = f"""
                MERGE INTO {v.get('TGT_SCHEMA')}.{v.get('TGT_TABLE')} AS TGT
                USING {v.get('TMP_SCHEMA')}.{v.get('TMP_TABLE')} AS TMP
                    ON TGT.ROW_ID = TMP.ROW_ID
                WHEN MATCHED THEN
                    UPDATE SET
                         TGT.ORDER_ID        = TMP.ORDER_ID
                        ,TGT.ORDER_DATE_KEY  = TMP.ORDER_DATE_KEY
                        ,TGT.SHIP_DATE_KEY   = TMP.SHIP_DATE_KEY
                        ,TGT.CUSTOMER_KEY    = TMP.CUSTOMER_KEY
                        ,TGT.PRODUCT_KEY     = TMP.PRODUCT_KEY
                        ,TGT.CITY_KEY        = TMP.CITY_KEY
                        ,TGT.SHIP_MODE_KEY   = TMP.SHIP_MODE_KEY
                        ,TGT.QUANTITY        = TMP.QUANTITY
                        ,TGT.SALES           = TMP.SALES
                        ,TGT.DISCOUNT        = TMP.DISCOUNT
                        ,TGT.DISCOUNT_AMOUNT = TMP.DISCOUNT_AMOUNT
                        ,TGT.REVENUE         = TMP.REVENUE
                        ,TGT.PROFIT          = TMP.PROFIT
                WHEN NOT MATCHED THEN
                    INSERT (ROW_ID, ORDER_ID, ORDER_DATE_KEY, SHIP_DATE_KEY,
                            CUSTOMER_KEY, PRODUCT_KEY, CITY_KEY, SHIP_MODE_KEY,
                            QUANTITY, SALES, DISCOUNT, DISCOUNT_AMOUNT, REVENUE, PROFIT)
                    VALUES (TMP.ROW_ID, TMP.ORDER_ID, TMP.ORDER_DATE_KEY, TMP.SHIP_DATE_KEY,
                            TMP.CUSTOMER_KEY, TMP.PRODUCT_KEY, TMP.CITY_KEY, TMP.SHIP_MODE_KEY,
                            TMP.QUANTITY, TMP.SALES, TMP.DISCOUNT, TMP.DISCOUNT_AMOUNT,
                            TMP.REVENUE, TMP.PROFIT);
            """
sf.execute_query(merge_query)

v.get("LOG").close()
