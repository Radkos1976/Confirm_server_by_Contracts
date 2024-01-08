SELECT a.part_no Indeks,nvl(ifsapp.inventory_part_api.Get_Description(contract ,a.part_no),' ') Opis,nvl(a.colection,' ') Kolekcja," +
                            "nvl(a.mag,0) Mag,PLANNER_BUYER,nvl(PART_PRODUCT_CODE,' ') Rodzaj,EXPECTED_LEADTIME Czas_dostawy," +
                            "ifsapp.work_time_calendar_api.Get_End_Date(ifsapp.site_api.Get_Manuf_Calendar_Id(contract),SYSDATE,EXPECTED_LEADTIME) Data_gwarancji," +
                            "ifsapp.Inventory_Part_API.Get_Weight_Net(contract, PART_NO) Weight_Net,round(ifsapp.Inventory_Part_API.Get_Volume_Net(contract, PART_NO),2) Volume_Net," +
                            "round(ifsapp.inventory_part_unit_cost_api.Get_Inventory_Value_By_Config(contract, PART_NO,'*'),2) Inventory_Value," +
                            "NOTE_ID " +
                          "FROM " +
                             "(SELECT part_no,PLANNER_BUYER,ifsapp.inventory_part_in_stock_api. Get_Plannable_Qty_Onhand (contract ,part_no,'*') Mag,TYPE_DESIGNATION colection," +
                             "PART_PRODUCT_CODE,EXPECTED_LEADTIME,NOTE_ID " +
                                "FROM ifsapp.inventory_part_pub " +
                             "WHERE substr(part_no,1,1) in ('5','6') and TYPE_CODE_DB='4' ) a