﻿using DB_Conect;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Threading;
using System.Threading.Tasks;


namespace Confirm_server_by_Contracts
{
    /// <summary>
    /// Gets informations about inventory part
    /// Class does not update automatically datatable
    /// </summary>       
    public class Inventory_part : Update_pstgr_from_Ora<Inventory_part.Inventory_part_row>
    {
        //  regex for all without 616% ^(5|6(?!16)).*
        public bool Updated_on_init;
        public List<Inventory_part_row> Inventory_part_list;
        private readonly Update_pstgr_from_Ora<Inventory_part_row> rw;
        private readonly string regex_part_no  = "";
        private readonly bool limit_not_zero_stock = false;
        private readonly List<Tuple<string, string>> limit_part_no;

        public Inventory_part(string regex, bool not_zero_stock = false, List<Tuple<string, string>> part_no_values =  null)
        {
            rw = new Update_pstgr_from_Ora<Inventory_part_row>("MAIN");
            regex_part_no = regex == "" ? "^(5|6).*" : regex;
            limit_not_zero_stock = not_zero_stock;
            limit_part_no = part_no_values;
        }
        public async Task<int> Update_dataset(List<Inventory_part_row> pstgrdtset, List<Inventory_part_row> oradtset,string Task_name, CancellationToken cancellationToken)
        {
            Changes_List<Inventory_part_row> tmp = rw.Changes(pstgrdtset, oradtset, new[] { "_id" }, new[] { "indeks", "contract" }, "", Task_name, cancellationToken);
            int result = await PSTRG_Changes_to_dataTable(tmp, "mag", new[] { "indeks", "contract" }, null, null, Task_name, cancellationToken);
            return result;
        }
            
        /// <summary>
        /// Get inventory_part stored in PSTGR
        /// </summary>
        /// <param name="rw"></param>
        /// <returns></returns>
        public async Task<List<Inventory_part_row>> Get_PSTGR_List(string Task_name, CancellationToken cancellationToken) => await rw.Get_PSTGR("" + 
            String.Format(@"Select * from mag {0}", regex_part_no != "^(5|6).*" ?
                String.Format("WHERE regexp_like(indeks, '{0}')", regex_part_no): ""), Task_name, cancellationToken);

        /// <summary>
        /// Get present list of inventory_part stored in ERP
        /// </summary>
        /// <returns>Present list of inventory_part</returns>
        public async Task<List<Inventory_part_row>> Get_Ora_list(string Task_name, CancellationToken cancellationToken) => Check_length(await rw.Get_Ora("" +
               String.Format(@"SELECT 
                    a.part_no Indeks,
                    contract,
                    nvl(ifsapp.inventory_part_api.Get_Description('ST',a.part_no),' ') Opis,
                    nvl(a.colection,' ') Kolekcja,
                    nvl(a.mag,0) Mag,PLANNER_BUYER,nvl(PART_PRODUCT_CODE,' ') Rodzaj,
                    EXPECTED_LEADTIME Czas_dostawy,
                    ifsapp.work_time_calendar_api.Get_End_Date(ifsapp.site_api.Get_Manuf_Calendar_Id('ST'),SYSDATE,EXPECTED_LEADTIME) Data_gwarancji,
                    ifsapp.Inventory_Part_API.Get_Weight_Net('ST',PART_NO) Weight_Net,
                    ifsapp.Inventory_Part_API.Get_Volume_Net('ST',PART_NO) Volume_Net,
                    ifsapp.inventory_part_unit_cost_api.Get_Inventory_Value_By_Config('ST',PART_NO,'*') Inventory_Value,
                    NOTE_ID 
                FROM (
                        SELECT 
                            part_no,
                            contract,
                            PLANNER_BUYER,
                            ifsapp.inventory_part_in_stock_api. Get_Plannable_Qty_Onhand (CONTRACT,part_no,'*') Mag,
                            TYPE_DESIGNATION colection,
                            PART_PRODUCT_CODE,
                            EXPECTED_LEADTIME,
                            NOTE_ID 
                        FROM 
                        ifsapp.inventory_part_pub 
                        WHERE REGEXP_LIKE(part_no,'{1}') {0} AND TYPE_CODE_DB='4' ) a
                ", limit_not_zero_stock ? 
                    String.Format("AND {0} ifsapp.inventory_part_in_stock_api. Get_Plannable_Qty_Onhand (CONTRACT,part_no,'*') > 0 {1}", limit_part_no.Count > 0 ? 
                       String.Format("((part_no , contract) IN ({0}) OR ", string.Join(",", limit_part_no.Select(t => string.Format("( '{0}', '{1}')", t.Item1, t.Item2)))): "",
                       limit_part_no.Count > 0 ? ")":""): "",
                   regex_part_no), Task_name, cancellationToken), cancellationToken);


        public class Inventory_part_row : IEquatable<Inventory_part_row>, IComparable<Inventory_part_row>
        {
            public string _Id
            {
                get
                {
                    return string.Format("{0}_{1}", Indeks, Contract);
                }
                set
                {

                }
            }
            public string Indeks { get; set; }
            public string Contract { get; set; }
            public string Opis { get; set; }
            public string Kolekcja { get; set; }
            public double Mag { get; set; }
            public string Planner_buyer { get; set; }
            public string Rodzaj { get; set; }
            public double Czas_dostawy { get; set; }
            public double Weight_net { get; set; }
            public double Volume_net { get; set; }
            public double Inventory_value { get; set; }
            public long Note_id { get; set; }

            
            /// <summary>
            /// default Comparer by Custid field
            /// </summary>
            /// <param name="other"></param>
            /// <returns></returns>
            public int CompareTo(Inventory_part_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    return this._Id.CompareTo(other._Id);
                }
            }
            public bool Equals(Inventory_part_row other)
            {
                if (other == null) return false;
                // return (this.Note_id.Equals(other.Note_id) && this.Contract.Equals(other.Contract));
                return this._Id.Equals(other._Id);
            }
        }
        public List<Inventory_part_row> Check_length(List<Inventory_part_row> source, CancellationToken cancellationToken)
        {
            Dictionary<string, int> inventory_part_len = Get_limit_of_fields.inventory_part_len;
            foreach (Inventory_part_row row in source)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                row.Indeks = row.Indeks.LimitDictLen("indeks", inventory_part_len);
                row.Contract = row.Contract.LimitDictLen("contract", inventory_part_len);
                row.Opis = row.Opis.LimitDictLen("opis", inventory_part_len);
                row.Kolekcja = row.Kolekcja.LimitDictLen("kolekcja", inventory_part_len);
                row.Planner_buyer = row.Planner_buyer.LimitDictLen("planner_buyer", inventory_part_len);
                row.Rodzaj = row.Rodzaj.LimitDictLen("state_conf", inventory_part_len);                
            }
            return source;
        }
    }
}
