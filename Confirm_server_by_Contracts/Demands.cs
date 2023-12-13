using DB_Conect;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Confirm_server_by_Contracts
{
    public class Demands: Update_pstgr_from_Ora<Demands.Simple_demands_row>
    {
        private readonly Update_pstgr_from_Ora<Simple_demands_row> rw;
        public async Task<List<Simple_demands_row>> Get_source_list(string regex) => await rw.Get_Ora("" +
            String.Format(@"SELECT 
                PART_NO,
                To_Date(DATE_REQUIRED) DATE_REQUIRED,
                round(Sum(QTY_SUPPLY),10) QTY_SUPPLY,
                round(Sum(QTY_DEMAND),10) QTY_DEMAND,
                Nvl(round(Sum(QTY_DEMAND_ZAM),10),0) DEMAND_ZAM,
                Nvl(round(Sum(QTY_DEMAND_DOP),10),0) QTY_DEMAND_DOP,
                ifsapp.work_time_calendar_api.Get_Next_Work_Day(ifsapp.site_api.Get_Manuf_Calendar_Id('ST'),To_Date(DATE_REQUIRED)) NextDay,
                Sum(chksum) chk_sum 
            FROM 
                (SELECT 
                    PART_NO,
                    DATE_REQUIRED,
                    0 QTY_SUPPLY,
                    QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(a.ROWID) chksum 
                FROM 
                    ifsapp.shop_material_alloc_demand a 
                WHERE regexp_like(part_no, {0})   
                UNION ALL  
                SELECT 
                    PART_NO,
                    DATE_REQUIRED,
                    0 QTY_SUPPLY,
                    QTY_DEMAND,
                    QTY_DEMAND QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(order_no||QTY_DEMAND||DATE_REQUIRED||ORDER_NO||LINE_NO||INFO) chksum
                FROM 
                    ifsapp.dop_order_demand_ext 
                WHERE regexp_like(part_no, {0}) 
                UNION ALL 
                SELECT 
                    PART_NO,
                    DATE_REQUIRED,
                    0 QTY_SUPPLY,
                    QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    QTY_DEMAND QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(ROW_ID||QTY_DEMAND||DATE_REQUIRED||QTY_PEGGED||QTY_RESERVED) chksum 
                FROM 
                    ifsapp.customer_order_line_demand_oe 
                WHERE regexp_like(part_no, {0}) 
                UNION ALL
                SELECT
                    PART_NO,
                    DATE_REQUIRED,
                    0 QTY_SUPPLY,
                    QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(ROWID||QTY_DEMAND||DATE_REQUIRED||STATUS_CODE) chksum 
                FROM 
                    ifsapp.material_requis_line_demand_oe 
                WHERE regexp_like(part_no, {0}) 
                UNION ALL  
                SELECT
                    PART_NO,
                    sysdate DATE_REQUIRED,
                    QTY_SUPPLY,
                    0 QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED||STATUS_CODE) chksum 
                FROM 
                    ifsapp.ARRIVED_PUR_ORDER_EXT  
                WHERE regexp_like(part_no, {0})  
                UNION ALL  
                SELECT 
                    PART_NO, 
                    DATE_REQUIRED,
                    QTY_SUPPLY,
                    0 QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum 
                FROM 
                    ifsapp.purchase_order_line_supply  
                WHERE regexp_like(part_no, {0})
               GROUP BY PART_NO, To_Date(DATE_REQUIRED)", regex), "Get_demands_from_IFS");
                            
        public class Simple_demands_row : IEquatable<Simple_demands_row>, IComparable<Simple_demands_row>
        {
            public string Part_no { get; set; }
            public string Contract {  get; set; }
            public DateTime Date_required {  get; set; }
            public double Qty_supply { get; set; }
            public double QTY_DEMAND { get; set; }
            public double DEMAND_ZAM { get; set; }
            public double QTY_DEMAND_DOP { get; set; }
            public DateTime NextDay { get; set; }
            public long chk_sum { get; set; }

            public int CompareTo(Simple_demands_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    int main = this.Part_no.CompareTo(other.Part_no);
                    if (main != 0)
                    {
                        return main;
                    }
                    return this.Contract.CompareTo(other.Contract);
                }
            }
            /// <summary>
            /// Default Equality  check b
            /// </summary>
            /// <param name="other"></param>
            /// <returns></returns>
            public bool Equals(Simple_demands_row other)
            {
                if (other == null) return false;
                return (this.Part_no.Equals(other.Part_no) && this.Contract.Equals(other.Contract));
            }
        }
    }
}
