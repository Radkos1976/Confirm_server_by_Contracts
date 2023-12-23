using DB_Conect;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static Confirm_server_by_Contracts.Inventory_part;


namespace Confirm_server_by_Contracts
{
    public class Simple_Demands : Update_pstgr_from_Ora<Simple_Demands.Simple_demands_row>
    {
        private readonly Update_pstgr_from_Ora<Simple_demands_row> rw;

        public readonly List<Tuple<string, string>> limit_part_no = new List<Tuple<string, string>>();
        public Simple_Demands()
        {
            rw = new Update_pstgr_from_Ora<Simple_demands_row>("MAIN");
        }

        public async Task<List<Simple_demands_row>> Get_source_list(string regex, bool create_tuple_off, string transaction_name, CancellationToken cancellationToken) => Add_field_Next_day(await rw.Get_Ora("" +
            string.Format(@"SELECT 
                PART_NO,
                contract,
                To_Date(DATE_REQUIRED) DATE_REQUIRED,
                round(Sum(QTY_SUPPLY),10) QTY_SUPPLY,
                round(Sum(QTY_DEMAND),10) QTY_DEMAND,
                Nvl(round(Sum(QTY_DEMAND_ZAM),10),0) DEMAND_ZAM,
                Nvl(round(Sum(QTY_DEMAND_DOP),10),0) QTY_DEMAND_DOP,                
                Sum(chksum) chk_sum 
            FROM 
                (SELECT 
                    PART_NO,
                    contract,
                    DATE_REQUIRED,
                    0 QTY_SUPPLY,
                    QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(a.ROWID) chksum 
                FROM 
                    ifsapp.shop_material_alloc_demand a 
                WHERE regexp_like(part_no, '{0}')   
                UNION ALL  
                SELECT 
                    PART_NO,
                    contract,
                    DATE_REQUIRED,
                    0 QTY_SUPPLY,
                    QTY_DEMAND,
                    QTY_DEMAND QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(order_no||QTY_DEMAND||DATE_REQUIRED||ORDER_NO||LINE_NO||INFO) chksum
                FROM 
                    ifsapp.dop_order_demand_ext 
                WHERE regexp_like(part_no, '{0}') 
                UNION ALL 
                SELECT 
                    PART_NO,
                    contract,
                    DATE_REQUIRED,
                    0 QTY_SUPPLY,
                    QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    QTY_DEMAND QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(ROW_ID||QTY_DEMAND||DATE_REQUIRED||QTY_PEGGED||QTY_RESERVED) chksum 
                FROM 
                    ifsapp.customer_order_line_demand_oe 
                WHERE regexp_like(part_no, '{0}') 
                UNION ALL
                SELECT
                    PART_NO,
                    contract,
                    DATE_REQUIRED,
                    0 QTY_SUPPLY,
                    QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(ROWID||QTY_DEMAND||DATE_REQUIRED||STATUS_CODE) chksum 
                FROM 
                    ifsapp.material_requis_line_demand_oe 
                WHERE regexp_like(part_no, '{0}') 
                UNION ALL  
                SELECT
                    PART_NO,
                    contract,
                    sysdate DATE_REQUIRED,
                    QTY_SUPPLY,
                    0 QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED||STATUS_CODE) chksum 
                FROM 
                    ifsapp.ARRIVED_PUR_ORDER_EXT  
                WHERE regexp_like(part_no, '{0}')  
                UNION ALL  
                SELECT 
                    PART_NO, 
                    contract,
                    DATE_REQUIRED,
                    QTY_SUPPLY,
                    0 QTY_DEMAND,
                    0 QTY_DEMAND_DOP,
                    0 QTY_DEMAND_ZAM,
                    owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum 
                FROM 
                    ifsapp.purchase_order_line_supply  
                WHERE regexp_like(part_no, '{0}')
                )
            GROUP BY PART_NO,contract,To_Date(DATE_REQUIRED)", regex), transaction_name, cancellationToken), create_tuple_off, cancellationToken);

        public List<Simple_demands_row> Add_field_Next_day(List<Simple_demands_row> source, bool create_tuple_off, CancellationToken cancellationToken)
        {
            if (create_tuple_off)
            {
                limit_part_no.Clear();
            }
            foreach (Simple_demands_row row in source)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                row.Next_day = Next_DAY.Get_next_day(row.Contract, row.Date_required);
                if (create_tuple_off)
                {
                    bool tuple_exist = limit_part_no.Any(m => m.Item1 == row.Part_no & m.Item2 == row.Contract);
                    if (!tuple_exist)
                    {
                        limit_part_no.Add(new Tuple<string, string>(row.Part_no, row.Contract));
                    }
                }
            }
            return source;
        }       
                            
        public class Simple_demands_row : IEquatable<Simple_demands_row>, IComparable<Simple_demands_row>
        {           
            public string Part_no { get; set; }
            public string Contract {  get; set; }
            public DateTime Date_required {  get; set; }
            public double Qty_supply { get; set; }
            public double QTY_DEMAND { get; set; }
            public double DEMAND_ZAM { get; set; }
            public double QTY_DEMAND_DOP { get; set; }
            public DateTime Next_day { get; set; } = DateTime.Now;
            public long Chk_sum { get; set; }
            
            public int CompareTo(Simple_demands_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {  
                    int res = this.Part_no.CompareTo(other.Part_no);
                    if (res != 0) { return res; }
                    int nex_res = this.Contract.CompareTo(other.Contract);
                    if (nex_res != 0) { return nex_res; }
                    return this.Date_required.CompareTo(other.Date_required);
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
                return this.Part_no.Equals(other.Part_no) && this.Contract.Equals(other.Contract) && this.Date_required.Equals(other.Date_required);
            }
        }
    }
    public class Demands
    {
        public class Demands_row
        {

        }
    }

}
