using DB_Conect;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using static Confirm_server_by_Contracts.Order_Demands;

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
            public int Chk_sum { get; set; }
            
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
                return 
                    this.Part_no.Equals(other.Part_no) && 
                    this.Contract.Equals(other.Contract) && 
                    this.Date_required.Equals(other.Date_required);
            }
        }
    }
    public class Demands : Update_pstgr_from_Ora<Demands.Demands_row>
    {
        public class Demands_row : IEquatable<Demands_row>, IComparable<Demands_row>
        {
            public string Part_no { get; set; }
            public DateTime Work_day { get; set; }
            public double Expected_leadtime { get; set; }
            public double Purch_qty { get; set; }
            public double Qty_demand { get; set; }
            public int Type_dmd { get; set; }
            public double Balance { get; set; }
            public double Bal_stock { get; set; }
            public string Koor { get; set; }
            public string Type { get; set; }
            public DateTime? Dat_shortage { get; set; }
            public Guid Id { get; set; }
            public double Chk_sum { get; set; }
            public DateTime Objversion { get; set; }
            public int Chksum { get; set; }
            public bool InDB { get; set; }
            public string Contract { get; set; }

            public int CompareTo(Demands_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                int res = this.Part_no.CompareTo(other.Part_no);
                if (res != 0) { return res; }
                int nex_res = this.Contract.CompareTo(other.Contract);
                if (nex_res != 0) { return nex_res; }
                return this.Work_day.CompareTo(other.Work_day);
            }

            public bool Equals(Demands_row other)
            {
                if (other == null) return false;
                return 
                    this.Part_no.Equals(other.Part_no) && 
                    this.Contract.Equals(other.Contract) && 
                    this.Work_day.Equals(other.Work_day);
            }
        }
    }
    public class Order_Demands: Update_pstgr_from_Ora<Order_Demands_row>
    {
        private readonly Update_pstgr_from_Ora<Order_Demands_row> rw;
        public Order_Demands()
        {
            rw = new Update_pstgr_from_Ora<Order_Demands_row>("MAIN");
        }

        public async Task<int> Update_from_executor(string  Task_name, CancellationToken cancellationToken)
        {
            int result = 0;
            Steps_executor.Register_step(Task_name);
            while (Dataset_executor.Count() > 0 && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    if (cancellationToken.IsCancellationRequested) { break; }
                    (string part_no, string contract, Tuple<DateTime?, DateTime?> dates) = Dataset_executor.Run_next();
                    if ((part_no, contract) != ("", ""))
                    {
                        result += await Update_dataset(part_no, contract, dates, string.Format("{0}:{1}:{2}", Task_name, part_no, contract), cancellationToken);
                        Dataset_executor.Report_end(part_no, contract);
                    }
                } 
                catch (Exception ex)
                {
                    Loger.Log(string.Format("Err => {0}", ex.Message)); 
                }
                             
            }
            Steps_executor.End_step(Task_name);
            return result;
        }

        /// <summary>
        /// Update selected rows in dataset
        /// </summary>
        /// <param name="part_no"></param>
        /// <param name="contract"></param>
        /// <param name="dates"></param>
        /// <param name="Task_name"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task<int> Update_dataset(string part_no, string contract,
            Tuple<DateTime?, DateTime?> dates, string Task_name, CancellationToken cancellationToken)
        {
            
            List<Order_Demands_row> Old = new List<Order_Demands_row>();
            List<Order_Demands_row> New = new List<Order_Demands_row>();
            Parallel.Invoke(
                    async() => 
                    {
                        Old = await Get_postgres(part_no, contract, dates, Task_name, cancellationToken);
                    },
                    async () =>
                    {
                        New = await Get_oracle(part_no, contract, dates, Task_name, cancellationToken);
                    });
            Changes_List<Order_Demands_row> Ch_dataset = Changes(Old, New, 
                new[] {"dop", "dop_lin", "int_ord", "line_no", "rel_no"}, 
                new[] { "dop", "dop_lin", "int_ord", "line_no", "rel_no", "id" }, 
                new[] {"id"},
                Task_name, cancellationToken);
            int result = await rw.PSTRG_Changes_to_dataTable(Ch_dataset, "ord_demands",
                        new[] { "id" }, null, null,
                        Task_name, cancellationToken);            
            return result;
        }
        /// <summary>
        /// Get Data from Postgresql by part_no,contract and date range's
        /// </summary>
        /// <param name="part_no"></param>
        /// <param name="contract"></param>
        /// <param name="dates"></param>
        /// <param name="Task_name"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task<List<Order_Demands_row>> Get_postgres(string part_no, string contract,
            Tuple<DateTime?, DateTime?> dates, string Task_name, CancellationToken cancellationToken) => await rw.Get_PSTGR("" +
                string.Format("select * " +
                    "from public.ord_demands " +
                    "where part_no = '{0}' AND CONTRACT = '{1}' AND DATE_REQUIRED between '{2}' and '{3}';", 
                    part_no, contract, dates.Item1.ToString(), dates.Item2.ToString()),
                Task_name, cancellationToken);
        /// <summary>
        /// Get Data from Oracle by part_no,contract and date range's
        /// </summary>
        /// <param name="part_no"></param>
        /// <param name="contract"></param>
        /// <param name="dates"></param>
        /// <param name="Task_name"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task<List<Order_Demands_row>> Get_oracle(string part_no, string contract,
            Tuple<DateTime?, DateTime?> dates, string Task_name, CancellationToken cancellationToken) => await rw.Get_Ora("" +
                string.Format(@"SELECT  
                    a.DOP,
                    Nvl(a.LINE_ITEM_NO,a.DOP_LIN) DOP_LIN,
                    Nvl(ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP,1),a.DATE_REQUIRED) Data_dop,
                    Nvl(ifsapp.work_time_calendar_api.Get_Work_Days_Between(
                        ifsapp.site_api.Get_Manuf_Calendar_Id(a.CONTRACT), 
                        ifsapp.dop_order_api.Get_Revised_Start_Date(a.DOP, a.DOP_LIN), 
                        ifsapp.dop_order_api.Get_Revised_Due_Date(a.DOP, 1)) + ifsapp.dop_order_api.Dop_Order_Slack(a.DOP, a.DOP_LIN) + 1, 0) DAy_shift,
                    a.ORDER_NO,
                    a.LINE_NO,
                    a.REL_NO,
                    Decode(LENGTH(TRIM(TRANSLATE(a.ORDER_NO, ' +-.0123456789',' '))),NULL,a.ORDER_NO,owa_opt_lock.checksum(a.ORDER_NO)*-1) int_ord,
                    a.CONTRACT,
                    a.ORDER_SUPPLY_DEMAND_TYPE,
                    a.WRKC,
                    a.NEXT_WRKC,
                    a.PART_NO,
                    ifsapp.inventory_part_api.Get_Description(CONTRACT, a.PART_NO) Descr,
                    a.PART_CODE,
                    a.DATE_REQUIRED,
                    a.ORD_STATE,
                    a.ORD_DATE,
                    a.PROD_QTY,
                    a.QTY_SUPPLY,
                    a.QTY_DEMAND,
                    To_Date(creat_date) dat_creat,
                    chksum 
                    from 
                    (
                        SELECT 
                            Nvl(ifsapp.dop_supply_shop_ord_api.Get_C_Dop_Id(order_no, line_no, rel_no), 0) DOP,
                            Nvl(REPLACE(SubStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no),
                                InStr(ifsapp.shop_ord_api.Get_Source(order_no, line_no, rel_no), '^', 10)), '^', ''), 0) DOP_LIN,
                            order_no,
                            line_no,
                            rel_no,
                            LINE_ITEM_NO,
                            ifsapp.shop_ord_api.Get_Part_No(order_no, line_no, rel_no) Cust_part_no,
                            CONTRACT,
                            ORDER_SUPPLY_DEMAND_TYPE,
                            ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, 
                                ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 1, 0)) WRKC,
                            ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(order_no, line_no, rel_no, 
                                ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(order_no, line_no, rel_no, 2, 0)) NEXT_WRKC,
                            PART_NO,
                            STATUS_CODE PART_CODE,
                            DATE_REQUIRED,
                            ifsapp.shop_ord_api.Get_State(order_no, line_no, rel_no) ORD_STATE,
                            ifsapp.shop_ord_api.Get_Revised_Due_Date(order_no, line_no, rel_no) ORD_date,
                            Nvl(ifsapp.shop_ord_api.Get_Revised_Qty_Due(order_no, line_no, rel_no)-ifsapp.shop_ord_api.Get_Qty_Complete(order_no, line_no, rel_no), 0) Prod_QTY,
                            0 QTY_SUPPLY,
                            QTY_DEMAND,
                            ifsapp.shop_ord_api.Get_Date_Entered(order_no, line_no, rel_no) creat_date,
                            owa_opt_lock.checksum(ROWID||QTY_DEMAND||QTY_PEGGED||QTY_RESERVED||To_Char(ifsapp.shop_order_operation_api.Get_Op_Start_Date(order_no,line_no,rel_no,ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op (order_no,line_no,rel_no,2,0)),'YYYYMMDDHH24miss')) chksum  
                        FROM 
                                ifsapp.shop_material_alloc_demand 
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND DATE_REQUIRED between '{2}' and '{3}' 
                        UNION ALL 
                        sELECT To_Number(ORDER_NO) DOP,
                            LINE_NO DOP_LIN,
                            '0' ORDER_NO,
                            '*' LINE_NO,
                            '*' REL_NO,
                            LINE_ITEM_NO,
                            NULL Cust_part_no,
                            CONTRACT,
                            ORDER_SUPPLY_DEMAND_TYPE,
                            ' ' WRKC,
                            ' ' NEXT_WRKC,
                            PART_NO,
                            STATUS_CODE PART_CODE,
                            DATE_REQUIRED,
                            ifsapp.dop_head_api.Get_Status(ORDER_NO) ORD_STATE,
                            ifsapp.dop_head_api.Get_Due_Date(ORDER_NO) ORD_DATE,
                            ifsapp.dop_head_api.Get_Qty_Demand(order_no) PROD_QTY,
                            0 QTY_SUPPLY,
                            QTY_DEMAND,
                            NULL creat_date,
                            owa_opt_lock.checksum(order_no||QTY_DEMAND||DATE_REQUIRED||ORDER_NO||LINE_NO||INFO) chksum  
                        FROM 
                            ifsapp.dop_order_demand_ext 
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND DATE_REQUIRED between '{2}' and '{3}' 
                        UNION ALL 
                        SELECT 
                            ifsapp.customer_order_line_api.Get_Pre_Accounting_Id(ORDER_NO, LINE_NO, REL_NO, LINE_ITEM_NO) DOP,
                            '0' DOP_LIN,
                            ORDER_NO,
                            LINE_NO,
                            REL_NO,
                            LINE_ITEM_NO,
                            PART_NO Cust_part_no,
                            CONTRACT,
                            ORDER_SUPPLY_DEMAND_TYPE,
                            ' ' WRKC,
                            ' ' NEXT_WRKC,
                            PART_NO,
                            STATUS_CODE PART_CODE,
                            DATE_REQUIRED,
                            STATUS_CODE ORD_STATE,
                            DATE_REQUIRED ORD_DATE,
                            0 PROD_QTY,
                            0 QTY_SUPPLY,
                            QTY_DEMAND,
                            ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date,
                            owa_opt_lock.checksum(ROW_ID||QTY_DEMAND||DATE_REQUIRED||QTY_PEGGED||QTY_RESERVED) chksum 
                        FROM 
                            ifsapp.customer_order_line_demand_oe 
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND DATE_REQUIRED between '{2}' and '{3}'  
                        UNION ALL 
                        SELECT 
                            0 DOP,
                            '0' DOP_LIN,
                            a.ORDER_NO,
                            a.LINE_NO,
                            a.REL_NO,
                            a.LINE_ITEM_NO,
                            a.PART_NO Cust_part_no,
                            a.CONTRACT,
                            a.ORDER_SUPPLY_DEMAND_TYPE,
                            ' ' WRKC,
                            ' ' NEXT_WRKC,
                            a.PART_NO,
                            a.STATUS_CODE PART_CODE,
                            a.DATE_REQUIRED,
                            a.STATUS_CODE ORD_STATE,
                            a.DATE_REQUIRED ORD_DATE,
                            0 PROD_QTY,
                            0 QTY_SUPPLY,
                            a.QTY_DEMAND,
                            b.DATE_ENTERED creat_date,
                            owa_opt_lock.checksum(a.ROWID||QTY_DEMAND||DATE_REQUIRED||a.STATUS_CODE) chksum  
                        FROM 
                            ifsapp.material_requis_line_demand_oe a, 
                            ifsapp.material_requis_line b 
                        WHERE b.OBJID = a.ROW_ID and a.part_no = '{0}' AND a.CONTRACT = '{1}' AND a.DATE_REQUIRED between '{2}' and '{3}'  
                        UNION ALL  
                        SELECT 
                            0 DOP,
                            '0' DOP_LIN,
                            ORDER_NO,
                            LINE_NO,
                            REL_NO,
                            LINE_ITEM_NO,
                            PART_NO Cust_part_no,
                            CONTRACT,
                            ORDER_SUPPLY_DEMAND_TYPE,
                            ' ' WRKC,
                            ' ' NEXT_WRKC,
                            PART_NO,
                            STATUS_CODE PART_CODE,
                            DATE_REQUIRED,
                            STATUS_CODE ORD_STATE,
                            DATE_REQUIRED ORD_DATE,
                            0 PROD_QTY,
                            QTY_SUPPLY,
                            0 QTY_DEMAND,
                            ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date,
                            owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED) chksum 
                        FROM 
                            ifsapp.purchase_order_line_supply 
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND DATE_REQUIRED between '{2}' and '{3}'  
                        UNION ALL 
                        SELECT 
                            0 DOP,
                            '0' DOP_LIN,
                            ORDER_NO,
                            LINE_NO,
                            REL_NO,
                            LINE_ITEM_NO,
                            PART_NO Cust_part_no,
                            CONTRACT,
                            ORDER_SUPPLY_DEMAND_TYPE,
                            ' ' WRKC,
                            ' ' NEXT_WRKC,
                            PART_NO,
                            STATUS_CODE PART_CODE,
                            DATE_REQUIRED,
                            STATUS_CODE ORD_STATE,
                            DATE_REQUIRED ORD_DATE,
                            0 PROD_QTY,
                            QTY_SUPPLY,
                            0 QTY_DEMAND,
                            ifsapp.customer_order_line_api.Get_Date_Entered(order_no, line_no, rel_no, line_item_no) creat_date,
                            owa_opt_lock.checksum(ROWID||QTY_SUPPLY||DATE_REQUIRED||STATUS_CODE) chksum 
                        FROM 
                            ifsapp.ARRIVED_PUR_ORDER_EXT 
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND DATE_REQUIRED between '{2}' and '{3}' ) a",
                    part_no, 
                    contract, 
                    dates.Item1.Value.ToString("yyyy-MM-dd"), 
                    dates.Item2.Value.ToString("yyyy-MM-dd")
                    ),
                Task_name, cancellationToken);

        public class Order_Demands_row : IEquatable<Order_Demands_row>, IComparable<Order_Demands_row>
        {
            public int Dop { get; set; }
            public int Dop_lin { get; set; }
            public DateTime Data_dop { get; set; }
            public int Day_shift { get; set; }
            public string Order_no { get; set; }
            public string Line_no { get; set; }
            public string Rel_no { get; set; }
            public int Int_ord { get; set; }
            public string Contract { get; set; }
            public string Order_supp_dmd { get; set; }
            public string Wrkc { get; set; }
            public string Next_wrkc { get; set; }
            public string Part_no { get; set; }
            public string Descr { get; set; }
            public string Part_code { get; set; }
            public DateTime Date_required { get; set; }
            public string Ord_state { get; set; }
            public DateTime Ord_date { get; set; }
            public double Prod_qty { get; set; }
            public double Qty_supply { get; set; }
            public double Qty_demand { get; set; }
            public DateTime? Dat_creat { get; set; }
            public int Chksum { get; set; }
            public Guid Id { get; set; }

            //dop desc,dop_lin,int_ord,LINE_NO,REL_NO
            public int CompareTo(Order_Demands_row other)
            {
                int res = this.Dop.CompareTo(other.Dop);
                if (res != 0) 
                { 
                    return res; 
                }
                int nex_res = this.Dop_lin.CompareTo(other.Dop_lin);
                if (nex_res != 0) 
                {
                    return nex_res; 
                }
                int second = this.Int_ord.CompareTo(other.Int_ord);
                if (second != 0) 
                { 
                    return second; 
                }
                int second_nxt = this.Line_no.CompareTo(other.Line_no);
                if (second_nxt !=  0) 
                { 
                    return second_nxt; 
                }
                return 
                    this.Rel_no.CompareTo(other.Rel_no);
            }

            public bool Equals(Order_Demands_row other)
            {
                return 
                    this.Dop.Equals(other.Dop) && 
                    this.Dop_lin.Equals(other.Dop_lin) && 
                    this.Int_ord.Equals(other.Int_ord) && 
                    this.Line_no.Equals(other.Line_no) && 
                    this.Rel_no.Equals(other.Rel_no);
            }
        }
    }

}
