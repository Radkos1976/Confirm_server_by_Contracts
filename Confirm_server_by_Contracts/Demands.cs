﻿using DB_Conect;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
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

        public async Task<List<Simple_demands_row>> Get_source_list(
            string regex,
            bool create_tuple_off,
            string transaction_name,
            CancellationToken cancellationToken)
            => await Add_field_Next_day(
                await rw.Get_Ora("" +
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
                    GROUP BY PART_NO,contract,To_Date(DATE_REQUIRED)",
                        regex),
                    transaction_name,
                    cancellationToken),
                create_tuple_off,
                cancellationToken);

        public Task<List<Simple_demands_row>> Add_field_Next_day(
            List<Simple_demands_row> source,
            bool create_tuple_off,
            CancellationToken cancellationToken,
            bool Fill_Next_Day = false)
        {
            if (create_tuple_off)
            {
                limit_part_no.Clear();
            }
            if (create_tuple_off || Fill_Next_Day)
            {
                foreach (Simple_demands_row row in source)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    if (Fill_Next_Day)
                    {
                        row.Next_day = Next_DAY.Get_next_day(row.Contract, row.Date_required);
                    }
                    if (create_tuple_off)
                    {
                        bool tuple_exist = limit_part_no.Any(m => m.Item1 == row.Part_no & m.Item2 == row.Contract);
                        if (!tuple_exist)
                        {
                            limit_part_no.Add(new Tuple<string, string>(row.Part_no, row.Contract));
                        }
                    }
                }
            }

            return Task.FromResult(source);
        }

        public class Simple_demands_row : IEquatable<Simple_demands_row>, IComparable<Simple_demands_row>
        {
            public string Part_no { get; set; }
            public string Contract { get; set; }
            public DateTime Date_required { get; set; }
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
    public class Order_Demands : Update_pstgr_from_Ora<Order_Demands_row>
    {
        private readonly Update_pstgr_from_Ora<Order_Demands_row> rw;
        public Order_Demands()
        {
            rw = new Update_pstgr_from_Ora<Order_Demands_row>("MAIN");
        }
        /// <summary>
        /// Get order_demand form range in Async dictionary
        /// </summary>
        /// <param name="Task_name"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<int> Update_from_executor(string Task_name, CancellationToken cancellationToken)
        {
            int result = 0;
            Steps_executor.Register_step(Task_name);           
            using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
            {
                await conO.OpenAsync(cancellationToken);
                OracleGlobalization info = conO.GetSessionInfo();
                info.DateFormat = "YYYY-MM-DD";
                conO.SetSessionInfo(info);
                using (OracleCommand cust = new OracleCommand(oracle_get_orders, conO))
                {
                    cust.Parameters.Add(":part_no", OracleDbType.Varchar2);
                    cust.Parameters.Add(":contract", OracleDbType.Varchar2);
                    cust.Parameters.Add(":date_from", OracleDbType.Date);
                    cust.Parameters.Add(":date_end", OracleDbType.Date);
                    cust.Prepare();

                    using (NpgsqlConnection conp = new NpgsqlConnection(npC))
                    {
                        await conp.OpenAsync(cancellationToken);
                        using (NpgsqlCommand cu = new NpgsqlCommand(postgres_get_orders, conp))
                        {

                            cu.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                            cu.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                            cu.Parameters.Add("date_from", NpgsqlTypes.NpgsqlDbType.Date);
                            cu.Parameters.Add("date_end", NpgsqlTypes.NpgsqlDbType.Date);
                            cu.Prepare();

                            while (Dataset_executor.Count() > 0 && !cancellationToken.IsCancellationRequested)
                            {
                                Count_oracle_conn.Wait_for_Oracle(cancellationToken);
                                try
                                {

                                    if (cancellationToken.IsCancellationRequested) { break; }
                                    (string part_no, string contract, Tuple<DateTime?, DateTime?> dates) = Dataset_executor.Run_next();
                                    if (!(part_no.Equals("") && contract.Equals("")))
                                    {
                                        cust.Parameters[":part_no"].Value = part_no;
                                        cust.Parameters[":contract"].Value = contract;
                                        cust.Parameters[":date_from"].Value = dates.Item1;
                                        cust.Parameters[":date_end"].Value = dates.Item2;

                                        cu.Parameters["part_no"].Value = part_no;
                                        cu.Parameters["contract"].Value = contract;
                                        cu.Parameters["date_from"].Value = dates.Item1;
                                        cu.Parameters["date_end"].Value = dates.Item2;

                                        result += await Update_dataset(
                                            cust,
                                            cu,
                                            string.Format(
                                                "{0}:{1}:{2}",
                                                Task_name,
                                                part_no,
                                                contract),
                                            cancellationToken).ConfigureAwait(false);
                                        Dataset_executor.Report_end(part_no, contract);
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Loger.Log(string.Format("Err => {0}", ex.Message));
                                }
                                Count_oracle_conn.Oracle_conn_ended();
                            }
                        }
                    }
                }
            }            
            Steps_executor.End_step(Task_name);
            return result;
        }

        public async Task<int> Update_from_executor_from_list(
            string Task_name,
            List<Small_upd_demands> dataset,
            CancellationToken cancellationToken)
        {
            int result = 0;
            Steps_executor.Register_step(Task_name);            
            using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
            {
                await conO.OpenAsync(cancellationToken);
                OracleGlobalization info = conO.GetSessionInfo();
                info.DateFormat = "YYYY-MM-DD";
                conO.SetSessionInfo(info);
                using (OracleCommand cust = new OracleCommand(oracle_get_orders, conO))
                {
                    cust.Parameters.Add(":part_no", OracleDbType.Varchar2);
                    cust.Parameters.Add(":contract", OracleDbType.Varchar2);
                    cust.Parameters.Add(":date_from", OracleDbType.Date);
                    cust.Parameters.Add(":date_end", OracleDbType.Date);
                    cust.Prepare();

                    using (NpgsqlConnection conp = new NpgsqlConnection(npC))
                    {
                        await conp.OpenAsync(cancellationToken);
                        using (NpgsqlCommand cu = new NpgsqlCommand(postgres_get_orders, conp))
                        {
                            cu.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                            cu.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                            cu.Parameters.Add("date_from", NpgsqlTypes.NpgsqlDbType.Date);
                            cu.Parameters.Add("date_end", NpgsqlTypes.NpgsqlDbType.Date);
                            cu.Prepare();

                            foreach (Small_upd_demands item in dataset)
                            {
                                Count_oracle_conn.Wait_for_Oracle(cancellationToken);
                                try
                                {
                                    if (cancellationToken.IsCancellationRequested) { break; }
                                    Tuple<DateTime?, DateTime?> dates = new Tuple<DateTime?, DateTime?>(item.Min_d, item.Max_d);
                                    cust.Parameters[":part_no"].Value = item.Part_no;
                                    cust.Parameters[":contract"].Value = item.Contract;
                                    cust.Parameters[":date_from"].Value = item.Min_d;
                                    cust.Parameters[":date_end"].Value = item.Max_d;

                                    cu.Parameters["part_no"].Value = item.Part_no;
                                    cu.Parameters["contract"].Value=item.Contract;
                                    cu.Parameters["date_from"].Value = item.Min_d; 
                                    cu.Parameters["date_end"].Value = item.Max_d;

                                    result += await Update_dataset(
                                        cust,
                                        cu,
                                        string.Format(
                                            "{0}:{1}:{2}",
                                            Task_name,
                                            item.Part_no,
                                            item.Contract),
                                        cancellationToken).ConfigureAwait(false);
                                    Dataset_executor.Report_end(item.Part_no, item.Contract);
                                }
                                catch (Exception ex)
                                {
                                    Loger.Log(string.Format("Err => {0}", ex.Message));
                                }
                                Count_oracle_conn.Oracle_conn_ended();
                            }
                        }
                    }
                }
            }            
            Steps_executor.End_step(Task_name);
            return result;
        }
        private async Task<int> Update_dataset(
            OracleCommand oracleCommand,
            NpgsqlCommand npgsqlCommand,
            string Task_name,
            CancellationToken cancellationToken)
        {

            List<Order_Demands_row> Old = new List<Order_Demands_row>();
            List<Order_Demands_row> New = new List<Order_Demands_row>();
            Parallel.Invoke(
                    async () =>
                    {
                        Old = await Postgr_read_dataset(
                            npgsqlCommand,
                            Task_name,
                            cancellationToken);
                    },
                    async () =>
                    {
                        New = await Check_length(
                            await Ora_read_dataset(
                                oracleCommand,
                                Task_name,
                                cancellationToken)
                            );
                    });
            Changes_List<Order_Demands_row> Ch_dataset = await Changes(
                Old,
                New,
                new[] { "dop", "dop_lin", "int_ord", "line_no", "rel_no" },
                new[] { "dop", "dop_lin", "int_ord", "line_no", "rel_no", "id" },
                new[] { "id" },
                Task_name,
                cancellationToken);
            int result = await rw.PSTRG_Changes_to_dataTable(
                Ch_dataset,
                "ord_demands", new[] { "id" },
                null,
                null,
                Task_name,
                cancellationToken).ConfigureAwait(false);
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
        private async Task<int> Update_dataset_old(
            string part_no,
            string contract,
            Tuple<DateTime?, DateTime?> dates,
            string Task_name,
            CancellationToken cancellationToken)
        {

            List<Order_Demands_row> Old = new List<Order_Demands_row>();
            List<Order_Demands_row> New = new List<Order_Demands_row>();
            Parallel.Invoke(
                    async () =>
                    {
                        Old = await Get_postgres(
                            part_no,
                            contract,
                            dates,
                            Task_name,
                            cancellationToken);
                    },
                    async () =>
                    {
                        New = await Check_length(
                            await Get_oracle(
                                part_no,
                                contract,
                                dates,
                                Task_name,
                                cancellationToken)
                            );
                    });
            Changes_List<Order_Demands_row> Ch_dataset = await Changes(
                Old,
                New,
                new[] { "dop", "dop_lin", "int_ord", "line_no", "rel_no" },
                new[] { "dop", "dop_lin", "int_ord", "line_no", "rel_no", "id" },
                new[] { "id" },
                Task_name,
                cancellationToken);
            int result = await rw.PSTRG_Changes_to_dataTable(
                Ch_dataset,
                "ord_demands", new[] { "id" },
                null,
                null,
                Task_name,
                cancellationToken).ConfigureAwait(false);
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
        private async Task<List<Order_Demands_row>> Get_postgres(
            string part_no,
            string contract,
            Tuple<DateTime?, DateTime?> dates,
            string Task_name,
            CancellationToken cancellationToken)
            => await rw.Get_PSTGR("" +
                string.Format("select * " +
                    "from public.ord_demands " +
                    "where part_no = '{0}' AND CONTRACT = '{1}' AND DATE_REQUIRED between '{2}' and '{3}';",
                    part_no,
                    contract,
                    dates.Item1.ToString(),
                    dates.Item2.ToString()),
                Task_name,
                cancellationToken);

        private readonly string postgres_get_orders = @"select * from public.ord_demands 
                    where part_no = @part_no AND CONTRACT = @contract AND DATE_REQUIRED between @date_from and @date_end;";
        private readonly string oracle_get_orders = @"SELECT  
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
                    a.ORDER_SUPPLY_DEMAND_TYPE order_supp_dmd,
                    a.WRKC,
                    a.NEXT_WRKC,
                    a.PART_NO,
                    ifsapp.inventory_part_api.Get_Description(CONTRACT, a.PART_NO) Descr,
                    a.PART_CODE,
                    To_Date(a.DATE_REQUIRED) DATE_REQUIRED,
                    a.ORD_STATE,
                    To_Date(a.ORD_DATE) ORD_DATE,
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
                            owa_opt_lock.checksum(ROWID) chksum  
                        FROM 
                                ifsapp.shop_material_alloc_demand 
                        WHERE part_no = :part_no AND CONTRACT = :contract AND To_Date(DATE_REQUIRED) between :date_from and :date_end 
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
                        WHERE part_no = :part_no AND CONTRACT = :contract AND To_Date(DATE_REQUIRED) between :date_from and :date_end  
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
                        WHERE part_no = :part_no AND CONTRACT = :contract AND To_Date(DATE_REQUIRED) between :date_from and :date_end   
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
                            owa_opt_lock.checksum(a.ROWID||a.QTY_DEMAND||a.DATE_REQUIRED||a.STATUS_CODE) chksum  
                        FROM 
                            ifsapp.material_requis_line_demand_oe a, 
                            ifsapp.material_requis_line b 
                        WHERE b.OBJID = a.ROW_ID and a.part_no = :part_no AND a.CONTRACT = :contract AND To_Date(a.DATE_REQUIRED) between :date_from and :date_end  
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
                        WHERE part_no = :part_no AND CONTRACT = :contract AND To_Date(DATE_REQUIRED) between :date_from and :date_end  
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
                        WHERE part_no = :part_no AND CONTRACT = :contract AND To_Date(DATE_REQUIRED) between :date_from and :date_end ) a";

        /// <summary>
        /// Get Data from Oracle by part_no,contract and date range's
        /// </summary>
        /// <param name="part_no"></param>
        /// <param name="contract"></param>
        /// <param name="dates"></param>
        /// <param name="Task_name"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        private async Task<List<Order_Demands_row>> Get_oracle(
            string part_no,
            string contract,
            Tuple<DateTime?, DateTime?> dates,
            string Task_name,
            CancellationToken cancellationToken)
            => await rw.Get_Ora("" +
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
                    a.ORDER_SUPPLY_DEMAND_TYPE order_supp_dmd,
                    a.WRKC,
                    a.NEXT_WRKC,
                    a.PART_NO,
                    ifsapp.inventory_part_api.Get_Description(CONTRACT, a.PART_NO) Descr,
                    a.PART_CODE,
                    To_Date(a.DATE_REQUIRED) DATE_REQUIRED,
                    a.ORD_STATE,
                    To_Date(a.ORD_DATE) ORD_DATE,
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
                            owa_opt_lock.checksum(ROWID) chksum  
                        FROM 
                                ifsapp.shop_material_alloc_demand 
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND To_Date(DATE_REQUIRED) between '{2}' and '{3}' 
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
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND To_Date(DATE_REQUIRED) between '{2}' and '{3}' 
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
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND To_Date(DATE_REQUIRED) between '{2}' and '{3}'  
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
                            owa_opt_lock.checksum(a.ROWID||a.QTY_DEMAND||a.DATE_REQUIRED||a.STATUS_CODE) chksum  
                        FROM 
                            ifsapp.material_requis_line_demand_oe a, 
                            ifsapp.material_requis_line b 
                        WHERE b.OBJID = a.ROW_ID and a.part_no = '{0}' AND a.CONTRACT = '{1}' AND To_Date(a.DATE_REQUIRED) between '{2}' and '{3}'  
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
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND To_Date(DATE_REQUIRED) between '{2}' and '{3}'  
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
                        WHERE part_no = '{0}' AND CONTRACT = '{1}' AND To_Date(DATE_REQUIRED) between '{2}' and '{3}' ) a",
                    part_no,
                    contract,
                    dates.Item1.Value.ToString("yyyy-MM-dd"),
                    dates.Item2.Value.ToString("yyyy-MM-dd")
                    ),
                Task_name, cancellationToken);
        private Task<List<Order_Demands_row>> Check_length(List<Order_Demands_row> source)
        {
            Dictionary<string, int> order_demands_len = Get_limit_of_fields.order_demands_len;
            foreach (Order_Demands_row row in source)
            {
                row.Descr = row.Descr.LimitDictLen("descr", order_demands_len);
                row.Contract = row.Contract.LimitDictLen("contract", order_demands_len);
                row.Order_no = row.Order_no.LimitDictLen("order_no", order_demands_len);
                row.Line_no = row.Line_no.LimitDictLen("line_no", order_demands_len);
                row.Rel_no = row.Rel_no.LimitDictLen("rel_no", order_demands_len);
                row.Contract = row.Contract.LimitDictLen("contract", order_demands_len);
                row.Order_supp_dmd = row.Order_supp_dmd.LimitDictLen("order_supp_dmd", order_demands_len);
                row.Wrkc = row.Wrkc.LimitDictLen("wrkc", order_demands_len);
                row.Next_wrkc = row.Next_wrkc.LimitDictLen("next_wrkc", order_demands_len);
                row.Part_no = row.Part_no.LimitDictLen("part_no", order_demands_len);
                row.Part_code = row.Part_code.LimitDictLen("part_code", order_demands_len);
                row.Ord_state = row.Ord_state.LimitDictLen("ord_state", order_demands_len);
            }
            return Task.FromResult(source);
        }

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
            public virtual int CompareTo(Order_Demands_row other)
            {
                if (other == null)
                {
                    return 1;
                }
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
                if (second_nxt != 0)
                {
                    return second_nxt;
                }
                int rel_no = this.Rel_no.CompareTo(other.Rel_no);
                if (rel_no != 0)
                {
                    return rel_no;
                }
                int order_no = this.Order_no.CompareTo(other.Order_no);
                if (order_no != 0)
                {
                    return order_no;
                }
                return this.Id.CompareTo(other.Id);
            }

            public virtual bool Equals(Order_Demands_row other)
            {
                if (other == null) return false;
                return
                    this.Dop.Equals(other.Dop) &&
                    this.Dop_lin.Equals(other.Dop_lin) &&
                    this.Int_ord.Equals(other.Int_ord) &&
                    this.Line_no.Equals(other.Line_no) &&
                    this.Rel_no.Equals(other.Rel_no) &&
                    this.Order_no.Equals(other.Order_no) &&
                    this.Id.Equals(other.Id);
            }
        }
    }

}
