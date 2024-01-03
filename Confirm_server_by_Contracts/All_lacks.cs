using DB_Conect;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using static Confirm_server_by_Contracts.Order_Demands;

namespace Confirm_server_by_Contracts
{
    /// <summary>
    /// Update All all Lack reports and formatka materialized view
    /// Class  self update after initialization
    /// </summary>
    public class All_lacks : Update_pstgr_from_Ora<Order_Demands_row>
    {
        private readonly Update_pstgr_from_Ora<Order_Demands_row> rw;
        private readonly Update_pstgr_from_Ora<Orders_lacks> lacks;
        public All_lacks(CancellationToken cancellationToken)
        {
            rw = new Update_pstgr_from_Ora<Order_Demands_row>("MAIN");
            lacks = new Update_pstgr_from_Ora<Orders_lacks>("MAIN");
            Parallel.Invoke(
            async () => {
                Steps_executor.Register_step("All_lacks");
                await Update_All_lacks(cancellationToken);
                Run_query query = new Run_query();
                await query.Execute_in_Postgres(new[] { "REFRESH MATERIALIZED VIEW formatka; " }, "All_lacks", cancellationToken);
                Steps_executor.End_step("All_lacks");
                query = null;
            });
        }
            

        public async Task<int> Update_All_lacks(CancellationToken cancellationToken)
        {
            return await rw.PSTRG_Changes_to_dataTable(
                rw.Changes(
                    await Get_source(cancellationToken),
                    await New_data(cancellationToken), 
                    new[] { "dop", "dop_lin", "int_ord", "line_no", "rel_no" },
                    new[] { "dop", "dop_lin", "int_ord", "line_no", "rel_no", "id" },
                    new[] { "id" },
                    "All_lacks",
                    cancellationToken
                    ),
                "ord_lack",
                new[] { "id" }, 
                null, 
                null, 
                "All_lacks", 
                cancellationToken 
                );
        }
        private async Task<List<Order_Demands_row>> Get_source(CancellationToken cancellationToken) => await rw.Get_PSTGR("Select * from ord_lacks", "All_lacks", cancellationToken);
        private async Task<List<Order_Demands_row>> New_data(CancellationToken cancellationToken)
        {
            List<Order_Demands_row> Returned = new List<Order_Demands_row>();
            List<Orders_lacks> _lacks = await lacks.Get_PSTGR("" +
                @"select 
                b.*,
                a.lack bal_stock 
                from 
                (
                    select 
                        a.part_no,
                        a.contract,
                        a.work_day,
                        b.min_lack,
                        a.bal_stock *-1 lack,
                        case when a.work_day=b.min_lack then case when a.bal_stock*-1=a.qty_demand then 'All' else 'Part' end else 'All' end stat_lack 
                    from 
                    (
                        select 
                            part_no,
                            contract,
                            work_day,
                            qty_demand,
                            bal_stock 
                        from 
                            demands 
                        where bal_stock<0 and qty_demand!=0 and work_day<date_fromnow(10,contract) and koor!='LUCPRZ'
                    ) a 
                	left join 
                	(
                        select 
                            part_no,
                            contract,
                            min(work_day) min_lack 
                        from 
                            demands 
                        where bal_stock<0 and qty_demand!=0 and work_day<date_fromnow(10,contract) group by part_no,contract
                    ) b 
                    on b.part_no=a.part_no and b.contract=a.contract 
                    where case when a.work_day=b.min_lack then case when a.bal_stock*-1=a.qty_demand then 'All' else 'Part' end else 'All' end='Part'
                ) a,
                ord_demands b 
                where b.part_no=a.part_no and b.contract=a.contract and b.date_required=a.work_day order by part_no,date_required,int_ord desc"
                , "All_lacks", cancellationToken)
                .ConfigureAwait(false);
            string Part_no = "";
            string Contract = "";
            DateTime nullDAT = Loger.Started().AddDays(1000);
            DateTime part_dat = nullDAT;
            double bil = 0;
            foreach (Orders_lacks _lack in _lacks)
            {
                if (cancellationToken.IsCancellationRequested) { break; }
                if ((Part_no, Contract, part_dat) != (_lack.Part_no, _lack.Contract, _lack.Date_required))
                {
                    Part_no = _lack.Part_no;
                    part_dat = _lack.Date_required;
                    Contract = _lack.Contract;
                    bil = _lack.Bal_stock;
                }
                if (bil > 0)
                {
                    Returned.Add(new Order_Demands_row
                    {
                        Dop = _lack.Dop,
                        Dop_lin = _lack.Dop_lin,
                        Data_dop = _lack.Data_dop,
                        Day_shift = _lack.Day_shift,
                        Order_no = _lack.Order_no,
                        Line_no = _lack.Line_no,
                        Rel_no = _lack.Rel_no,
                        Int_ord = _lack.Int_ord,
                        Contract = _lack.Contract,
                        Order_supp_dmd = _lack.Order_supp_dmd,
                        Wrkc = _lack.Wrkc,
                        Next_wrkc = _lack.Next_wrkc,
                        Part_no = _lack.Part_no,
                        Descr = _lack.Descr,
                        Part_code = _lack.Part_code,
                        Date_required = _lack.Date_required,
                        Ord_state = _lack.Ord_state,
                        Ord_date = _lack.Ord_date,
                        Prod_qty = _lack.Prod_qty,
                        Qty_supply = _lack.Qty_supply,
                        Qty_demand = _lack.Qty_demand,
                        Dat_creat = _lack.Dat_creat,
                        Chksum = _lack.Chksum,
                        Id = _lack.Id,
                    });
                    bil -= _lack.Qty_demand;
                }
            }
            return Returned;            
        }
        public class Orders_lacks : Order_Demands_row
        {
            public double Bal_stock { get; set; }
        }
    } 
}
