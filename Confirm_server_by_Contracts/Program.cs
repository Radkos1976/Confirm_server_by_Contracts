using DB_Conect;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Confirm_server_by_Contracts
{
    internal class Program
    {
        
        static void Main(string[] args)
        {
            Loger.Srv_start();
            CancellationToken active_token = Steps_executor.cts.Token;
            
            Parallel.Invoke(
            new ParallelOptions { MaxDegreeOfParallelism = 50 },
            new Action[]
            {
                () =>
                {
                    // migrate demands and inventory_part and run main_loop for each non 616%  part_no
                    Steps_executor.Register_step("Demands 616 ");
                    Simple_Demands part_616 = new Simple_Demands();

                    List<Simple_Demands.Simple_demands_row> only_616 = new List<Simple_Demands.Simple_demands_row>();
                    List<Tuple<string,string>> part_no_tup = new List<Tuple<string,string>>();
                    List<Tuple<string,string>> part_no_zero_tup = new List<Tuple<string,string>>();
                    List<Inventory_part.Inventory_part_row>  pstgr = new List<Inventory_part.Inventory_part_row>();
                    List<Inventory_part.Inventory_part_row>  oracle= new List<Inventory_part.Inventory_part_row>();

                    Parallel.Invoke(
                    async () =>
                    {
                        only_616 = await part_616.Get_source_list("^616.*", true, "Demands for 616 ", active_token);
                        Steps_executor.End_step("Demands 616 ");
                    },
                    async () =>
                    {
                        Steps_executor.Register_step("Inventory part 616 ");
                        Steps_executor.Register_step("Inventory part 616 presets ");
                        Inventory_part inventory_616_in_PSTGR = new Inventory_part("^616.*", true, null);
                        List<Inventory_part.Inventory_part_row> pstgr_presets = await inventory_616_in_PSTGR.Get_PSTGR_List("Inventory part 616 presets ", active_token);

                        (part_no_tup, part_no_zero_tup) = inventory_616_in_PSTGR.Get_tuple_of_part_no(pstgr_presets);
                        if (part_no_tup.Count > 0)
                        {
                            inventory_616_in_PSTGR.limit_part_no  = part_no_tup;
                            oracle = await inventory_616_in_PSTGR.Get_Ora_list("Inventory part 616 presets ", active_token);
                            await inventory_616_in_PSTGR.Update_dataset(
                                pstgr_presets,
                                oracle,
                                "Inventory part 616 ",
                                active_token);
                        }
                        // add to part no tuple parts with weistent inventory
                        (part_no_tup, part_no_zero_tup) = inventory_616_in_PSTGR.Get_tuple_of_part_no(oracle);
                        Steps_executor.End_step("Inventory part 616 presets ");
                        inventory_616_in_PSTGR = null;
                        pstgr_presets = null;
                    });

                    bool end_with_no_err1 = Steps_executor.Wait_for(new string[] { "Demands 616 ",  "Inventory part 616 presets " }, "Inventory part 616 ", active_token);
                    if (end_with_no_err1)
                    {
                        Inventory_part inventory_616 = new Inventory_part("^616.*", false, part_616.limit_part_no);
                        part_616 = null;
                        oracle = inventory_616.Limit_in_and_create_Except(part_no_tup, part_no_zero_tup, oracle, false);
                        Parallel.Invoke(
                        async () =>
                        {
                            pstgr = await inventory_616.Get_PSTGR_List("Inventory part 616 ", active_token);
                        },
                        async () =>
                        {
                            List<Inventory_part.Inventory_part_row> tmp = await inventory_616.Get_Ora_list("Inventory part 616 ", active_token);
                            oracle.AddRange(tmp);
                            oracle.Sort();
                            await inventory_616.Update_dataset(
                                pstgr,
                                oracle,
                                "Inventory part 616 ",
                                active_token);
                            Steps_executor.End_step("Inventory part 616 ");
                        });
                        inventory_616 = null;
                        bool end_with_no_err = Steps_executor.Wait_for(new string[] { "Demands 616 ", "Inventory part 616 " }, "Main_loop 616 ", active_token);
                        if (end_with_no_err)
                        {
                            Steps_executor.Register_step("Main_loop 616 ");
                            Main_loop main_Loop = new Main_loop();
                            int result = main_Loop.Update_Main_Tables("^616.*", "Main_loop 616 ", only_616, oracle, active_token);
                            main_Loop.Get_thre_workers("Main_loop 616 Executor", active_token);
                            only_616 = null;
                            oracle = null;
                            main_Loop = null;
                        }
                    }
                },
                () =>
                {
                    //  migrate customer orders                        
                    Cust_orders cust_Orders = new Cust_orders(true, active_token);
                    cust_Orders = null;
                },
                () =>
                {
                    // migrate all calendars                        
                    Calendar calendar = new Calendar(true, active_token);
                    calendar = null;
                },
                () =>
                {
                    // migrate demands and inventory_part and run main_loop for each non 616%  part_no
                    Simple_Demands part_except_616 = new Simple_Demands();
                    Inventory_part inventory_except_616 = new Inventory_part("^(5|6[^616]).+", false, null);
                    List<Simple_Demands.Simple_demands_row> no_616 = new List<Simple_Demands.Simple_demands_row>();
                    List<Inventory_part.Inventory_part_row>  pstgr = new List<Inventory_part.Inventory_part_row>();
                    List<Inventory_part.Inventory_part_row>  oracle= new List<Inventory_part.Inventory_part_row>();
                    Parallel.Invoke(
                    async () =>
                    {
                        Steps_executor.Register_step("Demands except 616 ");
                        no_616 = await part_except_616.Get_source_list("^(5|6[^616]).+", false, "Demands except 616 ", active_token);
                        Steps_executor.End_step("Demands except 616 ");
                        part_except_616 = null;
                    },
                    async () =>
                    {
                        Steps_executor.Register_step("Inventory part except 616 ");
                        Parallel.Invoke(
                            async () =>
                            {
                                oracle = await inventory_except_616.Get_Ora_list("Inventory part except 616 ", active_token);
                            },
                            async () =>
                            {
                                pstgr = await inventory_except_616.Get_PSTGR_List("Inventory part except 616 ", active_token);
                            });
                        int result = await inventory_except_616.Update_dataset(
                            pstgr,
                            oracle,
                            "Inventory part except 616 ",
                            active_token);
                        Steps_executor.End_step("Inventory part except 616 ");
                        inventory_except_616 = null;
                        pstgr = null;
                    });
                    bool end_with_no_err = Steps_executor.Wait_for(new string[] { "Demands except 616 ", "Inventory part except 616 " }, "Main_loop except 616 ", active_token);
                    if (end_with_no_err)
                    {
                        Steps_executor.Register_step("Main_loop except 616 ");
                        Main_loop main_Loop = new Main_loop();
                        int result = main_Loop.Update_Main_Tables("^(5|6[^616]).+", "Main_loop except 616 ", no_616, oracle, active_token);
                            Parallel.Invoke(
                            () =>
                            {
                                main_Loop.Get_thre_workers("Main_loop first 616 Executor", active_token);
                            },
                            () => {
                                main_Loop.Get_thre_workers("Main_loop second 616 Executor", active_token);
                            });
                        no_616 = null;
                        oracle = null;
                        main_Loop = null;
                    }
                }
            });
            Steps_executor.Register_step("Prepare data for Reports");
            bool with_no_err = Steps_executor.Wait_for(new string[] { "Main_loop except 616 ", "Main_loop 616 ", 
                "Main_loop except 616 Executor1", "Main_loop except 616 Executor2" , "Main_loop except 616 Executor3",
                "Main_loop first 616 Executor1", "Main_loop first 616 Executor2", "Main_loop first 616 Executor3",
                "Main_loop second 616 Executor1", "Main_loop second 616 Executor2", "Main_loop second 616 Executor3",
                "Calendar", "cust_ord"}, "Prepare data for Reports", active_token);

            if (with_no_err)
            {
                if  (Dataset_executor.Count()  > 0)
                {
                    Loger.Log("Error on Dataset_executor => some tasks are not calulated");
                    Thread.Sleep(1000);
                }
                Dataset_executor.Clear();

                Run_query query = new Run_query();
                Parallel.Invoke(
                        async () =>
                        {
                            await query.Execute_in_Postgres(new[] {
                    "REFRESH MATERIALIZED VIEW bilans_val" }, "Refresh bilans_val", active_token);
                        });
                Parallel.Invoke(
                async () =>
                {
                    await query.Execute_in_Postgres(new[] {
                    @"update public.cust_ord a
                    SET zest = case when a.dop_connection_db = 'AUT' then
                     case when a.line_state= 'Aktywowana' then
                         case when dop_made = 0 then
                            case when substring(a.part_no,1,1) not in ('5','6','2') 
                            then b.zs
                            else null	end
                          else null end
                     else null end else null end
                     from
                        (select ZEST_ID, CASE WHEN zest>1 THEN zest_id ELSE null END as zs
                            from
                               (select a.order_no, a.line_no, b.zest, a.order_no||'_'||coalesce(a.customer_po_line_no, a.line_no)||'_'||a.prom_week ZEST_ID
                                  from
                                     cust_ord a
                                      left join
                                      (select id, count(zest) zest
                                          from
                                             (select order_no||'_'||coalesce(customer_po_line_no, line_no)||'_'||prom_week id, part_no zest
                                               from cust_ord
                                         where line_state!= 'Zarezerwowana' and dop_connection_db = 'AUT' and seria0 = false
                                         and data0 is null group by order_no||'_'||coalesce(customer_po_line_no, line_no)||'_'||prom_week, part_no ) a
                                      group by id) b
                                      on b.id=a.order_no||'_'||coalesce(a.customer_po_line_no, a.line_no)||'_'||a.prom_week
                                 where substring(part_no,1,1) not in ('5','6','2') ) a) b
                          where a.order_no||'_'||coalesce(a.customer_po_line_no, a.line_no)||'_'||a.prom_week=b.ZEST_ID"
                    ,
                    @"DELETE FROM public.ord_demands 
                    where ord_demands.id in 
                    (
                    select 
                        id 
                    from
                    (
                        (
                            select 
                                ord_demands.id,
                                part_no,
                                contract,
                                date_required 
                            from 
                                ord_demands
                        ) a 
                        left join 
                        (select part_no,contract,work_day from demands) b 
                        on b.part_no=a.part_no and b.contract=a.contract and b.work_day=a.date_required) 
                    where work_day is null);"
                    ,
                    @"update demands 
                    set indb=up.chk_in 
                    from 
                    (
                        select 
                            id,
                            chk_in 
                        from 
                        (
                            select 
                                a.id,
                                a.part_no,
                                a.contract,
                                a.work_day,
                                a.indb,
                                CASE WHEN b.part_no is null THEN false ELSE true END as chk_in 
                            from 
                            (
                                select 
                                    id,
                                    part_no,
                                    contract,
                                    work_day,
                                    indb 
                                from 
                                    demands
                            ) a 
                            left join 
                            (
                                select 
                                    part_no,
                                    contract,
                                    date_required
                                from 
                                    ord_demands 
                                group by part_no,contract,date_required
                            ) b 
                            on b.part_no=a.part_no and b.contract=a.contract and b.date_required=a.work_day
                        ) as b 
                        where indb is null or indb!=chk_in
                    ) as up 
                    where demands.id=up.id;" }, "Refresh Demand and Order_demands", active_token);
                });

                Steps_executor.Register_step("Validate demands");
                Steps_executor.Wait_for(new string[] { "Refresh bilans_val", "Refresh Demand and Order_demands" }, "Validate demands", active_token);
                Parallel.Invoke(
                    () =>
                    {
                        Lack_report lack_Report = new Lack_report(active_token);
                        lack_Report = null;
                    },
                    () =>
                    {
                        All_lacks all_Lacks = new All_lacks(active_token);
                        all_Lacks = null;
                    });
                Loger.Log("Wait END");
                Steps_executor.Wait_for(new string[] { "All_lacks", "Lack_report" }, "Wait END", active_token);
            }
            Loger.Srv_stop();
            Steps_executor.cts.Dispose();
        }
    }
}
