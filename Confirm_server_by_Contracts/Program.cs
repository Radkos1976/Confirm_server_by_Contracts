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
        public static void Get_thre_workers (String Main_task, CancellationToken cancellationToken)
        {
            Parallel.Invoke(
            async () => {
                Order_Demands order_Demands_except1 = new Order_Demands();
                await order_Demands_except1.Update_from_executor(string.Format("{0}1", Main_task), cancellationToken);
                order_Demands_except1 = null;
            },
            async () => {
                Order_Demands order_Demands_except2 = new Order_Demands();
                await order_Demands_except2.Update_from_executor(string.Format("{0}2", Main_task), cancellationToken);
                order_Demands_except2 = null;
            },
            async () => {
                Order_Demands order_Demands_except3 = new Order_Demands();
                await order_Demands_except3.Update_from_executor(string.Format("{0}3", Main_task), cancellationToken);
                order_Demands_except3 = null;
            });
        }
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
                        pstgr = await inventory_616_in_PSTGR.Get_PSTGR_List("Inventory part 616 presets ", active_token);

                        (part_no_tup, part_no_zero_tup) = inventory_616_in_PSTGR.Get_tuple_of_part_no(pstgr);
                        if (part_no_tup.Count > 0)
                        {
                            inventory_616_in_PSTGR.limit_part_no  = part_no_tup;
                            oracle = await inventory_616_in_PSTGR.Get_Ora_list("Inventory part 616 presets ", active_token);
                            await inventory_616_in_PSTGR.Update_dataset(
                                pstgr,
                                oracle,
                                "Inventory part 616 ",
                                active_token);
                        }
                        Steps_executor.End_step("Inventory part 616 presets ");
                        inventory_616_in_PSTGR = null;
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
                            Get_thre_workers("Main_loop 616 Executor", active_token);
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
                                Get_thre_workers("Main_loop except 616 Executor", active_token);
                            },
                            () => {
                                Get_thre_workers("Main_loop 616 Executor", active_token);
                            } );
                                               
                    }
                }
            });
            Steps_executor.Register_step("Prepare data for Reports");
            bool with_no_err = Steps_executor.Wait_for(new string[] { "Main_loop except 616 ", "Main_loop 616 ", 
                "Main_loop except 616 Executor1", "Main_loop except 616 Executor2" , "Main_loop except 616 Executor3",
                "Main_loop 616 Executor1", "Main_loop 616 Executor2", "Main_loop 616 Executor3",
                "Calendar", "cust_ord"}, "Prepare data for Reports", active_token);

            if (with_no_err)
            {
                if  (Dataset_executor.Count()  > 0)
                {
                    Loger.Log("Error on Dataset_executor => some tasks are not calulated");
                    Thread.Sleep(1000);
                }
                Dataset_executor.Clear();

                run_query query = new run_query();
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
                Loger.Log("Wait END");
            }
            Loger.Srv_stop();
            Steps_executor.cts.Dispose();
        }
    }
}
