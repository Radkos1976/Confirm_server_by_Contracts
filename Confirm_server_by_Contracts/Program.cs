using DB_Conect;
using System;
using System.Collections.Generic;
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
                        Parallel.Invoke(
                           async () =>
                           {
                               only_616 = await part_616.Get_source_list("^616.*", true, "Demands for 616 ", active_token);
                               Steps_executor.End_step("Demands 616 ");
                           });
                        bool end_with_no_err1 = Steps_executor.Wait_for(new string[] { "Demands 616 " }, "Inventory part 616 ", active_token);
                        if (end_with_no_err1)
                        {
                            Steps_executor.Register_step("Inventory part 616 ");
                            Inventory_part inventory_616 = new Inventory_part("^616.*", true, part_616.limit_part_no);
                            List<Inventory_part.Inventory_part_row>  pstgr = new List<Inventory_part.Inventory_part_row>();
                            List<Inventory_part.Inventory_part_row>  oracle= new List<Inventory_part.Inventory_part_row>();
                            Parallel.Invoke(
                               async () =>
                               {
                                   pstgr = await inventory_616.Get_PSTGR_List("Inventory part 616 ", active_token);
                               },
                               async () =>
                               {
                                   oracle = await inventory_616.Get_Ora_list("Inventory part 616 ", active_token);
                               }
                           );
                           Parallel.Invoke(
                               async () =>
                               {
                                 await inventory_616.Update_dataset(
                                    pstgr,
                                    oracle,
                                    "Inventory part 616 ",
                                    active_token);
                                    Steps_executor.End_step("Inventory part 616 ");
                               });

                            bool end_with_no_err = Steps_executor.Wait_for(new string[] { "Demands 616 ", "Inventory part 616 " }, "Main_loop 616 ", active_token);
                            if (end_with_no_err)
                            {
                                Steps_executor.Register_step("Main_loop 616 ");

                                Steps_executor.End_step("Main_loop 616 ");
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
                        Inventory_part inventory_except_616 = new Inventory_part("^(5|6(?!16)).*", false, null);
                        List<Simple_Demands.Simple_demands_row> no_616;
                        Parallel.Invoke(
                            async () =>
                            {
                                Steps_executor.Register_step("Demands except 616 ");
                                no_616 = await part_except_616.Get_source_list("^(5|6(?!16)).*", false, "Demands except 616 ", active_token);
                                Steps_executor.End_step("Demands except 616 ");
                            },
                            async () =>
                            {
                                Steps_executor.Register_step("Inventory part except 616 ");
                                int result = await inventory_except_616.Update_dataset(
                                    await inventory_except_616.Get_PSTGR_List("Inventory part except 616 ", active_token),
                                    await inventory_except_616.Get_Ora_list("Inventory part except 616 ", active_token),
                                    "Inventory part except 616 ",
                                    active_token);
                                Steps_executor.End_step("Inventory part except 616 ");
                            });
                        bool end_with_no_err = Steps_executor.Wait_for(new string[] { "Demands except 616 ", "Inventory part except 616 " }, "Main_loop except 616 ", active_token);
                        if (end_with_no_err)
                        {
                            Steps_executor.Register_step("Main_loop except 616 ");

                            Steps_executor.End_step("Main_loop except 616 ");
                        }
                    }
                }

            );
            Loger.Srv_stop();
            Steps_executor.cts.Dispose();
        }
    }
}
