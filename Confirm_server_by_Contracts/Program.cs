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
            CancellationToken active_token = Steps_executor.cts.Token;
            Parallel.Invoke(
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
                    async() =>
                    {
                        // migrate demands and inventory_part and run main_loop for each
                        Simple_Demands part_except_616 = new Simple_Demands();
                        Inventory_part inventory_except_616 = new Inventory_part("^(5|6(?!16)).*", false , null);
                        List<Simple_Demands.Simple_demands_row> no_616;
                        Parallel.Invoke(
                            async () =>
                            {
                                Steps_executor.Register_step("Demands except 616");
                                no_616 = await part_except_616.Get_source_list("^(5|6(?!16)).*", false, active_token);
                                Steps_executor.End_step("Demands except 616");
                            },
                            async () =>
                            {
                                Steps_executor.Register_step("Inventory part except 616");
                                await inventory_except_616.Update_dataset(
                                    await inventory_except_616.Get_PSTGR_List("Inventory part except 616", active_token),
                                    await inventory_except_616.Get_Ora_list("Inventory part except 616", active_token),
                                    "Inventory part except 616",
                                    active_token);
                                Steps_executor.End_step("Inventory part except 616");
                            });
                        bool end_with_no_err  = Steps_executor.Wait_for(new string[] { "Demands except 616", "Inventory part except 616" }, "Main_loop except 616", active_token);
                        if (end_with_no_err)
                        {
                            Steps_executor.Register_step("Main_loop except 616");

                            Steps_executor.End_step("Main_loop except 616");
                        }
                    }

            );

            Steps_executor.cts.Dispose();
        }
    }
}
