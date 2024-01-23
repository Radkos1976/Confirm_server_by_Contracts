using Confirm_server_by_Contracts;
using DB_Conect;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static Confirm_server_by_Contracts.Inventory_part;

namespace Confirm_server_by_Contracts
{
    class Serv_instance
    {
        public async void Start_calc()
        {
            Loger.Srv_start();
            CancellationToken active_token = Steps_executor.cts.Token;

            Parallel.Invoke(
            new ParallelOptions { MaxDegreeOfParallelism = 50 },
            new Action[] {
                async () =>
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

                        (part_no_tup, part_no_zero_tup) = await inventory_616_in_PSTGR.Get_tuple_of_part_no(pstgr_presets);
                        if (part_no_tup.Count > 0)
                        {
                            if (!active_token.IsCancellationRequested)
                            {
                                inventory_616_in_PSTGR.limit_part_no  = part_no_tup;
                                oracle = await inventory_616_in_PSTGR.Get_Ora_list("Inventory part 616 presets ", active_token);
                                await inventory_616_in_PSTGR.Update_dataset(
                                    pstgr_presets,
                                    oracle,
                                    "Inventory part 616 ",
                                    active_token);
                            }

                        }
                        if (!active_token.IsCancellationRequested)
                        {
                            (part_no_tup, part_no_zero_tup) = await inventory_616_in_PSTGR.Get_tuple_of_part_no(oracle);
                        }
                        // add to part no tuple parts with weistent inventory
                        
                        Steps_executor.End_step("Inventory part 616 presets ");
                        inventory_616_in_PSTGR = null;
                        pstgr_presets = null;
                    });
                    
                    if (Steps_executor.Wait_for(new string[] { "Demands 616 ",  "Inventory part 616 presets " }, "Inventory part 616 ", active_token))
                    {
                        Inventory_part inventory_616 = new Inventory_part("^616.*", false, part_616.limit_part_no);
                        part_616 = null;
                        oracle = await inventory_616.Limit_in_and_create_Except(
                            part_no_tup,
                            part_no_zero_tup,
                            oracle,
                            false);
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
                        if (Steps_executor.Wait_for(new string[] { "Demands 616 ", "Inventory part 616 " }, "Main_loop 616 ", active_token))
                        {
                            Steps_executor.Register_step("Main_loop 616 ");
                            Main_loop main_Loop = new Main_loop();
                            int result = await main_Loop.Update_Main_Tables(
                                "^616.*",
                                "Main_loop 616 ",
                                only_616,
                                oracle,
                                active_token);
                            if (Steps_executor.Wait_for(new string[] { string.Format("{0}:{1}", "Main_loop 616 ", "Fill_executor") }, "Main_loop 616 ", active_token))
                            {
                                main_Loop.Get_thre_workers("Main_loop 616 Executor", active_token);
                            }
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
                async () =>
                {
                    // migrate demands and inventory_part and run main_loop for each non 616%  part_no
                    Simple_Demands part_except_616 = new Simple_Demands();
                    Inventory_part inventory_except_616 = new Inventory_part("^(5|6[^1]|61[^6]).+", false, null);
                    List<Simple_Demands.Simple_demands_row> no_616 = new List<Simple_Demands.Simple_demands_row>();
                    List<Inventory_part.Inventory_part_row>  pstgr = new List<Inventory_part.Inventory_part_row>();
                    List<Inventory_part.Inventory_part_row>  oracle= new List<Inventory_part.Inventory_part_row>();
                    Parallel.Invoke(
                    async () =>
                    {
                        Steps_executor.Register_step("Demands except 616 ");
                        no_616 = await part_except_616.Get_source_list(
                            "^(5|6[^1]|61[^6]).+",
                            false,
                            "Demands except 616 ",
                            active_token);
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
                        if (!active_token.IsCancellationRequested)
                        {
                            int result = await inventory_except_616.Update_dataset(
                                pstgr,
                                oracle,
                                "Inventory part except 616 ",
                                active_token);
                        }
                        Steps_executor.End_step("Inventory part except 616 ");
                        inventory_except_616 = null;
                        pstgr = null;
                    });
                    
                    if (Steps_executor.Wait_for(new string[] { "Demands except 616 ", "Inventory part except 616 " }, "Main_loop except 616 ", active_token))
                    {
                        Steps_executor.Register_step("Main_loop except 616 ");
                        Main_loop main_Loop = new Main_loop();
                        int result = await main_Loop.Update_Main_Tables(
                            "^(5|6[^1]|61[^6]).+",
                            "Main_loop except 616 ",
                            no_616,
                            oracle,
                            active_token);
                        if (Steps_executor.Wait_for(new string[] { string.Format("{0}:{1}", "Main_loop except 616 ", "Fill_executor") }, "Main_loop except 616 ", active_token))
                        {
                            Parallel.Invoke(
                            () =>
                            {
                                main_Loop.Get_thre_workers("Main_loop first 616 Executor", active_token);
                            },
                            () => {
                                main_Loop.Get_thre_workers("Main_loop second 616 Executor", active_token);
                            });
                        }
                        no_616 = null;
                        oracle = null;
                        main_Loop = null;
                    }
                }
            });
            Steps_executor.Register_step("Prepare data for Reports");

            if (Steps_executor.Wait_for(new string[] { "Main_loop except 616 ", "Main_loop 616 ",
                "Main_loop except 616 Executor1", "Main_loop except 616 Executor2" , "Main_loop except 616 Executor3",
                "Main_loop first 616 Executor1", "Main_loop first 616 Executor2", "Main_loop first 616 Executor3",
                "Main_loop second 616 Executor1", "Main_loop second 616 Executor2", "Main_loop second 616 Executor3",
                "Calendar", "cust_ord"}, "Prepare data for Reports", active_token))
            {
                if (Dataset_executor.Count() > 0)
                {
                    Loger.Log("Error on Dataset_executor => some tasks are not calulated");
                    Thread.Sleep(1000);
                }
                Dataset_executor.Clear();

                Run_query query = new Run_query();
                await query.Execute_in_Postgres(new[] {
                    "UPDATE public.datatbles SET start_update=current_timestamp, in_progress=true WHERE table_name='bil_val'",
                    "UPDATE public.datatbles SET start_update=current_timestamp, in_progress=true WHERE table_name='ord_dem'"
                }, "Wait point Demand and Order_demands", active_token);
                Parallel.Invoke(
                async () => {
                    await query.Execute_in_Postgres(new[] {
                    "REFRESH MATERIALIZED VIEW bilans_val",
                    "UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE table_name='bil_val'"},
                    "Refresh bilans_val",
                    active_token);
                }
                ,
                async () => {
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
                    where demands.id=up.id;",
                    "UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE table_name='ord_dem'"
                    },
                    "Refresh Demand and Order_demands",
                    active_token);
                });
                Steps_executor.Register_step("Validate demands");
                if (Steps_executor.Wait_for(new string[] { "Refresh bilans_val", "Refresh Demand and Order_demands" }, "Validate demands", active_token))
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(Postegresql_conn.Connection_pool["MAIN"].ToString()))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where (substring(table_name,1,7)='bil_val' or table_name='ord_dem' ) and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                        conA.Close();
                    }
                    Steps_executor.Register_step("Check_order_demands");
                    // check order_demands for duplicated data amt wrong balances
                    Update_pstgr_from_Ora<Small_upd_demands> upd_dem = new Update_pstgr_from_Ora<Small_upd_demands>("MAIN");
                    List<Small_upd_demands> not_corr_dem = await upd_dem.Get_PSTGR("" +
                        @"select a.part_no, a.contract, min(a.min_d), max(a.max_d) from (
	                        (select b.part_no,b.contract,b.date_required min_d, b.date_required max_d
                        from 
                            (select dop,dop_lin,int_ord,part_no,contract,line_no,rel_no,count(order_supp_dmd) as il 
                                from ord_demands 
                                group by dop,dop_lin,int_ord,part_no,contract,line_no,rel_no) a,
                            (select part_no,contract,date_required,dop,dop_lin,int_ord,line_no,rel_no from ord_demands) b 
                         where a.il >1 and (a.dop=b.dop and a.dop_lin=b.dop_lin and a.int_ord=b.int_ord and a.part_no=b.part_no and a.contract=b.contract
                            and a.line_no=b.line_no and a.rel_no=b.rel_no) 
                         group by b.part_no,b.contract,b.date_required order by part_no) 
                         union ALL
                         (select indeks part_no,umiejsc contract,mn min_d,max(data_dost) max_d
                        from 
                            (select c.indeks,c.umiejsc,c.data_dost,c.mag,c.sum_dost-sum(a.qty_supply) supp,c.sum_potrz-sum(a.qty_demand) dmd,min(a.date_required) mn,b.ma 
                                from 
                                    (select * 
                                        from public.data 
                                        where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości','Opóźniona dostawa') 
                                        and planner_buyer!='LUCPRZ')c,
                                    ord_demands a,
                                    (select part_no,contract,min(work_day) ma 
                                        from demands group by part_no, contract) b 
                                 where c.bilans<0 and b.part_no=c.indeks and a.part_no=c.indeks 
                                 and (a.date_required<=c.data_dost or a.date_required<=current_date) 
                                 group by c.indeks, c.umiejsc,c.opis,c.data_dost,c.bilans,c.mag,c.sum_dost,c.sum_potrz,b.ma) a 
                            where  supp not between -0.001 and 0.001 or dmd not between -0.001 and 0.001 group by indeks,umiejsc,mn,ma)
                        ) a
                        group by a.part_no, a.contract;",
                        "Check_order_demands",
                        active_token);
                    List<Small_upd_demands> list1 = new List<Small_upd_demands>();
                    List<Small_upd_demands> list2 = new List<Small_upd_demands>();
                    List<Small_upd_demands> list3 = new List<Small_upd_demands>();
                    List<Small_upd_demands> list4 = new List<Small_upd_demands>();
                    int cnt = 0;
                    foreach (Small_upd_demands item in not_corr_dem)
                    {
                        if (cnt == 0)
                        {
                            list1.Add(item);
                        }
                        else if (cnt == 1)
                        {
                            list2.Add(item);
                        }
                        else if (cnt == 2)
                        {
                            list3.Add(item);
                        }
                        else if (cnt == 3)
                        {
                            list4.Add(item);
                        }
                        cnt++;
                        if (cnt > 3) { cnt = 0; }
                    }
                    Parallel.Invoke(
                    async () =>
                    {
                        Order_Demands order_Demands_except1 = new Order_Demands();
                        await order_Demands_except1.Update_from_executor_from_list("Check_order_demands1", list1, active_token);
                        order_Demands_except1 = null;
                    },
                    async () =>
                    {
                        Order_Demands order_Demands_except2 = new Order_Demands();
                        await order_Demands_except2.Update_from_executor_from_list("Check_order_demands2", list2, active_token);
                        order_Demands_except2 = null;
                    },
                    async () =>
                    {
                        Order_Demands order_Demands_except3 = new Order_Demands();
                        await order_Demands_except3.Update_from_executor_from_list("Check_order_demands3", list3, active_token);
                        order_Demands_except3 = null;
                    },
                    async () =>
                    {
                        Order_Demands order_Demands_except4 = new Order_Demands();
                        await order_Demands_except4.Update_from_executor_from_list("Check_order_demands4", list4, active_token);
                        order_Demands_except4 = null;
                    });
                    Steps_executor.End_step("Check_order_demands");
                    if (Steps_executor.Wait_for(new string[] { "Check_order_demands", "Check_order_demands1", "Check_order_demands2", "Check_order_demands3", "Check_order_demands4" }, "Validate demands", active_token))
                    {
                        Parallel.Invoke(
                        () =>
                        {
                            Lack_report lack_Report = new Lack_report(active_token);
                            lack_Report = null;
                            if (Steps_executor.Wait_for(new string[] { "Lack_report" }, "Validate demands", active_token))
                            {
                                All_lacks all_Lacks = new All_lacks(active_token);
                                all_Lacks = null;
                            }
                        },
                        () =>
                        {
                            using (NpgsqlConnection conA = new NpgsqlConnection(Postegresql_conn.Connection_pool["MAIN"].ToString()))
                            {
                                conA.Open();
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "UPDATE public.datatbles	" +
                                    "SET start_update=current_timestamp, in_progress=true " +
                                    "WHERE substring(table_name,1,7)='cal_ord'", conA))
                                {
                                    cmd.ExecuteNonQuery();
                                }
                            }
                            Calculate_cust_ord calculate_Cust_Ord = new Calculate_cust_ord(active_token);
                            calculate_Cust_Ord = null;

                        });
                    }

                }
                Steps_executor.Register_step("Modify_prod_date");
                Steps_executor.Register_step("send_mail");
                Steps_executor.Register_step("Send_mail");
                Steps_executor.Register_step("Mail");
                if (Steps_executor.Wait_for(new string[] { "All_lacks", "Lack_report", "Lack_report1", "Lack_report2", "Update To_Mail", "Lack_bil", "Calculate_cust_order" }, "Mail", active_token))
                {
                    Steps_executor.End_step("Validate demands");
                    await query.Execute_in_Postgres(
                        new string[] { "UPDATE public.datatbles SET start_update=current_timestamp, in_progress=true WHERE substring(table_name,1,7)='cal_ord'" },
                        "Start_update mail",
                        active_token);
                    int result = await query.Execute_in_Postgres(
                        new string[] {"" +
                        @"INSERT INTO public.mail
                        (ordid,dop, koor, order_no, line_no, rel_no, part_no, descr, country, prom_date, prom_week, load_id, ship_date, date_entered, cust_id, 
                        prod, prod_week, planner_buyer, indeks, opis, typ_zdarzenia, status_informacji, zest, info_handlo, logistyka, seria0, data0, cust_line_stat,
                        ord_objver)
                        select * from (
                           select a.* from to_mail a 
                           left join 
                           (
                               select cust_id from mail
                           ) b 
                           on b.cust_id=a.cust_id where a.cust_id is not null and b.cust_id is null  
                           order by order_no,line_no,rel_no
                        ) a  
                        group by a.ordid,a.dop,a.koor,a.order_no,a.line_no,a.rel_no,a.part_no,a.descr,a.country,a.prom_date,a.prom_week,a.load_id,a.ship_date,a.date_entered,a.cust_id,
                        a.prod,a.prod_week,a.planner_buyer,a.indeks,a.opis,a.typ_zdarzenia,a.status_informacji,a.zest,a.info_handlo,a.logistyka,a.seria0,a.data0,a.cust_line_stat,a.ord_objver"
                        ,
                        @"UPDATE public.mail a 
                        SET ordid=b.ordid,dop=b.dop,koor=b.koor,order_no=b.order_no,line_no=b.line_no,rel_no=b.rel_no,part_no=b.part_no,descr=b.descr,country=b.country,prom_date=b.prom_date,
                        prom_week=b.prom_week,load_id=b.load_id,ship_date=b.ship_date,date_entered=b.date_entered,prod=b.prod,prod_week=b.prod_week,planner_buyer=b.planner_buyer,indeks=b.indeks,
                        opis=b.opis,typ_zdarzenia=b.typ_zdarzenia,status_informacji=b.status_informacji,zest=b.zest,info_handlo=b.info_handlo,logistyka=b.logistyka,seria0=b.seria0,data0=b.data0,
                        cust_line_stat=b.cust_line_stat 
                        from 
                        (
                            select * 
                            from 
                            (
                                select a.ordID,a.dop,a.koor,a.order_no,a.line_no,a.rel_no,a.part_no,a.descr,a.country,a.prom_date,a.prom_week,a.load_id,a.ship_date,a.date_entered,a.cust_id,a.prod,
                                    a.prod_week,a.planner_buyer,a.indeks,a.opis,a.typ_zdarzenia,a.status_informacji,a.zest,a.info_handlo,a.logistyka,a.seria0,a.data0,a.cust_line_stat 
                                from 
                                    to_mail a, 
                                    mail b 
                                where b.cust_id=a.cust_id except select ordID,dop,koor,order_no,line_no,rel_no,part_no,descr,country,prom_date,prom_week,load_id,ship_date,date_entered,
                                cust_id,prod,prod_week,planner_buyer,indeks,opis,typ_zdarzenia,status_informacji,zest,info_handlo,logistyka,seria0,data0,cust_line_stat from mail 
                            ) a
                        ) b  
                        where b.cust_id=a.cust_id;"
                        ,
                        @"update public.mail a 
                        SET ordid=b.ordid,dop=b.dop,koor=b.koor,order_no=b.order_no,line_no=b.line_no,rel_no=b.rel_no,part_no=b.part_no,descr=b.descr,country=b.country,prom_date=b.prom_date,
                        prom_week=b.prom_week,load_id=b.load_id,ship_date=b.ship_date,date_entered=b.date_entered,prod=b.prod,prod_week=b.prod_week,info_handlo=b.info_handlo,logistyka=b.logistyka,
                        seria0=b.seria0,data0=b.data0,cust_line_stat=b.cust_line_stat 
                        from 
                        (
                            select a.* from 
                            (
                                Select 
                                case when c.dop_connection_db!='AUT' then 'O '||c.order_no else 'D'||to_char(c.dop_id,'9999999999') end ordID,
                                c.dop_id as dop,
                                c.koor,c.order_no,c.line_no,c.rel_no,c.part_no,c.descr,c.country,
                                c.prom_date,cast(c.prom_week as integer) as prom_week,
                                c.load_id,c.ship_date, c.date_entered,c.id as cust_id,c.data_dop as prod,
                                case when to_date(to_char(c.data_dop,'iyyyiw'),'iyyyiw') + shipment_day(c.country,c.cust_no,c.zip_code,c.addr1)-1>c.data_dop then cast(to_char(c.data_dop,'iyyyiw') as integer) else 
                                    cast(to_char(c.data_dop + interval '6 day','iyyyiw') as integer) end as prod_week,
                                c.planner_buyer,c.indeks,c.opis,c.status_informacji,
                                c.zest,
                                case when case when to_date(to_char(c.data_dop,'iyyyiw'),'iyyyiw') + shipment_day(c.country,c.cust_no,c.zip_code,c.addr1)-1>c.data_dop then cast(to_char(c.data_dop,'iyyyiw') as integer) else 
                                    cast(to_char(c.data_dop + interval '6 day','iyyyiw') as integer) end>cast(c.prom_week as integer) then true else false end as info_handlo,
                                case when c.ship_date is null then false else case when c.data_dop<c.ship_date then false else true  end end as logistyka,
                                c.seria0,c.data0,c.line_state as cust_line_stat 
                                from 
                                (
                                    select c.*,
                                        a.planner_buyer,
                                        a.indeks,a.opis,
                                        a.status_informacji 
                                    from 
                                        mail a 
                                    left join 
                                        to_mail b 
                                    on b.cust_id=a.cust_id left join cust_ord c on c.id=a.cust_id where b.cust_id is null 
                                )c 
                                except 
                                select 
                                    ordID,dop,koor,order_no,line_no,rel_no,part_no,descr,country,
                                    prom_date,prom_week,load_id,ship_date,date_entered,cust_id,prod,
                                    prod_week,planner_buyer,indeks,opis,status_informacji,zest,
                                    info_handlo,logistyka,seria0,data0,cust_line_stat  
                                from 
                                    mail 
                            ) a
                        ) b 
                        where a.cust_id=b.cust_id"
                        ,
                        @"DELETE FROM public.mail 
                        WHERE cust_id in 
                        (
                            select 
                                a.cust_id 
                            from 
                                mail a 
                            left join 
                                to_mail b 
                            on b.cust_id=a.cust_id 
                            left join 
                                cust_ord c 
                            on c.id=a.cust_id 
                            where b.cust_id is null and is_for_mail(a.status_informacji)=true and c.data_dop>=a.prod and cast(c.prom_week as integer)>=a.prod_week and (c.ship_date is null or c.ship_date>a.prod)
                        )",
                        "delete from braki_hist where objversion<current_timestamp - interval '7 day'"
                        ,
                        "delete from mail_hist where date_addd<current_timestamp - interval '7 day'"
                        ,
                        "delete from mail where status_informacji='NOT IMPLEMENT' or cust_line_stat!='Aktywowana'"
                        ,
                        "DELETE FROM public.mail WHERE cust_id not in (select id from public.cust_ord)"
                        ,
                        "DELETE FROM public.mail WHERE cust_id in (select a.cust_id from mail a left join to_mail b on b.cust_id=a.cust_id where b.cust_id is null and (is_for_mail(a.status_informacji)=false or a.status_informacji='POPRAWIĆ'))",
                        "UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE substring(table_name,1,7)='cal_ord'"
                        }, 
                        "Mail", 
                        active_token);

                }
            }                
            if (Steps_executor.Wait_for(new string[] { "Mail", }, "Send_mail", active_token))
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(Postegresql_conn.Connection_pool["MAIN"].ToString()))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select cast(count(table_name) as integer) busy " +
                        "from public.datatbles " +
                        "where substring(table_name,1,7)='cal_ord' and in_progress=true", conA))
                    {
                        int busy_il = 1;
                        while (busy_il > 0)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                        }
                    }
                    conA.Close();
                }
                Old_code old_Code = new Old_code();
                await old_Code.Modify_prod_date(active_token);
                old_Code = null;
            } 
            if (Steps_executor.Wait_for(new string[] { "Modify_prod_date", "send_mail" }, "Wait for last steps", active_token))
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(Postegresql_conn.Connection_pool["MAIN"].ToString()))
                {
                    conA.Open();    
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select cast(count(table_name) as integer) busy " +
                        "from public.datatbles " +
                        "where (table_name='send_mail' or table_name ='pot' or table_name ='fr' or table_name ='popraw' or table_name ='logist' or table_name ='niepotw' or table_name ='niezam' or table_name ='seriaz') and in_progress=true", conA))
                    {
                        int busy_il = 1;
                        while (busy_il > 0)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                        }
                    }
                }
            }
            Steps_executor.Register_step("Server_STOP");
            Loger.Srv_stop();
            //Steps_executor.cts.Dispose();
        }
    }
    public class Small_upd_demands: IComparable<Small_upd_demands>, IEquatable<Small_upd_demands>
    {
        public string Part_no { get; set; }
        public string  Contract {  get; set; }
        public DateTime Min_d { get; set; }
        public DateTime Max_d { get; set;}

        public int CompareTo(Small_upd_demands other)
        {
            if (other == null)
            {
                return 1;
            }
            int var1 = this.Part_no.CompareTo(other.Part_no);
            if (var1  != 0)
            {
                return  var1;
            }
            return this.Contract.CompareTo(other.Contract);
        }

        public bool Equals(Small_upd_demands other)
        {
            if (other == null) return false;
            return this.Part_no.Equals(other.Part_no) && this.Contract.Equals(other.Contract);
        }
    }
}
