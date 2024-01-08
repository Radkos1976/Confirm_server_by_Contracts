using Confirm_server_by_Contracts;
using DB_Conect;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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

                    bool end_with_no_err1 = Steps_executor.Wait_for(new string[] { "Demands 616 ",  "Inventory part 616 presets " }, "Inventory part 616 ", active_token);
                    if (end_with_no_err1)
                    {
                        Inventory_part inventory_616 = new Inventory_part("^616.*", false, part_616.limit_part_no);
                        part_616 = null;
                        oracle = await inventory_616.Limit_in_and_create_Except(part_no_tup, part_no_zero_tup, oracle, false);
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
                            int result = await main_Loop.Update_Main_Tables("^616.*", "Main_loop 616 ", only_616, oracle, active_token);
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
                    bool end_with_no_err = Steps_executor.Wait_for(new string[] { "Demands except 616 ", "Inventory part except 616 " }, "Main_loop except 616 ", active_token);
                    if (end_with_no_err)
                    {
                        Steps_executor.Register_step("Main_loop except 616 ");
                        Main_loop main_Loop = new Main_loop();
                        int result = await main_Loop.Update_Main_Tables("^(5|6[^616]).+", "Main_loop except 616 ", no_616, oracle, active_token);
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
            bool with_no_err = Steps_executor.Wait_for(new string[] { "Main_loop except 616 ", "Main_loop 616 ",
                "Main_loop except 616 Executor1", "Main_loop except 616 Executor2" , "Main_loop except 616 Executor3",
                "Main_loop first 616 Executor1", "Main_loop first 616 Executor2", "Main_loop first 616 Executor3",
                "Main_loop second 616 Executor1", "Main_loop second 616 Executor2", "Main_loop second 616 Executor3",
                "Calendar", "cust_ord"}, "Prepare data for Reports", active_token);

            if (with_no_err)
            {
                if (Dataset_executor.Count() > 0)
                {
                    Loger.Log("Error on Dataset_executor => some tasks are not calulated");
                    Thread.Sleep(1000);
                }
                Dataset_executor.Clear();

                Run_query query = new Run_query();
                Parallel.Invoke(
                async () =>  {
                    await query.Execute_in_Postgres(new[] {
                    "REFRESH MATERIALIZED VIEW bilans_val" }, "Refresh bilans_val", active_token);
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
                    where demands.id=up.id;" }, "Refresh Demand and Order_demands", active_token);
                });
                Steps_executor.Register_step("Validate demands");
                with_no_err = Steps_executor.Wait_for(new string[] { "Refresh bilans_val", "Refresh Demand and Order_demands" }, "Validate demands", active_token);
                if (with_no_err)
                {
                    Parallel.Invoke(
                    () =>
                    {
                        Lack_report lack_Report = new Lack_report(active_token);
                        lack_Report = null;
                        Steps_executor.Wait_for(new string[] { "Lack_report" }, "Validate demands", active_token);
                        All_lacks all_Lacks = new All_lacks(active_token);
                        all_Lacks = null;
                    },
                    () =>
                    {
                        Calculate_cust_ord calculate_Cust_Ord = new Calculate_cust_ord(active_token);
                        calculate_Cust_Ord = null;
                    });
                }
                Steps_executor.Register_step("Mail");
                if (Steps_executor.Wait_for(new string[] { "All_lacks", "Lack_report", "Lack_report1", "Lack_report2", "Update To_Mail", "Lack_bil", "Calculate_cust_order" }, "Mail", active_token))
                {
                    Steps_executor.End_step("Validate demands");
                    int result = await query.Execute_in_Postgres(new string[] {"" +
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
                        }, "Mail", active_token);
                    

                    Steps_executor.Register_step("Send_mail");
                    if (Steps_executor.Wait_for(new string[] { "Mail",  }, "Send_mail", active_token))
                    {
                        Old_code old_Code = new Old_code();
                        await old_Code.Modify_prod_date(active_token);
                    }
                }
            }
            Loger.Srv_stop();
            Steps_executor.cts.Dispose();
        }
    }
}
