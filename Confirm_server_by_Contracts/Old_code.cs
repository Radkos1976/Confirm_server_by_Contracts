using DB_Conect;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Confirm_server_by_Contracts
{
    internal class Old_code
    {
        readonly string npC = Postegresql_conn.Connection_pool["MAIN"].ToString();
        DateTime start = Loger.Started();
        private string npA;

        async Task<int> lack_bil(CancellationToken cancellationToken)
        {
            Loger.Log("Start BALANCE LACK");
            try
            {                
                using (DataTable aggr = new DataTable())
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {                        
                        using (NpgsqlTransaction TR_updtlack = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            Steps_executor.Register_step("ord_lack_bil");                           
                            using (NpgsqlCommand cmd = new NpgsqlCommand("DELETE FROM public.ord_lack_bil", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                @"INSERT INTO public.ord_lack_bil
                                    (dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord, contract, 
                                    order_supp_dmd, wrkc, next_wrkc, part_no, descr, part_code, date_required,
                                    ord_state, ord_date, prod_qty, qty_supply, qty_demand, dat_creat, chksum, id) 
                                select b.* 
                                from 
                                    (
                                        select
                                            a.part_no,
                                            a.work_day,
                                            b.min_lack,
                                            a.balance *-1 lack,
                                            case when a.work_day=b.min_lack then case when a.balance*-1=a.qty_demand then 'All' else 'Part' end else 'All' end stat_lack 
                                        from 
                                        (
                                            select 
                                                part_no,
                                                contract,
                                                work_day,
                                                qty_demand,
                                                balance 
                                            from 
                                                demands 
                                            where balance<0 and qty_demand!=0 and koor!='LUCPRZ'
                                        ) a 
                                        left join 
                                        (
                                            select 
                                                part_no,
                                                contract,
                                                min(work_day) min_lack
                                            from 
                                                demands 
                                            where balance<0 and qty_demand!=0 group by part_no, contract
                                        ) b 
                                        on b.part_no=a.part_no and b.contract=a.contract
                                        where case when a.work_day=b.min_lack then case when a.balance*-1=a.qty_demand then 'All' else 'Part' end else 'All' end='All'
                                    ) a,
                                    ord_demands b 
                                    where b.part_no=a.part_no and b.contract=a.contract and b.date_required=a.work_day", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }                            
                            TR_updtlack.Commit();
                            Steps_executor.End_step("ord_lack_bil");
                        }
                        conA.Close();
                    }
                    Steps_executor.Register_step("Old_code: lack_bil");
                    bool with_no_err = Steps_executor.Wait_for(new[] { "ord_lack_bil" }, "lack_bil", cancellationToken);
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            @"select
b.*,
a.lack bal_stock 
from 
(
select 
a.part_no,
a.work_day,
b.min_lack,
a.balance *-1 lack,
case when a.work_day=b.min_lack then case when a.balance*-1=a.qty_demand then 'All' else 'Part' end else 'All' end stat_lack 
from 
(
select 
part_no,
work_day,
qty_demand,
balance 
from 
demands 
where balance<0 and qty_demand!=0 and koor!='LUCPRZ'
) a 
left join 
(
select 
part_no,
min(work_day) min_lack
from 
demands 
where balance<0 and qty_demand!=0 group by part_no
) b 
on b.part_no=a.part_no 
where case when a.work_day=b.min_lack then case when a.balance*-1=a.qty_demand then 'All' else 'Part' end else 'All' end='Part'
) a,
ord_demands b 
where b.part_no=a.part_no and b.date_required=a.work_day order by part_no,date_required,int_ord desc", conA))
                        {
                            using (NpgsqlDataReader po = cmd.ExecuteReader())
                            {
                                using (DataTable sch = po.GetSchemaTable())
                                {
                                    foreach (DataRow a in sch.Rows)
                                    {
                                        aggr.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                        aggr.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                    }
                                    aggr.Columns.Add("Stat");
                                    aggr.Columns["Stat"].AllowDBNull = true;
                                }
                                aggr.Load(po);
                                Loger.Log("Data for Balanace for orders_bil");
                            }
                        }
                        conA.Close();
                    }
                    string Part_no = "";
                    DateTime nullDAT = start.AddDays(1000);
                    DateTime part_dat = nullDAT;
                    double bil = 0;
                    foreach (DataRowView rek in aggr.DefaultView)
                    {
                        if (Part_no != rek["Part_no"].ToString() || part_dat != (DateTime)rek["date_required"])
                        {
                            Part_no = rek["Part_no"].ToString();
                            part_dat = (DateTime)rek["date_required"];
                            bil = Convert.ToDouble(rek["bal_stock"]);
                        }
                        if (bil > 0)
                        {
                            rek["Stat"] = "ADD";
                            bil = bil - Convert.ToDouble(rek["qty_demand"]);
                        }
                    }
                    DataRow[] rwA = aggr.Select("Stat='ADD'");
                    Loger.Log("RECORDS LACK_bil add: " + rwA.Length);
                    if (rwA.Length > 0)
                    {
                        using (NpgsqlConnection conB = new NpgsqlConnection(npC))
                        {
                            await conB.OpenAsync();
                            using (NpgsqlTransaction TR_ins_ord_lack = conB.BeginTransaction(IsolationLevel.ReadCommitted))
                            {
                                using (NpgsqlCommand cmd2 = new NpgsqlCommand("INSERT INTO public.ord_lack_bil(dop, dop_lin, data_dop, day_shift, order_no, line_no, rel_no, int_ord ,contract, order_supp_dmd, wrkc, next_wrkc, part_no, descr, part_code, date_required, ord_state, ord_date, prod_qty, qty_supply, qty_demand,dat_creat , chksum, id) VALUES (@dop,@dop_lin,@data_dop,@day_shift,@order_no,@line_no,@rel_no,@int_ord,@contract,@order_supp_dmd,@wrkc,@next_wrkc,@part_no,@descr,@part_code,@date_required,@ord_state,@ord_date,@prod_qty,@qty_supply,@qty_demand,@dat_creat,@chksum,@id);", conB))
                                {
                                    cmd2.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("day_shift", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("int_ord", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("contract", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("order_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("next_wrkc", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("date_required", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd2.Parameters.Add("ord_date", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd2.Parameters.Add("qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd2.Parameters.Add("qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                    cmd2.Parameters.Add("dat_creat", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd2.Parameters.Add("chksum", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd2.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                    cmd2.Prepare();
                                    foreach (DataRow rA in rwA)
                                    {
                                        for (int i = 0; i < 24; i++)
                                        {
                                            cmd2.Parameters[i].Value = rA[i];
                                        }
                                        cmd2.ExecuteNonQuery();
                                    }
                                    Loger.Log("END ADD LACK_bil:" + (DateTime.Now - start));
                                }
                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE table_name='ord_lack_bil'", conB))
                                {
                                    cmd1.ExecuteNonQuery();
                                }
                                TR_ins_ord_lack.Commit();
                            }
                            conB.Close();
                        }
                    }
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("select cast(count(table_name) as integer) busy from public.datatbles where table_name='ord_lack_bil' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(1000); }
                            }
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("REFRESH MATERIALIZED VIEW formatka_bil; ", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        conA.Close();
                    }
                }
                return 0;
            }
            catch (Exception e)
            {
                Loger.Log("ERORR in LACK_bil RPT" + e);
                return 1;
            }
        }
        async Task<int> calculate_cust_ord()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("select cast(count(table_name) as integer) busy from public.datatbles where (substring(table_name,1,6)='worker' or table_name='demands'  or table_name='wrk_del' or  table_name='data' or table_name='cust_ord') and in_progress=true", conA))
                    {
                        int busy_il = 1;
                        while (busy_il > 0)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0) { System.Threading.Thread.Sleep(1000); }
                        }
                    }
                    conA.Close();
                }
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.datatbles	SET start_update=current_timestamp, in_progress=true WHERE substr(table_name,1,5)='braki' or substring(table_name,1,7)='cal_ord'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Loger.Log("Start query for problematic orders Time");
                using (DataTable mat_ord = new DataTable())
                {
                    using (DataTable mat_dmd = new DataTable())
                    {
                        using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                        {
                            conA.Open();
                            using (NpgsqlCommand cmd = new NpgsqlCommand("select c.indeks,c.mag,count(case when order_supp_dmd!='Zam. zakupu' then a.part_no end) il,case when c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end data_dost,c.wlk_dost,c.sum_dost,c.bilans,c.mag-sum(a.qty_demand)+case when c.typ_zdarzenia in ('Dzisiejsza dostawa','Opóźniona dostawa') then 0 else sum(a.qty_supply) end bil_chk,c.typ_zdarzenia,e.max_prod_date,sum(a.qty_supply) dost,sum(a.qty_demand) potrz,sum_dost-sum_dost qty from (select c.indeks,c.data_dost,max(date_shift_days(case when  c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end,a.day_shift)) as max_prod_date from (select * from public.data where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c,ord_demands a where c.bilans<0 and a.part_no=c.indeks and (a.date_required<=c.data_dost or a.date_required<=current_date) group by indeks,data_dost) e,(select * from public.data where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c,ord_demands a left join cust_ord b on b.dop_id=a.dop where c.bilans<0 and a.part_no=c.indeks and e.indeks=c.indeks and ((c.typ_zdarzenia='Brakujące ilości' and a.date_required<c.data_dost) or (c.typ_zdarzenia!='Brakujące ilości' and a.date_required<=c.data_dost) or a.date_required<=current_date) and e.data_dost=c.data_dost group by c.indeks,c.mag,c.data_gwarancji,c.data_dost,c.wlk_dost,c.sum_dost,c.bilans,c.typ_zdarzenia,e.max_prod_date order by max_prod_date desc,c.indeks,data_dost desc ", conA))
                            {
                                using (NpgsqlDataReader po = cmd.ExecuteReader())
                                {
                                    mat_dmd.Load(po);
                                    Loger.Log("Balanace for materials");
                                }
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("select case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end ordID,f.l_ordid ,c.indeks,c.opis,c.planner_buyer,c.mag, c.data_dost ,a.date_required,c.wlk_dost,c.bilans,c.typ_zdarzenia,c.status_informacji,a.dop,a.dop_lin,a.data_dop,a.order_no zlec,date_shift_days(case when  c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end,a.day_shift) as prod_date,to_date(b.prom_week, 'iyyyiw')+shipment_day(b.country,b.cust_no,b.zip_code,b.addr1)-1 as max_posible_prod,e.max_prod_date,a.order_supp_dmd,a.part_code,a.ord_state,a.prod_qty,a.qty_supply,a.qty_demand,b.koor,b.order_no,b.line_no,b.rel_no,b.part_no,b.descr,b.configuration,b.last_mail_conf,b.prom_date,b.prom_week,b.load_id,b.ship_date,b.state_conf,b.line_state,b.cust_order_state,b.country,shipment_day(b.country,b.cust_no,b.zip_code,b.addr1),COALESCE (b.date_entered,a.dat_creat) as date_entered,case when to_date(b.prom_week, 'iyyyiw')+shipment_day(b.country,b.cust_no,b.zip_code,b.addr1)-1<date_shift_days(c.data_dost,a.day_shift) then COALESCE (b.date_entered,a.dat_creat) else  COALESCE (b.date_entered,a.dat_creat,'2014-01-01')+ interval '200 day' end  as sort_ord,b.zest,qty_demand-qty_demand ord_assinged,b.id custid from (select c.indeks,c.data_dost,max(date_shift_days(case when  c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end,a.day_shift)) as max_prod_date,count(case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end) from (select * from public.data where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c,ord_demands a where c.bilans<0 and a.part_no=c.indeks and (a.date_required<=c.data_dost or a.date_required<=current_date) group by indeks,data_dost) e,(select case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end ordid,count(case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end) l_ordid from (select * from public.data where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c,ord_demands a where c.bilans<0 and a.part_no=c.indeks and (a.date_required<=c.data_dost or a.date_required<=current_date) group by case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end) f,(select * from public.data where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ')c,ord_demands a left join cust_ord b on b.dop_id=a.dop where a.order_supp_dmd!='Zam. zakupu' and f.ordid=case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end and c.bilans<0 and a.part_no=c.indeks and e.indeks=c.indeks and (case when typ_zdarzenia='Brakujące ilości' then a.date_required<c.data_dost else a.date_required<=c.data_dost end or a.date_required<=current_date) and e.data_dost=c.data_dost order by max_prod_date desc,c.indeks,data_dost,sort_ord desc", conA))
                            {
                                using (NpgsqlDataReader po = cmd.ExecuteReader())
                                {
                                    using (DataTable sch = po.GetSchemaTable())
                                    {
                                        foreach (DataRow a in sch.Rows)
                                        {
                                            mat_ord.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                            mat_ord.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                        }
                                    }
                                    mat_ord.Load(po);
                                    Loger.Log("Data for Balanace for orders");
                                }
                            }
                            conA.Close();
                        }
                        using (DataTable zes = new DataTable())
                        {
                            zes.Columns.Add("zest_id", System.Type.GetType("System.String"));
                            zes.Columns.Add("ilo", System.Type.GetType("System.Int32"));
                            string zst = "";
                            foreach (DataRow qwe in mat_ord.Rows)
                            {
                                if (!qwe.IsNull("zest"))
                                {
                                    if (zst.IndexOf("'Zes" + qwe["zest"].ToString() + "'") == -1)
                                    {
                                        zst = zst + "'Zes" + qwe["zest"].ToString() + "'";
                                        DataRow rek = zes.NewRow();
                                        rek["zest_id"] = qwe["zest"];
                                        rek["ilo"] = 1;
                                        zes.Rows.Add(rek);
                                    }
                                    else
                                    {
                                        foreach (DataRow rw in zes.Rows)
                                        {
                                            if (rw["zest_id"] == qwe["zest"])
                                            {
                                                rw["ilo"] = Convert.ToDecimal(rw["ilo"]) + 1;
                                                rw.AcceptChanges();
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            List<DataRow> rw_delete = new List<DataRow>();
                            foreach (DataRow rw in zes.Rows)
                            {
                                if (Convert.ToDecimal(rw["ilo"]) == 1)
                                {
                                    zst.Replace("'Zes" + rw["zest_id"].ToString() + "'", "");
                                    rw_delete.Add(rw);
                                }
                            }
                            foreach (DataRow rw in rw_delete)
                            {
                                zes.Rows.Remove(rw);
                            }
                            zes.AcceptChanges();
                            using (DataTable tmp_ord = mat_ord.Clone())
                            {
                                //przypisz zmienne
                                string isfull = "";

                                Loger.Log("Calculate balance");
                                for (int j = 0; j < mat_dmd.Rows.Count; j++)
                                {
                                    DataTable ord_ids = zes.Clone();
                                    DataRow rek = mat_dmd.Rows[j];
                                    string sear_id = "";
                                    int iter = Convert.ToInt32(rek["il"]);
                                    double qt = Convert.ToDouble(rek["bil_chk"]);
                                    double bil = Convert.ToDouble(rek["qty"]);
                                    if (bil > qt)
                                    {
                                        for (int i = 0; i < iter; i++)
                                        {
                                            DataRow rw = mat_ord.Rows[i];
                                            if (rek["indeks"].ToString() == rw["indeks"].ToString())
                                            {
                                                if (Convert.ToDouble(rw["ord_assinged"]) == 0)
                                                {
                                                    if (bil > qt)
                                                    {
                                                        if (rw["ord_state"].ToString() != "Rozpoczęte")
                                                        {
                                                            if (Convert.ToDecimal(rw["l_ordid"]) > 1)
                                                            {
                                                                if (sear_id.IndexOf("'" + rw["ordid"].ToString() + "'") == -1)
                                                                {
                                                                    DataRow row = ord_ids.NewRow();
                                                                    row[0] = rw["ordid"];
                                                                    row[1] = rw["l_ordid"];
                                                                    ord_ids.Rows.Add(row);
                                                                    sear_id = sear_id + "'" + rw["ordid"].ToString() + "'";
                                                                }
                                                                else
                                                                {
                                                                    DataRow[] strf = ord_ids.Select("zest_id='" + rw["ordid"].ToString() + "'");
                                                                    if (strf.Length > 0)
                                                                    {
                                                                        foreach (DataRow ra in strf)
                                                                        {
                                                                            if (Convert.ToDecimal(ra["ilo"]) == 1) { sear_id.Replace("'" + ra["zest_id"] + "'", ""); }
                                                                            ra["ilo"] = Convert.ToDecimal(ra["ilo"]) - 1;
                                                                            ra.AcceptChanges();
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            if (!rw.IsNull("zest"))
                                                            {
                                                                if (sear_id.IndexOf("'Zes" + rw["zest"].ToString() + "'") == -1)
                                                                {
                                                                    if (zst.IndexOf("'Zes" + rw["zest"].ToString() + "'") > -1)
                                                                    {
                                                                        foreach (DataRow re in zes.Rows)
                                                                        {
                                                                            if (re["zest_id"] == rw["zest"])
                                                                            {
                                                                                DataRow row = ord_ids.NewRow();
                                                                                row[0] = re[0];
                                                                                row[1] = re[1];
                                                                                ord_ids.Rows.Add(row);
                                                                                break;
                                                                            }
                                                                        }
                                                                        sear_id = sear_id + "'Zes" + rw["zest"].ToString() + "'";
                                                                    }
                                                                }
                                                                else
                                                                {
                                                                    DataRow[] strf = ord_ids.Select("zest_id='Zes" + rw["zest"].ToString() + "'");
                                                                    if (strf.Length > 0)
                                                                    {
                                                                        foreach (DataRow ra in strf)
                                                                        {
                                                                            if (Convert.ToDecimal(ra["ilo"]) == 1) { sear_id.Replace("'" + ra["zest_id"] + "'", ""); }
                                                                            ra["ilo"] = Convert.ToDecimal(ra["ilo"]) - 1;
                                                                            ra.AcceptChanges();
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            bil = bil - Convert.ToDouble(rw["qty_demand"]);
                                                            rw["ord_assinged"] = Convert.ToDouble(rw["qty_demand"]);
                                                            rw.AcceptChanges();
                                                            tmp_ord.ImportRow(rw);
                                                        }
                                                    }
                                                    else
                                                    {
                                                        isfull = isfull + "'" + rek["indeks"].ToString() + "'_'" + rek["data_dost"].ToString() + "'";
                                                        rek["qty"] = bil;
                                                        rek.AcceptChanges();
                                                        break;
                                                    }
                                                }
                                                else
                                                {
                                                    Loger.Log("Calc_bal coś nie tak tego rekordu nie powinno być(ord_assing):" + rek["indeks"].ToString());
                                                }
                                            }
                                            else
                                            {
                                                iter = iter + 1;
                                            }
                                        }
                                        for (int i = 0; i < iter; i++)
                                        {
                                            mat_ord.Rows[i].Delete();
                                        }
                                        mat_ord.AcceptChanges();
                                        List<DataRow> row_delete = new List<DataRow>();
                                        if (sear_id != "")
                                        {
                                            foreach (DataRow rw in mat_ord.Rows)
                                            {
                                                if (sear_id.IndexOf("'" + rw["ordid"].ToString() + "'") > -1 || sear_id.IndexOf("'Zes" + rw["zest"].ToString() + "'") > -1)
                                                {
                                                    DataRow[] strf = ord_ids.Select("(zest_id='" + rw["ordid"].ToString() + "' or zest_id='Zes" + rw["zest"].ToString() + "') and ilo>0");
                                                    if (strf.Length > 0)
                                                    {
                                                        foreach (DataRow ra in strf)
                                                        {
                                                            if (Convert.ToDecimal(ra["ilo"]) == 1) { sear_id.Replace("'" + ra["zest_id"] + "'", ""); }
                                                            ra["ilo"] = Convert.ToDecimal(ra["ilo"]) - 1;
                                                            ra.AcceptChanges();
                                                        }
                                                    }
                                                    for (int k = j; k < mat_dmd.Rows.Count; k++)
                                                    {
                                                        DataRow rk = mat_dmd.Rows[k];
                                                        if (rk["indeks"].ToString() == rw["indeks"].ToString() && (DateTime)rk["data_dost"] == (DateTime)rw["data_dost"])
                                                        {
                                                            if (rw["ord_state"].ToString() != "Rozpoczęte")
                                                            {
                                                                if (isfull.IndexOf("'" + rw["indeks"].ToString() + "'_'" + rw["data_dost"].ToString() + "'") == -1)
                                                                {
                                                                    rk["qty"] = Convert.ToDouble(rk["qty"]) - Convert.ToDouble(rw["qty_demand"]);
                                                                    rw["ord_assinged"] = Convert.ToDouble(rw["qty_demand"]);
                                                                    tmp_ord.ImportRow(rw);
                                                                }
                                                            }
                                                            rk["il"] = (Int64)rk["il"] - 1;
                                                            row_delete.Add(rw);
                                                            if (Convert.ToDouble(rk["qty"]) - Convert.ToDouble(rw["qty_demand"]) <= Convert.ToDouble(rk["bil_chk"])) { isfull = isfull + "'" + rk["indeks"].ToString() + "'_'" + rk["data_dost"].ToString() + "'"; }
                                                            rw.AcceptChanges();
                                                            rk.AcceptChanges();
                                                            mat_dmd.AcceptChanges();
                                                        }
                                                    }
                                                }
                                                if (sear_id.Length < 3) { break; }
                                            }
                                            foreach (DataRow rw in row_delete)
                                            {
                                                mat_ord.Rows.Remove(rw);
                                            }
                                            mat_ord.AcceptChanges();
                                            int c = mat_ord.Rows.Count;
                                        }
                                    }
                                }
                                GC.Collect();

                                Loger.Log("End Calculate Data for Balanace for orders");
                                using (DataTable sou_braki = new DataTable())
                                {
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                    {
                                        Loger.Log("Saving data to tmp table");
                                        await conA.OpenAsync();

                                        using (NpgsqlCommand cmd = new NpgsqlCommand("select * from public.braki order by max_prod_date desc,indeks,data_dost,sort_ord desc", conA))
                                        {
                                            using (NpgsqlDataReader po = cmd.ExecuteReader())
                                            {
                                                using (DataTable sch = po.GetSchemaTable())
                                                {
                                                    foreach (DataRow a in sch.Rows)
                                                    {
                                                        sou_braki.Columns.Add(a["ColumnName"].ToString(), System.Type.GetType(a["DataType"].ToString()));
                                                        sou_braki.Columns[a["ColumnName"].ToString()].AllowDBNull = true;
                                                    }
                                                }
                                                sou_braki.Load(po);
                                            }
                                        }
                                        conA.Close();
                                    }
                                    DataRow[] new_rek = tmp_ord.Select("ord_assinged>0");
                                    if (new_rek.Length > 0)
                                    {
                                        using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                        {
                                            await conA.OpenAsync();
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.datatbles	SET start_update=current_timestamp, in_progress=true WHERE table_name='cal_ord1'", conA))
                                            {
                                                cmd.ExecuteNonQuery();
                                            }
                                            using (NpgsqlTransaction TR_BRAki = conA.BeginTransaction())
                                            {
                                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("DELETE FROM public.braki_tmp; ", conA))
                                                {
                                                    cmd1.ExecuteNonQuery();
                                                }
                                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("INSERT INTO public.braki_tmp (ordid, l_ordid, indeks, opis, planner_buyer, mag, data_dost, date_reuired, wlk_dost, bilans, typ_zdarzenia, status_informacji, dop, dop_lin, data_dop, zlec, prod_date, max_posible_prod, max_prod_date, ord_supp_dmd, part_code, ord_state, prod_qty, qty_supply, qty_demand, koor, order_no, line_no, rel_no, part_no, descr, configuration, last_mail_conf, prom_date, prom_week, load_id, ship_date, state_conf, line_state, cust_ord_state, country, shipment_day, date_entered, sort_ord, zest, ord_assinged,id,cust_id) VALUES (@ordid, @l_ordid, @indeks, @opis, @planner_buyer, @mag, @data_dost, @date_reuired, @wlk_dost, @bilans, @typ_zdarzenia, @status_informacji, @dop, @dop_lin, @data_dop, @zlec, @prod_date, @max_posible_prod, @max_prod_date, @ord_supp_dmd, @part_code, @ord_state, @prod_qty, @qty_supply, @qty_demand, @koor, @order_no, @line_no, @rel_no, @part_no, @descr, @configuration, @last_mail_conf, @prom_date, @prom_week, @load_id, @ship_date, @state_conf, @line_state, @cust_ord_state, @country, @shipment_day, @date_entered, @sort_ord, @zest, @ord_assinged,@id,@custid); ", conA))
                                                {
                                                    cmd1.Parameters.Add("@ordid", NpgsqlTypes.NpgsqlDbType.Text);
                                                    cmd1.Parameters.Add("@l_ordid", NpgsqlTypes.NpgsqlDbType.Bigint);
                                                    cmd1.Parameters.Add("@indeks", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@opis", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@planner_buyer", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@mag", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@data_dost", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@date_reuired", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@wlk_dost", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@bilans", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@typ_zdarzenia", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@status_informacji", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                                    cmd1.Parameters.Add("@dop_lin", NpgsqlTypes.NpgsqlDbType.Integer);
                                                    cmd1.Parameters.Add("@data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@zlec", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@prod_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@max_posible_prod", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@max_prod_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@ord_supp_dmd", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@part_code", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@prod_qty", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@qty_supply", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@qty_demand", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@koor", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@descr", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@configuration", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@last_mail_conf", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@prom_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@prom_week", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@load_id", NpgsqlTypes.NpgsqlDbType.Bigint);
                                                    cmd1.Parameters.Add("@ship_date", NpgsqlTypes.NpgsqlDbType.Date);
                                                    cmd1.Parameters.Add("@state_conf", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@line_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@cust_ord_state", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@country", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@shipment_day", NpgsqlTypes.NpgsqlDbType.Integer);
                                                    cmd1.Parameters.Add("@date_entered", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                                    cmd1.Parameters.Add("@sort_ord", NpgsqlTypes.NpgsqlDbType.Timestamp);
                                                    cmd1.Parameters.Add("@zest", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                    cmd1.Parameters.Add("@ord_assinged", NpgsqlTypes.NpgsqlDbType.Double);
                                                    cmd1.Parameters.Add("@id", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                    cmd1.Parameters.Add("@custid", NpgsqlTypes.NpgsqlDbType.Uuid);
                                                    cmd1.Prepare();
                                                    foreach (DataRow r in new_rek)
                                                    {
                                                        for (int i = 0; i < sou_braki.Columns.Count - 2; i++)
                                                        {
                                                            cmd1.Parameters[i].Value = r[i] ?? DBNull.Value;
                                                        }
                                                        cmd1.Parameters[sou_braki.Columns.Count - 2].Value = System.Guid.NewGuid();
                                                        cmd1.Parameters[sou_braki.Columns.Count - 1].Value = r[sou_braki.Columns.Count - 2] ?? DBNull.Value;
                                                        cmd1.ExecuteNonQuery();
                                                    }
                                                }
                                                using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE table_name='cal_ord1'", conA))
                                                {
                                                    cmd1.ExecuteNonQuery();
                                                }
                                                TR_BRAki.Commit();
                                            }
                                            conA.Close();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                try
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.datatbles	SET start_update=current_timestamp, in_progress=true WHERE table_name='cal_ord2'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("select cast(count(table_name) as integer) busy from public.datatbles where table_name='cal_ord1' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(1000); }
                            }
                        }
                        using (NpgsqlTransaction TR_Braki = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            Loger.Log("Update main table step1");
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("DELETE FROM public.braki WHERE id in (select a.id from (select id,cust_id,ordid,dop_lin,data_dost,indeks from braki) a left join (select cust_id,ordid,dop_lin,data_dost,indeks from braki_tmp) b on b.ordid=a.ordid and b.dop_lin=a.dop_lin and b.data_dost=a.data_dost and b.indeks=a.indeks where b.ordid is null)", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Loger.Log("Update main table step2");
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.braki	SET l_ordid=a.l_ordid,opis=a.opis,planner_buyer=a.planner_buyer, mag=a.mag,date_reuired=a.date_reuired, wlk_dost=a.wlk_dost,bilans=a.bilans, typ_zdarzenia=a.typ_zdarzenia, status_informacji=a.status_informacji, dop= a.dop,data_dop=a.data_dop, zlec=a.zlec, prod_date=a.prod_date,max_posible_prod=a.max_posible_prod, max_prod_date=a.max_prod_date, ord_supp_dmd=a.ord_supp_dmd, part_code=a.part_code, ord_state=a.ord_state,prod_qty=a.prod_qty, qty_supply=a.qty_supply, qty_demand=a.qty_demand, koor=a.koor, order_no=a.order_no, line_no=a.line_no, rel_no=a.rel_no, part_no=a.part_no, descr= a.descr,configuration=a.configuration, last_mail_conf=a.last_mail_conf, prom_date=a.prom_date, prom_week=a.prom_week, load_id=a.load_id, ship_date=a.ship_date, state_conf=a.state_conf,line_state=a.line_state, cust_ord_state=a.cust_ord_state, country=a.country, shipment_day=a.shipment_day, date_entered=a.date_entered, sort_ord=a.sort_ord,zest=a.zest,ord_assinged=a.ord_assinged,cust_id=a.cust_id from (select a.*,b.id from(select ordid, l_ordid, indeks, opis, planner_buyer, mag, data_dost, date_reuired, wlk_dost, bilans, typ_zdarzenia, status_informacji, dop, dop_lin, data_dop, zlec, prod_date, max_posible_prod, max_prod_date, ord_supp_dmd, part_code, ord_state, prod_qty, qty_supply, qty_demand, koor, order_no, line_no, rel_no, part_no, descr, configuration, last_mail_conf, prom_date, prom_week, load_id, ship_date, state_conf, line_state, cust_ord_state, country, shipment_day, date_entered, sort_ord, zest, ord_assinged,cust_id from braki_tmp a  except  select ordid, l_ordid, indeks, opis, planner_buyer, mag, data_dost, date_reuired, wlk_dost, bilans, typ_zdarzenia, status_informacji, dop, dop_lin, data_dop, zlec, prod_date, max_posible_prod, max_prod_date, ord_supp_dmd, part_code, ord_state, prod_qty, qty_supply, qty_demand, koor, order_no, line_no, rel_no, part_no, descr, configuration, last_mail_conf, prom_date, prom_week, load_id, ship_date, state_conf, line_state, cust_ord_state, country, shipment_day, date_entered, sort_ord, zest, ord_assinged,cust_id from braki ) a left join (select cust_id,ordid,dop_lin,data_dost,indeks,id from braki) b on b.ordid=a.ordid and b.dop_lin=a.dop_lin and b.data_dost=a.data_dost and b.indeks=a.indeks where b.ordid is not null) a where public.braki.id=a.id", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Loger.Log("Update main table step3:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("insert into public.braki(ordid, l_ordid, indeks, opis, planner_buyer, mag, data_dost, date_reuired, wlk_dost, bilans, typ_zdarzenia, status_informacji, dop, dop_lin, data_dop, zlec, prod_date, max_posible_prod, max_prod_date, ord_supp_dmd, part_code, ord_state, prod_qty, qty_supply, qty_demand, koor, order_no, line_no, rel_no, part_no, descr, configuration, last_mail_conf, prom_date, prom_week, load_id, ship_date, state_conf, line_state, cust_ord_state, country, shipment_day, date_entered, sort_ord, zest, ord_assinged, id, cust_id) select a.* from (select * from braki_tmp) a left join (select cust_id,ordid,dop_lin,data_dost,indeks from braki) b on b.ordid=a.ordid and b.dop_lin=a.dop_lin and b.data_dost=a.data_dost and b.indeks=a.indeks where b.ordid is null", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.datatbles SET start_update=current_timestamp, in_progress=false,updt_errors=false WHERE substr(table_name,1,4)='mail'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE table_name='cal_ord2'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            TR_Braki.Commit();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.datatbles	SET start_update=current_timestamp, in_progress=true WHERE table_name='cal_ord3'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.datatbles	SET start_update=current_timestamp, in_progress=true WHERE table_name='cal_ord5'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("select cast(count(table_name) as integer) busy from public.datatbles where table_name='cal_ord2' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(1000); }
                            }
                        }
                        using (NpgsqlTransaction TR_refr = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("REFRESH MATERIALIZED VIEW to_mail", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE table_name='cal_ord5'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            TR_refr.Commit();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("select cast(count(table_name) as integer) busy from public.datatbles where table_name='cal_ord5' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(1000); }
                            }
                        }
                        using (NpgsqlTransaction TR_MAILDEL = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("delete from mail where status_informacji='NOT IMPLEMENT' or cust_line_stat!='Aktywowana'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Loger.Log("Update mail step1:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("DELETE FROM public.mail WHERE cust_id not in (select id from public.cust_ord)", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Loger.Log("Update mail step:" + (DateTime.Now - start));
                            // using (NpgsqlCommand cmd1 = new NpgsqlCommand("DELETE FROM public.mail WHERE cust_id in (select a.cust_id from mail a left join braki c on c.cust_id=a.cust_id or c.zest=a.zest,cust_ord b where a.indeks!='ZESTAW' and a.cust_id=b.id and b.data_dop<a.prod and c.cust_id is null)", conA))
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("DELETE FROM public.mail	WHERE cust_id in (select a.cust_id from mail a left join to_mail b on b.cust_id=a.cust_id where b.cust_id is null and (is_for_mail(a.status_informacji)=false or a.status_informacji='POPRAWIĆ'))", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE table_name='cal_ord3'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            TR_MAILDEL.Commit();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.datatbles	SET start_update=current_timestamp, in_progress=true WHERE table_name='cal_ord4'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("select cast(count(table_name) as integer) busy from public.datatbles where table_name='cal_ord3' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(1000); }
                            }
                        }
                        using (NpgsqlTransaction TR_MAIL = conA.BeginTransaction(IsolationLevel.ReadCommitted))
                        {
                            Loger.Log("Update mail step0:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("INSERT INTO public.mail(ordid,dop, koor, order_no, line_no, rel_no, part_no, descr, country, prom_date, prom_week, load_id, ship_date, date_entered, cust_id, prod, prod_week, planner_buyer, indeks, opis, typ_zdarzenia, status_informacji, zest, info_handlo, logistyka, seria0, data0, cust_line_stat, ord_objver) select * from (select a.* from to_mail a left join (select cust_id from mail) b on b.cust_id=a.cust_id where a.cust_id is not null and b.cust_id is null  order by order_no,line_no,rel_no) a  group by a.ordid,a.dop,a.koor,a.order_no,a.line_no,a.rel_no,a.part_no,a.descr,a.country,a.prom_date,a.prom_week,a.load_id,a.ship_date,a.date_entered,a.cust_id,a.prod,a.prod_week,a.planner_buyer,a.indeks,a.opis,a.typ_zdarzenia,a.status_informacji,a.zest,a.info_handlo,a.logistyka,a.seria0,a.data0,a.cust_line_stat,a.ord_objver", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            Loger.Log("Update mail step2:" + (DateTime.Now - start));
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.mail a SET ordid=b.ordid,dop=b.dop,koor=b.koor,order_no=b.order_no,line_no=b.line_no,rel_no=b.rel_no,part_no=b.part_no,descr=b.descr,country=b.country,prom_date=b.prom_date,prom_week=b.prom_week,load_id=b.load_id,ship_date=b.ship_date,date_entered=b.date_entered,prod=b.prod,prod_week=b.prod_week,planner_buyer=b.planner_buyer,indeks=b.indeks,opis=b.opis,typ_zdarzenia=b.typ_zdarzenia,status_informacji=b.status_informacji,zest=b.zest,info_handlo=b.info_handlo,logistyka=b.logistyka,seria0=b.seria0,data0=b.data0,cust_line_stat=b.cust_line_stat from (select * from (select a.ordID,a.dop,a.koor,a.order_no,a.line_no,a.rel_no,a.part_no,a.descr,a.country,a.prom_date,a.prom_week,a.load_id,a.ship_date,a.date_entered,a.cust_id,a.prod,a.prod_week,a.planner_buyer,a.indeks,a.opis,a.typ_zdarzenia,a.status_informacji,a.zest,a.info_handlo,a.logistyka,a.seria0,a.data0,a.cust_line_stat from to_mail a, mail b where b.cust_id=a.cust_id except select ordID,dop,koor,order_no,line_no,rel_no,part_no,descr,country,prom_date,prom_week,load_id,ship_date,date_entered,cust_id,prod,prod_week,planner_buyer,indeks,opis,typ_zdarzenia,status_informacji,zest,info_handlo,logistyka,seria0,data0,cust_line_stat from mail ) a) b  where b.cust_id=a.cust_id;", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            // usuń z mail już przesunięte zamówienia dla których nie było żadnych zagrożeń 
                            Loger.Log("Update mail step3:" + (DateTime.Now - start));

                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("update public.mail a SET ordid=b.ordid,dop=b.dop,koor=b.koor,order_no=b.order_no,line_no=b.line_no,rel_no=b.rel_no,part_no=b.part_no,descr=b.descr,country=b.country,prom_date=b.prom_date,prom_week=b.prom_week,load_id=b.load_id,ship_date=b.ship_date,date_entered=b.date_entered,prod=b.prod,prod_week=b.prod_week,info_handlo=b.info_handlo,logistyka=b.logistyka,seria0=b.seria0,data0=b.data0,cust_line_stat=b.cust_line_stat from (select a.* from (Select case when c.dop_connection_db!='AUT' then 'O '||c.order_no else 'D'||to_char(c.dop_id,'9999999999') end ordID,c.dop_id as dop,c.koor,c.order_no,c.line_no,c.rel_no,c.part_no,c.descr,c.country,c.prom_date,cast(c.prom_week as integer) as prom_week,c.load_id,c.ship_date, c.date_entered,c.id as cust_id,c.data_dop as prod,case when to_date(to_char(c.data_dop,'iyyyiw'),'iyyyiw') + shipment_day(c.country,c.cust_no,c.zip_code,c.addr1)-1>c.data_dop then cast(to_char(c.data_dop,'iyyyiw') as integer) else cast(to_char(c.data_dop + interval '6 day','iyyyiw') as integer) end as prod_week,c.planner_buyer,c.indeks,c.opis,c.status_informacji,c.zest,case when case when to_date(to_char(c.data_dop,'iyyyiw'),'iyyyiw') + shipment_day(c.country,c.cust_no,c.zip_code,c.addr1)-1>c.data_dop then cast(to_char(c.data_dop,'iyyyiw') as integer) else cast(to_char(c.data_dop + interval '6 day','iyyyiw') as integer) end>cast(c.prom_week as integer) then true else false end as info_handlo,case when c.ship_date is null then false else case when c.data_dop<c.ship_date then false else true  end end as logistyka,c.seria0,c.data0,c.line_state as cust_line_stat from (select c.*,a.planner_buyer,a.indeks,a.opis,a.status_informacji from mail a left join to_mail b on b.cust_id=a.cust_id left join cust_ord c on c.id=a.cust_id where b.cust_id is null )c except select ordID,dop,koor,order_no,line_no,rel_no,part_no,descr,country,prom_date,prom_week,load_id,ship_date,date_entered,cust_id,prod,prod_week,planner_buyer,indeks,opis,status_informacji,zest,info_handlo,logistyka,seria0,data0,cust_line_stat  from mail ) a) b where a.cust_id=b.cust_id", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE substr(table_name,1,4)='mail'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("DELETE FROM public.mail	WHERE cust_id in (select a.cust_id from mail a left join to_mail b on b.cust_id=a.cust_id left join cust_ord c on c.id=a.cust_id where b.cust_id is null and is_for_mail(a.status_informacji)=true and c.data_dop>=a.prod and cast(c.prom_week as integer)>=a.prod_week and (c.ship_date is null or c.ship_date>a.prod))", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("delete from braki_hist where objversion<current_timestamp - interval '7 day'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("delete from mail_hist where date_addd<current_timestamp - interval '7 day'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE table_name='cal_ord4'", conA))
                            {
                                cmd1.ExecuteNonQuery();
                            }
                            TR_MAIL.Commit();
                        }
                        conA.Close();
                    }
                }
                catch (Exception e)
                {
                    Loger.Log("Błąd Problem z aktualizacją braków:" + e);
                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd1 = new NpgsqlCommand("delete from braki_hist where id in (select b.id from braki a,braki_hist b where a.id=b.id)", conA))
                        {
                            cmd1.ExecuteNonQuery();
                        }
                        conA.Close();
                    }
                }
                GC.Collect();
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.datatbles SET last_modify=current_timestamp, in_progress=false,updt_errors=false WHERE substr(table_name,1,5)='braki'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                int rwa = await modify_prod_date();
                return 0;
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.datatbles SET in_progress=false,updt_errors=true WHERE substr(table_name,1,5)='braki'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Loger.Log("Problem z obliczaniem bilansów braków");
                return 1;
            }
        }
        async Task<int> modify_prod_date()
        {
            try
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("select cast(count(table_name) as integer) busy from public.datatbles where substring(table_name,1,7)='cal_ord' and in_progress=true", conA))
                    {
                        int busy_il = 1;
                        while (busy_il > 0)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0) { System.Threading.Thread.Sleep(1000); }
                        }
                    }
                    conA.Close();
                }
                //int ra = await Send_mail_lack();
                Loger.Log("Start modyfing prod date in ORD_DOP");
                using (DataTable ord_mod = new DataTable())
                {
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        await conA.OpenAsync();
                        //using (NpgsqlCommand cmd = new NpgsqlCommand("select order_no,line_no,rel_no,prod from mail where status_informacji='WYKONANIE' or (info_handlo=false and logistyka=false and status_informacji!='POPRAWIĆ')", conA))
                        using (NpgsqlCommand cmd = new NpgsqlCommand("select order_no,line_no,rel_no,prod,typ_zdarzenia,status_informacji,dop,indeks,opis,get_date_dop(cust_id) data_dop from mail where prod>=current_date and ((status_informacji='WYKONANIE' and info_handlo=false) or (is_confirm(status_informacji)=true and get_date_dop(cust_id)!=prod and seria0=false) or (is_alter(status_informacji)=true and get_date_dop(cust_id)!=prod ) and info_handlo=false) and get_dopstat(cust_id) is not null", conA))
                        {
                            using (NpgsqlDataReader po = cmd.ExecuteReader())
                            {
                                ord_mod.Load(po);
                            }
                        }
                        conA.Close();
                    }
                    using (OracleConnection conO = new OracleConnection(Oracle_conn.Connection_string))
                    {
                        await conO.OpenAsync();
                        OracleGlobalization info = conO.GetSessionInfo();
                        info.DateFormat = "YYYY-MM-DD";
                        conO.SetSessionInfo(info);
                        using (OracleCommand comm = new OracleCommand("ifsapp.c_customer_order_line_api.Cancel_Schedule", conO))
                        {
                            comm.CommandType = CommandType.StoredProcedure;
                            comm.Parameters.Add("order_no", OracleDbType.Varchar2);
                            comm.Parameters.Add("line_no", OracleDbType.Varchar2);
                            comm.Parameters.Add("rel_no", OracleDbType.Varchar2);
                            comm.Parameters.Add("line_item_no", OracleDbType.Decimal);
                            comm.Prepare();
                            foreach (DataRowView rek in ord_mod.DefaultView)
                            {
                                try
                                {
                                    comm.Parameters[0].Value = rek[0];
                                    comm.Parameters[1].Value = rek[1];
                                    comm.Parameters[2].Value = rek[2];
                                    comm.Parameters[3].Value = 0;
                                    comm.ExecuteNonQuery();
                                }
                                catch (Exception e)
                                {
                                    Loger.Log("Nie zdjęto harmonogramu " + e);
                                }
                            }
                        }
                        using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                        {
                            await conA.OpenAsync();
                            //using (NpgsqlCommand cmd = new NpgsqlCommand("select order_no,line_no,rel_no,prod from mail where status_informacji='WYKONANIE' or (info_handlo=false and logistyka=false and status_informacji!='POPRAWIĆ')", conA))
                            using (NpgsqlCommand cmd = new NpgsqlCommand("INSERT INTO public.mod_date(order_no, line_no, rel_no, prod, typ_zdarzenia, status_informacji, dop, err,indeks,opis,data_dop,date_add) VALUES (@order_no, @line_no, @rel_no, @prod, @typ_zdarzenia, @status_informacji, @dop, @err,@indeks,@opis,@data_dop,current_timestamp)", conA))
                            {
                                cmd.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("prod", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Parameters.Add("typ_zdarzenia", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("status_informacji", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                cmd.Parameters.Add("err", NpgsqlTypes.NpgsqlDbType.Boolean);
                                cmd.Parameters.Add("indeks", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("opis", NpgsqlTypes.NpgsqlDbType.Varchar);
                                cmd.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                cmd.Prepare();
                                using (OracleCommand comm = new OracleCommand("ifsapp.c_customer_order_line_api.Set_Planned_Manuf_Date", conO))
                                {
                                    comm.CommandType = CommandType.StoredProcedure;
                                    comm.Parameters.Add("order_no", OracleDbType.Varchar2);
                                    comm.Parameters.Add("line_no", OracleDbType.Varchar2);
                                    comm.Parameters.Add("rel_no", OracleDbType.Varchar2);
                                    comm.Parameters.Add("line_item_no", OracleDbType.Decimal);
                                    comm.Parameters.Add("planned_date", OracleDbType.Date);
                                    comm.Prepare();
                                    foreach (DataRowView rek in ord_mod.DefaultView)
                                    {
                                        try
                                        {
                                            comm.Parameters[0].Value = rek[0];
                                            comm.Parameters[1].Value = rek[1];
                                            comm.Parameters[2].Value = rek[2];
                                            comm.Parameters[3].Value = 0;
                                            comm.Parameters[4].Value = rek[3];
                                            comm.ExecuteNonQuery();
                                            cmd.Parameters[0].Value = rek[0];
                                            cmd.Parameters[1].Value = rek[1];
                                            cmd.Parameters[2].Value = rek[2];
                                            cmd.Parameters[3].Value = rek[3];
                                            cmd.Parameters[4].Value = rek[4];
                                            cmd.Parameters[5].Value = rek[5];
                                            cmd.Parameters[6].Value = rek[6];
                                            cmd.Parameters[7].Value = false;
                                            cmd.Parameters[8].Value = rek[7];
                                            cmd.Parameters[9].Value = rek[8];
                                            cmd.Parameters[10].Value = rek[9];
                                            cmd.ExecuteNonQuery();
                                        }
                                        catch
                                        {
                                            cmd.Parameters[0].Value = rek[0];
                                            cmd.Parameters[1].Value = rek[1];
                                            cmd.Parameters[2].Value = rek[2];
                                            cmd.Parameters[3].Value = rek[3];
                                            cmd.Parameters[4].Value = rek[4];
                                            cmd.Parameters[5].Value = rek[5];
                                            cmd.Parameters[6].Value = rek[6];
                                            cmd.Parameters[7].Value = true;
                                            cmd.Parameters[8].Value = rek[7];
                                            cmd.Parameters[9].Value = rek[8];
                                            cmd.Parameters[10].Value = rek[9];
                                            cmd.ExecuteNonQuery();
                                            Loger.Log("Nie przeplanowano :" + rek[0] + "_" + rek[1] + "_" + rek[2] + " na datę:" + rek[3]);
                                        }
                                    }
                                }
                            }
                            conA.Close();
                        }
                        conO.Close();
                    }
                }
                Loger.Log("END modyfing prod date in ORD_DOP");
                return 0;
            }
            catch (Exception e)
            {
                Loger.Log("Błąd modyfikacji daty produkcji:" + e);
                return 1;
            }
        }
    }
}
