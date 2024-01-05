using DB_Conect;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static Confirm_server_by_Contracts.Calculate_cust_ord;

namespace Confirm_server_by_Contracts
{
    public class Calculate_cust_ord : Update_pstgr_from_Ora<Order_lack_row>
    {
        private readonly Update_pstgr_from_Ora<Order_lack_row> rw;
        private readonly Update_pstgr_from_Ora<Balance_materials_row> bal_r;
        public Calculate_cust_ord(CancellationToken cancellationToken)
        {
            Steps_executor.Register_step("Calculate_cust_order");
            rw = new Update_pstgr_from_Ora<Order_lack_row>("MAIN");
            bal_r = new Update_pstgr_from_Ora<Balance_materials_row>("MAIN");
            Parallel.Invoke(
            async () => 
            {
                await Update(cancellationToken).ConfigureAwait(false);
            });
            if (Steps_executor.Wait_for(new string[] { "Calculate_cust_order" }, "Validate demands", cancellationToken))
            {
                Steps_executor.Register_step("Update To_Mail");
                Run_query query = new Run_query();
                Parallel.Invoke(async () =>
                {
                    await query.Execute_in_Postgres(new[] { "" +
                        "REFRESH MATERIALIZED VIEW to_mail" }, "Update To_Mail", cancellationToken).ConfigureAwait(false);
                    Steps_executor.End_step("Update To_Mail");
                });   
            }


        }

        private async Task<int> Update(CancellationToken cancellationToken)
        {
            List<Order_lack_row> Old = await Old_data(cancellationToken);
            List<Order_lack_row> New = await Calculate(cancellationToken);
            Old.Sort(delegate (Order_lack_row c1, Order_lack_row c2)
            {
                int val1 = c1.Ordid.CompareTo(c2.Ordid);
                if (val1 != 0) { return val1; }
                int val2 = c1.Indeks.CompareTo(c2.Indeks);
                if (val2 != 0) { return val2; }
                return c1.Data_dost.CompareTo(c2.Data_dost);
            });
            New.Sort(delegate (Order_lack_row c1, Order_lack_row c2)
            {
                int val1 = c1.Ordid.CompareTo(c2.Ordid);
                if (val1 != 0) { return val1; }
                int val2 = c1.Indeks.CompareTo(c2.Indeks);
                if (val2 != 0) { return val2; }
                return c1.Data_dost.CompareTo(c2.Data_dost);
            });
            int result = await rw.PSTRG_Changes_to_dataTable(
                await rw.Changes(
                    Old,
                    New,
                    new[] { "ordid", "indeks", "data_dost" },
                    new[] { "id" },
                    new[] {"id"},
                    "Calculate_cust_order",
                    cancellationToken
                    ),
                "braki",
                new[] { "id" },
                null,
                null,
                "Calculate_cust_order",
                cancellationToken
                );
            Steps_executor.End_step("Calculate_cust_order");
            return result;
        }

        private async Task<List<Order_lack_row>> Old_data(CancellationToken cancellationToken) => await rw.Get_PSTGR("" +
            "Select * FROM braki;", "Calculate_cust_order", cancellationToken );

        private async Task<List<Order_lack_row>> New_Data(CancellationToken cancellationToken) => await rw.Get_PSTGR("" +
            @"select 
                case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end ordID,
                f.l_ordid ,
                c.indeks,
                c.umiejsc,
                c.opis,
                c.planner_buyer,
                c.mag, 
                c.data_dost ,
                a.date_required date_reuired,
                c.wlk_dost,
                c.bilans,
                c.typ_zdarzenia,
                c.status_informacji,
                c.data_gwarancji,
                a.dop,
                a.dop_lin,
                a.data_dop,
                a.order_no zlec,
                date_shift_days(case when  c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end,a.day_shift,c.umiejsc) as prod_date,
                to_date(b.prom_week, 'iyyyiw')+shipment_day(b.country,b.cust_no,b.zip_code,b.addr1)-1 as max_posible_prod,
                e.max_prod_date,
                a.order_supp_dmd ord_supp_dmd,
                a.part_code,
                a.ord_state,
                a.prod_qty,
                a.qty_supply,
                a.qty_demand,
                b.koor,
                b.order_no,
                b.line_no,
                b.rel_no,
                b.part_no,
                b.descr,
                b.configuration,
                b.last_mail_conf,
                b.prom_date,
                b.prom_week,
                b.load_id,
                b.ship_date,
                b.state_conf,
                b.line_state,
                b.cust_order_state cust_ord_state,
                b.country,
                shipment_day(b.country,b.cust_no,b.zip_code,b.addr1),COALESCE (b.date_entered,a.dat_creat) as date_entered,
                case when to_date(b.prom_week, 'iyyyiw')+shipment_day(b.country,b.cust_no,b.zip_code,b.addr1)-1<date_shift_days(c.data_dost,a.day_shift,c.umiejsc) then 
	                COALESCE (b.date_entered,a.dat_creat) else  COALESCE (b.date_entered,a.dat_creat,'2014-01-01')+ interval '200 day' end  as sort_ord,
                b.zest,
                qty_demand-qty_demand ord_assinged,
                b.id cust_id 
                from 
                (
	                select 
	                c.indeks,
	                c.umiejsc,
	                c.data_dost,
	                max(date_shift_days(case when  c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end,a.day_shift, c.umiejsc)) as max_prod_date,
	                count(case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end) 
	                from 
	                (
		                select * from public.data 
		                where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ'
	                )c,
	                ord_demands a 
	                where c.bilans<0 and a.part_no=c.indeks and a.contract=c.umiejsc
	                and (a.date_required<=c.data_dost or a.date_required<=current_date) 
	                group by indeks,umiejsc,data_dost
                ) e,
	            (
		            select 
		            case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end ordid,
		            count(case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end) l_ordid 
		            from 
		            (
			            select * from public.data 
			            where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ'
		            )c,
		            ord_demands a 
		            where c.bilans<0 and a.part_no=c.indeks and 
		            (a.date_required<=c.data_dost or a.date_required<=current_date) 
		            group by case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end
	            ) f,
	            (
		            select * from public.data 
		            where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ'
	            )c,
	            ord_demands a 
	            left join 
	            cust_ord b 
	            on b.dop_id=a.dop 
	            where a.order_supp_dmd!='Zam. zakupu' and f.ordid=case when a.dop=0 then 'O '||a.order_no else 'D'||to_char(a.dop,'9999999999') end 
	            and c.bilans<0 and a.part_no=c.indeks and e.indeks=c.indeks and a.contract=c.umiejsc and e.umiejsc=c.umiejsc
	            and (case when typ_zdarzenia='Brakujące ilości' then a.date_required<c.data_dost else a.date_required<=c.data_dost end or a.date_required<=current_date) 
	            and e.data_dost=c.data_dost",
                    "Calculate_cust_ord",
                    cancellationToken);

        private async Task<List<Balance_materials_row>> Get_new_balances(CancellationToken cancellationToken) => await bal_r.Get_PSTGR("" +
            @"select 
                c.indeks,
                c.umiejsc,
                c.mag,
                count(case when order_supp_dmd!='Zam. zakupu' then a.part_no end) il,
                case when c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end data_dost,
                c.wlk_dost,
                c.sum_dost,
                c.bilans,
                c.mag-sum(a.qty_demand)+case when c.typ_zdarzenia in ('Dzisiejsza dostawa','Opóźniona dostawa') then 0 else sum(a.qty_supply) end bil_chk,
                c.typ_zdarzenia,
                e.max_prod_date,
                sum(a.qty_supply) dost,
                sum(a.qty_demand) potrz,
                sum_dost-sum_dost qty 
            from 
                (
                    select 
                        c.indeks,
                        c.umiejsc,
                        c.data_dost,
                        max(date_shift_days(case when  c.typ_zdarzenia='Braki w gwarantowanej dacie' then c.data_gwarancji else c.data_dost end,a.day_shift)) as max_prod_date 
                     from 
                        (
                            select * from public.data 
                            where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') 
                            and planner_buyer!='LUCPRZ'
                        )c,
                        ord_demands a 
                        where c.bilans<0 and a.part_no=c.indeks and a.contract=c.umiejsc 
                            and (a.date_required<=c.data_dost or a.date_required<=current_date) 
                        group by indeks,umiejsc,data_dost
                ) e,
            (
                select * from public.data 
                where typ_zdarzenia not in ('Brak zamówień zakupu','Dostawa na dzisiejsze ilości') and planner_buyer!='LUCPRZ'
            )c,
            ord_demands a 
            left join 
            cust_ord b 
            on b.dop_id=a.dop 
            where c.bilans<0 and a.part_no=c.indeks and a.contract=c.umiejsc and e.indeks=c.indeks 
            and e.umiejsc=c.umiejsc and ((c.typ_zdarzenia='Brakujące ilości' and a.date_required<c.data_dost) or 
            (c.typ_zdarzenia!='Brakujące ilości' and a.date_required<=c.data_dost) or a.date_required<=current_date) 
            and e.data_dost=c.data_dost group by c.indeks,c.umiejsc,c.mag,c.data_gwarancji,c.data_dost,c.wlk_dost,c.sum_dost,c.bilans,c.typ_zdarzenia,e.max_prod_date",
            "Calculate_cust_ord",
             cancellationToken);

        private async Task<List<Order_lack_row>> Calculate(CancellationToken cancellationToken)
        {
            List<Order_lack_row> mat_ord = await New_Data(cancellationToken);
            List<Balance_materials_row> mat_dmd = await Get_new_balances(cancellationToken);
            Dictionary<string, List<int>> zest = new Dictionary<string, List<int>>();
            Dictionary<string, List<int>> ordid = new Dictionary<string, List<int>>();
            Dictionary<string, DateTime, int> poz_dmd = new Dictionary<string, DateTime, int>();
            int counter = 0;
            int cnt = 0;
            Parallel.Invoke(
                () =>
                {
                    foreach (Order_lack_row mat in mat_ord)
                    {
                        if (cancellationToken.IsCancellationRequested) { break; }
                        if (mat.Zest != null)
                        {
                            if (!zest.ContainsKey(mat.Zest))
                            {
                                zest.Add(mat.Zest, new List<int> { counter });
                            }
                            else
                            {
                                zest[mat.Zest].Add(counter);
                            }

                        }
                        if (!ordid.ContainsKey(mat.Ordid))
                        {
                            ordid.Add(mat.Ordid, new List<int> { counter });
                        }
                        else
                        {
                            ordid[mat.Ordid].Add(counter);
                        }
                        counter++;
                    }
                },
                () =>
                {
                    foreach (Balance_materials_row dmd in mat_dmd)
                    {
                        if (cancellationToken.IsCancellationRequested) { break; }
                        poz_dmd.Add(dmd.Indeks, dmd.Data_dost, cnt);
                        cnt++;
                    }
                });

            (string, DateTime) get_key_pair(int item)
            {
                return mat_ord[item].Typ_zdarzenia == "Braki w gwarantowanej dacie" ? (mat_ord[item].Indeks, mat_ord[item].Data_gwarancji) : (mat_ord[item].Indeks, mat_ord[item].Data_dost);
            }

            double get_bil(List<int> range, double prev_bil, bool check_state = false)
            {
                bool is_started = false;
                if (check_state)
                {
                    foreach (int item in range)
                    {
                        if (cancellationToken.IsCancellationRequested) { break; }
                        (string ind, DateTime dat) = get_key_pair(item);
                        is_started = cnt == poz_dmd[ind, dat] && mat_ord[item].Ord_state == "Rozpoczęte";
                        if (is_started) { break; }
                    }
                }
                if (!is_started)
                {
                    foreach (int item in range)
                    {
                        if (cancellationToken.IsCancellationRequested) { break; }
                        if (counter < item && mat_ord[item].Ord_assinged == 0)
                        {
                            (string ind, DateTime dat) = get_key_pair(item);
                            if (cnt == poz_dmd[ind, dat])
                            {
                                prev_bil -= mat_ord[item].Qty_demand;
                                mat_ord[item].Ord_assinged = mat_ord[item].Qty_demand;
                            }
                            else
                            {
                                mat_dmd[poz_dmd[ind, dat]].Qty -= mat_ord[item].Qty_demand;
                                mat_ord[item].Ord_assinged = mat_ord[item].Qty_demand;
                            }                                                   
                        }
                    }
                }
                return prev_bil;
            }

            cnt = -1;
            counter = -1;
            string part_no = "";
            string contract = "";
            double qt = 0;
            double bil = 0;
            foreach (Order_lack_row rw in mat_ord)
            {
                if (cancellationToken.IsCancellationRequested) { break; }
                counter++;
                if ((part_no, contract) != (rw.Indeks, rw.Umiejsc))
                {
                    cnt++;
                    qt = mat_dmd[cnt].Bil_chk;
                    bil = mat_dmd[cnt].Qty;
                    (part_no, contract) = (rw.Indeks, rw.Umiejsc);
                }
                if ( bil > qt )
                {
                    if (rw.Ord_state != "Rozpoczęte" && rw.Ord_assinged == 0)
                    {                        
                        if (rw.Zest != null && zest.ContainsKey(rw.Zest))
                        {
                            bil = get_bil(zest[rw.Zest], bil, true);
                        }
                        else if (ordid.ContainsKey(rw.Ordid))
                        {                           
                            bil = get_bil(ordid[rw.Ordid], bil);  
                        }
                        bil -= rw.Qty_demand;
                        rw.Ord_assinged = rw.Qty_demand;
                        mat_dmd[cnt].Qty -= rw.Qty_demand;
                    }
                }
            }
            return (List<Order_lack_row>)mat_ord.Where(x => x.Ord_assinged > 0).ToList();
        }
        
        public class Order_lack_row : IEquatable<Order_lack_row> , IComparable<Order_lack_row>
        {
            public string Ordid { get; set; }
            public long L_ordid { get; set; }
            public string Indeks { get; set; }
            public string Umiejsc { get; set; }
            public string Opis { get; set; }
            public string Planner_buyer { get; set; }
            public double Mag { get; set; }
            public DateTime Data_gwarancji { get; set; }
            public DateTime Data_dost { get; set; }
            public DateTime Date_reuired { get; set; }
            public double Wlk_dost { get; set; }
            public double Bilans { get; set; }
            public string Typ_zdarzenia { get; set; }
            public string Status_informacji { get; set; }
            public int Dop { get; set; }
            public int Dop_lin { get; set; }
            public DateTime Data_dop { get; set; }
            public string Zlec { get; set; }
            public DateTime Prod_date { get; set; }
            public DateTime Max_posible_prod { get; set; }
            public DateTime Max_prod_date { get; set; }
            public string Ord_supp_dmd { get; set; }
            public string Part_code { get; set; }
            public string Ord_state { get; set; }
            public double Prod_qty { get; set; }
            public double Qty_supply { get; set; }
            public double Qty_demand { get; set; }
            public string Koor { get; set; }
            public string Order_no { get; set; }
            public string Line_no { get; set; }
            public string Rel_no { get; set; }
            public string Part_no { get; set; }
            public string Descr { get; set; }
            public string Configuration { get; set; }
            public DateTime? Last_mail_conf { get; set; }
            public DateTime? Prom_date { get; set; }
            public string Prom_week { get; set; }
            public long? Load_id { get; set; }
            public DateTime? Ship_date { get; set; }
            public string State_conf { get; set; }
            public string Line_state { get; set; }
            public string Cust_ord_state { get; set; }
            public string Country { get; set; }
            public int Shipment_day { get; set; }
            public DateTime Date_entered { get; set; }
            public DateTime Sort_ord { get; set; }
            public string Zest { get; set; }
            public double Ord_assinged { get; set; }
            public Guid Id { get; set; }
            public Guid? Cust_id { get; set; }

            public int CompareTo(Order_lack_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    int res1 = this.Max_prod_date.CompareTo(other.Max_prod_date) * -1;
                    if (res1 != 0)
                    {
                        return res1;
                    }
                    int res2 = this.Indeks.CompareTo(other.Indeks);
                    if (res2 != 0)
                    {
                        return res2;
                    }
                    int res3 = this.Umiejsc.CompareTo(other.Umiejsc);
                    if (res3 != 0)
                    {
                        return res3;
                    }
                    int res4 = this.Data_dost.CompareTo(other.Data_dost);
                    if (res4 != 0)
                    {
                        return res4;
                    }
                    return this.Sort_ord.CompareTo(other.Sort_ord) * -1;
                }
            }

            public bool Equals(Order_lack_row other)
            {
                if (other == null) return false;
                return
                    (
                    this.Max_prod_date.Equals(other.Max_prod_date) &&
                    this.Indeks.Equals(other.Indeks) &&
                    this.Umiejsc.Equals(other.Umiejsc) &&
                    this.Data_dost.Equals(other.Data_dost) &&
                    this.Sort_ord.Equals(other.Sort_ord)
                    );
            }
        }

        public class Balance_materials_row : IEquatable <Balance_materials_row>, IComparable<Balance_materials_row>
        { 
            public string Indeks { get; set; }
            public string Umiejsc {  get; set; }
            public double Mag {  get; set; }
            public long Il { get; set; }
            public DateTime Data_dost {  get; set; }
            public double Wlk_dost { get; set; }
            public double Sum_dost { get; set; }
            public double Bilans {  get; set; }
            public double Bil_chk {  get; set; }    
            public string Typ_zdarzenia { get; set; }
            public DateTime Max_prod_date { get; set; }
            public double Dost {  get; set; }
            public double Potrz { get; set; }
            public double Qty { get; set; }

            public int CompareTo(Balance_materials_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    int res1 = this.Max_prod_date.CompareTo(other.Max_prod_date) * -1;
                    if (res1 != 0)
                    {
                        return res1;
                    }
                    int res2 = this.Indeks.CompareTo(other.Indeks);
                    if (res2 != 0)
                    {
                        return res2;
                    }
                    int res3 = this.Umiejsc.CompareTo(other.Umiejsc);
                    if (res3 != 0)
                    {
                        return res3;
                    }
                    return this.Data_dost.CompareTo(other.Data_dost) * -1;
                }
            }

            public bool Equals(Balance_materials_row other)
            {
                if (other == null) return false;
                return
                    (
                    this.Max_prod_date.Equals(other.Max_prod_date) &&
                    this.Indeks.Equals(other.Indeks) &&
                    this.Umiejsc.Equals(other.Umiejsc) &&
                    this.Data_dost.Equals(other.Data_dost)
                    );
            }
        }
    }
}
