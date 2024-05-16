using DB_Conect;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using static Confirm_server_by_Contracts.Demands;
using static Confirm_server_by_Contracts.Inventory_part;
using static Confirm_server_by_Contracts.Main_loop;
using static Confirm_server_by_Contracts.Simple_Demands;

namespace Confirm_server_by_Contracts
{
    public class Main_loop : Update_pstgr_from_Ora<Buyer_info_row>
    {
        private readonly Update_pstgr_from_Ora<Buyer_info_row> rw;
        private readonly Update_pstgr_from_Ora<Demands_row> dmr;
        private DateTime Range_Dat { get; set; }
        private readonly Dictionary<string, string, DateTime> max_dates = new Dictionary<string, string, DateTime>();
        public List<Tuple<string, string>> Erase_dont_exist = new List<Tuple<string, string>>();
        public Main_loop()
        {
            rw = new Update_pstgr_from_Ora<Buyer_info_row>("MAIN");
            dmr = new Update_pstgr_from_Ora<Demands_row>("MAIN");
            using (NpgsqlConnection conB = new NpgsqlConnection(Postegresql_conn.Connection_pool["MAIN"].ToString()))
            {
                conB.Open();
                {
                    using (NpgsqlCommand cmd = new NpgsqlCommand("select date_fromnow(16);", conB))
                    {
                        try
                        {
                            Range_Dat = (DateTime)cmd.ExecuteScalar();
                        }
                        catch
                        {
                            Range_Dat = DateTime.Now.AddDays(16);
                        }
                    }
                }                
            }
        }
        /// <summary>
        /// Fill Dataset executor with changes in Demands
        /// </summary>
        /// <param name="changes_List"></param>
        /// <returns></returns>
        private void Fill_executor(Changes_List<Demands_row> changes_List, string Task_name, CancellationToken cancellationToken)
        {
            Dictionary<string, string, Tuple<DateTime, DateTime>> range_dates = 
                new Dictionary<string, string, Tuple<DateTime, DateTime>>();
            Loger.Log(
                string.Format(
                    "Fill Dataset_executor {0}",
                    Task_name)
                );
            void min_max(List<Demands_row> changes)
            {
                foreach (Demands_row demand in changes)
                {
                    if (cancellationToken.IsCancellationRequested) { break; }
  
                    if (max_dates.ContainsKey(demand.Part_no, demand.Contract))
                    {
                        if (demand.Work_day <= max_dates[demand.Part_no, demand.Contract])
                        {
                            if (range_dates.ContainsKey(demand.Part_no, demand.Contract))
                            {
                                (DateTime min_d, DateTime max_d) =
                                    range_dates[demand.Part_no, demand.Contract];
                                int changed = 0;
                                if (min_d > demand.Work_day)
                                {
                                    changed = 1;
                                }
                                else if (max_d < demand.Work_day && max_dates[demand.Part_no, demand.Contract] >= demand.Work_day)
                                {
                                    changed = 2;
                                }

                                if (changed > 0)
                                {
                                    range_dates[demand.Part_no, demand.Contract] =
                                        new Tuple<DateTime, DateTime>(changed == 1 ? demand.Work_day : min_d, changed == 2 ? demand.Work_day : max_d);
                                }
                            }
                            else
                            {
                                range_dates.Add(demand.Part_no, demand.Contract,
                                    new Tuple<DateTime, DateTime>(demand.Work_day, demand.Work_day));
                            }
                        }
                    }                                       
                }                
            }
            if (changes_List.Insert!=null)
            {
                min_max(changes_List.Insert);
            }
            if (changes_List.Delete != null)
            {
                min_max(changes_List.Delete);
            }
            if (changes_List.Update != null)
            {
                min_max(changes_List.Update);
            }
            
            foreach(Tuple<string, string> set in range_dates.Keys)
            {
                if (cancellationToken.IsCancellationRequested) { break; }
                (DateTime min_d, DateTime max_d) =
                            range_dates[set.Item1, set.Item2];
                Dataset_executor.Add_task(set.Item1, set.Item2, min_d, max_d);
            }
        }
        /// <summary>
        /// Update Tables => DEMANDS and DATA (Dataset for Buyers)
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="Task_name"></param>
        /// <param name="Demands"></param>
        /// <param name="Inv_Part"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<int> Update_Main_Tables(string regex, string Task_name,
            List<Simple_Demands.Simple_demands_row> Demands, List<Inventory_part.Inventory_part_row> Inv_Part,
            CancellationToken cancellationToken)
        {
            List<Demands_row> DemandSet = new List<Demands_row>();
            List<Buyer_info_row> DataSet = new List<Buyer_info_row>();

            List<Demands_row> SourceDemandSet = new List<Demands_row>();
            List<Buyer_info_row> SourceDataSet = new List<Buyer_info_row>();

            Changes_List<Demands_row> Changes = new Changes_List<Demands_row>();
            int returned = 0;

            Steps_executor.Register_step(
                string.Format(
                    "{0}:{1}",
                    Task_name,
                    "Calculate")
                );
            (DataSet, DemandSet) = await Calculate(
                Demands,
                Inv_Part,
                cancellationToken);
            Demands = null; Inv_Part = null;

            Steps_executor.End_step(
                string.Format(
                    "{0}:{1}",
                    Task_name,
                    "Calculate")
                );

            Steps_executor.Register_step(
                string.Format(
                    "{0}:{1}",
                    Task_name,
                    "Parallel Calc")
                );
            if (Steps_executor.Wait_for(new string[] { string.Format("{0}:{1}", Task_name, "Calculate") }, string.Format("{0}:{1}", Task_name, "Parallel Calc"), cancellationToken))
            {
                //new[] { string.Format("DELETE FROM public.ord_demands WHERE (part_no,contract) in ({0});", string.Join(",", Erase_dont_exist.Select(t => string.Format("( '{0}', '{1}')", t.Item1, t.Item2)))) }
                Parallel.Invoke(
                async () =>
                {
                    Steps_executor.Register_step(
                        string.Format(
                            "{0}:{1}",
                            Task_name,
                            "Update Demands")
                        );

                    SourceDemandSet = await dmr.Get_PSTGR("" +
                        string.Format(
                            @"SELECT * FROM public.demands WHERE regexp_like(part_no, '{0}')",
                            regex),
                        string.Format(
                            "{0}:{1}",
                            Task_name,
                            "Update Demands"),
                        cancellationToken);

                    Changes = await dmr.Changes(
                        SourceDemandSet,
                        DemandSet,
                        new[] { "part_no", "contract", "work_day" },
                        new[] { "part_no", "contract", "work_day", "id", "dat_shortage", "objversion", "indb" },
                        new[] { "id", "dat_shortage", "indb" },
                        Task_name,
                        cancellationToken);

                    Steps_executor.Register_step(
                        string.Format("{0}:{1}",
                        Task_name,
                        "Fill_executor")
                        );

                    Fill_executor(
                        Changes,
                        string.Format(
                            "{0}:{1}",
                            Task_name,
                            "Fill_executor"),
                        cancellationToken);

                    Steps_executor.End_step(
                        string.Format(
                            "{0}:{1}",
                            Task_name,
                            "Fill_executor")
                        );

                    returned += await dmr.PSTRG_Changes_to_dataTable(
                        Changes,
                        "demands",
                        new[] { "id" },
                        null,
                        null,
                        string.Format(
                            "{0}:{1}",
                            Task_name,
                            "Update Demands"),
                        cancellationToken);
                    Steps_executor.End_step(
                        string.Format(
                            "{0}:{1}",
                            Task_name,
                            "Update Demands")
                        );
                    Changes = null;                    
                },
                async () =>
                {
                    Steps_executor.Register_step(
                        string.Format(
                            "{0}:{1}",
                            Task_name,
                            "Buyer_info")
                        );
                    Run_query query = new Run_query();
                    await query.Execute_in_Postgres(
                        new[] {
                            string.Format(
                                "UPDATE public.datatbles SET start_update=current_timestamp, in_progress=true WHERE table_name='{0}'",
                                Task_name) },
                        string.Format(
                            "Start {0}",
                            Task_name), 
                        cancellationToken);

                    SourceDataSet = await Limit_length(
                        await rw.Get_PSTGR("" +
                            string.Format(
                                @"SELECT * FROM public.data WHERE regexp_like(indeks, '{0}')",
                                regex),
                            string.Format(
                                "{0}:{1}",
                                Task_name,
                                "Buyer_info"),
                            cancellationToken)
                        );

                    Changes_List<Buyer_info_row> Zak_changes = await rw.Changes(
                        SourceDataSet,
                        await Limit_length(DataSet),
                        new[] { "indeks", "umiejsc", "data_dost" },
                        new[] { "indeks", "umiejsc", "data_dost", "status_informacji", "informacja", "id", "widoczny_od_dnia" , "refr_date" },
                        new[] { "id", "widoczny_od_dnia", "informacja" },
                        string.Format(
                            "{0}:{1}", 
                            Task_name,
                            "Buyer_info"),
                        cancellationToken);

                    returned += await rw.PSTRG_Changes_to_dataTable(
                        Zak_changes,
                        "data",
                        new[] { "id" },
                        null,
                        new[] {"" +
                            string.Format(
                                @"UPDATE public.data a 
                                    SET widoczny_od_dnia=b.ab 
                                    from 
                                    (
                                        select
                                        a.id,
                                        min(b.dat_shortage) ab 
                                    from 
                                        (
                                            select 
                                                id,
                                                indeks,
                                                umiejsc,
                                                data_dost,
                                                data_braku
                                                from
                                                public.data 
                                            where regexp_like(indeks, '{0}') and widoczny_od_dnia is null) a,
                                        (
                                            select 
                                                part_no,
                                                contract,
                                                work_day,
                                                dat_shortage 
                                                from 
                                                demands 
                                            where regexp_like(part_no, '{0}') and dat_shortage is not null) b 
                                        where b.part_no=a.indeks and a.umiejsc=b.contract and b.work_day between a.data_braku and a.data_dost group by a.id) b 
                                   where a.id=b.id",
                                regex)
                            ,
                            string.Format(
                                @"UPDATE public.data b 
                                    SET status_informacji=a.potw, informacja=a.info 
                                    from 
                                    (
                                    select 
                                        a.id,
                                        case when a.typ_zdarzenia in ('Dzisiejsza dostawa','Opóźniona dostawa','Nieaktualne Dostawy') then null else coalesce(b.rodzaj_potw,'BRAK') end potw,
                                        a.status_informacji,
                                        b.info 
                                    from 
                                        (SELECT * from public.data
                                            WHERE regexp_like(indeks, '{0}'))a
                                        left join 
                                        potw b 
                                        on b.indeks=a.indeks and b.umiejsc=a.umiejsc and (b.data_dost=a.data_dost or b.rodzaj_potw='NIE ZAMAWIAM') 
                                    where 
                                        coalesce (a.status_informacji,'N')!=coalesce(case when a.typ_zdarzenia in ('Dzisiejsza dostawa','Opóźniona dostawa','Nieaktualne Dostawy') then null 
                                            else coalesce(b.rodzaj_potw,'BRAK') end,'N') 
                                        or coalesce(a.informacja,'n')!=coalesce(b.info,'n')) a  
                                    where b.id=a.id",
                                regex)
                            ,
                            string.Format(
                                @"UPDATE public.data a 
                                SET widoczny_od_dnia=b.refr 
                                from 
                                (
                                    select 
                                        b.id,
                                        b.refr 
                                    from 
                                    (
                                        select
                                            a.id,
                                            min(b.dat_shortage) refr
                                        from
                                        (
                                            select * from public.data where regexp_like(indeks, '{0}')
                                        ) a,
                                        (
                                            select 
                                                part_no,
                                                contract,
                                                work_day,
                                                dat_shortage 
                                            from demands
                                            WHERE regexp_like(part_no, '{0}')
                                        ) b 
                                        where b.part_no=a.indeks and b.work_day between a.data_braku and a.data_dost group by a.id) b,
                                        public.data c where b.id=c.id and b.refr!=c.widoczny_od_dnia) b WHERE a.id=b.id;",
                                regex)
                            ,
                            string.Format(
                                @"UPDATE public.datatbles 
                                    SET last_modify=current_timestamp, in_progress=false,updt_errors=false 
                                    WHERE table_name='{0}'",
                                Task_name)
                            ,
                            @"UPDATE public.datatbles 
                                SET last_modify=current_timestamp, in_progress=false,updt_errors=false 
                                WHERE table_name='data'"
                            },
                        string.Format(
                            "{0}:{1}",
                            Task_name,
                            "Buyer_info"),
                        cancellationToken);
                    using (NpgsqlConnection conA = new NpgsqlConnection(Postegresql_conn.Connection_pool["MAIN"].ToString()))
                    {
                        await conA.OpenAsync();
                        using (NpgsqlCommand cmd = new NpgsqlCommand(
                            string.Format(@"select cast(count(table_name) as integer) busy 
                            from public.datatbles 
                            where table_name='{0}' and in_progress=true", Task_name), conA))
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
                    query = null;
                    Zak_changes = null;
                    Steps_executor.End_step(string.Format(
                        "{0}:{1}",
                        Task_name,
                        "Buyer_info")
                        );
                });
            }
            if(Steps_executor.Wait_for(new string[] { string.Format("{0}:{1}", Task_name, "Buyer_info"), string.Format("{0}:{1}", Task_name, "Update Demands"), string.Format("{0}:{1}", Task_name, "Fill_executor") }, string.Format("{0}:{1}", Task_name, "Parallel Calc"), cancellationToken))
            {
                Steps_executor.End_step(
                    string.Format(
                        "{0}:{1}",
                        Task_name, 
                        "Parallel Calc")
                    );
                Steps_executor.End_step(Task_name);
            }            
            return returned;
        }
        public void Get_thre_workers(String Main_task, CancellationToken cancellationToken)
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
        /// <summary>
        /// Csalculate dataset for Buyers and Demands
        /// </summary>
        /// <param name="DMND_ORA"></param>
        /// <param name="StMag"></param>
        /// <returns></returns>
        public Task<(List<Buyer_info_row>, List<Demands_row>)> Calculate (
            List<Simple_demands_row> DMND_ORA, 
            List<Inventory_part_row> StMag, 
            CancellationToken cancellationToken)
        {
            DateTime nullDAT = Loger.Serw_run.AddDays(10);            

            List<Buyer_info_row> DataSet = new List<Buyer_info_row>();
            List<Buyer_info_row> TmpDataSet = new List<Buyer_info_row>();
            List<Demands_row> DemandSet = new List<Demands_row>();            

            string Part_no = "";
            string Contract = "";
            int ind_mag = 0;
            
            DateTime Date_reQ;
            byte TYP_dmd = 0;
            byte b_DOP = byte.Parse("2");
            byte b_ZAM = byte.Parse("4");
            byte b_zlec = byte.Parse("1");
            string KOOR = "";
            string TYPE = "";
            double SUM_QTY_SUPPLY = 0;
            double Sum_dost_op = 0;
            double Sum_potrz_op = 0;
            double SUM_QTY_DEMAND = 0;
            double QTY_SUPPLY;
            double QTY_DEMAND;
            double DEMAND_ZAM;
            double QTY_DEMAND_DOP;
            double STAN_mag = 0;
            double Chk_Sum ;
            double bilans;
            double balance;
            double Balane_mag ;
            double dmnNext;
            int chksumD;
            double leadtime = 0;
            DateTime widoczny;
            int counter = -1;
            int max = DMND_ORA.Count;

            DateTime start = Loger.Started();
            DateTime gwar_DT = nullDAT;
            DateTime Data_Braku = nullDAT;
            int par = Convert.ToInt32(DateTime.Now.Date.ToOADate());
            if (par % 2 == 0) { par = 1; } else { par = 0; }
            DateTime DATNOW = DateTime.Now.Date;            
            Simple_demands_row NEXT_row = DMND_ORA[0];
            
            DateTime rpt_short = nullDAT;
            DateTime dta_rap = nullDAT;
            try
            {
                foreach (Simple_demands_row rek in DMND_ORA)
                {
                    if (cancellationToken.IsCancellationRequested) { break; }
                    if (counter < max) { counter++; }
                    // Zmiana obliczanego indeksu 

                    bool var = !(rek.Part_no.Equals(Part_no) && rek.Contract.Equals(Contract));
                    if (var)
                    {
                        //type_DMD - maska bitowa 0001 - zlec ;0010 - DOP ;0100-zam-klient
                        TYP_dmd = 0;
                        Data_Braku = nullDAT;
                        SUM_QTY_SUPPLY = 0;
                        SUM_QTY_DEMAND = 0;
                        Sum_dost_op = 0;
                        Chk_Sum = 0;
                        balance = 0;
                        Balane_mag = 0;
                        bilans = 0;
                        dmnNext = 0;
                        Sum_dost_op = 0;
                        Sum_potrz_op = 0;


                        Part_no = rek.Part_no;
                        Contract = rek.Contract;

                        rpt_short = nullDAT;
                        dta_rap = nullDAT;

                        while (!(StMag[ind_mag].Indeks.Equals(Part_no) && StMag[ind_mag].Contract.Equals(Contract)))
                        {
                            Erase_dont_exist.Add(new Tuple<string, string>(StMag[ind_mag].Indeks, StMag[ind_mag].Contract));
                            ind_mag++;
                        }
                        STAN_mag = StMag[ind_mag].Mag;
                        gwar_DT = StMag[ind_mag].Data_gwarancji;
                        leadtime = StMag[ind_mag].Czas_dostawy;
                        KOOR = StMag[ind_mag].Planner_buyer;
                        TYPE = StMag[ind_mag].Rodzaj ?? "NULL";
                    }
                    Date_reQ = rek.Date_required;
                    QTY_SUPPLY = rek.Qty_supply;
                    QTY_DEMAND = rek.QTY_DEMAND;
                    DEMAND_ZAM = rek.DEMAND_ZAM;
                    QTY_DEMAND_DOP = rek.QTY_DEMAND_DOP;
                    chksumD = rek.Chk_sum;
                    // Sprawdź pochodzenie potrzeby 
                    if (QTY_DEMAND_DOP > 0) { TYP_dmd = (byte)(TYP_dmd | b_DOP); }
                    if (DEMAND_ZAM > 0) { TYP_dmd = (byte)(TYP_dmd | b_ZAM); }
                    if (QTY_DEMAND != QTY_DEMAND_DOP + DEMAND_ZAM) { TYP_dmd = (byte)(TYP_dmd | b_zlec); }
                    balance = STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND;
                    SUM_QTY_SUPPLY += QTY_SUPPLY;
                    SUM_QTY_DEMAND += QTY_DEMAND;
                    Balane_mag = STAN_mag - SUM_QTY_DEMAND;
                    if (dta_rap == nullDAT)
                    {
                        if (Balane_mag < 0 && Date_reQ < Range_Dat)
                        {
                            dta_rap = Date_reQ;
                        }
                    }
                    if ((STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND < 0 ||
                        (Balane_mag < 0 && Date_reQ <= DATNOW)) && Data_Braku == nullDAT)
                    {
                        Data_Braku = Date_reQ;
                        widoczny = start;
                    }
                    Chk_Sum = (leadtime + SUM_QTY_SUPPLY + SUM_QTY_DEMAND) * TYP_dmd - (QTY_SUPPLY + QTY_DEMAND) + par;
                    dmnNext = 0;
                    if (counter < max - 1)
                    {
                        NEXT_row = DMND_ORA[counter + 1];
                    } else
                    {
                        NEXT_row = DMND_ORA[0];
                    }
                    bilans = STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND - QTY_SUPPLY;
                    DemandSet.Add(new Demands_row
                    {
                        Part_no = Part_no,
                        Contract = Contract,
                        Work_day = Date_reQ,
                        Expected_leadtime = leadtime,
                        Purch_qty = QTY_SUPPLY,
                        Qty_demand = QTY_DEMAND,
                        Type_dmd = TYP_dmd,
                        Balance = balance,
                        Bal_stock = Balane_mag,
                        Koor = KOOR,
                        Type = TYPE,
                        Dat_shortage = Balane_mag < 0 ? start : (DateTime?)null,
                        Chk_sum = Chk_Sum,
                        Objversion = start,
                        Chksum = chksumD
                    });

                    if (Date_reQ <= DATNOW)
                    {
                        Sum_dost_op += QTY_SUPPLY;
                        Sum_potrz_op += QTY_DEMAND;
                        if (STAN_mag - Sum_potrz_op < 0 && Date_reQ == DATNOW)
                        {
                            rpt_short = Date_reQ;
                        }

                        if (QTY_SUPPLY > 0)
                        {
                            string state = Date_reQ == DATNOW ? "Dzisiejsza dostawa" : "Opóźniona dostawa";
                            TmpDataSet.Add(new Buyer_info_row
                            {
                                Indeks = Part_no,
                                Umiejsc = StMag[ind_mag].Contract,
                                Opis = StMag[ind_mag].Opis,
                                Kolekcja = StMag[ind_mag].Kolekcja,
                                Mag = STAN_mag,
                                Planner_buyer = KOOR,
                                Rodzaj = StMag[ind_mag].Rodzaj,
                                Czas_dostawy = StMag[ind_mag].Czas_dostawy,
                                Data_gwarancji = gwar_DT,
                                Przyczyna = TYP_dmd,
                                Wlk_dost = QTY_SUPPLY,
                                Data_dost = Date_reQ,
                                Typ_zdarzenia = state,
                                Widoczny_od_dnia = state == "Opóźniona dostawa" ? start : (DateTime?)null,
                                Status_informacji = "NOT IMPLEMENT",
                                Refr_date = start,
                                Chk = Chk_Sum
                            });
                        }
                    }
                    if (TmpDataSet.Count > 0)
                    {                        
                        if (Date_reQ > DATNOW || !(Part_no.Equals(NEXT_row.Part_no) && Contract.Equals(NEXT_row.Contract)))
                        {
                            if (dta_rap == nullDAT)
                            {
                                if (STAN_mag - Sum_potrz_op < 0)
                                {
                                    dta_rap = Date_reQ;
                                }
                            }
                            foreach (Buyer_info_row row in TmpDataSet)
                            {
                                DataSet.Add(new Buyer_info_row
                                {
                                    Indeks = row.Indeks,
                                    Umiejsc = row.Umiejsc,
                                    Opis = row.Opis,
                                    Kolekcja = row.Kolekcja,
                                    Mag = row.Mag,
                                    Planner_buyer = row.Planner_buyer,
                                    Rodzaj = row.Rodzaj,
                                    Czas_dostawy = row.Czas_dostawy,
                                    Data_gwarancji = row.Data_gwarancji,
                                    Przyczyna = row.Przyczyna,
                                    Data_dost = row.Data_dost,
                                    Wlk_dost = row.Wlk_dost,
                                    Typ_zdarzenia = row.Typ_zdarzenia,
                                    Widoczny_od_dnia = row.Widoczny_od_dnia,
                                    Status_informacji = row.Status_informacji,
                                    Refr_date = row.Refr_date,
                                    Bilans = STAN_mag - Sum_potrz_op,
                                    Bil_dost_dzień = STAN_mag - Sum_potrz_op - dmnNext,
                                    Data_braku = Data_Braku != nullDAT ? Data_Braku : Date_reQ,
                                    Sum_dost = Sum_dost_op,
                                    Sum_potrz = Sum_potrz_op,
                                    Sum_dost_opóźnion = Sum_dost_op,
                                    Sum_potrz_opóźnion = Sum_potrz_op,
                                    Chk = row.Chk,
                                    Id = System.Guid.NewGuid()
                                });
                            }
                        }
                        TmpDataSet.Clear();
                    }
                    if (QTY_SUPPLY > 0)
                    {
                        if (Date_reQ > DATNOW && (bilans < 0 || balance < 0))
                        {
                            if (balance < 0)
                            {
                                rpt_short = Date_reQ;
                            }
                            string state = balance < 0 ? "Brakujące ilości" : "Dostawa na dzisiejsze ilości";
                            DataSet.Add(new Buyer_info_row
                            {
                                Indeks = Part_no,
                                Umiejsc = StMag[ind_mag].Contract,
                                Opis = StMag[ind_mag].Opis,
                                Kolekcja = StMag[ind_mag].Kolekcja,
                                Mag = STAN_mag,
                                Planner_buyer = KOOR,
                                Rodzaj = StMag[ind_mag].Rodzaj,
                                Czas_dostawy = StMag[ind_mag].Czas_dostawy,
                                Data_gwarancji = gwar_DT,
                                Przyczyna = TYP_dmd,
                                Data_dost = Date_reQ,
                                Wlk_dost = QTY_SUPPLY,
                                Bilans = balance,
                                Bil_dost_dzień = bilans,
                                Data_braku = Data_Braku != nullDAT ? Data_Braku : Date_reQ,
                                Typ_zdarzenia = state,
                                Widoczny_od_dnia = start,
                                Sum_dost = SUM_QTY_SUPPLY,
                                Sum_potrz = SUM_QTY_DEMAND,
                                Sum_dost_opóźnion = Sum_dost_op,
                                Sum_potrz_opóźnion = Sum_potrz_op,
                                Status_informacji = "NOT IMPLEMENT",
                                Refr_date = start,
                                Chk = Chk_Sum,
                                Id = System.Guid.NewGuid()
                            });
                        }
                    }
                    else
                    {
                        if (bilans < 0)
                        {
                            if (!(Part_no.Equals(NEXT_row.Part_no) && Contract.Equals(NEXT_row.Contract)) || (Date_reQ <= gwar_DT && NEXT_row.Date_required > gwar_DT))
                            {
                                string state = Date_reQ <= gwar_DT ? "Braki w gwarantowanej dacie" : "Brak zamówień zakupu";
                                if (Date_reQ <= gwar_DT)
                                {
                                    rpt_short = Date_reQ;
                                }
                                DataSet.Add(new Buyer_info_row
                                {
                                    Indeks = Part_no,
                                    Umiejsc = StMag[ind_mag].Contract,
                                    Opis = StMag[ind_mag].Opis,
                                    Kolekcja = StMag[ind_mag].Kolekcja,
                                    Mag = STAN_mag,
                                    Planner_buyer = KOOR,
                                    Rodzaj = StMag[ind_mag].Rodzaj,
                                    Czas_dostawy = StMag[ind_mag].Czas_dostawy,
                                    Data_gwarancji = gwar_DT,
                                    Przyczyna = TYP_dmd,
                                    Data_dost = Date_reQ,
                                    Wlk_dost = QTY_SUPPLY,
                                    Bilans = bilans,
                                    Bil_dost_dzień = bilans,
                                    Data_braku = Data_Braku != nullDAT ? Data_Braku : Date_reQ,
                                    Typ_zdarzenia = state,
                                    Widoczny_od_dnia = start,
                                    Sum_dost = SUM_QTY_SUPPLY,
                                    Sum_potrz = SUM_QTY_DEMAND,
                                    Sum_dost_opóźnion = Sum_dost_op,
                                    Sum_potrz_opóźnion = Sum_potrz_op,
                                    Status_informacji = "NOT IMPLEMENT",
                                    Refr_date = start,
                                    Chk = Chk_Sum,
                                    Id = System.Guid.NewGuid()
                                });
                            }
                        }
                    }
                    if ((STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND >= 0 && Date_reQ > DATNOW) && Data_Braku != nullDAT)
                    {
                        Data_Braku = nullDAT;
                    }
                    if (!(Part_no.Equals(NEXT_row.Part_no) && Contract.Equals(NEXT_row.Contract)))
                    {
                        max_dates.Add(Part_no, Contract, dta_rap>rpt_short? dta_rap: rpt_short);
                    }
                }          
            
            }
            catch (Exception ex)
            {
                Loger.Log("Error in Main_loop " + ex);

            }
            return Task.FromResult((DataSet, DemandSet));
        }

        public class Eras_Shedul_row : IEquatable<Eras_Shedul_row>, IComparable<Eras_Shedul_row>
        {
            public string Part_no { get; set; }
            public DateTime Dat { get; set; }
            public int CompareTo(Eras_Shedul_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                int first = this.Part_no.CompareTo(other.Part_no);
                if (first != 0)
                {
                    return first;
                }
                return this.Dat.CompareTo(other.Dat);
            }

            public bool Equals(Eras_Shedul_row other)
            {
                return this.Part_no.Equals(other.Part_no) && this.Dat.Equals(other.Dat);
            }
        }

        public class Shedul_row : IEquatable<Shedul_row>, IComparable<Shedul_row>
        {
            public string Part_no { get; set; }
            public DateTime Dat { get; set; }
            public bool In_DB { get; set; }
            public int CompareTo(Shedul_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                int first = this.Part_no.CompareTo(other.Part_no);
                if (first != 0)
                {
                    return first;
                }
                return this.Dat.CompareTo(other.Dat);
            }

            public bool Equals(Shedul_row other)
            {
                return this.Part_no.Equals(other.Part_no) && this.Dat.Equals(other.Dat);
            }
        }

        public class MShedul_row : IEquatable<MShedul_row>, IComparable<MShedul_row>
        {
            public string Part_no { get; set; }
            public DateTime Dat { get; set; }
            public bool Purch_rpt { get; set; }
            public DateTime Purch_rpt_dat { get; set; }
            public bool Braki_rpt { get; set; }
            public DateTime Braki_rpt_dat { get; set; }
            public int Kol { get; set; }

            public int CompareTo(MShedul_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                int first = this.Part_no.CompareTo(other.Part_no);
                if (first != 0)
                {
                    return first;
                }
                return this.Dat.CompareTo(other.Dat);
            }

            public bool Equals(MShedul_row other)
            {
                return this.Part_no.Equals(other.Part_no) && this.Dat.Equals(other.Dat);
            }
        }
        public Task<List<Buyer_info_row>> Limit_length (List<Buyer_info_row> dataset)
        {
            Dictionary<string, int> calendar_len = Get_limit_of_fields.buyer_info_len;
            foreach (Buyer_info_row row in dataset)
            {
                row.Indeks = row.Indeks.LimitDictLen("indeks", calendar_len);
                row.Umiejsc = row.Umiejsc.LimitDictLen("umiejsc", calendar_len); 
                row.Opis = row.Opis.LimitDictLen("opis", calendar_len);
                row.Kolekcja = row.Kolekcja.LimitDictLen("kolekcja", calendar_len);
                row.Planner_buyer = row.Planner_buyer.LimitDictLen("planner_buyer", calendar_len);
                row.Rodzaj = row.Rodzaj.LimitDictLen("rodzaj", calendar_len);
                row.Typ_zdarzenia = row.Typ_zdarzenia.LimitDictLen("typ_zdarzenia", calendar_len);
                row.Status_informacji = row.Status_informacji.LimitDictLen("status_informacji", calendar_len);
                row.Informacja = row.Informacja.LimitDictLen("informacja", calendar_len);
            }
            return Task.FromResult(dataset);
        }

        public class Buyer_info_row : IEquatable<Buyer_info_row>, IComparable<Buyer_info_row>
        {
            public string Indeks { get; set; }
            public string Umiejsc { get; set; }
            public string Opis { get; set; }
            public string Kolekcja { get; set; }
            public double Mag { get; set; }
            public string Planner_buyer { get; set; }
            public string Rodzaj { get; set; }
            public double Czas_dostawy { get; set; }
            public DateTime Data_gwarancji { get; set; }
            public DateTime Data_dost { get; set; }
            public double Wlk_dost { get; set; }
            public double Bilans { get; set; }
            public DateTime Data_braku { get; set; }
            public double Bil_dost_dzień { get; set; }
            public string Typ_zdarzenia { get; set; }
            public DateTime? Widoczny_od_dnia { get; set; }
            public double Sum_dost { get; set; }
            public double Sum_potrz { get; set; }
            public double Sum_dost_opóźnion { get; set; }
            public double Sum_potrz_opóźnion { get; set; }
            public string Status_informacji { get; set; }
            public DateTime Refr_date { get; set; }
            public Guid Id { get; set; }
            public double Chk { get; set; }
            public int Przyczyna { get; set; }
            public string Informacja { get; set; }

            public int CompareTo(Buyer_info_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    int main = this.Indeks.CompareTo(other.Indeks);
                    if (main != 0)
                    {
                        return main;
                    }
                    int second = this.Umiejsc.CompareTo(other.Umiejsc);
                    if (second != 0)
                    {
                        return second;
                    }
                    return this.Data_dost.CompareTo(other.Data_dost);
                }
            }

            public bool Equals(Buyer_info_row other)
            {
                if (other == null) return false;
                return (this.Indeks.Equals(other.Indeks) && this.Data_dost.Equals(other.Data_dost) && this.Umiejsc.Equals(other.Umiejsc));
            }
        }      
       
    }
}