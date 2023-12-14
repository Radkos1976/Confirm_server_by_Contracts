using DB_Conect;
using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Security.Cryptography;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;


namespace Confirm_server_by_Contracts
{
    public class Main_loop: Update_pstgr_from_Ora<Main_loop.Buyer_info_row>
    {
        public async Task<List<Buyer_info_row>> Calculate(List<Simple_Demands.Simple_demands_row> DMND_ORA, List<Inventory_part.Inventory_part_row> StMag)
        {
            DateTime nullDAT = Loger.Serw_run.AddDays(1000);
            DateTime range_Dat = nullDAT;
            using (NpgsqlConnection conB = new NpgsqlConnection(Postegresql_conn.Connection_pool["MAIN"].ToString()))
            {
                await conB.OpenAsync();
                {
                    using (NpgsqlCommand cmd = new NpgsqlCommand("select date_fromnow(10);", conB))
                    {
                        range_Dat = (DateTime)cmd.ExecuteScalar();
                    }
                }
                conB.Close();
            }
            string Part_no = "";
            int ind_mag = 0;
            int ind_Demands = 0;
            int ind_DTA = 0;
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
            double QTY_SUPPLY = 0;
            double QTY_DEMAND = 0;
            double DEMAND_ZAM = 0;
            double QTY_DEMAND_DOP = 0;
            double STAN_mag = 0;
            double Chk_Sum = 0;
            double bilans = 0;
            double balance = 0;
            double Balane_mag = 0;
            double dmnNext = 0;
            int chksumD = 0;
            Int16 leadtime = 0;
            DateTime widoczny = nullDAT;
            int counter = -1;
            int max = DMND_ORA.Count;
            
            DateTime gwar_DT = nullDAT;
            DateTime Data_Braku = nullDAT;
            int par = Convert.ToInt32(DateTime.Now.Date.ToOADate());
            if (par % 2 == 0) { par = 1; } else { par = 0; }
            DateTime DATNOW = DateTime.Now.Date;
            DateTime dat_CREA = nullDAT;
            Simple_Demands.Simple_demands_row NEXT_row = DMND_ORA[0];
            Boolean TMP_ARR = false;
            DateTime rpt_short = nullDAT;
            DateTime dta_rap = nullDAT;
            foreach (Simple_Demands.Simple_demands_row rek in DMND_ORA)
            {
                if (counter < max) { counter++; }
                // Zmiana obliczanego indeksu
                #region USTAW ZMIENNE Skasuj nie występujące indeksy z tabel głównych
                if (Part_no != rek.Part_no;
                {
                    try
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
                        TMP_ARR = false;
                        if (Part_no != "")
                        {
                            if ((rpt_short != nullDAT || dta_rap != nullDAT) && KOOR != "LUCPRZ" && Part_no.Substring(1, 4) != "5700" && Part_no.Substring(1, 4) != "5800")
                            {
                                bool AM = false;
                                foreach (DataRowView row in Shedul.DefaultView)
                                {
                                    if (((DateTime)row["Dat"] <= rpt_short && rpt_short != nullDAT && (bool)row["in_DB"] != true) || (((DateTime)row["Dat"] >= dta_rap || DATNOW == dta_rap) && dta_rap != nullDAT) && (DateTime)row["Dat"] < range_Dat && (bool)row["in_DB"] != true)
                                    {
                                        AM = true;
                                        DataRow rw = MShedul.NewRow();
                                        rw["part_no"] = row["part_no"];
                                        rw["Dat"] = row["Dat"];
                                        if (rpt_short != nullDAT)
                                        {
                                            rw["Purch_rpt"] = true;
                                            rw["Purch_rpt_dat"] = rpt_short;
                                        }
                                        else
                                        {
                                            rw["Purch_rpt"] = false;


                                        }
                                        if (dta_rap != nullDAT)
                                        {
                                            rw["Braki_rpt"] = true;
                                            rw["Braki_rpt_dat"] = dta_rap;
                                        }
                                        else
                                        {
                                            rw["Braki_rpt"] = false;
                                        }
                                        rw["kol"] = kol;
                                        MShedul.Rows.Add(rw);
                                    }
                                    else if ((((DateTime)row["Dat"] > rpt_short && rpt_short != nullDAT && (DateTime)row["Dat"] < dta_rap && dta_rap != nullDAT) || ((DateTime)row["Dat"] < dta_rap && dta_rap != nullDAT && rpt_short == nullDAT) || ((DateTime)row["Dat"] > rpt_short && rpt_short != nullDAT && dta_rap == nullDAT)) && (bool)row["in_DB"] == true)
                                    {
                                        DataRow rw = eras_Shedul.NewRow();
                                        rw["part_no"] = Part_no;
                                        rw["dat"] = row["Dat"];
                                        eras_Shedul.Rows.Add(rw);
                                    }
                                }
                                if (AM)
                                {
                                    kol++;
                                    if (kol == 5) { kol = 0; }
                                }
                            }
                            else
                            {
                                if (in_DB)
                                {
                                    DataRow row = eras_Shedul.NewRow();
                                    row["part_no"] = Part_no;
                                    eras_Shedul.Rows.Add(row);
                                }
                            }
                        }
                        Part_no = (string)rek["Part_no"];
                        Shedul.Clear();
                        rpt_short = nullDAT;
                        dta_rap = nullDAT;
                        in_DB = false;
                        while (Part_no != StMag.DefaultView[ind_mag].Row["Indeks"].ToString())
                        {
                            string str_mag = StMag.DefaultView[ind_mag].Row["Indeks"].ToString();
                            if (MAX_DEMANDs > ind_Demands)
                            {
                                int tmp_count = 0;
                                while (str_mag == DEMANDS.DefaultView[ind_Demands].Row["Part_NO"].ToString())
                                {
                                    if (tmp_count == 0)
                                    {
                                        DataRow row = eras_Shedul.NewRow();
                                        row["part_no"] = Part_no;
                                        eras_Shedul.Rows.Add(row);

                                        DataRow rw = TASK_DEMANDS.NewRow();
                                        rw["PART_NO"] = str_mag;
                                        rw["Czynnosc"] = "DEL_PART_DEM";
                                        rw["Status"] = "Zapl.";
                                        TASK_DEMANDS.Rows.Add(rw);
                                    }
                                    tmp_count++;
                                    ind_Demands++;
                                    if (MAX_DEMANDs <= ind_Demands) { break; }
                                }
                            }

                            if (MAX_Dta > ind_DTA)
                            {
                                int tmp_count = 0;
                                while (str_mag == dta.DefaultView[ind_DTA].Row["Indeks"].ToString())
                                {
                                    if (tmp_count == 0)
                                    {
                                        DataRow rw = TASK_dat.NewRow();
                                        rw["indeks"] = str_mag;
                                        rw["Czynnosc"] = "DEL_PART_DEM";
                                        rw["Status"] = "Zapl.";
                                        TASK_dat.Rows.Add(rw);
                                    }
                                    tmp_count++;
                                    ind_DTA++;
                                    if (MAX_Dta <= ind_DTA) { break; }
                                }

                            }
                            ind_mag++;
                        }
                        STAN_mag = Convert.ToDouble(StMag.DefaultView[ind_mag].Row["MAG"]);
                        gwar_DT = (DateTime)StMag.DefaultView[ind_mag].Row["Data_gwarancji"];
                        leadtime = Convert.ToInt16(StMag.DefaultView[ind_mag].Row["Czas_dostawy"]);
                        KOOR = (string)StMag.DefaultView[ind_mag].Row["PLANNER_BUYER"];
                        TYPE = (string)StMag.DefaultView[ind_mag].Row["RODZAJ"] ?? "NULL";
                    }
                    catch (Exception e)
                    {
                        using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                        {
                            await conA.OpenAsync();
                            using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.datatbles SET in_progress=false,updt_errors=true WHERE table_name='main_loop'", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            conA.Close();
                        }
                        Log("Błąd przypisania danych magazynowych: " + e);
                        return 1;
                    }
                }
            }


        }
        private readonly Update_pstgr_from_Ora<Buyer_info_row> rw;
        public class Buyer_info_row: IEquatable<Buyer_info_row>, IComparable<Buyer_info_row>
        {
            private readonly Dictionary<string, int> buyer_info_len = Get_limit_of_fields.buyer_info_len;
            public string Indeks { get { return Indeks; } set => Indeks = value.LimitDictLen("indeks", buyer_info_len); }
            public string Umiejsc { get { return Umiejsc; } set => Umiejsc = value.LimitDictLen("umiejsc", buyer_info_len); }
            public string Opis { get { return Opis; } set => Opis = value.LimitDictLen("opis", buyer_info_len); }
            public string Kolekcja { get { return Kolekcja; } set => Kolekcja = value.LimitDictLen("kolekcja", buyer_info_len); }
            public double Mag { get; set; }
            public string Planner_buyer { get { return Planner_buyer; } set => Planner_buyer = value.LimitDictLen("planner_buyer", buyer_info_len); }
            public string Rodzaj { get { return Indeks; } set => Indeks = value.LimitDictLen("indeks", buyer_info_len); }
            public double Czas_dostawy { get; set; }
            public DateTime Data_gwarancji { get; set; }
            public DateTime Data_dost { get; set; }
            public double Wlk_dost { get; set; }
            public double Bilans { get; set; }
            public DateTime Data_braku { get; set; }
            public double Bil_dost_dzień { get; set; }
            public string Typ_zdarzenia { get { return Indeks; } set => Indeks = value.LimitDictLen("indeks", buyer_info_len); }
            public DateTime Widoczny_od_dnia { get; set; }
            public double Sum_dost { get; set; }
            public double Sum_potrz { get; set; }
            public double Sum_dost_opóźnion { get; set; }
            public double Sum_potrz_opóźnion { get; set; }
            public string Status_informacji { get { return Indeks; } set => Indeks = value.LimitDictLen("indeks", buyer_info_len); }
            public DateTime Refr_date { get; set; }
            public string Id { get { return Indeks; } set => Indeks = value.LimitDictLen("indeks", buyer_info_len); }
            public double Chk { get; set; }
            public int Przyczyna { get; set; }
            public string Informacja { get { return Indeks; } set => Indeks = value.LimitDictLen("indeks", buyer_info_len); }

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
                return (this.Indeks.Equals(other.Indeks) && this.Data_dost.Equals(other.Data_dost)  && this.Umiejsc.Equals(other.Umiejsc));
            }
        }                
    }


}
