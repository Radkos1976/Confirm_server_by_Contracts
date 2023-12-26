using DB_Conect;
using Npgsql;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.InteropServices.ComTypes;
using System.Security.Cryptography;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using static Confirm_server_by_Contracts.Demands;
using static Confirm_server_by_Contracts.Main_loop;


namespace Confirm_server_by_Contracts
{
    public class Main_loop : Update_pstgr_from_Ora<Main_loop.Buyer_info_row>
    {
        private readonly Update_pstgr_from_Ora<Buyer_info_row> rw;

        public Main_loop()
        {
            rw = new Update_pstgr_from_Ora<Buyer_info_row>("MAIN");
        }

        public async Task<(List<Buyer_info_row>, List<Demands_row>)> Calculate (List<Simple_Demands.Simple_demands_row> DMND_ORA, List<Inventory_part.Inventory_part_row> StMag)
        {
            DateTime nullDAT = Loger.Serw_run.AddDays(1000);
            DateTime range_Dat = nullDAT;
            List<Buyer_info_row> DataSet = new List<Buyer_info_row>();
            List<Buyer_info_row> TmpDataSet = new List<Buyer_info_row>();
            List<Demands_row> DemandSet = new List<Demands_row>();
            List<Shedul_row> Shedul = new List<Shedul_row>();
            List<MShedul_row> MShedul = new List<MShedul_row>();
            List<Eras_Shedul_row> Eras_Shedul = new List<Eras_Shedul_row>();


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
            int leadtime = 0;
            DateTime widoczny;
            int counter = -1;
            int max = DMND_ORA.Count;

            DateTime start = Loger.Started();
            DateTime gwar_DT = nullDAT;
            DateTime Data_Braku = nullDAT;
            int par = Convert.ToInt32(DateTime.Now.Date.ToOADate());
            if (par % 2 == 0) { par = 1; } else { par = 0; }
            DateTime DATNOW = DateTime.Now.Date;            
            Simple_Demands.Simple_demands_row NEXT_row = DMND_ORA[0];
            
            DateTime rpt_short;
            DateTime dta_rap = nullDAT;

            foreach (Simple_Demands.Simple_demands_row rek in DMND_ORA)
            {
                if (counter < max) { counter++; }
                // Zmiana obliczanego indeksu                
                if (!rek.Part_no.Equals(Part_no) && !rek.Contract.Equals(Contract))
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
                    
                    while (Part_no != StMag[ind_mag].Indeks && Contract != StMag[ind_mag].Contract)
                    {
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
                    if (Balane_mag < 0 && Date_reQ < range_Dat)
                    {
                        dta_rap = Date_reQ;
                    }
                }
                if ((STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND < 0 || (Balane_mag < 0 && Date_reQ <= DATNOW)) && Data_Braku == nullDAT)
                {
                    Data_Braku = Date_reQ;
                    widoczny = start;
                }
                Chk_Sum = (leadtime + SUM_QTY_SUPPLY + SUM_QTY_DEMAND) * TYP_dmd - (QTY_SUPPLY + QTY_DEMAND) + par;
                dmnNext = 0;
                if (counter < max - 1)
                {
                    NEXT_row = DMND_ORA[counter + 1];
                }
                bilans = STAN_mag + SUM_QTY_SUPPLY - SUM_QTY_DEMAND - QTY_SUPPLY;
                DemandSet.Add(new Demands_row
                {
                    Part_no = Part_no,
                    Work_day = Date_reQ,
                    Expected_leadtime = leadtime,
                    Purch_qty = QTY_SUPPLY,
                    Qty_demand = QTY_DEMAND,
                    Type_dmd = TYP_dmd,
                    Balance = balance,
                    Bal_stock = Balane_mag,
                    Koor = KOOR,
                    Type = TYPE,
                    Dat_shortage = start,
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
                            Widoczny_od_dnia = start,
                            Status_informacji = "NOT IMPLEMENT",
                            Refr_date = start,
                            Chk = Chk_Sum
                        });
                    }
                }
                if (TmpDataSet.Count > 0)
                {
                    if (Date_reQ > DATNOW || (Part_no != NEXT_row.Part_no && Contract != NEXT_row.Contract))
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
                }
                if (QTY_SUPPLY > 0)
                {
                    if (Date_reQ > DATNOW && (bilans < 0 || balance < 0))
                    {
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
                        if ((Part_no != NEXT_row.Part_no && Contract != NEXT_row.Contract)|| (Date_reQ <= gwar_DT && NEXT_row.Date_required > gwar_DT))
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
            }
            return (DataSet, DemandSet);
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
            public DateTime Widoczny_od_dnia { get; set; }
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