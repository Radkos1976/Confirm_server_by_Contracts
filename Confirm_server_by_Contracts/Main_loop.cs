using DB_Conect;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;


namespace Confirm_server_by_Contracts
{
    public class Main_loop: Update_pstgr_from_Ora<Main_loop.Buyer_info_row>
    {
        private readonly Update_pstgr_from_Ora<Main_loop.Buyer_info_row> rw;
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
