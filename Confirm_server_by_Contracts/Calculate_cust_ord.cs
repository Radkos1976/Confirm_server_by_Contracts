using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Confirm_server_by_Contracts
{
    public class Calculate_cust_ord
    {
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
