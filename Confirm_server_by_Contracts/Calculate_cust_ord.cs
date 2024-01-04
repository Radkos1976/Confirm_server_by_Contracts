using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Security.Cryptography;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;

namespace Confirm_server_by_Contracts
{
    public class Calculate_cust_ord
    {
        public class Order_balancr_row: Order_lack_row
        {

        }
        public virtual class Order_lack_row : IEquatable<Order_lack_row> , IComparable<Order_lack_row>
        {
            public string Ordid { get; set; }
            public long L_ordid { get; set; }
            public string Indeks { get; set; }
            public string Umiejsc { get; set; }
            public string Opis { get; set; }
            public string Planner_buyer { get; set; }
            public double Mag { get; set; }
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
            public DateTime Last_mail_conf { get; set; }
            public DateTime Prom_date { get; set; }
            public string Prom_week { get; set; }
            public long Load_id { get; set; }
            public DateTime Ship_date { get; set; }
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
            public Guid Cust_id { get; set; }

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
                    return this.Data_dost.CompareTo(other.Data_dost) * -1;
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
                    this.Data_dost.Equals(other.Data_dost)
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
