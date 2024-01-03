using DB_Conect;
using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static Confirm_server_by_Contracts.Lack_report;
using static Confirm_server_by_Contracts.Order_Demands;

namespace Confirm_server_by_Contracts
{
    public class Lack_report : Update_pstgr_from_Ora<Lack_report_row>
    {
        private readonly Update_pstgr_from_Ora<Lack_report_row> rw;
        public Lack_report(CancellationToken cancellationToken)
        {
            rw = new Update_pstgr_from_Ora<Lack_report_row>("MAIN");
            Parallel.Invoke(
            async () => {
                Steps_executor.Register_step("Lack_report");
                await Update_Lack_reports(cancellationToken);
                Run_query query = new Run_query();
                await query.Execute_in_Postgres(new[] { 
                    "REFRESH MATERIALIZED VIEW braki_gniazd; ",
                    "REFRESH MATERIALIZED VIEW braki_poreal; "
                }, "Lack_report", cancellationToken);
                Steps_executor.End_step("Lack_report");
                query = null;
            });
        }
        public async Task<int> Update_Lack_reports(CancellationToken cancellationToken)
        {
            return await rw.PSTRG_Changes_to_dataTable(
                rw.Changes(
                    await Old_data(cancellationToken),
                    await New_data(cancellationToken),
                    new[] { "work_day", "contract", "typ", "wrkc", "next_wrkc" },
                    new[] { "work_day", "contract", "typ", "wrkc", "next_wrkc" },
                    null,
                    "Lack_report",
                    cancellationToken
                    ),
                "day_qty",
                new[] { "work_day", "contract", "typ", "wrkc", "next_wrkc" },
                null,
                null,
                "Lack_report",
                cancellationToken
                );
        }
        private async Task<List<Lack_report_row>> Old_data(CancellationToken cancellationToken) => await rw.Get_PSTGR("SELECT * FROM day_qty", "Lack_report", cancellationToken);
        private async Task<List<Lack_report_row>> New_data(CancellationToken cancellationToken) => await rw.Get_Ora("" +
                @"SELECT                     
                    a.* 
                FROM 
                (
                    SELECT 
                        Decode(Sign(REVISED_DUE_DATE-SYSDATE),'-1',To_Date(SYSDATE),REVISED_DUE_DATE) WORK_DAY,
                        Decode(source,'','MRP','DOP') TYP,
                        contract,
                        NVL(ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO, SEQUENCE_NO, RELEASE_NO, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO, SEQUENCE_NO, RELEASE_NO, 1, 0)), '-') WRKC,
                        NVL(ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO, SEQUENCE_NO, RELEASE_NO, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO, SEQUENCE_NO, RELEASE_NO, 2, 0)), '-') NEXT_WRKC,
                        Sum(REVISED_QTY_DUE-QTY_COMPLETE) QTY_ALL
                    FROM 
                        ifsapp.shop_ord 
                    WHERE                         
                        OBJSTATE<>(select ifsapp.SHOP_ORD_API.FINITE_STATE_ENCODE__('Zamknięte') from dual) AND OBJSTATE<>(select ifsapp.SHOP_ORD_API.FINITE_STATE_ENCODE__('Wstrzymany') from dual) 
                        AND REVISED_DUE_DATE < (select ifsapp.work_time_calendar_api.Get_End_Date(ifsapp.site_api.Get_Manuf_Calendar_Id(contract), To_Date(SYSDATE), 10) from dual) 
                    GROUP BY 
                        Decode(Sign(REVISED_DUE_DATE - SYSDATE), '-1', To_Date(SYSDATE), REVISED_DUE_DATE),
                        Decode(source, '', 'MRP', 'DOP'),
                        contract,
                        ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO, SEQUENCE_NO, RELEASE_NO, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO, SEQUENCE_NO, RELEASE_NO, 1, 0)),
                        ifsapp.shop_order_operation_list_api.Get_Next_Op_Work_Center(ORDER_NO, SEQUENCE_NO, RELEASE_NO, ifsapp.shop_order_operation_list_api.Get_Prev_Non_Parallel_Op(ORDER_NO, SEQUENCE_NO, RELEASE_NO, 2, 0)) 
                        ORDER BY work_day,typ,WRKC,NEXT_WRKC) a",
                        "Lack_report",
                        cancellationToken
                );
        public class Lack_report_row : IEquatable<Lack_report_row>, IComparable<Lack_report_row>
        {           
           public DateTime Work_day { get; set; }
           public string Contract { get; set; }
           public string Typ {  get; set; }
           public string Wrkc { get; set; }
           public string Next_wrkc { get; set; }
           public double Qty_all { get; set; }

            public int CompareTo(Lack_report_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    int res1 = this.Work_day.CompareTo(other.Work_day);
                    if (res1 != 0)
                    {
                        return res1;
                    }
                    int res2 = this.Contract.CompareTo(other.Contract);
                    if (res2 != 0)
                    {
                        return res2;
                    }
                    int res3 = this.Typ.CompareTo(other.Typ);
                    if (res3 != 0)
                    {
                        return res3;
                    }
                    int res4 = this.Wrkc.CompareTo(other.Wrkc);
                    if (res4 != 0)
                    {
                        return res4;
                    }
                    return this.Next_wrkc.CompareTo(other.Next_wrkc);
                }
            }

            public bool Equals(Lack_report_row other)
            {
                if (other == null) return false;
                return 
                    (
                    this.Work_day.Equals(other.Work_day) && 
                    this.Contract.Equals(other.Contract) && 
                    this.Typ.Equals(other.Typ) && 
                    this.Wrkc.Equals(other.Wrkc) && 
                    this.Next_wrkc.Equals(other.Next_wrkc)
                    );
            }
        }
    }
}
