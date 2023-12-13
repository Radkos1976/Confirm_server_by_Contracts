using DB_Conect;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Confirm_server_by_Contracts.Calendar;

namespace Confirm_server_by_Contracts
{
    /// <summary>
    /// Gets informations about active customer orders
    /// Class update its self
    /// </summary>       
    public class Calendar : Update_pstgr_from_Ora<Calendar.Calendar_row>
    {
        public bool Updated_on_init;
        public List<Calendar_row> Calendar_list;
        private readonly Update_pstgr_from_Ora<Calendar_row> rw;
        public Calendar(bool updt)
        {
            try
            {
                rw = new Update_pstgr_from_Ora<Calendar_row>("MAIN");
                Parallel.Invoke(async () =>
                {
                    if (updt)
                    {
                        Steps_executor.Register_step("Calendar");
                        await Update_cust();
                        Updated_on_init = true;
                        Steps_executor.End_step("Calendar");
                    }
                    else
                    {
                        Calendar_list = await Get_PSTGR_List("ALL");
                        Calendar_list.Sort();
                        Updated_on_init = false;
                    }
                });
            }
            catch (Exception e)
            {
                Loger.Log("Error on initialize Calendar object:" + e);
            }
        }
        public Calendar()
        {
            try
            {
                rw = new Update_pstgr_from_Ora<Calendar_row>("MAIN");
                Parallel.Invoke(async () =>
                {
                    Calendar_list = await Get_PSTGR_List("ALL");
                    if (Calendar_list.Count == 0)
                    {
                        Steps_executor.Register_step("Calendar");
                        Updated_on_init = true;
                        await Update_cust();
                        Steps_executor.End_step("Calendar");
                        Calendar_list = await Get_PSTGR_List("ALL");
                    }
                    else
                    {
                        Calendar_list.Sort();
                        Updated_on_init = false;
                    }
                });
            }
            catch (Exception e)
            {
                Steps_executor.Step_error("Calendar");
                Loger.Log("Error on initialize Calendar object:" + e);
            }
        }
        /// <summary>
        /// Update customer order table
        /// </summary>
        /// <returns></returns>
        public async Task<int> Update_cust()
        {
            try
            {
                int returned = 0;
                foreach (string contract in Postegresql_conn.Contracts_kalendar.Keys)
                {

                    //rw = new Update_pstgr_from_Ora<Orders_row>();
                    List<Calendar_row> list_ora = new List<Calendar_row>();
                    List<Calendar_row> list_pstgr = new List<Calendar_row>();
                    Parallel.Invoke(
                        async () => {
                            list_ora = await Get_Ora_list(Postegresql_conn.Contracts_kalendar[contract]); list_ora.Sort();
                        },
                        async () => {
                            list_pstgr = await Get_PSTGR_List(Postegresql_conn.Contracts_kalendar[contract]); list_pstgr.Sort();
                        }
                    );
                    Changes_List<Calendar_row> tmp = rw.Changes(list_pstgr, list_ora, new[] { "calendar_id", "counter" }, new[] { "calendar_id", "counter" }, "counter", "Calendar");
                    list_ora = null;
                    list_pstgr = null;
                    returned += await PSTRG_Changes_to_dataTable(tmp, "work_cal", new[] { "calendar_id", "counter" }, null, new[] {
                        String.Format(@"Delete from public.work_cal
                          where calendar_id not in ({0})", String.Join(", ", Postegresql_conn.Contracts_kalendar.Select(x => (String.Format("'{0}'",x.Value))).ToArray())) } , "Calendar");
                }
                return returned;
            }
            catch (Exception e)
            {
                Steps_executor.Step_error("calendar_updt");
                Loger.Log("Error in import Calendar object:" + e);
                return 1;
            }
        }
        /// <summary>
        /// Get calendar from PSTGR
        /// </summary>
        /// <param name="rw"></param>
        /// <returns></returns>
        public async Task<List<Calendar_row>> Get_PSTGR_List(string calendar_id) => await rw.Get_PSTGR(String.Format("Select * from work_cal {0}", calendar_id == "ALL"?"":String.Format("WHERE CALENDAR_ID='{0}'", calendar_id)), "Calendar");

        /// <summary>
        /// Get calendars from ERP
        /// </summary>
        /// <returns></returns>
        public async Task<List<Calendar_row>> Get_Ora_list(string calendar_id) => await rw.Get_Ora("" +
            String.Format(@"SELECT
                                calendar_id,
                                counter,
                                to_date(work_day) work_day,
                                day_type,
                                working_time,
                                working_periods,
                                objid,
                                objversion 
                            FROM 
                                ifsapp.work_time_counter 
                            WHERE CALENDAR_ID='{0}'", calendar_id), "Calendar");

        public class Calendar_row : IEquatable<Calendar_row>, IComparable<Calendar_row>
        {
            private readonly Dictionary<string, int> calendar_len = Get_limit_of_fields.calendar_len;

            public string Calendar_id { get { return Calendar_id; } set => Calendar_id = value.LimitDictLen("calendar_id", calendar_len); }            
            public int Counter { get; set; }
            public DateTime Work_day { get; set; }
            public string Day_type { get { return Day_type; } set => Day_type = value.LimitDictLen("day_type", calendar_len); }
            public double Working_time { get; set; }
            public int Working_periods { get; set; }
            public string Objid { get { return Objid; } set => Objid = value.LimitDictLen("objid", calendar_len); }
            public string Objersion { get { return Objersion; } set => Objersion = value.LimitDictLen("objersion", calendar_len); }

            public int CompareTo(Calendar_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    int prim = this.Counter.CompareTo(other.Counter);
                    if (prim != 0) { return prim; }
                    return this.Calendar_id.CompareTo(other.Calendar_id);
                }
            }
           
            public bool Equals(Calendar_row other)
            {
                if (other == null) return false;
                return (this.Counter.Equals(other.Counter) && this.Calendar_id.Equals(other.Calendar_id));
            }
        }
    }
}
