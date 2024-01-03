using DB_Conect;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Confirm_server_by_Contracts
{
    /// <summary>
    /// Gets informations about active Calendars
    /// ,Class update its self
    /// </summary>       
    public class Calendar : Update_pstgr_from_Ora<Calendar.Calendar_row>
    {
        public bool Updated_on_init;
        public List<Calendar_row> Calendar_list;
        private readonly Update_pstgr_from_Ora<Calendar_row> rw;
        public Calendar(bool updt, CancellationToken cancellationToken)
        {
            try
            {
                rw = new Update_pstgr_from_Ora<Calendar_row>("MAIN");
                Parallel.Invoke(async () =>
                {
                    if (updt)
                    {
                        Steps_executor.Register_step("Calendar");
                        await Update_cal(cancellationToken);
                        Updated_on_init = true;
                        Steps_executor.End_step("Calendar");
                    }
                    else
                    {
                        Calendar_list = await Get_PSTGR_List("ALL", cancellationToken);
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
        public Calendar(CancellationToken cancellationToken)
        {
            try
            {
                rw = new Update_pstgr_from_Ora<Calendar_row>("MAIN");
                Parallel.Invoke(async () =>
                {
                    Calendar_list = await Get_PSTGR_List("ALL", cancellationToken);
                    if (Calendar_list.Count == 0)
                    {
                        Steps_executor.Register_step("Calendar");
                        Updated_on_init = true;
                        await Update_cal(cancellationToken);
                        Steps_executor.End_step("Calendar");
                        Calendar_list = await Get_PSTGR_List("ALL", cancellationToken);
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
        /// Update Calendar table
        /// </summary>
        /// <returns></returns>
        public async Task<int> Update_cal(CancellationToken cancellationToken)
        {
            try
            {
                List<string> checked_calendar = new List<string>();
                int returned = 0;
                foreach (string contract in Postegresql_conn.Contracts_kalendar.Keys)
                {
                    if (!checked_calendar.Contains(Postegresql_conn.Contracts_kalendar[contract]) && returned == 0) 
                    {
                        //rw = new Update_pstgr_from_Ora<Orders_row>();
                        checked_calendar.Add(Postegresql_conn.Contracts_kalendar[contract]);
                        List<Calendar_row> list_ora = new List<Calendar_row>();
                        List<Calendar_row> list_pstgr = new List<Calendar_row>();
                        Parallel.Invoke(
                            async () => {
                                list_ora = await Get_Ora_list(Postegresql_conn.Contracts_kalendar[contract], cancellationToken);
                            },
                            async () => {
                                list_pstgr = await Get_PSTGR_List(Postegresql_conn.Contracts_kalendar[contract], cancellationToken);
                            }
                        );
                        Changes_List<Calendar_row> tmp = await rw.Changes(list_pstgr, list_ora, new[] { "work_day", "calendar_id" }, new[] { "work_day", "calendar_id" }, null, "Calendar", cancellationToken);
                        list_ora = null;
                        list_pstgr = null;
                        returned += await PSTRG_Changes_to_dataTable(tmp, "work_cal", new[] { "work_day", "calendar_id" }, null, new[] {
                        String.Format(@"Delete from public.work_cal
                          where calendar_id not in ({0})", String.Join(", ", Postegresql_conn.Contracts_kalendar.Select(x => (String.Format("'{0}'",x.Value))).ToArray())) }, "Calendar", cancellationToken);
                        tmp = null;
                    }                   
                }
                return returned;
            }
            catch (Exception e)
            {                
                Loger.Log("Error in import Calendar object:" + e);
                Steps_executor.Step_error("calendar_updt");
                return 1;
            }
        }
        /// <summary>
        /// Get calendar from PSTGR
        /// </summary>
        /// <param name="rw"></param>
        /// <returns></returns>
        public async Task<List<Calendar_row>> Get_PSTGR_List(string calendar_id, CancellationToken cancellationToken) => await rw.Get_PSTGR(String.Format("Select * from work_cal {0}", calendar_id == "ALL"?"":String.Format("WHERE CALENDAR_ID='{0}'", calendar_id)), "Calendar", cancellationToken);

        /// <summary>
        /// Get calendars from ERP
        /// </summary>
        /// <returns></returns>
        public async Task<List<Calendar_row>> Get_Ora_list(string calendar_id, CancellationToken cancellationToken) => await rw.Get_Ora("" +
            String.Format(@"SELECT
                                calendar_id,
                                counter,
                                to_date(work_day) work_day,
                                day_type,
                                working_time,
                                working_periods,                               
                                objversion 
                            FROM 
                                ifsapp.work_time_counter 
                            WHERE CALENDAR_ID='{0}'", calendar_id), "Calendar", cancellationToken);

        public class Calendar_row : IEquatable<Calendar_row>, IComparable<Calendar_row>
        {  
            public string Calendar_id { get; set; }
            public int Counter { get; set; }
            public DateTime Work_day { get; set; }
            public string Day_type { get; set; }
            public double Working_time { get; set; }
            public int Working_periods { get; set; }            
            public string Objersion { get; set; }        
            
            public int CompareTo(Calendar_row other)
            {
                if (other == null)
                {
                    return 1;
                }
                else
                {
                    int res = this.Work_day.CompareTo(other.Work_day);
                    if (res != 0) { return res; }
                    return this.Calendar_id.CompareTo(other.Calendar_id);
                }
            }
           
            public bool Equals(Calendar_row other)
            {
                if (other == null) return false;
                return this.Work_day.Equals(other.Work_day) && this.Calendar_id.Equals(other.Calendar_id);
            }
        }
        public List<Calendar_row> Check_length(List<Calendar_row> source)
        {
            Dictionary<string, int> calendar_len = Get_limit_of_fields.calendar_len;
            foreach (Calendar_row row in source)
            {
                row.Calendar_id = row.Calendar_id.LimitDictLen("calendar_id", calendar_len);
                row.Day_type = row.Day_type.LimitDictLen("day_type", calendar_len);                
                row.Objersion = row.Objersion.LimitDictLen("objersion", calendar_len);               
            }
            return source;
        }
    }
}
