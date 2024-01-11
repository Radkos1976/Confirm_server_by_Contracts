using System;
using System.Timers;
using Confirm_server_by_Contracts;
using DB_Conect;
using Npgsql;

namespace Purch_Confirm_server
{
    public class Conf_serv
    {
        readonly Timer _timer;
        
        public Conf_serv()
        {            
            _timer = new Timer(10000) { AutoReset = true };
            _timer.Elapsed += (sender, eventArgs) =>
            {
                try
                {
                    string npA = Postegresql_conn.Connection_pool["CONFIRM_SERVICE"].ToString();
                    int ser_run, cnt_serw = 0;
                    using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                    {
                        conA.Open();
                        using (NpgsqlCommand cmd = new NpgsqlCommand(string.Format(@"SELECT count(application_name) il 
                                                                        FROM pg_catalog.pg_stat_activity where application_name='{0}'", Postegresql_conn.App_name), conA))
                        {
                            cnt_serw = Convert.ToInt32(cmd.ExecuteScalar());
                        }
                        if (cnt_serw < 2)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand(@"Select 
                                case when d.state='STOP' then 0 else case when b.in_progress=true then case when b.start_update-interval '5 minutes'<current_timestamp then 0 else 
                                   case when c.il>0 then case when a.start_update+interval '4 minutes' < current_timestamp then 1 else 0 end	
                                    else case when a.start_update+interval '1 hour' < current_timestamp then 1 else 0 end end end else case when c.il>0 then 
                                case when a.start_update+interval '4 minutes' < current_timestamp then 1 else 0 end else case when a.start_update+interval '1 hour' < current_timestamp then 1 else 0 end end end end stat 
                                from (select start_update from datatbles where table_name='server_progress') a,
                                (select in_progress,start_update from datatbles where table_name='ifs_CONN') b,
                                (SELECT count(application_name) il FROM pg_catalog.pg_stat_activity where application_name='CLIENT') c,
                                (SELECT typ ,case when current_timestamp between (current_date + cast(to_char(start_idle,'HH24:MI:SS') as time)-interval '5 minutes') and 
                                (current_date + cast(to_char(stop_idle,'HH24:MI:SS') as time)+interval '5 minutes' ) then 'STOP' else  'RUN' end state from 
                                serv_idle 
                                where current_timestamp<=current_date + cast(to_char(stop_idle,'HH24:MI:SS') as time)+interval '15 minutes'  
                                order by current_date + cast(to_char(stop_idle,'HH24:MI:SS') as time) limit 1 ) d", conA))
                            {
                                ser_run = Convert.ToInt32(cmd.ExecuteScalar());
                            }
                        }
                        else
                        {
                            ser_run = 0;
                        }
                    }
                    if (ser_run > 0)
                    {
                        _timer.Stop();
                        Serv_instance srv = new Serv_instance();                        
                        srv.Start_calc();
                        Steps_executor.Reset_diary_of_steps();
                        srv = null;
                        _timer.Start();                        
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);                    
                }
            };

        }
        public void Start() { _timer.Start(); }
        public void Stop()
        {
            try
            {
                string npA = Postegresql_conn.Connection_pool["MAIN"].ToString();
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("select cast(count(table_name) as integer) busy  from datatbles where table_name='server_progress' and in_progress=true", conA))
                    {
                        int busy_il = 1;
                        while (busy_il > 0 && !Steps_executor.cts.IsCancellationRequested)
                        {
                            busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                            if (busy_il > 0)
                            {
                                Steps_executor.cts.Cancel();
                                System.Threading.Thread.Sleep(100);
                            }
                        }
                    }
                }
            }
            finally 
            { 
                _timer.Stop(); 
            } 
        }

    }
}
