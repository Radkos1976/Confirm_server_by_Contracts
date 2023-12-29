using System;
using System.Linq;
using System.Xml.Linq;
using Npgsql;
using Common;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Diagnostics;
using System.Collections;

namespace DB_Conect
{
    public static class Postgres_helpers
    {
        /// <summary>
        /// Dictionary of Postgress data types
        /// </summary>
        public static Dictionary<string, NpgsqlTypes.NpgsqlDbType> PostegresTyp = new Dictionary<string, NpgsqlTypes.NpgsqlDbType>
        {
            {"name",NpgsqlTypes.NpgsqlDbType.Name },
            {"oidvector",NpgsqlTypes.NpgsqlDbType.Oidvector },
            {"refcursor",NpgsqlTypes.NpgsqlDbType.Refcursor },
            {"char",NpgsqlTypes.NpgsqlDbType.Char },
            {"varchar",NpgsqlTypes.NpgsqlDbType.Varchar },
            {"character varying",NpgsqlTypes.NpgsqlDbType.Varchar },
            {"text_nonbinary",NpgsqlTypes.NpgsqlDbType.Text },
            {"text",NpgsqlTypes.NpgsqlDbType.Text },
            {"bytea",NpgsqlTypes.NpgsqlDbType.Bytea },
            {"bit",NpgsqlTypes.NpgsqlDbType.Bit },
            {"bool",NpgsqlTypes.NpgsqlDbType.Boolean },
            {"boolean",NpgsqlTypes.NpgsqlDbType.Boolean },
            {"int2",NpgsqlTypes.NpgsqlDbType.Smallint },
            {"smallint",NpgsqlTypes.NpgsqlDbType.Smallint },
            {"int4",NpgsqlTypes.NpgsqlDbType.Integer },
            {"integer",NpgsqlTypes.NpgsqlDbType.Integer},
            {"int8",NpgsqlTypes.NpgsqlDbType.Bigint },
            {"float4",NpgsqlTypes.NpgsqlDbType.Real },
            {"float8",NpgsqlTypes.NpgsqlDbType.Double },
            {"double precision",NpgsqlTypes.NpgsqlDbType.Double },
            {"numeric",NpgsqlTypes.NpgsqlDbType.Numeric },
            {"money",NpgsqlTypes.NpgsqlDbType.Money },
            {"date",NpgsqlTypes.NpgsqlDbType.Date },
            {"timetz",NpgsqlTypes.NpgsqlDbType.TimeTz },
            {"time",NpgsqlTypes.NpgsqlDbType.Time },
            {"timestamptz",NpgsqlTypes.NpgsqlDbType.TimestampTz },
            {"timestamp with time zone",NpgsqlTypes.NpgsqlDbType.TimestampTz },
            {"timestamp without time zone",NpgsqlTypes.NpgsqlDbType.Timestamp },
            {"timestamp",NpgsqlTypes.NpgsqlDbType.Timestamp },
            {"point",NpgsqlTypes.NpgsqlDbType.Point },
            {"box",NpgsqlTypes.NpgsqlDbType.Box },
            {"lseg",NpgsqlTypes.NpgsqlDbType.LSeg },
            {"path",NpgsqlTypes.NpgsqlDbType.Path },
            {"polygon",NpgsqlTypes.NpgsqlDbType.Polygon },
            {"circle",NpgsqlTypes.NpgsqlDbType.Circle },
            {"inet",NpgsqlTypes.NpgsqlDbType.Inet },
            {"macaddr",NpgsqlTypes.NpgsqlDbType.MacAddr },
            {"uuid",NpgsqlTypes.NpgsqlDbType.Uuid },
            {"xml",NpgsqlTypes.NpgsqlDbType.Xml },
            {"interval",NpgsqlTypes.NpgsqlDbType.Interval }
        };
    }
    /// <summary>
    /// Dictionary for 2 keys
    /// </summary>
    /// <typeparam name="TKey1"></typeparam>
    /// <typeparam name="TKey2"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class Dictionary<TKey1, TKey2, TValue> : Dictionary<Tuple<TKey1, TKey2>, TValue>, IDictionary<Tuple<TKey1, TKey2>, TValue>
    {

        public TValue this[TKey1 key1, TKey2 key2]
        {
            get { return base[Tuple.Create(key1, key2)]; }
            set { base[Tuple.Create(key1, key2)] = value; }
        }

        public void Add(TKey1 key1, TKey2 key2, TValue value)
        {
            base.Add(Tuple.Create(key1, key2), value);
        }

        public void Remove(TKey1 key1, TKey2 key2)
        {
            base.Remove(Tuple.Create(key1, key2));
        }
        public bool ContainsKey(TKey1 key1, TKey2 key2)
        {
            return base.ContainsKey(Tuple.Create(key1, key2));
        }
    }

    /// <summary>
    /// Quick fix/speedup query
    /// </summary>
    public class Next_DAY
    {
        public Next_DAY()
        {
            using (NpgsqlConnection conO = new NpgsqlConnection(Postegresql_conn.Connection_pool["MAIN"].ToString()))
            {
                conO.Open();
                foreach (string contract in Postegresql_conn.Contracts_kalendar.Keys)
                {
                    using (NpgsqlCommand cust = new NpgsqlCommand(String.Format("Select '{0}' contract, work_day, wrk_day(wrk_count(work_day,'{0}')+1,'{0}') next_day from work_cal where calendar_id = '{0}' ", contract), conO))
                    {
                        using (NpgsqlDataReader reader = cust.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Calendar_next_day[reader.GetString(0), reader.GetDateTime(1)] = reader.GetDateTime(2);
                            }
                        }
                    }
                }
            }
        }
        private static readonly Dictionary<string, DateTime, DateTime> Calendar_next_day= new Dictionary<string, DateTime, DateTime>();
        public static DateTime Get_next_day(string Contract, DateTime Base_Day)
        {
            if (Calendar_next_day.ContainsKey(Contract, Base_Day))
            {
                return Calendar_next_day[Contract, Base_Day];
            }
            return Base_Day;
        }
    }

    /// <summary>
    /// Extension for string type => for limit length
    /// </summary>
    public static class StringExtensions
    {
        /// <summary>
        /// Method that limits the length of text to a defined length.
        /// </summary>
        /// <param name="source">The source text.</param>
        /// <param name="maxLength">The maximum limit of the string to return.</param>
        public static string LimitLength(this string source, int maxLength)
        {

            if (source == null || source.Length <= maxLength)
            {
                return source;
            }

            return source.Substring(0, maxLength);
        }
        /// <summary>
        /// Limit string by presets from Postgress schema 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="name"></param>
        /// <param name="field_val"></param>
        /// <returns></returns>
        public static string LimitDictLen(this string source, string name, Dictionary<string, int> field_val)
        {
            if (field_val.ContainsKey(name) && field_val[name] != 0)
            {
                return source.LimitLength(field_val[name]);
            }
            return source;
        }
    }

    /// <summary>
    /// Class for limited fields in DB
    /// All tables for update must be saved in dictionaries(by method Set_postgres_limit)
    /// </summary>
    public static class Get_limit_of_fields
    { 
        /// <summary>
        /// Schema for cust_ord
        /// </summary>
        public static Dictionary<string, int> cust_ord_len = new Dictionary<string, int>();
        /// <summary>
        /// Schema for inventory_part
        /// </summary>
        public static Dictionary<string, int> inventory_part_len = new Dictionary<string, int>();
        /// <summary>
        /// Schema for work_cal
        /// </summary>
        public static Dictionary<string, int> calendar_len = new Dictionary<string, int>();
        /// <summary>
        /// Schema for planners / buyers
        /// </summary>
        public static Dictionary<string, int> buyer_info_len = new Dictionary<string, int>();
        static Get_limit_of_fields()
        {
            buyer_info_len = new Dictionary<string, int>().Set_postgres_limit("MAIN", "data");
            calendar_len = new Dictionary<string, int>().Set_postgres_limit("MAIN", "work_cal");
            inventory_part_len = new Dictionary<string, int>().Set_postgres_limit("MAIN", "mag");
            cust_ord_len = new Dictionary<string, int>().Set_postgres_limit("MAIN", "cust_ord");
        }
        /// Method for  fill dictionary with list of fields in table of POSGRESs
        /// Database must exist in Connection poll dictionary
        /// </summary>
        /// <param name="dict"></param>
        /// <param name="key_conn"></param>
        /// <param name="Table_name"></param>
        /// <returns></returns>
        public static Dictionary<string, int> Set_postgres_limit(this Dictionary<string, int> dict, string key_conn, string Table_name)
        {
            using (NpgsqlConnection conO = new NpgsqlConnection(Postegresql_conn.Connection_pool[key_conn].ToString()))
            {
                conO.Open();
                var Tmp = conO.GetSchema("Columns", new string[] { null, null, Table_name });
                foreach (DataRow row in Tmp.Rows)
                {
                    dict.Add(row["column_name"].ToString(), (int)(row["character_maximum_length"].GetType()==typeof(System.DBNull) ? 0: row["character_maximum_length"]));
                }
            }
            return dict;
        }        
    }

    /// <summary>
    /// Get connetions settings to Oracle
    /// </summary>
    public static class Oracle_conn
    {
        /// <summary>
        /// Get settings for connections with ORACLE
        /// </summary>
        public static string Connection_string { get; set; }
        public static int Limit_oracle_conn { get; set; }
        /// <summary>
        /// Initialize data from XML
        /// </summary>
        static Oracle_conn()
        {
            try
            {
                XDocument Doc = XDocument.Load("C:\\serv\\Settings.xml");
                var oraconn = Doc.Descendants("ORACLE")
                    .Select(x => new
                    {
                        XConnection_string = (string)x.Element("ConnectionString"),
                        XLimit_oracle_conn =  (int)x.Element("Limit_oracle_conn")
                    });
                foreach (var res in oraconn)
                {
                    Connection_string = res.XConnection_string;
                    Limit_oracle_conn = res.XLimit_oracle_conn;
                }
            }
            catch (Exception e)
            {
                Loger.Log(String.Format("Error width XML file for ORACLE: {0}", e));
            }
        }
    }
    /// <summary>
    /// Settings for postegresql Database conections
    /// </summary>
    public static class Postegresql_conn
    {
        private static NpgsqlConnectionStringBuilder Conn_set { get; set; } = new NpgsqlConnectionStringBuilder();
        public static Dictionary<string, NpgsqlConnectionStringBuilder> Connection_pool {get; set;} = new Dictionary<string, NpgsqlConnectionStringBuilder>();
        public static string[] Contract_lst {  get; set; }
        public static Dictionary<string, string> Contracts_kalendar { get; set; } = new Dictionary<string, string>();
        public static Dictionary<string, string> Kalendar_eunm { get; set; } = new Dictionary< string, string>();
        private static string Host { get; set; }
        private static int Port { get; set; }
        private static int CommandTimeout { get; set; }
        private static int ConnectionIdleLifetime { get; set; }
        private static string ApplicationName { get; set; }
        private static string Username { get; set; }
        private static string Password { get; set; }
        private static string Database { get; set; }
        private static string Contracts { get; set; }
        private static string Kalendar_name { get; set; }

        /// <summary>
        /// Initialize data from XML
        /// </summary>
        static Postegresql_conn()
        {
            try
            {
                (Conn_set, Contracts) = Get_values("POSTEGRESQL_MAIN");
                Connection_pool.Add("MAIN",Conn_set);
                if (Contracts.Length > 0 )
                {
                    Contract_lst =  new string[] { "" };
                    Contract_lst = Contracts.Split(',');
                    foreach ( string str in Contract_lst) {
                        (Connection_pool[str], Contracts) = Get_values(String.Format("POSTEGRESQL_{0}", str));                        
                    }
                }
            }
            catch (Exception e)
            {
                Loger.Log(String.Format("Error width XML file for POSTEGRESQL: {0} ", e));
            }
        }
        /// <summary>
        /// Get values from XML file
        /// </summary>
        /// <param name="decendant"></param>
        /// <returns></returns>
        static (NpgsqlConnectionStringBuilder, string) Get_values(string decendant)
        {
            XDocument Doc = XDocument.Load("C:\\serv\\Settings.xml");
            try
            {
                var pstgr = Doc.Descendants(decendant)
                .Select(x => new
                {
                    XHost = (string)x.Element("Host"),
                    XPort = Convert.ToInt32((string)x.Element("Port")),
                    XCommandTimeout = Convert.ToInt32((string)x.Element("CommandTimeout")),
                    XConnectionIdleLifetime = Convert.ToInt32((string)x.Element("ConnectionIdleLifetime")),
                    XApplicationName = (string)x.Element("ApplicationName"),
                    XUsername = (string)x.Element("Username"),
                    XPassword = (string)x.Element("Password"),
                    XDatabase = (string)x.Element("Database"),
                    XKalendar_name = decendant.Contains("MAIN") ? "" : (string)x.Element("Calendar_id"),
                    XContracts = decendant.Contains("MAIN") ? (string)x.Element("Contracts") : ""
                });
                foreach (var res in pstgr)
                {
                    Host = res.XHost;
                    Port = res.XPort;
                    CommandTimeout = res.XCommandTimeout;
                    ConnectionIdleLifetime = res.XConnectionIdleLifetime;
                    ApplicationName = res.XApplicationName;
                    Username = res.XUsername;
                    Password = res.XPassword;
                    Database = res.XDatabase;
                    Contracts = res.XContracts;
                    Kalendar_name = res.XKalendar_name;
                }
                Conn_set = new NpgsqlConnectionStringBuilder()
                {
                    Host = Host,
                    Port = Port,
                    ConnectionIdleLifetime = ConnectionIdleLifetime,
                    CommandTimeout = CommandTimeout,
                    ApplicationName = ApplicationName,
                    Username = Username,
                    Password = Password,
                    Database = Database,
                    IncludeErrorDetail = true
                };
            }
            catch
            {
                Conn_set = new NpgsqlConnectionStringBuilder();
            }
            finally 
            {
                if (!decendant.Contains("MAIN"))
                {
                    var valgr = Doc.Descendants(decendant)
                   .Select(x => new
                   {
                       XKalendar_name = decendant.Contains("MAIN") ? "" : (string)x.Element("Calendar_id")
                   });
                    foreach (var res in valgr)
                    {
                        Kalendar_name = res.XKalendar_name;
                    }
                    Contracts_kalendar.Add(decendant, Kalendar_name);
                    if (!Kalendar_eunm.ContainsKey(Kalendar_name))
                    {
                        Kalendar_eunm.Add(Kalendar_name, Convert.ToString(Kalendar_eunm.Count() + 1));
                    }
                }                
            }         
            return (Conn_set, Contracts);
        }
    }
    /// <summary>
    /// List of ent for scan in DB
    /// </summary>
    public class Dataset_executor
    {
        private static readonly Dictionary<string, string, Tuple<DateTime?, DateTime?>> wait_task = new Dictionary<string, string, Tuple<DateTime?, DateTime?>>();
        private static readonly Dictionary<string, string, Tuple<DateTime?, DateTime?>> on_work_task = new Dictionary<string, string, Tuple<DateTime?, DateTime?>>();
        public static void Add_task(string part_no, string contract, DateTime Start, DateTime End)
        {
            wait_task.Add(part_no, contract, new Tuple<DateTime?, DateTime?>(Start, End));
        }
        public static (string, string, Tuple<DateTime?, DateTime?>) Run_next ()
        {           
            if (wait_task.Count > 0)
            {
                bool chk = false;
                string part_no = "";
                string contract = "";
                Tuple<DateTime?, DateTime?> range = new Tuple<DateTime?, DateTime?>((DateTime?)null, (DateTime?)null);
                while (!chk)
                {
                    try
                    {
                        if (wait_task.Count > 0)
                        {
                            (part_no, contract) = wait_task.Keys.First();
                            range = wait_task[part_no, contract];
                            chk = wait_task.ContainsKey(part_no, contract);
                            if (chk)
                            {
                                wait_task.Remove(part_no, contract);
                                on_work_task.Add(part_no, contract, range);
                            }
                        }
                        else
                        {
                            chk= true;
                            part_no = "";
                            contract = "";
                            range = new Tuple<DateTime?, DateTime?>((DateTime?)null, (DateTime?)null);
                        }
                        
                    }
                    catch
                    {
                        chk = false;
                    }
                }                   
                
                return (part_no, contract, range);
            }
            return ("", "", new Tuple<DateTime?, DateTime?>((DateTime?)null, (DateTime?)null));
        }
        public static int Count()
        {
            return wait_task.Count;
        }
        public static void Report_end (string part_no, string contract)
        {
            on_work_task.Remove(part_no, contract);
        }
    }

    /// <summary>
    /// Get_informations about STEPS
    /// </summary>
    public class Steps_executor
    {
        public static CancellationTokenSource cts;
        private readonly static Dictionary<string, DateTime> Active_steps = new Dictionary<string, DateTime>();
        private readonly static Dictionary<string, DateTime> Reccent_steps = new Dictionary<string, DateTime>();
        private readonly static Dictionary<string, DateTime> Steps_with_error = new Dictionary<string, DateTime>();

        static Steps_executor() 
        {             
            cts = new CancellationTokenSource();
        }

        /// <summary>
        /// Reset list of registered Steps
        /// </summary>
        public static void Reset_diary_of_steps()
        {
            Active_steps.Clear();
            Reccent_steps.Clear();
            Steps_with_error.Clear();
            Steps_executor.cts.Dispose();
            cts = new CancellationTokenSource();
        }

        /// <summary>
        /// Get State or Step
        /// </summary>
        /// <param name="step"></param>
        /// <returns></returns>
        public static (int, string, DateTime?) Step_Status(string step)
        {
            if (Active_steps.ContainsKey(step)) { return (0, "Active", Active_steps[step]); }
            if (Reccent_steps.ContainsKey(step)) { return (1, "Ready", Reccent_steps[step]); }
            if (Steps_with_error.ContainsKey(step)) { return (2, "Error", Steps_with_error[step]); }
            return (3, "Not Started", null);
        }
        /// <summary>
        /// Wait for end all Steps in array of string
        /// </summary>
        /// <param name="task_list"></param>
        /// <returns></returns>
        public static bool Wait_for(string[] task_list, string step, CancellationToken cancellationToken)
        {
            bool not_started_pending = true;
            bool on_error = false;
            Loger.Log(String.Format("Step {0} is frozen and will wait for end another process'es => {1}", step, string.Join(",", task_list)));
            while (not_started_pending == true & on_error == false)
            {
                not_started_pending = false;
                foreach (string task in task_list)
                {
                    (int state, string desc, DateTime? started) = Step_Status(task);
                    if (state == 0)
                    {
                        not_started_pending = true;
                    }
                    if (state == 2 || cancellationToken.IsCancellationRequested)
                    {
                        on_error = true;
                        break;
                    }
                }
                System.Threading.Thread.Sleep(200);
            }
            if (on_error)
            {
                return false;
            }
            return true;
        }
        /// <summary>
        /// Report error of Step
        /// </summary>
        /// <param name="step"></param>
        /// <returns></returns>
        public static bool Step_error(string step)
        {
            (int state, _, DateTime? started) = Step_Status(step);
            if (started == null || state == 0)
            {
                Loger.Log(String.Format("Step {0} error", step));
                Steps_with_error.Add(step, DateTime.Now);
                if (state == 0)
                {
                    Active_steps.Remove(step);
                }
                cts.Cancel();
                return true;
            }
            return false;
        }
        /// <summary>
        /// Report Step ending
        /// </summary>
        /// <param name="step"></param>
        /// <returns></returns>
        public static bool End_step(string step)
        {
            (int state, _, DateTime? started) = Step_Status(step);
            if (started == null || state != 1)
            {
                Loger.Log(String.Format("Step {0} was end work with success", step));
                Reccent_steps.Add(step, DateTime.Now);
                if (state == 0)
                {
                    Active_steps.Remove(step);
                }
                else if (state == 2)
                {
                    Steps_with_error.Remove(step);
                }
                return true;
            }
            return false;
        }
        /// <summary>
        /// Report Step Start
        /// </summary>
        /// <param name="step"></param>
        /// <returns></returns>
        public static bool Register_step(string step)
        {
            bool try_ = false;
            while (!try_)
            {
                (int state, _, DateTime? started) = Step_Status(step);
                if (started == null || state == 2)
                {

                    try
                    {
                        Loger.Log(String.Format("Step {0} was registered", step));
                        Active_steps.Add(step, DateTime.Now);
                        if (state == 2)
                        {
                            Steps_with_error.Remove(step);
                        }
                        try_ = true;
                        return true;
                    }
                    catch
                    {
                        try_ = false;
                    }

                }                
            }
            return false;
        }

    }

    /// <summary>
    /// Limit Oracle Connections
    /// </summary>
    public class Count_oracle_conn
    {
        private static readonly int max_connections = Oracle_conn.Limit_oracle_conn;
        private static int count = 0;
        static Count_oracle_conn()
        {
            max_connections = Oracle_conn.Limit_oracle_conn;
        }
        /// <summary>
        /// Freezes task For limit max_connections Const
        /// </summary>
        public static void Wait_for_Oracle(CancellationToken cancellationToken)
        {
            while (count >= max_connections && !cancellationToken.IsCancellationRequested)
            {
                System.Threading.Thread.Sleep(250);
            }
            count++;            
        }

        public static void Oracle_conn_ended()
        {
            count--;
        }

    }
        

    /// <summary>
    /// Simple logger class
    /// </summary>     
    public class Loger : IDB_Loger
    {
        /// <summary>
        /// DateTime of Start service
        /// </summary>
        private static DateTime serw_run = DateTime.Now;
        /// <summary>
        /// String with data logs
        /// </summary>
        public static string Log_rek = "";
        public static DateTime Started() { return serw_run; }
        public static void Log(string txt)
        {
            txt = String.Format("{0} => {1}", Time_stamp(), txt);
            Debug.WriteLine(txt);
            Log_rek = Log_rek + Environment.NewLine + txt;
        }
        private static string Time_stamp()
        {
            TimeSpan diff = DateTime.Now - serw_run;
            return diff.TotalSeconds.ToString();
        }
        public static void Srv_start()
        {
            if (Log_rek != "") { Srv_stop(); }
            Serw_run = DateTime.Now;
            Log_rek = "Logs Started at " + Serw_run;
        }
        public static void Srv_stop()
        {
            Save_stat_refr();
            Log_rek = "";
        }
        private static void Save_stat_refr()
        {
            try
            {
                string npA = Postegresql_conn.Connection_pool["MAIN"].ToString();
                using (NpgsqlConnection conA = new NpgsqlConnection(npA))
                {
                    conA.Open();
                    using (NpgsqlTransaction tr_savelogs = conA.BeginTransaction())
                    {
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            @"INSERT INTO public.server_query
                            (start_date, end_dat, errors_found, log, id)
                            VALUES
                            (@run,@end,@er,@log,@id); ", conA))
                        {
                            cmd.Parameters.Add("run", NpgsqlTypes.NpgsqlDbType.Timestamp);
                            cmd.Parameters.Add("end", NpgsqlTypes.NpgsqlDbType.Timestamp);
                            cmd.Parameters.Add("er", NpgsqlTypes.NpgsqlDbType.Integer);
                            cmd.Parameters.Add("log", NpgsqlTypes.NpgsqlDbType.Text);
                            cmd.Parameters.Add("id", NpgsqlTypes.NpgsqlDbType.Uuid);
                            cmd.Prepare();
                            string searchTerm = "Error";
                            string[] source = Log_rek.Split(new char[] { '.', '?', '!', ' ', ';', ':', ',', '\n' }, StringSplitOptions.RemoveEmptyEntries);
                            var matchQuery = from word in source
                                             where word.ToLowerInvariant() == searchTerm.ToLowerInvariant()
                                             select word;
                            cmd.Parameters[0].Value = Serw_run;
                            cmd.Parameters[1].Value = DateTime.Now;
                            cmd.Parameters[2].Value = matchQuery.Count();
                            cmd.Parameters[3].Value = Log_rek;
                            cmd.Parameters[4].Value = System.Guid.NewGuid();
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            @"DELETE FROM public.server_query 
                            WHERE start_date<current_timestamp - interval '14 day '", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        tr_savelogs.Commit();
                    }
                }
            }
            catch (Exception e)
            {
                Log("Eeee coś nie działa: " + e);
            }
        }
        void IDB_Loger.Log(string txt)
        {
            Log(txt);
        }
        void IDB_Loger.Srv_start()
        {
            Srv_start();
        }
        void IDB_Loger.Srv_stop()
        {
            Srv_stop();
        }
        DateTime IDB_Loger.Serw_run
        {
            get
            {
                return Serw_run;
            }
        }

        public static DateTime Serw_run { get => serw_run; set => serw_run = value; }
    }

}
