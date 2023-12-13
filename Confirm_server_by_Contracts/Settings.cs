using System;
using System.Linq;
using System.Xml.Linq;
using Npgsql;
using Common;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Data;
using System.Threading.Tasks;
using System.Web;
using System.Threading;
using System.Diagnostics.Metrics;

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
            {"text_nonbinary",NpgsqlTypes.NpgsqlDbType.Text },
            {"text",NpgsqlTypes.NpgsqlDbType.Text },
            {"bytea",NpgsqlTypes.NpgsqlDbType.Bytea },
            {"bit",NpgsqlTypes.NpgsqlDbType.Bit },
            {"bool",NpgsqlTypes.NpgsqlDbType.Boolean },
            {"int2",NpgsqlTypes.NpgsqlDbType.Smallint },
            {"int4",NpgsqlTypes.NpgsqlDbType.Integer },
            {"int8",NpgsqlTypes.NpgsqlDbType.Bigint },
            {"float4",NpgsqlTypes.NpgsqlDbType.Real },
            {"float8",NpgsqlTypes.NpgsqlDbType.Double },
            {"numeric",NpgsqlTypes.NpgsqlDbType.Numeric },
            {"money",NpgsqlTypes.NpgsqlDbType.Money },
            {"date",NpgsqlTypes.NpgsqlDbType.Date },
            {"timetz",NpgsqlTypes.NpgsqlDbType.TimeTz },
            {"time",NpgsqlTypes.NpgsqlDbType.Time },
            {"timestamptz",NpgsqlTypes.NpgsqlDbType.TimestampTz },
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
            if (source.Length <= maxLength)
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
            if (field_val.ContainsKey(name) & field_val[name] != 0)
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
        public static Dictionary<string, int> cust_ord_len = new Dictionary<string, int>().Set_postgres_limit("MAIN", "cust_ord");
        public static Dictionary<string, int> inventory_part_len = new Dictionary<string, int>().Set_postgres_limit("MAIN", "mag");
        /// <summary>
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
                    dict.Add(row["column_name"].ToString(), (int) (row["character_maximum_length"]!=null ?  row["character_maximum_length"]:  0));
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
        /// <summary>
        /// Initialize data from XML
        /// </summary>
        static Oracle_conn()
        {
            try
            {
                XDocument Doc = XDocument.Load("Settings.xml");
                var oraconn = Doc.Descendants("ORACLE")
                    .Select(x => new
                    {
                        XConnection_string = (string)x.Element("ConnectionString")
                    });
                foreach (var res in oraconn)
                {
                    Connection_string = res.XConnection_string;
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
        private static NpgsqlConnectionStringBuilder Conn_set { get; set; }
        public static Dictionary<string, NpgsqlConnectionStringBuilder> Connection_pool {get; set;}
        public static string[] Contract_lst {  get; set; }
        private static string Host { get; set; }
        private static int Port { get; set; }
        private static int CommandTimeout { get; set; }
        private static int ConnectionIdleLifetime { get; set; }
        private static string ApplicationName { get; set; }
        private static string Username { get; set; }
        private static string Password { get; set; }
        private static string Database { get; set; }
        private static string Contracts { get; set; }
        /// <summary>
        /// Initialize data from XML
        /// </summary>
        static Postegresql_conn()
        {
            try
            {
                (Conn_set, Contracts) = get_values("POSTEGRESQL_MAIN");
                Connection_pool["MAIN"] = Conn_set;
                if (Contracts.Length > 0 )
                {
                    Contract_lst =  new string[] { "" };
                    Contract_lst = Contracts.Split(',');
                    foreach ( string str in Contract_lst) {
                        (Connection_pool[str], Contracts) = get_values(String.Format("POSTEGRESQL_{0}", str));                        
                    }
                }
            }
            catch (Exception e)
            {
                Loger.Log(String.Format("Error width XML file for POSTEGRESQL: {0} ", e));
            }
        }
        static (NpgsqlConnectionStringBuilder, string) get_values(string decendant)
        {
            XDocument Doc = XDocument.Load("Settings.xml");
            var pstgr = Doc.Descendants("POSTEGRESQL_MAIN")
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
                    XContracts = decendant.Contains("MAIN") ? (string)x.Element("Contracts"):""
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
                Database = Database
            };
            return (Conn_set, Contracts);
        }
    }

    /// <summary>
    /// Get_informations about STEPS
    /// </summary>
    public class Steps_executor
    {
        private readonly Dictionary<string, DateTime> Active_steps;
        private readonly Dictionary<string, DateTime> Reccent_steps;
        private readonly Dictionary<string, DateTime> Steps_with_error;

        public void Reset_diary_of_steps()
        {
            Active_steps.Clear();
            Reccent_steps.Clear();
            Steps_with_error.Clear();
        }

        public (int, string, DateTime?) Step_Status(string step)
        {
            if (Active_steps.ContainsKey(step)) { return (0, "Active", Active_steps[step]); }
            if (Reccent_steps.ContainsKey(step)) { return (1, "Ready", Reccent_steps[step]); }
            if (Steps_with_error.ContainsKey(step)) { return (2, "Error", Steps_with_error[step]); }
            return (0, "Not Started", null);
        }

        public bool Wait_for(string[] task_list)
        {
            bool not_started_pending = true;
            bool on_error = false;
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
                    if (state == 2)
                    {
                        on_error = true;
                        break;
                    }
                }
                System.Threading.Thread.Sleep(500);
            }
            if (on_error)
            {
                return false;
            }
            return true;
        }

        public bool Step_error(string step)
        {
            (int state, string desc, DateTime? started) = Step_Status(step);
            if (started == null || state == 0)
            {
                Steps_with_error.Add(step, DateTime.Now);
                if (state == 0)
                {
                    Active_steps.Remove(step);
                }
                return true;
            }
            return false;
        }

        public bool End_step(string step)
        {
            (int state, string desc, DateTime? started) = Step_Status(step);
            if (started == null || state != 1)
            {
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

        public bool Register_step(string step)
        {
            (int state, string desc, DateTime? started) = Step_Status(step);
            if (started == null || state == 2)
            {
                Active_steps.Add(step, DateTime.Now);
                if (state == 2)
                {
                    Steps_with_error.Remove(step);
                }
                return true;
            }
            return false;
        }

    }

    /// <summary>
    /// Limit Oracle Connections
    /// </summary>
    public class Count_oracle_conn
    {
        private const int max_connections = 6;
        private static int count = 0;
        /// <summary>
        /// Freezes task For limit max_connections Const
        /// </summary>
        public static void Wait_for_Oracle()
        {
            while (count >= max_connections)
            {
                System.Threading.Thread.Sleep(500);
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
        public static void Log(string txt)
        {
            Log_rek = Log_rek + Environment.NewLine + txt;
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
