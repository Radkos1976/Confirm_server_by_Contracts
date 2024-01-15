using DB_Conect;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Confirm_server_by_Contracts
{
    public static class SendMailEx
    {
        public static Task SendMailExAsync(
            this System.Net.Mail.SmtpClient @this,
            System.Net.Mail.MailMessage message,
            CancellationToken token = default)
        {
            // use Task.Run to negate SynchronizationContext
            return Task.Run(() => SendMailExImplAsync(@this, message, token));
        }

        private static async Task SendMailExImplAsync(
            System.Net.Mail.SmtpClient client,
            System.Net.Mail.MailMessage message,
            CancellationToken token)
        {
            token.ThrowIfCancellationRequested();

            var tcs = new TaskCompletionSource<bool>();
            System.Net.Mail.SendCompletedEventHandler handler = null;
            void unsubscribe() => client.SendCompleted -= handler;
            handler = async (s, e) =>
            {
                unsubscribe();
                await Task.Yield();
                if (e.UserState != tcs)
                    tcs.TrySetException(new InvalidOperationException("Unexpected UserState"));
                else if (e.Cancelled)
                    tcs.TrySetCanceled();
                else if (e.Error != null)
                    tcs.TrySetException(e.Error);
                else
                    tcs.TrySetResult(true);
            };

            client.SendCompleted += handler;
            try
            {
                client.SendAsync(message, tcs);
                using (token.Register(() => client.SendAsyncCancel(), useSynchronizationContext: false))
                {
                    await tcs.Task;
                }
            }
            finally
            {
                unsubscribe();
            }
        }
    }
    internal class Old_code
    {
        readonly string npC = Postegresql_conn.Connection_pool["MAIN"].ToString();
        readonly string Str_oracle_conn = Oracle_conn.Connection_string;        
        static readonly TaskScheduler main_cal = TaskScheduler.Default;
        private readonly ParallelOptions srv_op = new ParallelOptions
        {
            CancellationToken = CancellationToken.None,
            MaxDegreeOfParallelism = 100,
            TaskScheduler = main_cal
        };
        public string HTMLEncode(string text)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            foreach (char c in text)
            {
                if (c > 122) // special chars
                    sb.Append(String.Format("&#{0};", (int)c));
                else
                if (c == 47)//|| c >57 & c < 65 || c > 90 & c < 97)
                {
                    sb.Append(String.Format("&#{0};", (int)c));
                }
                else
                {
                    sb.Append(c);
                }
            }
            return sb.ToString();
        }
        private string TD(String strIn, Boolean oldco = false, Boolean bold = false, Boolean nowrap = false)
        {
            char newlin = (Char)13;
            string a1 = "";
            string a2 = "";
            string a3 = "";
            if (oldco) { a1 = a1 + " bgcolor=" + (Char)34 + "yellow" + (Char)34; }
            if (bold & oldco)
            {
                a2 = "<b>";
                a3 = "</b>";
            }
            if (nowrap) { a1 = a1 + " nowrap=" + (Char)34 + "nowrap" + (Char)34; }
            a1 += ">";
            return "<td" + a1 + a2 + strIn.Replace("00:00:00", "").Replace(newlin.ToString(), "<br />") + a3 + " </td>";
        }
        private async Task<int> Kick_off_err_orders(int dop_id, DateTime mod_date)
        {
            if (Oracle_conn.Set_Planned_Manuf_Date)
            {
                try
                {
                    //  procedura do przesuwania zamówień po wystapieniu błedu w użyciu standardowych instrukcji
                    using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                    {
                        string anonymous_block = " DECLARE " +
                                    " empty VARCHAR2(32000) := NULL; " +
                                    " b_ VARCHAR2(32000); " +
                                    " c_ VARCHAR2(32000); " +
                                    " de_ DATE := :move_date; " +
                                    " d_ VARCHAR2(32000); " +
                                    " dop VARCHAR2(32000) := :DP_ID; " +
                                    " BEGIN " +
                                    " d_:= 'DUE_DATE' || chr(31) || To_Char(de_, 'YYYY-MM-DD-HH24.MI.SS') || chr(30) || 'REPLICATE_CHANGES' || chr(31) || 'TRUE' || chr(30) || 'SEND_CHANGE_MESSAGE' || chr(31) || 'FALSE' || chr(30); " +
                                    " SELECT objid into b_ FROM ifsapp.dop_head WHERE dop_id = dop; " +
                                    " SELECT objversion into c_ FROM ifsapp.dop_head WHERE dop_id = dop; " +
                                    " IFSAPP.DOP_HEAD_API.MODIFY__(empty, b_, c_, d_, 'DO'); " +
                                    " IFSAPP.Dop_Order_API.Modify_Top_Level_Date__(empty, dop, 1, de_, 'YES'); " +
                                    " IFSAPP.Dop_Demand_Gen_API.Modify_Connected_Order_Line(empty, dop, 'DATE', 'FALSE', 'TRUE'); " +
                                    " FOR item IN " +
                                    " ( " +
                                    " select DOP_ID, DOP_ORDER_ID, IDENTITY1, IDENTITY2, IDENTITY3, SOURCE_DB from IFSAPP.DOP_ALARM where IFSAPP.DOP_ORDER_API.Get_State(dop_id, DOP_ORDER_ID) != 'Rozp.' and DOP_ID = dop and IFSAPP.Dop_Order_API.Get_Objstate(DOP_ID, DOP_ORDER_ID) <> 'Closed' " +
                                    ") " +
                                    " LOOP " +
                                    " IFSAPP.Dop_Supply_Gen_API.Adjust_Pegged_Date(item.IDENTITY1, item.IDENTITY2, item.IDENTITY3, item.SOURCE_DB, item.DOP_ID, item.DOP_ORDER_ID); " +
                                    " END LOOP; " +
                                    " COMMIT; " +
                                    " END; ";
                        await conO.OpenAsync();
                        OracleGlobalization info = conO.GetSessionInfo();
                        info.DateFormat = "YYYY-MM-DD";
                        conO.SetSessionInfo(info);
                        using (OracleCommand comm = conO.CreateCommand())
                        {
                            comm.CommandText = anonymous_block;
                            comm.Parameters.Add(":DP_ID", OracleDbType.Varchar2).Value = dop_id.ToString();
                            comm.Parameters.Add(":move_date", OracleDbType.Varchar2).Value = mod_date;
                            comm.Prepare();
                            comm.ExecuteNonQuery();
                        }
                        conO.Close();
                    }
                    return 0;
                }
                catch (Exception e)
                {
                    Loger.Log("Error => Błąd modyfikacji daty produkcji (procedura awaryjna):" + e);
                    return 1;
                }
            }
            return 0;
        }
        public async Task<int> Modify_prod_date(CancellationToken cancellationToken)
        {
            try
            {
                Steps_executor.Register_step("Modify_prod_date");
                int ra = await Send_mail_lack(cancellationToken);
                Loger.Log("Start modyfing prod date in ORD_DOP");
                if (Oracle_conn.Set_Planned_Manuf_Date)
                {
                    using (DataTable ord_mod = new DataTable())
                    {
                        using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                        {
                            conA.Open();
                            //using (NpgsqlCommand cmd = new NpgsqlCommand("select order_no,line_no,rel_no,prod from mail where status_informacji='WYKONANIE' or (info_handlo=false and logistyka=false and status_informacji!='POPRAWIĆ')", conA))
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "select a.*,get_date_dop(a.cust_id) data_dop " +
                                "from mail a " +
                                "left join " +
                                "(select cust_id,max(date_addd) from mail_hist group by cust_id having max(date_addd)>current_timestamp - interval '10 minute') b " +
                                "on b.cust_id=a.cust_id " +
                                "where b.cust_id is null and a.prod>=current_date and ((a.status_informacji='WYKONANIE' and a.info_handlo=false) " +
                                    "or (is_confirm(a.status_informacji)=true and get_date_dop(a.cust_id)!=prod and a.seria0=false) or (is_alter(a.status_informacji)=true and get_date_dop(a.cust_id)!=prod ) " +
                                    "and a.info_handlo=false) and get_dopstat(a.cust_id) is not null", conA))
                            {
                                using (NpgsqlDataReader po = cmd.ExecuteReader())
                                {
                                    ord_mod.Load(po);
                                }
                            }
                            conA.Close();
                            foreach (DataColumn col in ord_mod.Columns)
                            {
                                col.ReadOnly = false;
                            }
                                
                        }
                        using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                        {
                            conO.Open();
                            OracleGlobalization info = conO.GetSessionInfo();
                            info.DateFormat = "YYYY-MM-DD";
                            conO.SetSessionInfo(info);
                            using (OracleCommand comm = new OracleCommand("ifsapp.c_customer_order_line_api.Cancel_Schedule", conO))
                            {
                                comm.CommandType = CommandType.StoredProcedure;
                                comm.Parameters.Add("order_no", OracleDbType.Varchar2);
                                comm.Parameters.Add("line_no", OracleDbType.Varchar2);
                                comm.Parameters.Add("rel_no", OracleDbType.Varchar2);
                                comm.Parameters.Add("line_item_no", OracleDbType.Decimal);
                                comm.Prepare();
                                foreach (DataRowView rek in ord_mod.DefaultView)
                                {
                                    try
                                    {
                                        comm.Parameters["order_no"].Value = (string)rek["order_no"];
                                        comm.Parameters["line_no"].Value = (string)rek["line_no"];
                                        comm.Parameters["rel_no"].Value = (string)rek["rel_no"];
                                        comm.Parameters["line_item_no"].Value = 0;
                                        comm.ExecuteNonQuery();
                                    }
                                    catch (Exception e)
                                    {
                                        Loger.Log("Error => Nie zdjęto harmonogramu " + e);
                                    }
                                }
                                //conO.Close();
                            }
                            using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                            {
                                conA.Open();
                                //using (NpgsqlCommand cmd = new NpgsqlCommand("select order_no,line_no,rel_no,prod from mail where status_informacji='WYKONANIE' or (info_handlo=false and logistyka=false and status_informacji!='POPRAWIĆ')", conA))
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "INSERT INTO public.mod_date" +
                                    "(order_no, line_no, rel_no, prod, typ_zdarzenia, status_informacji, dop, err,indeks,opis,data_dop,date_add) " +
                                    "VALUES " +
                                    "(@order_no, @line_no, @rel_no, @prod, @typ_zdarzenia, @status_informacji, @dop, @err,@indeks,@opis,@data_dop,current_timestamp)", conA))
                                {
                                    cmd.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("line_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("rel_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("prod", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Parameters.Add("typ_zdarzenia", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("status_informacji", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("dop", NpgsqlTypes.NpgsqlDbType.Integer);
                                    cmd.Parameters.Add("err", NpgsqlTypes.NpgsqlDbType.Boolean);
                                    cmd.Parameters.Add("indeks", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("opis", NpgsqlTypes.NpgsqlDbType.Varchar);
                                    cmd.Parameters.Add("data_dop", NpgsqlTypes.NpgsqlDbType.Date);
                                    cmd.Prepare();
                                    using (OracleCommand comm = new OracleCommand("ifsapp.c_customer_order_line_api.Set_Planned_Manuf_Date", conO))
                                    {
                                        comm.CommandType = CommandType.StoredProcedure;
                                        comm.Parameters.Add("order_no", OracleDbType.Varchar2);
                                        comm.Parameters.Add("line_no", OracleDbType.Varchar2);
                                        comm.Parameters.Add("rel_no", OracleDbType.Varchar2);
                                        comm.Parameters.Add("line_item_no", OracleDbType.Decimal);
                                        comm.Parameters.Add("planned_date", OracleDbType.Date);
                                        comm.Prepare();
                                        foreach (DataRowView rek in ord_mod.DefaultView)
                                        {
                                            try
                                            {
                                                comm.Parameters["order_no"].Value = (string)rek["order_no"];
                                                comm.Parameters["line_no"].Value = (string)rek["line_no"];
                                                comm.Parameters["rel_no"].Value = (string)rek["rel_no"];
                                                comm.Parameters["line_item_no"].Value = 0;
                                                comm.Parameters["planned_date"].Value = (DateTime)rek["prod"];
                                                comm.ExecuteNonQuery();
                                                if (!(bool)rek["info_handlo"])
                                                {
                                                    
                                                    rek.BeginEdit();       
                                                    rek["ordid"] = "";
                                                    rek.EndEdit();
                                                }
                                                cmd.Parameters["order_no"].Value = (string)rek["order_no"];
                                                cmd.Parameters["line_no"].Value = (string)rek["line_no"];
                                                cmd.Parameters["rel_no"].Value = (string)rek["rel_no"];
                                                cmd.Parameters["prod"].Value = (DateTime)rek["prod"];
                                                cmd.Parameters["typ_zdarzenia"].Value = (string)rek["typ_zdarzenia"];
                                                cmd.Parameters["status_informacji"].Value = (string)rek["status_informacji"];
                                                cmd.Parameters["dop"].Value = (int)rek["dop"];
                                                cmd.Parameters["err"].Value = false;
                                                cmd.Parameters["indeks"].Value = (string)rek["indeks"];
                                                cmd.Parameters["opis"].Value = (string)rek["opis"];
                                                cmd.Parameters["data_dop"].Value = (DateTime)rek["data_dop"];
                                                cmd.ExecuteNonQuery();
                                            }
                                            catch
                                            {
                                                int check_err = await Kick_off_err_orders((int)rek["dop"], (DateTime)rek["prod"]);
                                                //int check_err = 1;
                                                if (check_err == 1)
                                                {
                                                    rek.BeginEdit();
                                                    rek["ordid"] = "";
                                                    rek.EndEdit();
                                                    cmd.Parameters["order_no"].Value = (string)rek["order_no"];
                                                    cmd.Parameters["line_no"].Value = (string)rek["line_no"];
                                                    cmd.Parameters["rel_no"].Value = (string)rek["rel_no"];
                                                    cmd.Parameters["prod"].Value = (DateTime)rek["prod"];
                                                    cmd.Parameters["typ_zdarzenia"].Value = (string)rek["typ_zdarzenia"];
                                                    cmd.Parameters["status_informacji"].Value = (string)rek["status_informacji"];
                                                    cmd.Parameters["dop"].Value = (int)rek["dop"];
                                                    cmd.Parameters["err"].Value = true;
                                                    cmd.Parameters["indeks"].Value = (string)rek["indeks"];
                                                    cmd.Parameters["opis"].Value = (string)rek["opis"];
                                                    cmd.Parameters["data_dop"].Value = (DateTime)rek["data_dop"];
                                                    cmd.ExecuteNonQuery();
                                                    Loger.Log("Nie przeplanowano :" + rek[0] + "_" + rek[1] + "_" + rek[2] + " na datę:" + rek[3]);
                                                }
                                            }
                                        }
                                    }
                                }
                                conA.Close();
                            }
                        }
                        DataRow[] new_rek = ord_mod.Select("ordid<>''");
                        {
                            if (new_rek.Length > 0)
                            {
                                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                {
                                    conA.Open();
                                    foreach (DataRow rw in new_rek)
                                    {
                                        bool chk;
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("select late_ord_exist(@cust_id)", conA))
                                        {
                                            // sprawdź czy adnotacja z zamówieniem opóźnionym nie istnieje
                                            cmd.Parameters.Add("@cust_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = rw["cust_id"];
                                            cmd.Prepare();
                                            chk = Convert.ToBoolean(cmd.ExecuteScalar());
                                        }
                                        if (!chk)
                                        {
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "INSERT INTO public.late_ord" +
                                                "(ordid, dop, koor, order_no, line_no, rel_no, part_no, descr, country, prom_date, prom_week, load_id, ship_date, date_entered, cust_id, prod, prod_week, planner_buyer, " +
                                                "indeks, opis, typ_zdarzenia, status_informacji, zest, info_handlo, logistyka, seria0, data0, cust_line_stat, ord_objver, data_dop) " +
                                                "VALUES " +
                                                "(@ordid, @dop, @koor, @order_no, @line_no, @rel_no, @part_no, @descr, @country, @prom_date, @prom_week, @load_id, @ship_date, @date_entered, @cust_id, @prod, @prod_week, " +
                                                "@planner_buyer, @indeks, @opis, @typ_zdarzenia, @status_informacji, @zest, @info_handlo, @logistyka, @seria0, @data0, @cust_line_stat, @ord_objver, @data_dop); ", conA))
                                            {
                                                cmd.Parameters.Add("@ordid", NpgsqlTypes.NpgsqlDbType.Text).Value = (string)rw["ordid"];
                                                cmd.Parameters.Add("@dop", NpgsqlTypes.NpgsqlDbType.Integer).Value = Convert.ToInt32(rw["dop"]);
                                                cmd.Parameters.Add("@koor", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["koor"];
                                                cmd.Parameters.Add("@order_no", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["order_no"];
                                                cmd.Parameters.Add("@line_no", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["line_no"];
                                                cmd.Parameters.Add("@rel_no", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["rel_no"];
                                                cmd.Parameters.Add("@part_no", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["part_no"];
                                                cmd.Parameters.Add("@descr", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["descr"];
                                                cmd.Parameters.Add("@country", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["country"];
                                                cmd.Parameters.Add("@prom_date", NpgsqlTypes.NpgsqlDbType.Date).Value = (DateTime)rw["prom_date"];
                                                cmd.Parameters.Add("@prom_week", NpgsqlTypes.NpgsqlDbType.Integer).Value = Convert.ToInt32(rw["prom_week"]);
                                                cmd.Parameters.Add("@load_id", NpgsqlTypes.NpgsqlDbType.Bigint).Value = rw["load_id"] ?? DBNull.Value;
                                                cmd.Parameters.Add("@ship_date", NpgsqlTypes.NpgsqlDbType.Date).Value = rw["ship_date"] ?? DBNull.Value;
                                                cmd.Parameters.Add("@date_entered", NpgsqlTypes.NpgsqlDbType.Timestamp).Value = (DateTime)rw["date_entered"];
                                                cmd.Parameters.Add("@cust_id", NpgsqlTypes.NpgsqlDbType.Uuid).Value = (Guid)rw["cust_id"];
                                                cmd.Parameters.Add("@prod", NpgsqlTypes.NpgsqlDbType.Date).Value = (DateTime)rw["prod"];
                                                cmd.Parameters.Add("@prod_week", NpgsqlTypes.NpgsqlDbType.Integer).Value = Convert.ToInt32(rw["prod_week"]);
                                                cmd.Parameters.Add("@planner_buyer", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["planner_buyer"];
                                                cmd.Parameters.Add("@indeks", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["indeks"];
                                                cmd.Parameters.Add("@opis", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["opis"];
                                                cmd.Parameters.Add("@typ_zdarzenia", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["typ_zdarzenia"];
                                                cmd.Parameters.Add("@status_informacji", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["status_informacji"];
                                                cmd.Parameters.Add("@zest", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw["zest"] ?? DBNull.Value;
                                                cmd.Parameters.Add("@info_handlo", NpgsqlTypes.NpgsqlDbType.Boolean).Value = (bool)rw["info_handlo"];
                                                cmd.Parameters.Add("@logistyka", NpgsqlTypes.NpgsqlDbType.Boolean).Value = (bool)rw["logistyka"];
                                                cmd.Parameters.Add("@seria0", NpgsqlTypes.NpgsqlDbType.Boolean).Value = (bool)rw["seria0"];
                                                cmd.Parameters.Add("@data0", NpgsqlTypes.NpgsqlDbType.Date).Value = rw["data0"] ?? DBNull.Value;
                                                cmd.Parameters.Add("@cust_line_stat", NpgsqlTypes.NpgsqlDbType.Varchar).Value = (string)rw["cust_line_stat"];
                                                cmd.Parameters.Add("@ord_objver", NpgsqlTypes.NpgsqlDbType.Timestamp).Value = (DateTime)rw["ord_objver"];
                                                cmd.Parameters.Add("@data_dop", NpgsqlTypes.NpgsqlDbType.Date).Value = (DateTime)rw["data_dop"];
                                                cmd.Prepare();
                                                cmd.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Steps_executor.End_step("Modify_prod_date");
                Loger.Log("END modyfing prod date in ORD_DOP");
                return 0;
            }
            catch (Exception e)
            {
                Loger.Log("Error => Błąd modyfikacji daty produkcji:" + e);
                return 1;
            }
        }
        private async Task<int> Confirm_ORd(CancellationToken cancellationToken)
        {
            try
            {                
                if (Steps_executor.Wait_for(new string[] { "TR_sendm" }, "Confirm_ORd", cancellationToken))
                {
                    if (Oracle_conn.Email_Order_Report)
                    {
                        using (DataTable conf_addr = new DataTable())
                        {
                            using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                            {
                                conA.Open();
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "select cast(count(table_name) as integer) busy " +
                                "from public.datatbles " +
                                "where  table_name='TR_sendm' and in_progress=true", conA))
                                {
                                    int busy_il = 1;
                                    while (busy_il > 0)
                                    {
                                        busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                        if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                                    }
                                }

                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "DELETE FROM public.conf_mail_null " +
                                    "WHERE order_no not in (SELECT a.order_no  " +
                                        "FROM ( SELECT a_1.*,get_refer(a_1.addr1) AS reference,CASE WHEN a_1.dop_connection_db::text = 'AUT'::text AND a_1.dop_state IS NULL THEN 1 ELSE 0 END AS gotowe " +
                                            "FROM cust_ord a_1 " +
                                            "LEFT JOIN " +
                                            "( SELECT cust_ord.order_no " +
                                                "FROM cust_ord " +
                                                "WHERE cust_ord.state_conf::text = 'Wydrukow.'::text AND cust_ord.last_mail_conf IS NOT NULL " +
                                                "GROUP BY cust_ord.order_no) c " +
                                            "ON c.order_no::text = a_1.order_no::text " +
                                            "WHERE (a_1.state_conf::text = 'Nie wydruk.'::text OR a_1.last_mail_conf IS NULL) " +
                                                "AND is_refer(a_1.addr1) = true AND substring(a_1.order_no::text, 1, 1) = 'S'::text " +
                                                "AND (a_1.cust_order_state::text <> ALL (ARRAY['Częściowo dostarczone'::character varying::text, 'Zaplanowane'::character varying::text])) " +
                                                "AND (substring(a_1.part_no::text, 1, 3) <> ALL (ARRAY['633'::text, '628'::text, '1K1'::text, '1U2'::text, '632'::text])) " +
                                                "AND (c.order_no IS NOT NULL AND a_1.dop_connection_db::text <> 'MAN'::text OR c.order_no IS NULL)) a " +
                                            "GROUP BY a.order_no, a.cust_no, a.reference, a.addr1, a.country " +
                                            "HAVING sum(a.gotowe) = 0 AND max(a.objversion) < (now() - '02:00:00'::interval))", conA))
                                {
                                    cmd.ExecuteNonQuery();
                                }
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                    "select a.order_no,a.cust_no,a.reference,cast('' as varchar )mail,a.country,current_date as date_add " +
                                    "from confirm_ord a " +
                                    "order by cust_no,reference,order_no", conA))
                                {
                                    using (NpgsqlDataReader po = cmd.ExecuteReader())
                                    {
                                        conf_addr.Load(po);
                                    }
                                    foreach (DataColumn col in conf_addr.Columns)
                                    {
                                        col.ReadOnly = false;
                                    }
                                }
                                conA.Close();
                            }
                            if (conf_addr.Rows.Count > 0)
                            {
                                using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                                {
                                    conO.Open();
                                    OracleGlobalization info = conO.GetSessionInfo();
                                    info.DateFormat = "YYYY-MM-DD";
                                    conO.SetSessionInfo(info);
                                    using (OracleCommand comm = new OracleCommand("" +
                                        "SELECT ifsapp.Comm_Method_API.Get_Name_Value('CUSTOMER',:cust_no,'E_MAIL',:reference, ifsapp.customer_order_api.Get_C_Final_Addr_No(:order_no),SYSDATE)  " +
                                        "FROM dual", conO))
                                    {
                                        comm.Parameters.Add(":cust_no", OracleDbType.Varchar2);
                                        comm.Parameters.Add(":reference", OracleDbType.Varchar2);
                                        comm.Parameters.Add(":order_no", OracleDbType.Varchar2);
                                        comm.Prepare();
                                        foreach (DataRow rw in conf_addr.Rows)
                                        {
                                            comm.Parameters[0].Value = rw["cust_no"];
                                            comm.Parameters[1].Value = rw["reference"];
                                            comm.Parameters[2].Value = rw["order_no"];
                                            string mail = Convert.ToString(comm.ExecuteScalar());
                                            rw[3] = mail;
                                        }
                                    }
                                    conO.Close();
                                }
                                DataRow[] nullmail = conf_addr.Select("mail=''");
                                if (nullmail.Length > 0)
                                {
                                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                                    {
                                        conA.Open();
                                        using (NpgsqlCommand cmd = new NpgsqlCommand("select date_add from conf_mail_null where order_no=@order_no", conA))
                                        {
                                            cmd.Parameters.Add("order_no", NpgsqlTypes.NpgsqlDbType.Varchar);
                                            cmd.Prepare();
                                            foreach (DataRow rw in nullmail)
                                            {
                                                if (cancellationToken.IsCancellationRequested) 
                                                {
                                                    break;
                                                }
                                                using (DataTable reko = new DataTable())
                                                {
                                                    cmd.Parameters[0].Value = rw[0];
                                                    using (NpgsqlDataReader po = cmd.ExecuteReader())
                                                    {
                                                        reko.Load(po);
                                                    }
                                                    if (reko.Rows.Count == 0)
                                                    {
                                                        using (NpgsqlConnection cob = new NpgsqlConnection(npC))
                                                        {
                                                            cob.Open();
                                                            using (NpgsqlCommand cm = new NpgsqlCommand("" +
                                                                "INSERT " +
                                                                "INTO public.conf_mail_null" +
                                                                "(order_no, cust_no, reference, country) " +
                                                                "VALUES " +
                                                                "(@rw1,@rw2,@rw3,@rw4)", cob))
                                                            {
                                                                cm.Parameters.Add("rw1", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[0];
                                                                cm.Parameters.Add("rw2", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[1];
                                                                cm.Parameters.Add("rw3", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[2];
                                                                cm.Parameters.Add("rw4", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[4];
                                                                cm.Prepare();
                                                                cm.ExecuteNonQuery();
                                                            }
                                                            cob.Close();
                                                        }
                                                    }
                                                    else
                                                    {
                                                        DataRow ra = reko.Rows[0];
                                                        if (!ra.IsNull("date_add"))
                                                        {
                                                            if ((DateTime)ra["date_add"] != DateTime.Now.Date)
                                                            {
                                                                using (NpgsqlConnection cob = new NpgsqlConnection(npC))
                                                                {
                                                                    cob.Open();
                                                                    using (NpgsqlCommand cm = new NpgsqlCommand("" +
                                                                        "UPDATE public.conf_mail_null " +
                                                                        "SET cust_no=@rw1, reference=@rw2, mail=null, country=@rw3, date_add=null	" +
                                                                        "WHERE order_no=@rw4;", cob))
                                                                    {
                                                                        cm.Parameters.Add("rw1", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[1];
                                                                        cm.Parameters.Add("rw2", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[2];
                                                                        cm.Parameters.Add("rw3", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[4];
                                                                        cm.Parameters.Add("rw4", NpgsqlTypes.NpgsqlDbType.Varchar).Value = rw[0];
                                                                        cm.Prepare();
                                                                        cm.ExecuteNonQuery();
                                                                    }
                                                                    cob.Close ();
                                                                }
                                                            }
                                                        }

                                                    }
                                                }
                                            }                                            
                                        }
                                        using (DataTable mail_list = new DataTable())
                                        {
                                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                "select b.mail " +
                                                "from " +
                                                    "(select * from " +
                                                    "conf_mail_null " +
                                                    "where date_add is null) a," +
                                                    "kontakty b " +
                                                "where b.country_coor=a.country " +
                                                "group by b.mail", conA))
                                            {
                                                using (NpgsqlDataReader po = cmd.ExecuteReader())
                                                {
                                                    mail_list.Load(po);
                                                }
                                            }
                                            if (mail_list.Rows.Count > 0)
                                            {
                                                using (DataTable kol = new DataTable())
                                                {
                                                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                                                        "select order_no as cust_ord,cust_no,reference,country " +
                                                        "from conf_mail_null", conA))
                                                    {
                                                        using (NpgsqlDataReader po = pot.ExecuteReader())
                                                        {
                                                            using (DataTable sch = po.GetSchemaTable())
                                                            {
                                                                foreach (DataRow a in sch.Rows)
                                                                {
                                                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                                                }
                                                            }
                                                        }
                                                    }
                                                    DataRow rw = kol.NewRow();
                                                    for (int i = 0; i < kol.Columns.Count; i++)
                                                    {
                                                        rw[i] = 0;
                                                    }
                                                    rw["cust_ord"] = 1;
                                                    rw["reference"] = 1;
                                                    kol.Rows.Add(rw);
                                                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                        "select a.order_no as cust_ord,a.cust_no,a.reference,a.country,' ' info " +
                                                        "from " +
                                                        "(select * from conf_mail_null where date_add is null) a," +
                                                        "kontakty b " +
                                                        "where b.country_coor=a.country and b.mail=@mail ", conA))
                                                    {
                                                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                                                       
                                                        foreach (DataRow rek in mail_list.Rows)
                                                        {
                                                            cmd.Parameters[0].Value = rek[0];
                                                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                                                            {
                                                                using (DataTable mal = new DataTable())
                                                                {
                                                                    mal.Load(re);
                                                                    if (mal.Rows.Count > 0)
                                                                    {
                                                                        int send = await Create_HTMLmail(
                                                                            mal,
                                                                            "Brak ustalonego adresu e-mail dla potwierdzenia zamówienia",
                                                                            rek[0].ToString().Replace("\r", ""),
                                                                            kol.Rows[0], cancellationToken,
                                                                            "*Powyższe zamówienia nie mogą zostać potwierdzone do czasu wprowadzenia brakujących informacji w kartotece klienta"
                                                                            );
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                using (NpgsqlCommand cmd = new NpgsqlCommand("UPDATE public.conf_mail_null SET date_add=current_date WHERE date_add is null", conA))
                                                {
                                                    cmd.ExecuteNonQuery();
                                                }
                                            }
                                        }
                                        conA.Close();
                                    }
                                }
                                using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                                {
                                    conO.Open();
                                    OracleGlobalization info = conO.GetSessionInfo();
                                    info.DateFormat = "YYYY-MM-DD";
                                    conO.SetSessionInfo(info);
                                    using (OracleCommand comm = new OracleCommand("ifsapp.Customer_Order_Flow_API.Email_Order_Report__", conO))
                                    {
                                        comm.CommandType = CommandType.StoredProcedure;
                                        comm.Parameters.Add("order_no", OracleDbType.Varchar2);
                                        comm.Parameters.Add("refer", OracleDbType.Varchar2);
                                        comm.Parameters.Add("lok", OracleDbType.Varchar2);
                                        comm.Parameters.Add("mail", OracleDbType.Varchar2);
                                        comm.Parameters.Add("cust_no", OracleDbType.Varchar2);
                                        comm.Parameters.Add("rep_nam", OracleDbType.Varchar2);
                                        comm.Prepare();
                                        foreach (DataRow rek in conf_addr.Rows)
                                        {
                                            if (cancellationToken.IsCancellationRequested) { break; }
                                            if (!rek.IsNull("mail"))
                                            {
                                                if (rek["mail"].ToString().Length > 5)
                                                {
                                                    {
                                                        comm.Parameters[0].Value = (string)rek["order_no"];
                                                        comm.Parameters[1].Value = (string)rek["reference"];
                                                        comm.Parameters[2].Value = "ST";
                                                        comm.Parameters[3].Value = (string)rek["mail"];
                                                        comm.Parameters[4].Value = (string)rek["cust_no"];
                                                        comm.Parameters[5].Value = "CUSTOMER_ORDER_CONF_REP";
                                                        comm.ExecuteNonQuery();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    conO.Close();
                                }
                            }
                        }
                    }
                }                
                return 0;            
            }
            catch (Exception e)
            {

                Loger.Log("Error => Błąd" + e);
                return 1;
            }
        }
        private async Task<int> Create_HTMLmail(DataTable rek, string StrTableStart, string mailto, DataRow spec_kol, CancellationToken cancellationToken, string tblfoot = "")
        {
            string txtwh = " ";
            Boolean haveshort = false;
            try
            {
                string firstbod = "<?xml version=" + (Char)34 + "1.0" + (Char)34 + " encoding =" + (Char)34 + "utf-8" + (Char)34 + " ?> <!DOCTYPE html PUBLIC " + (Char)34 + "-//W3C//DTD XHTML 1.0 Transitional//EN" + (Char)34 + " " + (Char)34 + "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd" + (Char)34 + "> <html xmlns=" + (Char)34 + "http://www.w3.org/1999/xhtml" + (Char)34 + "> <head> <meta http-equiv=" + (Char)34 + "Content-Type" + (Char)34 + " content =" + (Char)34 + "text/html charset=UTF-8" + (Char)34 + " />  <meta name=" + (Char)34 + "viewport" + (Char)34 + " content=" + (Char)34 + "width=device-width,initial-scale=1.0" + (Char)34 + "/>  <title>" + HTMLEncode(StrTableStart) + "</title>  <style type=" + (Char)34 + "text/css" + (Char)34 + ">  body,table{font-family:verdana,arial,sans-serif;font-size:12px;border-collapse:collapse;}  td,th{margin:3px;border:1px solid #BBB;} </style> </head> <body>";
                string Shortcut = " <p> <a href=" + (Char)34 + "http://sitsifsapp1.sits.local:59080/client/runtime/Ifs.Fnd.Explorer.application?url=ifsapf%3AtbwOverviewCustOrdLine%3Faction%3Dget";
                string StrTableStart1 = " <h5>WITAM<br />" + StrTableStart + "</h5> ";
                string strTableBeg = "<table>";
                string strTableEnd = "</table><i>" + tblfoot + "</i><h5>Pozdrawiam<br />Serwis potwierdzeń</h5>";
                string strTableHeader = "<tr bgcolor=" + (Char)34 + "lightblue" + (Char)34 + "> ";
                string lastbod = "</body></html>";
                foreach (DataColumn cl in spec_kol.Table.Columns)
                {
                    strTableHeader = strTableHeader + "<th>" + HTMLEncode(cl.ColumnName) + "</th>";
                }
                strTableHeader += "</tr>";
                //Build HTML Output for the DataSet
                string strTableBody = StrTableStart1 + strTableBeg + strTableHeader;
                int ind = 1;
                bool tst = false;
                bool nowr = false;
                foreach (DataRowView row in rek.DefaultView)
                {
                    if (txtwh.IndexOf(row["cust_ord"].ToString()) < 0)
                    {
                        txtwh = txtwh + row["cust_ord"].ToString() + ";";
                    }
                    strTableBody += "<tr>";
                    for (int i = 0; i < spec_kol.Table.Columns.Count; i++)
                    {
                        if (spec_kol.Table.Columns[i].ColumnName == "PROD_DATE" || spec_kol.Table.Columns[i].ColumnName == "CORR") { nowr = true; }
                        if (spec_kol.Table.Columns[i].ColumnName.ToUpper() == "C_LIN") { haveshort = true; }
                        if (Convert.ToDouble(spec_kol[i]) == 1) { tst = true; }
                        strTableBody += TD(HTMLEncode(row[i].ToString()), tst, tst, nowr);
                        tst = false;
                        nowr = false;
                    }
                    strTableBody += "</tr>";
                    if (row["info"] != null & row["info"].ToString() != "")
                    {
                        if (haveshort)
                        {
                            strTableBody = strTableBody + "<tr> <td colspan=" + (Char)34 + spec_kol.Table.Columns.Count + (Char)34 + "><b>" + HTMLEncode(row["info"].ToString().Replace("::", ":")) + "</td></b></tr>";
                        }
                    }
                    if (haveshort)
                    {
                        Shortcut = Shortcut + "%26key" + ind + "%3D0%255E" + row["C_lin"] + "%255E" + row["cust_ord"] + "%255E" + row["c_rel"];
                    }
                    ind++;
                }
                Shortcut = Shortcut + "%26COMPANY%3DSITS" + (Char)34 + ">" + HTMLEncode("Pokaż linie zamówień") + "</a> </p>" + "<br />Dotyczy zam:" + txtwh.Substring(0, txtwh.Length - 1) + "<br /> ";
                strTableBody = firstbod + strTableBody + strTableEnd + Shortcut + lastbod;
                StrTableStart += txtwh.Substring(0, txtwh.Length - 1);
                // Command line argument must the the SMTP host.
                SmtpClient client = new SmtpClient()
                {
                    Port = Mail_conn.Port,
                    DeliveryFormat = SmtpDeliveryFormat.International,
                    Host = Mail_conn.Host,
                    EnableSsl = Mail_conn.EnableSsl,
                    Timeout = Mail_conn.Timeout,
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = Mail_conn.UseDefaultCredentials,
                    Credentials = new System.Net.NetworkCredential(Mail_conn.UserName, Mail_conn.Password)
                };
                //MailAddress bcc = new MailAddress("radek.kosobucki@sits.pl");
                MailMessage mm = new MailMessage("radkomat@sits.pl", mailto, StrTableStart, strTableBody)
                {
                    BodyTransferEncoding = System.Net.Mime.TransferEncoding.Base64,
                    SubjectEncoding = System.Text.Encoding.UTF8,
                    BodyEncoding = System.Text.Encoding.UTF8,
                    Priority = MailPriority.High,
                    HeadersEncoding = System.Text.Encoding.UTF8,
                    IsBodyHtml = true,
                };
                //mm.Bcc.Add(bcc);
                await client.SendMailExAsync(mm, cancellationToken);
                mm.Dispose();
                client.Dispose();
                GC.Collect();
                Loger.Log("Wysłano maila:" + StrTableStart + txtwh + " Do:" + mailto);
                return 0;
            }
            catch (Exception e)
            {
                Loger.Log("Error => Błąd w wysłaniu maila:" + StrTableStart + txtwh + " To:" + mailto + " errors" + e);
                return 1;
            }
        }
        private async Task<int> Prep_potw(DataRow[] rek, CancellationToken cancellationToken)
        {
            Steps_executor.Register_step("Prep_potw");
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["PROD_WEEK"] = 1;
                rw["PROD_DATE"] = 1;
                kol.Rows.Add(rw);

                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED,info from send_mail where mail=@mail and typ='MAIL' and is_confirm(status_informacji) is true and last_mail is null and created + interval '1 hour' < current_timestamp order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            if (cancellationToken.IsCancellationRequested) { break; }
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(
                                            mal, 
                                            "Proszę o zmianę daty obiecanej", 
                                            erw[0].ToString().Replace("\r", ""), 
                                            kol.Rows[0], 
                                            cancellationToken, 
                                            "*Powyższe linie zamówień zostały już przesunięte w produkcji na termin gwarantujący dostawę brakujących komponentów"
                                            );
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("UPDATE public.send_mail	SET last_mail=current_date WHERE  mail=@mail and typ='MAIL' and is_confirm(status_informacji) is true and last_mail is null and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                    }
                    conA.Close();
                    Steps_executor.End_step("Prep_potw");
                }
            }
            return 0;
        }
        private async Task<int> Prep_FR(DataRow[] rek, CancellationToken cancellationToken)
        {
            Steps_executor.Register_step("Prep_FR");
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["PROD_WEEK"] = 1;
                rw["PROD_DATE"] = 1;
                rw["SHORTAGE_PART"] = 1;
                rw["SHORT_NAM"] = 1;
                kol.Rows.Add(rw);
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED,info " +
                        "from send_mail " +
                        "where mail=@mail and typ='MAIL' " +
                        "and is_alter(status_informacji) is true and last_mail is null and created + interval '1 hour' < current_timestamp " +
                        "order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            if (cancellationToken.IsCancellationRequested) { break; }
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(
                                            mal,
                                            "Proszę o zmianę daty obiecanej lub podmianę komponentu",
                                            erw[0].ToString().Replace("\r", ""),
                                            kol.Rows[0],
                                            cancellationToken
                                            );
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail " +
                                                "and typ='MAIL' and is_alter(status_informacji) is true and last_mail is null and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }
                    conA.Close();
                    Steps_executor.End_step("Prep_FR");
                }
            }
            return 0;
        }
        private async Task<int> Prep_seriaz(DataRow[] rek, CancellationToken cancellationToken)
        {
            Steps_executor.Register_step("Prep_seriaz");
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,TYP_ZDARZENIA,STATUS_INFORMACJI,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["PROD_WEEK"] = 1;
                rw["PROD_DATE"] = 1;
                rw["SHORTAGE_PART"] = 1;
                rw["SHORT_NAM"] = 1;
                kol.Rows.Add(rw);
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,PROD_DATE,TYP_ZDARZENIA,STATUS_INFORMACJI,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED,info " +
                        "from send_mail " +
                        "where mail=@mail and typ='Seria Zero' and last_mail is null " +
                        "and created + interval '1 hour' < current_timestamp order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            if (cancellationToken.IsCancellationRequested) { break; }
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(
                                            mal, 
                                            "Proszę o zmianę daty obiecanej/zmianę daty wykonania SERII ZERO", 
                                            erw[0].ToString().Replace("\r", ""), 
                                            kol.Rows[0], 
                                            cancellationToken
                                            );
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='Seria Zero' and last_mail is null " +
                                                "and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                    }
                    conA.Close();
                    Steps_executor.End_step("Prep_seriaz");
                }
            }
            return 0;
        }
        private async Task<int> Prep_NIEzam(DataRow[] rek, CancellationToken cancellationToken)
        {
            Steps_executor.Register_step("Prep_NIEzam");
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["SHORTAGE_PART"] = 1;
                rw["SHORT_NAM"] = 1;
                kol.Rows.Add(rw);
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED,info " +
                        "from send_mail " +
                        "where mail=@mail and typ='MAIL' and is_dontpurch(status_informacji) is true " +
                        "and last_mail is null and created + interval '1 hour' < current_timestamp " +
                        "order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            if (cancellationToken.IsCancellationRequested) { break; }
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Komponent nie zamawiany / wycofany ", erw[0].ToString().Replace("\r", ""), kol.Rows[0], cancellationToken, "Produkcja powyższych zamówień jest zagrożona ze względu na użycie komponentów wycofanych z kolekcji / nie zamawianych");
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='MAIL' and is_dontpurch(status_informacji) is true " +
                                                "and last_mail is null and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                    }
                    conA.Close();
                    Steps_executor.End_step("Prep_NIEzam");
                }
            }
            return 0;
        }
        private async Task<int> Prep_NIEpotw(DataRow[] rek, CancellationToken cancellationToken)
        {
            Steps_executor.Register_step("Prep_NIEpotw");
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,TYP_ZDARZENIA,STATUS_INFORMACJI,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["SHORTAGE_PART"] = 1;
                rw["SHORT_NAM"] = 1;
                kol.Rows.Add(rw);
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PROD_WEEK,TYP_ZDARZENIA,STATUS_INFORMACJI,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,CREATED,info " +
                        "from send_mail where mail=@mail and typ='NIE POTWIERDZAĆ' and last_mail is null and created + interval '1 hour' < current_timestamp order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            if (cancellationToken.IsCancellationRequested) { break; }
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Brak możliwości automatycznego potwierdzenia zamówienia", erw[0].ToString().Replace("\r", ""), kol.Rows[0], cancellationToken ,"Produkcja powyższych zamówień jest zagrożona ze względu na status zamówienia,błędne daty obiecane,braki materiałowe,użycie komponentów wycofanych z kolekcji / nie zamawianych");
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='NIE POTWIERDZAĆ' and last_mail is null " +
                                                "and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                    }
                    conA.Close();
                    Steps_executor.End_step("Prep_NIEpotw");
                }
            }
            return 0;
        }
        private async Task<int> Send_logist(DataRow[] rek, CancellationToken cancellationToken)
        {
            Steps_executor.Register_step("Send_logist");
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,LOAD_ID,SHIP_DATE,PROM_WEEK,PROD_WEEK,PROD_DATE as NEW_SHIP_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                rw["PROD_WEEK"] = 1;
                rw["SHIP_DATE"] = 1;
                rw["NEW_SHIP_DATE"] = 1;
                kol.Rows.Add(rw);

                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,LOAD_ID,SHIP_DATE,PROM_WEEK,PROD_WEEK,PROD_DATE as NEW_SHIP_DATE,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED,info " +
                        "from send_mail where mail=@mail and typ='MAIL LOG' and last_mail is null " +
                        "and created + interval '1 hour' < current_timestamp " +
                        "order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            if (cancellationToken.IsCancellationRequested) { break; }
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Proszę o zmianę daty wysyłki", erw[0].ToString().Replace("\r", ""), kol.Rows[0], cancellationToken,"*Powyższe linie zamówień zostały już przesunięte w produkcji na termin gwarantujący dostawę brakujących komponentów");
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='MAIL LOG' and last_mail is null " +
                                                "and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }
                    conA.Close();
                    Steps_executor.End_step("Send_logist");
                }
            }
            return 0;
        }
        private async Task<int> Popraw(DataRow[] rek, CancellationToken cancellationToken)
        {
            Steps_executor.Register_step("Popraw");
            using (DataTable kol = new DataTable())
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand pot = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED " +
                        "from send_mail", conA))
                    {
                        using (NpgsqlDataReader po = pot.ExecuteReader())
                        {
                            using (DataTable sch = po.GetSchemaTable())
                            {
                                foreach (DataRow a in sch.Rows)
                                {
                                    kol.Columns.Add(a["ColumnName"].ToString().ToUpper(), System.Type.GetType("System.Int32"));
                                }
                            }
                        }
                    }
                    conA.Close();
                }
                DataRow rw = kol.NewRow();
                for (int i = 0; i < kol.Columns.Count; i++)
                {
                    rw[i] = 0;
                }
                kol.Rows.Add(rw);

                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    conA.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "select CORR,CUST_ORD,C_LIN,C_REL,CATALOG_DESC,C_RY,PROM_WEEK,PART_BUYER,SHORTAGE_PART,SHORT_NAM,DOP,how_many(dop) CONF_COUNT,CREATED,info " +
                        "from send_mail " +
                        "where mail=@mail and typ='MAIL' and status_informacji='POPRAWIĆ' and last_mail is null " +
                        "and created + interval '1 hour' < current_timestamp " +
                        "order by CORR,CUST_ORD,C_LIN,C_REL", conA))
                    {
                        cmd.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar);
                        cmd.Prepare();
                        foreach (DataRow erw in rek)
                        {
                            if (cancellationToken.IsCancellationRequested) { break; }
                            cmd.Parameters[0].Value = erw[0];
                            using (NpgsqlDataReader re = cmd.ExecuteReader())
                            {
                                using (DataTable mal = new DataTable())
                                {
                                    mal.Load(re);
                                    if (mal.Rows.Count > 0)
                                    {
                                        int send = await Create_HTMLmail(mal, "Proszę o poprawę dat obiecanych - problem z potwierdzeniem", erw[0].ToString().Replace("\r", ""), kol.Rows[0], cancellationToken, "*Dla powyższych linii występuje problem z ustaleniem daty obiecanej - raport zprawdza linie z nieaktywnym DOP");
                                        if (send == 0)
                                        {
                                            using (NpgsqlCommand cmd1 = new NpgsqlCommand("" +
                                                "UPDATE public.send_mail	" +
                                                "SET last_mail=current_date " +
                                                "WHERE  mail=@mail and typ='MAIL' and status_informacji='POPRAWIĆ' " +
                                                "and created + interval '1 hour' < current_timestamp", conA))
                                            {
                                                cmd1.Parameters.Add("mail", NpgsqlTypes.NpgsqlDbType.Varchar).Value = erw[0].ToString();
                                                cmd1.Prepare();
                                                cmd1.ExecuteNonQuery();
                                            }
                                        }
                                    }
                                }
                            }
                        }                       
                    }
                    conA.Close();
                    Steps_executor.End_step("Popraw");
                }
            }
            return 0;
        }
        private async Task<int> Send_mail_lack(CancellationToken cancellationToken)
        {
            try
            {
                Loger.Log("Zaczynam przetwarzanie informacji o przepotwierdzeniach");
                using (DataTable adres_list = new DataTable())
                {

                    using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                    {
                        conA.Open();
                        Steps_executor.Register_step("TR_sendm");
                        Steps_executor.Register_step("send_mail");
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "UPDATE public.datatbles " +
                            "SET start_update=current_timestamp, in_progress=true,updt_errors=false " +
                            "WHERE table_name='TR_sendm'", conA))
                        {
                            cmd.ExecuteNonQuery();
                        }
                        using (NpgsqlTransaction TR_sendm = conA.BeginTransaction())
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET start_update=current_timestamp, in_progress=true,updt_errors=false " +
                                "WHERE table_name='send_mail'", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "delete from send_mail where dop||'_'||mail||'_'||typ in (select a.dop||'_'||a.mail||'_'||a.typ " +
                                "from " +
                                    "(select *  from " +
                                    "send_mail except select * from fill_sendmail)a)", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "INSERT INTO public.send_mail" +
                                "(mail, typ, typ_zdarzenia, status_informacji, info, corr, cust_ord, c_lin, c_rel, catalog_desc, c_ry,load_id,ship_date, prom_week, prod_week, prod_date, part_buyer, shortage_part, " +
                                "short_nam, dop, created, last_mail) " +
                                "select mail, typ, typ_zdarzenia, status_informacji, info, corr, cust_ord, c_lin, c_rel, catalog_desc, c_ry,load_id,ship_date, prom_week, prod_week, " +
                                "prod_date, part_buyer, shortage_part, short_nam, dop, created,null last_mail " +
                                    "from (select * from fill_sendmail except select * from send_mail)a", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                "UPDATE public.datatbles " +
                                "SET  last_modify=current_timestamp,in_progress=false,updt_errors=false " +
                                "WHERE table_name='TR_sendm'", conA))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            TR_sendm.Commit();
                            Steps_executor.End_step("TR_sendm");
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select cast(count(table_name) as integer) busy " +
                            "from public.datatbles " +
                            "where  table_name='TR_sendm' and in_progress=true", conA))
                        {
                            int busy_il = 1;
                            while (busy_il > 0)
                            {
                                busy_il = Convert.ToInt16(cmd.ExecuteScalar());
                                if (busy_il > 0) { System.Threading.Thread.Sleep(250); }
                            }
                        }
                        if (Steps_executor.Wait_for(new string[] { "TR_sendm" }, "Confirm_ORd", cancellationToken))
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                            "select mail,typ,is_confirm(status_informacji) confirm, is_alter(status_informacji) alt,is_dontpurch(status_informacji) niezam," +
                            "case when typ!='NIE POTWIERDZAĆ' then status_informacji end tp " +
                            "from send_mail " +
                            "where last_mail is null and ((cast(created as date)<current_date and typ='NIE POTWIERDZAĆ') or (created + interval '1 hour' < current_timestamp and typ!='NIE POTWIERDZAĆ')) " +
                            "group by mail,is_confirm(status_informacji) ,is_alter(status_informacji) , is_dontpurch(status_informacji),typ,case when typ!='NIE POTWIERDZAĆ' then status_informacji end", conA))
                            {
                                using (NpgsqlDataReader re = cmd.ExecuteReader())
                                {
                                    adres_list.Load(re);
                                }
                            }
                        }
                        conA.Close();
                    }
                    if (Steps_executor.Wait_for(new string[] { "TR_sendm" }, "Confirm_ORd", cancellationToken))
                    {
                        int R_pot = 0;
                        int R_alter = 0;
                        int R_dontpurch = 0;
                        int dontconf = 0;
                        int seria_z = 0;
                        int log = 0;
                        int popr = 0;
                        int conirm = 0;
                        conirm = await Confirm_ORd(cancellationToken);
                        Parallel.Invoke(srv_op,
                            async () => R_pot = await Prep_potw(adres_list.Select("confirm = true"), cancellationToken),
                            async () => R_alter = await Prep_FR(adres_list.Select("alt = true"), cancellationToken),
                            async () => R_dontpurch = await Prep_NIEzam(adres_list.Select("niezam = true"), cancellationToken),
                            async () => dontconf = await Prep_NIEpotw(adres_list.Select("typ = 'NIE POTWIERDZAĆ'"), cancellationToken),
                            async () => seria_z = await Prep_seriaz(adres_list.Select("typ = 'Seria Zero'"), cancellationToken),
                            async () => log = await Send_logist(adres_list.Select("typ = 'MAIL LOG'"), cancellationToken),
                            async () => popr = await Popraw(adres_list.Select("typ = 'MAIL' and tp='POPRAWIĆ'"), cancellationToken),
                            () =>
                            {
                                if (Steps_executor.Wait_for(new string[] { "Prep_potw", "Prep_FR", "Prep_seriaz", "Prep_NIEzam", "Prep_NIEpotw", "Prep_seriaz", "Send_logist", "Popraw" }, "send_mail", cancellationToken))
                                {
                                    Steps_executor.End_step("send_mail");
                                }
                            });                                                   

                   
                        //Parallel.Invoke(async () => R_pot = await Prep_potw(adres_list.Select("confirm = true")), async () => R_alter = await Prep_FR(adres_list.Select("alt = true")), async () => R_dontpurch = await Prep_NIEzam(adres_list.Select("niezam = true")), async () => seria_z = await Prep_seriaz(adres_list.Select("typ = 'Seria Zero'")), async () => log = await Send_logist(adres_list.Select("typ = 'MAIL LOG'")), async () => popr = await Popraw(adres_list.Select("typ = 'MAIL' and tp='POPRAWIĆ'")), async () => conirm = await Confirm_ORd());
                        
                        Loger.Log("Koniec wysyłania informacji o przepotwierdzeniach");
                    }
                    
                }
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET  last_modify=current_timestamp,in_progress=false,updt_errors=false " +
                        "WHERE table_name='send_mail'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                return 0;
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conA = new NpgsqlConnection(npC))
                {
                    await conA.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        "UPDATE public.datatbles " +
                        "SET  last_modify=current_timestamp,in_progress=false,updt_errors=true " +
                        "WHERE table_name='send_mail'", conA))
                    {
                        cmd.ExecuteNonQuery();
                    }
                    conA.Close();
                }
                Loger.Log("Error => Błąd w wysłaniu informacji o przepotwierdzeniach:" + e);
                Steps_executor.Step_error("send_mail");
                return 1;
            }
        }
    }
}
