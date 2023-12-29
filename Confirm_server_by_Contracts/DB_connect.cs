using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using Npgsql;
using System.Linq;
using System.Threading.Tasks;
using System.Reflection;
using System.Collections.Concurrent;
using System.Data;
using System.Threading;

namespace DB_Conect
{
    /// <summary>
    /// Universal class for update dataset from oracle into postegresql tables
    /// </summary>
    public class Update_pstgr_from_Ora<T> where T : class, new()
    {
        public Update_pstgr_from_Ora(string KEY_CONN)
        {
            npC = Postegresql_conn.Connection_pool[KEY_CONN].ToString();
            Str_oracle_conn = Oracle_conn.Connection_string;
        }
        public Update_pstgr_from_Ora()
        {
            npC = Postegresql_conn.Connection_pool["MAIN"].ToString(); 
            Str_oracle_conn = Oracle_conn.Connection_string;
        }

        readonly string Str_oracle_conn;
        readonly string npC;
   
        /// <summary>
        /// Get datasets from ORACLE - use this override when columns in query and in class T is same and create prepared parameters 
        /// </summary>
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name, ORA_parameters parameters, CancellationToken cancellationToken)
        {
            Dictionary<string, int> D_columns = new Dictionary<string, int>();
            Dictionary<int, string> P_columns = new Dictionary<int, string>();
            Dictionary<int, Type> P_types = new Dictionary<int, Type>();
            T Row = new T();
            IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                     .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
            int counter = 0;
            foreach (var p in Accessors)
            {
                P_types.Add(counter, p.PropertyInfo.PropertyType);
                P_columns.Add(counter, p.PropertyInfo.Name.ToLower());
                counter++;
            }
            return await Get_Ora(Sql_ora, Task_name, D_columns, P_columns, P_types, cancellationToken, parameters);
        }
        /// <summary>
        /// Get datasets from ORACLE - use this override when columns in query and in class T is diferent and use prepared parameters 
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <param name="D_columns"></param>
        /// <param name="P_columns"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name, Dictionary<string, int> D_columns, Dictionary<int, string> P_columns, Dictionary<int, Type> P_types, CancellationToken cancellationToken, ORA_parameters parameters)
        {
            List<T> Rows = new List<T>();
            try
            {
                Count_oracle_conn.Wait_for_Oracle(cancellationToken);
                if (!cancellationToken.IsCancellationRequested)
                {
                    using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                    {
                        await conO.OpenAsync(cancellationToken);
                        OracleGlobalization info = conO.GetSessionInfo();
                        info.DateFormat = "YYYY-MM-DD";
                        conO.SetSessionInfo(info);
                        bool list_columns = false;
                        T Row = new T();
                        IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                        .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();

                        using (OracleCommand cust = new OracleCommand(Sql_ora, conO))
                        {

                            using (OracleDataReader reader = cust.ExecuteReader())
                            {
                                reader.FetchSize = cust.RowSize * 200;
                                while (await reader.ReadAsync(cancellationToken))
                                {
                                    if (!list_columns)
                                    {
                                        if (D_columns.Count == 0)
                                        {
                                            for (int col = 0; col < reader.FieldCount; col++)
                                            {
                                                string nam = reader.GetName(col).ToLower();
                                                D_columns.Add(nam, col);
                                            }
                                        }
                                        list_columns = true;
                                    }
                                    Row = new T();
                                    int counter = 0;
                                    foreach (var Accessor in Accessors)
                                    {
                                        string metod = P_columns[counter];
                                        if (D_columns.ContainsKey(metod))
                                        {
                                            int col = D_columns[metod];
                                            object readData = reader.GetValue(D_columns[metod]);
                                            if (readData != System.DBNull.Value)
                                            {
                                                Type pt = P_types[counter];
                                                Accessor.SetValue(Row, Convert.ChangeType(readData, Nullable.GetUnderlyingType(pt) ?? pt, null));
                                            }
                                        }
                                        counter++;
                                    }
                                    Rows.Add(Row);
                                }
                            }
                        }
                    }
                    Rows.Sort();
                }                               
                return Rows;
            }
            catch (Exception e)
            {
                Loger.Log("Error on Get_Ora width ORA_parameters parameters):" + Task_name + e);
                Steps_executor.Step_error(Task_name);               
                return Rows;
            }
            finally
            {
                Count_oracle_conn.Oracle_conn_ended();
            }

        }
        /// <summary>
        /// Get datasets from ORACLE - use this override when columns in query and in class T is diferent  
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <param name="D_columns"></param>
        /// <param name="P_columns"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name, Dictionary<string, int> D_columns, Dictionary<int, string> P_columns, Dictionary<int, Type> P_types, CancellationToken cancellationToken)
        {
            List<T> Rows = new List<T>();
            try
            {
                Count_oracle_conn.Wait_for_Oracle(cancellationToken);
                if (!cancellationToken.IsCancellationRequested)
                {
                    using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                    {
                        await conO.OpenAsync(cancellationToken);
                        OracleGlobalization info = conO.GetSessionInfo();
                        info.DateFormat = "YYYY-MM-DD";
                        conO.SetSessionInfo(info);
                        bool list_columns = false;
                        T Row = new T();
                        IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                        .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
                        using (OracleCommand cust = new OracleCommand(Sql_ora, conO))
                        {
                            using (OracleDataReader reader = cust.ExecuteReader())
                            {
                                reader.FetchSize = cust.RowSize * 200;
                                while (await reader.ReadAsync(cancellationToken))
                                {
                                    if (!list_columns)
                                    {
                                        if (D_columns.Count == 0)
                                        {
                                            for (int col = 0; col < reader.FieldCount; col++)
                                            {
                                                string nam = reader.GetName(col).ToLower();
                                                D_columns.Add(nam, col);
                                            }
                                        }
                                        list_columns = true;
                                    }
                                    Row = new T();
                                    int counter = 0;
                                    foreach (var Accessor in Accessors)
                                    {
                                        string metod = P_columns[counter];
                                        if (D_columns.ContainsKey(metod))
                                        {
                                            int col = D_columns[metod];
                                            object readData = reader.GetValue(D_columns[metod]);
                                            if (readData != System.DBNull.Value)
                                            {
                                                Type pt = P_types[counter];
                                                Accessor.SetValue(Row, Convert.ChangeType(readData, Nullable.GetUnderlyingType(pt) ?? pt, null));
                                            }
                                        }
                                        counter++;
                                    }
                                    Rows.Add(Row);
                                }
                            }
                        }
                    }
                    Rows.Sort();
                }
                return Rows;
            }
            catch (Exception e)
            {                
                Loger.Log("Error on GET_ORA :" + Task_name + e);
                Steps_executor.Step_error(Task_name);
                return Rows;
            }
            finally
            {
                Count_oracle_conn.Oracle_conn_ended();
            }
        }
        /// <summary>
        /// Get datasets from ORACLE - use this override when columns in query and in class T is same  
        /// </summary>
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name, CancellationToken cancellationToken)
        {
            Dictionary<string, int> D_columns = new Dictionary<string, int>();
            Dictionary<int, string> P_columns = new Dictionary<int, string>();
            Dictionary<int, Type> P_types = new Dictionary<int, Type>();
            T Row = new T();
            IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                     .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
            int counter = 0;
            foreach (var p in Accessors)
            {                
                P_types.Add(counter, p.PropertyInfo.PropertyType);
                P_columns.Add(counter, p.PropertyInfo.Name.ToLower());
                counter++;
            }
            return await Get_Ora(Sql_ora, Task_name, D_columns, P_columns, P_types, cancellationToken);
        }
        /// <summary>
        /// Get datasets from POSTEGRES - use this override when columns in query and in class T is diferent  
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <param name="D_columns"></param>
        /// <param name="P_columns"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_PSTGR(string Sql_ora, string Task_name, Dictionary<string, int> D_columns, Dictionary<int, string> P_columns, Dictionary<int, Type> P_types, CancellationToken cancellationToken)
        {
            List<T> Rows = new List<T>();
            try
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    using (NpgsqlConnection conO = new NpgsqlConnection(npC))
                    {
                        await conO.OpenAsync(cancellationToken);
                        bool list_columns = false;
                        T Row = new T();
                        IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                        .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
                        using (NpgsqlCommand cust = new NpgsqlCommand(Sql_ora, conO))
                        {
                            using (NpgsqlDataReader reader = cust.ExecuteReader())
                            {
                                while (await reader.ReadAsync(cancellationToken))
                                {
                                    if (!list_columns)
                                    {
                                        if (D_columns.Count == 0)
                                        {
                                            for (int col = 0; col < reader.FieldCount; col++)
                                            {
                                                D_columns.Add(reader.GetName(col).ToLower(), col);
                                            }
                                        }
                                        list_columns = true;
                                    }
                                    Row = new T();
                                    int counter = 0;
                                    foreach (var Accessor in Accessors)
                                    {
                                        string metod = P_columns[counter];
                                        if (D_columns.ContainsKey(metod))
                                        {
                                            int col = D_columns[metod];
                                            object readData = reader.GetValue(D_columns[metod]);
                                            if (readData != System.DBNull.Value)
                                            {
                                                Type pt = P_types[counter];
                                                Accessor.SetValue(Row, Convert.ChangeType(readData, Nullable.GetUnderlyingType(pt) ?? pt, null));
                                            }
                                        }
                                        counter++;
                                    }
                                    Rows.Add(Row);
                                }
                            }
                        }
                    }
                    Rows.Sort();
                }                
                return Rows;
            }
            catch (Exception e)
            {
                Loger.Log(String.Format("Error of modification Table: {0} => {1}", Task_name, e));
                Steps_executor.Step_error(Task_name);                
                return Rows;
            }
        }
        /// <summary>
        /// Get datasets from POSTEGRES - use this override when columns in query and in class T is same
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_PSTGR(string Sql_ora, string Task_name, CancellationToken cancellationToken )
        {
            Dictionary<string, int> D_columns = new Dictionary<string, int>();
            Dictionary<int, string> P_columns = new Dictionary<int, string>();
            Dictionary<int, Type> P_types = new Dictionary<int, Type>();
            T Row = new T();
            IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                     .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
            int counter = 0;
            foreach (var p in Accessors)
            {
                P_types.Add(counter, p.PropertyInfo.PropertyType);
                P_columns.Add(counter, p.PropertyInfo.Name.ToLower());
                counter++;
            }
            return await Get_PSTGR(Sql_ora, Task_name, D_columns, P_columns, P_types, cancellationToken);
        }
        /// <summary>
        /// Compare columns in rows by order assigned in ID array,
        /// Used to check the order in which table rows appear 
        /// </summary>
        /// <param name="new_row"></param>
        /// <param name="old_row"></param>
        /// <param name="ID"></param>
        /// <param name="Accessors"></param>
        /// <param name="P_types"></param>
        /// <returns></returns>
        public int Compare_rows(T new_row, T old_row, int[] ID, IPropertyAccessor[] Accessors, Dictionary<int, Type> P_types)
        {
            int result = 0;
            foreach (int item in ID)
            {
                Type pt = P_types[item];
                if (pt == typeof(string))
                {
                    result = String.Compare(
                        (string)Convert.ChangeType(
                            Accessors[item].GetValue(new_row),
                            Nullable.GetUnderlyingType(pt) ?? pt, null),
                        (string)Convert.ChangeType(
                            Accessors[item].GetValue(old_row),
                            Nullable.GetUnderlyingType(pt) ?? pt, null));
                }
                else if (pt == typeof(int) || pt == typeof(long)) 
                {
                    long new_item = Convert.ToInt64(Accessors[item].GetValue(new_row));
                    long old_item = Convert.ToInt64(Accessors[item].GetValue(old_row));
                    if (new_item > old_item)
                    {
                        result = 1;
                    }  
                    else if (new_item == old_item)
                    {
                        result = 0;
                    } 
                    else
                    {
                        result = -1;
                    }
                }
                if (result != 0)
                {
                    break;
                }
                
            }
            return result;
        }

        /// <summary>
        /// Find changes beetwen List's <typeparamref name="T"/>
        /// </summary>
        /// <param name="Old_list"></param>
        /// <param name="New_list"></param>
        /// <param name="ID_column"></param>
        /// <param name="not_compare"></param>
        /// <param name="guid_col"></param>
        /// <returns>  </returns>
        public Changes_List<T> Changes(List<T> Old_list, List<T> New_list, string[] ID_column, string[] not_compare, string[] guid_col, string Task_name, CancellationToken cancellationToken)
        {
            Changes_List<T> modyfications = new Changes_List<T>();
            try
            {
                List<T> _operDEl = new List<T>();
                List<T> _operINS = new List<T>();
                List<T> _operMOD = new List<T>();
                int[] ID = Enumerable.Range(0, ID_column.Length).ToArray();
                List<int> found = new List<int>();
                List<int> dont_check = new List<int>();
                int counter = 0;
                List<int> guid_id = new List<int>();
                Dictionary<int, Type> P_types = new Dictionary<int, Type>();
                T Row = new T();
                IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                   .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
                if (!cancellationToken.IsCancellationRequested)
                {
                    foreach (var p in Accessors)
                    {
                        string pt_name = p.PropertyInfo.Name.ToLower();
                        if (ID_column.Contains(pt_name))
                        {
                            ID[Array.IndexOf(ID_column, pt_name)] = counter;
                            found.Add(counter);
                        }
                        if (not_compare != null && not_compare.Contains(pt_name))
                        {
                            dont_check.Add(counter);
                        }
                        if (guid_col != null && guid_col.Contains(pt_name)) 
                        { 
                            guid_id.Add(counter); 
                        }
                        P_types.Add(counter, p.PropertyInfo.PropertyType);
                        counter++;
                    }
                    if (ID.Length > found.Count)
                    {
                        throw new Exception(String.Format("Task {1},Some Parameters of ID_column like field name:{0} have fields name width no existence in DataSet", ID_column, Task_name));
                    }                    
                    counter = 0;
                    int max_old_rows = Old_list.Count;
                    bool add_Record = false;
                    foreach (T rows in New_list)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            break;
                        }
                        if (max_old_rows > counter)
                        {
                            int compare = Compare_rows(rows, Old_list[counter], ID, Accessors, P_types);
                            while (compare == 1)
                            {
                                _operDEl.Add(Old_list[counter]);
                                counter++;
                                if (max_old_rows <= counter) { break; }
                                compare = Compare_rows(rows, Old_list[counter], ID, Accessors, P_types);                               
                            }
                            if (max_old_rows > counter)
                            {                               
                                if (compare == 0)
                                {
                                    bool changed = false;
                                    int col = 0;
                                    foreach (var rw in Accessors)
                                    {
                                        if (!ID.Contains(col) && !dont_check.Contains(col))
                                        {

                                            Type pt = P_types[col];
                                            var val1 = rw.GetValue(rows) == null ? null : Convert.ChangeType(rw.GetValue(rows), Nullable.GetUnderlyingType(pt) ?? pt, null);
                                            var val2 = rw.GetValue(Old_list[counter]) == null ? null : Convert.ChangeType(rw.GetValue(Old_list[counter]), Nullable.GetUnderlyingType(pt) ?? pt, null);
                                            if (val1 == null)
                                            {
                                                if (val2 != null)
                                                {
                                                    changed = true;
                                                }
                                            }
                                            else
                                            {
                                                if (val2 == null)
                                                {
                                                    if (val1 != null)
                                                    {
                                                        changed = true;
                                                    }
                                                }
                                                else
                                                {
                                                    if (!val1.Equals(val2))
                                                    {
                                                        changed = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        col++;
                                    }
                                    if (changed)
                                    {
                                        Row = new T();
                                        col = 0;
                                        foreach (var p in Accessors)
                                        {
                                            if (guid_id.Contains(col) && Accessors[col].GetValue(Old_list[counter]) != null)
                                            {
                                                p.SetValue(Row, Accessors[col].GetValue(Old_list[counter]));
                                            }
                                            else
                                            {
                                                p.SetValue(Row, p.GetValue(rows));
                                            }
                                            col++;
                                        }
                                        _operMOD.Add(Row);
                                    }
                                    counter++;
                                }
                                else
                                {
                                    add_Record = true;
                                }
                            }
                            else
                            {
                                add_Record = true;
                            }

                        }
                        else
                        {
                            add_Record = true;
                        }
                        if (add_Record)
                        {
                            _operINS.Add(rows);                           
                            add_Record = false;
                        }
                    }
                    var dataset = new Changes_List<T>
                    {
                        Insert = _operINS,
                        Delete = _operDEl,
                        Update = _operMOD
                    };
                    Loger.Log(string.Format("Found modfications in Task {0}: INSERT {1}, DELETE {2}, UPDATE {3}", Task_name, _operINS.Count(), _operDEl.Count(), _operMOD.Count()));
                    modyfications = dataset;
                    return modyfications;
                }
                return modyfications;
            }
            catch (Exception e)
            {
                Loger.Log(String.Format("Error in compare procedure for Task {1} : {0}", e, Task_name));
                Steps_executor.Step_error(Task_name);                
                return modyfications;
            }
        }
        /// <summary>
        /// Return schema of postegrsql table
        /// </summary>
        /// <param name="Table_name"></param>
        /// <returns></returns>
        public async Task<List<Npgsql_Schema_fields>> Get_shema(string Table_name, Dictionary<string, int> P_columns, CancellationToken cancellationToken)
        {
            List<Npgsql_Schema_fields> schema = new List<Npgsql_Schema_fields>();
            if (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    using (NpgsqlConnection conO = new NpgsqlConnection(npC))
                    {
                        await conO.OpenAsync(cancellationToken);
                        var Tmp = await conO.GetSchemaAsync("Columns", new string[] { null, null, Table_name }, cancellationToken);
                        foreach (DataRow row in Tmp.Rows)
                        {
                            //Loger.Log(string.Format("CHecking Schema for {2} => Field Name {1} => Field Type {0}  => type field exist in Postgres_helpers.PostegresTyp {3}", row["data_type"].ToString(), row["column_name"].ToString(), Table_name, Postgres_helpers.PostegresTyp.Keys.Contains(row["data_type"].ToString())));
                            if (!Postgres_helpers.PostegresTyp.Keys.Contains(row["data_type"].ToString()))
                            {
                                Loger.Log(string.Format("Error Please contact width developer  => Schema for {0} , Field Name {1} =>  Field Type dont exist in system dictionary Postgres_helpers.PostegresTyp  => {2}", Table_name, row["column_name"].ToString(), row["data_type"].ToString()));
                            }
                            Npgsql_Schema_fields rw = new Npgsql_Schema_fields
                            {
                                Field_name = row["column_name"].ToString(),
                                DB_Col_number = Convert.ToInt32(row["ordinal_position"]) - 1,
                                Field_type = Postgres_helpers.PostegresTyp.Keys.Contains(row["data_type"].ToString()) ? Postgres_helpers.PostegresTyp[row["data_type"].ToString()]: NpgsqlTypes.NpgsqlDbType.Varchar,
                                Dtst_col = P_columns.ContainsKey(row["column_name"].ToString().ToLower()) ? P_columns[row["column_name"].ToString().ToLower()] : 10000,
                                Char_max_len = row["character_maximum_length"].GetType() == typeof(int) ? (int)row["character_maximum_length"] : 0
                            };
                            schema.Add(rw);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Loger.Log(String.Format("Error in Get_shema : {0} =>  {1}", Table_name, ex));
                }
                
            }           
            return schema;
        }
        /// <summary>
        /// Set changes in database table
        /// </summary>
        /// <param name="_list"></param>
        /// <param name="name_table"></param>
        /// <param name="guid_col"></param>
        /// <returns></returns>
        public async Task<int> PSTRG_Changes_to_dataTable(Changes_List<T> _list, string name_table, string[] guid_col, string[] query_before, string[] query_after, string Task_name, CancellationToken cancellationToken)
        {
            bool dblogon = true;
            try
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    Dictionary<string, int> P_columns = new Dictionary<string, int>();
                    Dictionary<int, Type> P_types = new Dictionary<int, Type>();
                    T Row = new T();
                    IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                             .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
                    int counter = 0;
                    foreach (var p in Accessors)
                    {
                        P_types.Add(counter, p.PropertyInfo.PropertyType);
                        P_columns.Add(p.PropertyInfo.Name.ToLower(), counter);
                        counter++;
                    }
                    List<Npgsql_Schema_fields> Schema = await Get_shema(name_table, P_columns, cancellationToken);
                    using (NpgsqlConnection conO = new NpgsqlConnection(npC))
                    {
                        await conO.OpenAsync(cancellationToken);
                        if (dblogon)
                        {
                            using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                @"UPDATE public.datatbles 
                                   SET start_update=current_timestamp, in_progress=true,updt_errors=false 
                                   WHERE table_name=@table_name", conO))
                            {
                                cmd.Parameters.Add("table_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = name_table;
                                await cmd.PrepareAsync(cancellationToken);
                                await cmd.ExecuteNonQueryAsync(cancellationToken);
                            }
                            if (query_before != null)
                            {
                                foreach (string comm in query_before)
                                {
                                    using (NpgsqlCommand cmd = new NpgsqlCommand(comm, conO))
                                    {
                                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                                    }
                                }
                            }
                        }
                        using (NpgsqlTransaction npgsqlTransaction = conO.BeginTransaction())
                        {                                                       
                            if (_list.Delete.Count > 0)
                            {
                                string comand = "DELETE FROM " + name_table;
                                string tbl_values = " WHERE ";
                                string param_values = "=";
                                List<string> guid_col_enum = new List<string>();
                                using (NpgsqlCommand cmd = new NpgsqlCommand())
                                {
                                    cmd.Connection = conO;
                                    foreach (Npgsql_Schema_fields _Fields in Schema)
                                    {
                                        string nam = _Fields.Field_name.ToLower();
                                        if (guid_col.Contains(nam) && _Fields.Dtst_col != 10000)
                                        {
                                            tbl_values += tbl_values != " WHERE " ? String.Format(" AND {0}", nam) : nam;
                                            param_values = "=@" + nam.ToLower();
                                            cmd.Parameters.Add("@" + nam.ToLower(), _Fields.Field_type);
                                            tbl_values += param_values;
                                            guid_col_enum.Add(nam.ToLower());
                                        }
                                    }
                                    cmd.CommandText = comand + tbl_values;
                                    await cmd.PrepareAsync(cancellationToken);
                                    foreach (T row in _list.Delete)
                                    {
                                        if (cancellationToken.IsCancellationRequested)
                                        {
                                            break;
                                        }
                                        int cnt = 0;
                                        foreach (string key in guid_col_enum)
                                        {
                                            cmd.Parameters[cnt].Value = Accessors[P_columns[key]].GetValue(row);
                                            cnt += 1;
                                        }

                                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                                    }
                                }
                            }
                            if (_list.Update.Count > 0)
                            {
                                string comand = "UPDATE " + name_table + " SET";
                                string tbl_values = " ";
                                string param_values = " WHERE ";
                                using (NpgsqlCommand cmd = new NpgsqlCommand())
                                {
                                    cmd.Connection = conO;
                                    foreach (Npgsql_Schema_fields _Fields in Schema)
                                    {
                                        string nam = _Fields.Field_name;
                                        if (_Fields.Dtst_col != 10000)
                                        {
                                            if (guid_col.Contains(nam))
                                            {
                                                param_values += (param_values == " WHERE " ? "" : " AND ") + nam + "=@" + nam;
                                            }
                                            else
                                            {
                                                tbl_values = tbl_values + nam + "=@" + nam.ToLower() + ",";
                                            }
                                            cmd.Parameters.Add("@" + nam.ToLower(), _Fields.Field_type);
                                        }
                                    }
                                    cmd.CommandText = comand + tbl_values.Substring(0, tbl_values.Length - 1) + " " + param_values;
                                    await cmd.PrepareAsync(cancellationToken);
                                    foreach (T row in _list.Update)
                                    {
                                        
                                        if (cancellationToken.IsCancellationRequested)
                                        {
                                            break;
                                        }
                                        //string param = "";
                                        foreach (Npgsql_Schema_fields _field in Schema)
                                        {                                            
                                            if (_field.Dtst_col != 10000)
                                            {
                                                //param += "_" + Convert.ToString(Accessors[_field.Dtst_col].GetValue(row));
                                                cmd.Parameters[_field.DB_Col_number].Value = Accessors[_field.Dtst_col].GetValue(row) ?? DBNull.Value;
                                            }
                                        }
                                        // Loger.Log(param);
                                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                                    }
                                }
                            }
                            if (_list.Insert.Count > 0)
                            {
                                string comand = "INSERT INTO " + name_table;
                                string tbl_values = "(";
                                string param_values = " VALUES (";
                                using (NpgsqlCommand cmd = new NpgsqlCommand())
                                {
                                    cmd.Connection = conO;
                                    foreach (Npgsql_Schema_fields _Fields in Schema)
                                    {
                                        string nam = _Fields.Field_name;
                                        if (_Fields.Dtst_col != 10000)
                                        {
                                            tbl_values = tbl_values + nam + ",";
                                            param_values = param_values + "@" + nam.ToLower() + ",";
                                            cmd.Parameters.Add("@" + nam.ToLower(), _Fields.Field_type);
                                        }
                                    }
                                    cmd.CommandText = comand + tbl_values.Substring(0, tbl_values.Length - 1) + ")" + param_values.Substring(0, param_values.Length - 1) + ")";
                                    await cmd.PrepareAsync(cancellationToken);
                                    foreach (T row in _list.Insert)
                                    {
                                        if (cancellationToken.IsCancellationRequested)
                                        {
                                            break;
                                        }
                                        foreach (Npgsql_Schema_fields _field in Schema)
                                        {
                                            if (_field.Dtst_col != 10000)
                                            {
                                                if (_field.Field_type == NpgsqlTypes.NpgsqlDbType.Uuid & guid_col.Contains(_field.Field_name))
                                                {
                                                    cmd.Parameters[_field.DB_Col_number].Value = Guid.NewGuid();
                                                }
                                                else
                                                {
                                                    cmd.Parameters[_field.DB_Col_number].Value = Accessors[_field.Dtst_col].GetValue(row) ?? DBNull.Value;
                                                }
                                            }
                                        }
                                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                                    }
                                }
                            }
                            if (query_after != null)
                            {
                                foreach (string comm in query_after)
                                {
                                    if (cancellationToken.IsCancellationRequested)
                                    {
                                        break;
                                    }
                                    using (NpgsqlCommand cmd = new NpgsqlCommand(comm, conO))
                                    {
                                        await cmd.ExecuteNonQueryAsync(cancellationToken);
                                    }
                                }
                            }
                            if (dblogon)
                            {
                                using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                   @"UPDATE public.datatbles
                                                    SET last_modify=current_timestamp, in_progress=false 
                                                    WHERE table_name=@table_name", conO))
                                {
                                    cmd.Parameters.Add("table_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = name_table;
                                    await cmd.PrepareAsync(cancellationToken);
                                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                                }
                            }                               
                            if (cancellationToken.IsCancellationRequested)
                            {
                                npgsqlTransaction.Rollback();
                            }
                            else 
                            {                                 
                                npgsqlTransaction.Commit();                               
                            }                            
                        }
                    }
                    return 0;
                }
                return 1;
            }
            catch (Exception e)
            {
                using (NpgsqlConnection conO = new NpgsqlConnection(npC))
                {
                    await conO.OpenAsync();
                    using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                            @"UPDATE public.datatbles 
                                                SET in_progress=false,updt_errors=true
                                                WHERE table_name=@table_name", conO))
                    {
                        cmd.Parameters.Add("table_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = name_table;
                        await cmd.PrepareAsync();
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
                
                Loger.Log(String.Format("Error in update table : {0} =>  {1}",name_table , e));
                Steps_executor.Step_error(Task_name);
                return 1;
            }
        }
    }
    public class ORA_prepared_parameters
    {
        public List<ORA_Schema_fields> Param_types { get; set; }
        public List<Tuple<string>> Param_values { get; set; }        
    }
    public class ORA_parameters
    {
        public ORA_Schema_fields Param_types { get; set; }
        public List<object> Param_values { get; set; }
    }
    public class PSTGR_parameters
    {
        public Npgsql_Schema_fields Param_types { get; set; }
        public List<object> Param_values { get; set; }
    }
    public class Npgsql_Schema_fields
    {
        public string Field_name { get; set; }
        public int DB_Col_number { get; set; }
        public NpgsqlTypes.NpgsqlDbType Field_type { get; set; }
        public int Dtst_col { get; set; }
        public int Char_max_len {get; set;} 
    }
    public class ORA_Schema_fields
    {
        public string Field_name { get; set; }
        public int DB_Col_number { get; set; }
        public OracleDbType Field_type { get; set; }
        public int Dtst_col { get; set; }
    }
    public class Changes_List<T> where T : class, new()
    {
        public List<T> Insert;
        public List<T> Update;
        public List<T> Delete;
    }
    public interface IPropertyAccessor
    {
        PropertyInfo PropertyInfo { get; }
        object GetValue(object source);
        void SetValue(object source, object value);
    }
    public static class PropertyInfoHelper
    {
        private static readonly ConcurrentDictionary<PropertyInfo, IPropertyAccessor> _cache =
            new ConcurrentDictionary<PropertyInfo, IPropertyAccessor>();

        public static IPropertyAccessor GetAccessor(PropertyInfo propertyInfo)
        {
            if (!_cache.TryGetValue(propertyInfo, out IPropertyAccessor result))
            {
                result = CreateAccessor(propertyInfo);
                _cache.TryAdd(propertyInfo, result); ;
            }
            return result;
        }

        public static IPropertyAccessor CreateAccessor(PropertyInfo PropertyInfo)
        {
            var GenType = typeof(PropertyWrapper<,>)
                .MakeGenericType(PropertyInfo.DeclaringType, PropertyInfo.PropertyType);
            return (IPropertyAccessor)Activator.CreateInstance(GenType, PropertyInfo);
        }
    }
    internal class PropertyWrapper<TObject, TValue> : IPropertyAccessor where TObject : class
    {
        private readonly Func<TObject, TValue> Getter;
        private readonly Action<TObject, TValue> Setter;

        public PropertyWrapper(PropertyInfo PropertyInfo)
        {
            this.PropertyInfo = PropertyInfo;

            MethodInfo GetterInfo = PropertyInfo.GetGetMethod(true);
            MethodInfo SetterInfo = PropertyInfo.GetSetMethod(true);

            Getter = (Func<TObject, TValue>)Delegate.CreateDelegate
                    (typeof(Func<TObject, TValue>), GetterInfo);
            Setter = (Action<TObject, TValue>)Delegate.CreateDelegate
                    (typeof(Action<TObject, TValue>), SetterInfo);
        }

        object IPropertyAccessor.GetValue(object source)
        {
            return Getter(source as TObject);
        }

        void IPropertyAccessor.SetValue(object source, object value)
        {
            Setter(source as TObject, (TValue)value);
        }

        public PropertyInfo PropertyInfo { get; private set; }
    }
    /// <summary>
    /// Class for report/notify steps of calculations in database
    /// </summary>
    public class Report
    {

    }
}
