﻿using System;
using System.Collections.Generic;
using Oracle.ManagedDataAccess.Client;
using Npgsql;
using System.Linq;
using System.Threading.Tasks;
using System.Reflection;
using System.Collections.Concurrent;
using System.Data;


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
        }
        public Update_pstgr_from_Ora()
        {
            npC = Postegresql_conn.Connection_pool["MAIN"].ToString();
        }
        
        static readonly string Str_oracle_conn = Oracle_conn.Connection_string;
        readonly string npC;
        /// <summary>
        /// Get datasets from ORACLE - use this override when columns in query and in class T is same and create prepared parameters 
        /// </summary>
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name, ORA_parameters parameters)
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
            return await Get_Ora(Sql_ora, Task_name, D_columns, P_columns, P_types, parameters);
        }
        /// <summary>
        /// Get datasets from ORACLE - use this override when columns in query and in class T is diferent and use prepared parameters 
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <param name="D_columns"></param>
        /// <param name="P_columns"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name, Dictionary<string, int> D_columns, Dictionary<int, string> P_columns, Dictionary<int, Type> P_types, ORA_parameters parameters)
        {
            List<T> Rows = new List<T>();
            try
            {
                Count_oracle_conn.Wait_for_Oracle();
                using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                {
                    await conO.OpenAsync();
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
                            while (await reader.ReadAsync())
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
                return Rows;
            }
            catch (Exception e)
            {
                Loger.Log("Błąd modyfikacji tabeli:" + Task_name + e);
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
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name, Dictionary<string, int> D_columns, Dictionary<int, string> P_columns, Dictionary<int, Type> P_types)
        {
            List<T> Rows = new List<T>();
            try
            {
                Count_oracle_conn.Wait_for_Oracle();
                using (OracleConnection conO = new OracleConnection(Str_oracle_conn))
                {
                    await conO.OpenAsync();
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
                            while (await reader.ReadAsync())
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
                return Rows;
            }
            catch (Exception e)
            {
                Loger.Log("Błąd modyfikacji tabeli:" + Task_name + e);
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
        public async Task<List<T>> Get_Ora(string Sql_ora, string Task_name)
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
            return await Get_Ora(Sql_ora, Task_name, D_columns, P_columns, P_types);
        }
        /// <summary>
        /// Get datasets from POSTEGRES - use this override when columns in query and in class T is diferent  
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <param name="D_columns"></param>
        /// <param name="P_columns"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_PSTGR(string Sql_ora, string Task_name, Dictionary<string, int> D_columns, Dictionary<int, string> P_columns, Dictionary<int, Type> P_types)
        {
            List<T> Rows = new List<T>();
            try
            {
                using (NpgsqlConnection conO = new NpgsqlConnection(npC))
                {
                    await conO.OpenAsync();
                    bool list_columns = false;
                    T Row = new T();
                    IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                    .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();
                    using (NpgsqlCommand cust = new NpgsqlCommand(Sql_ora, conO))
                    {
                        using (NpgsqlDataReader reader = cust.ExecuteReader())
                        {
                            while (await reader.ReadAsync())
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
                return Rows;
            }
            catch (Exception e)
            {
                Loger.Log(String.Format("Error of modification Table: {0} => {1}",Task_name, e));
                return Rows;
            }
        }
        /// <summary>
        /// Get datasets from POSTEGRES - use this override when columns in query and in class T is same
        /// </summary>
        /// <param name="Sql_ora"></param>
        /// <param name="Task_name"></param>
        /// <returns></returns>
        public async Task<List<T>> Get_PSTGR(string Sql_ora, string Task_name)
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
            return await Get_PSTGR(Sql_ora, Task_name, D_columns, P_columns, P_types);
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
        public Changes_List<T> Changes(List<T> Old_list, List<T> New_list, string[] ID_column, string[] not_compare, string guid_col)
        {
            Changes_List<T> modyfications = new Changes_List<T>();
            try
            {
                List<T> _operDEl = new List<T>();
                List<T> _operINS = new List<T>();
                List<T> _operMOD = new List<T>();
                int[] ID = Enumerable.Range(0, ID_column.Length).ToArray(); 
                int[] found = new int[0]; 
                int[] dont_check = new int[0];
                int counter = 0;
                int guid_id = 10000;
                Dictionary<int, Type> P_types = new Dictionary<int, Type>();
                T Row = new T();
                IPropertyAccessor[] Accessors = Row.GetType().GetProperties()
                                   .Select(pi => PropertyInfoHelper.CreateAccessor(pi)).ToArray();

                foreach (var p in Accessors)
                {
                    string pt_name = p.PropertyInfo.Name.ToLower();
                    if (ID_column.Contains(pt_name)) { 
                        ID[Array.IndexOf(ID_column, pt_name)] = counter;
                        found.Append(counter);
                    }  
                    if (not_compare.Contains(pt_name))
                    {
                        dont_check.Append(counter);
                    }
                    if (pt_name == guid_col.ToLower()) { guid_id = counter; }
                    P_types.Add(counter, p.PropertyInfo.PropertyType);
                    counter++;
                }
                if (ID.Length > found.Length)
                {
                    throw new Exception(String.Format("Some Parameters of ID_column {0} have fields name width no existence in DataSet", ID_column));
                }
                counter = 0;
                int max_old_rows = Old_list.Count;
                bool add_Record = false;
                foreach (T rows in New_list)
                {
                    if (max_old_rows > counter)
                    {
                        while (Compare_rows(rows, Old_list[counter], ID, Accessors, P_types) == 1)
                        {
                            _operDEl.Add(Old_list[counter]);
                            counter++;
                            if (max_old_rows <= counter) { break; }
                        }
                        if (max_old_rows > counter)
                        {
                            if (Compare_rows(rows, Old_list[counter], ID, Accessors, P_types) == 0)
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
                                        if (guid_id == col)
                                        {
                                            p.SetValue(Row, Accessors[guid_id].GetValue(Old_list[counter]));
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
                        counter++;
                        add_Record = false;
                    }
                }
                var dataset = new Changes_List<T>
                {
                    Insert = _operINS,
                    Delete = _operDEl,
                    Update = _operMOD
                };
                modyfications = dataset;
                return modyfications;

            }
            catch (Exception e)
            {
                Loger.Log(String.Format("Error in compare procedure :  {0}", e));
                return modyfications;
            }
        }
        /// <summary>
        /// Return schema of postegrsql table
        /// </summary>
        /// <param name="Table_name"></param>
        /// <returns></returns>
        public async Task<List<Npgsql_Schema_fields>> Get_shema(string Table_name, Dictionary<string, int> P_columns)
        {
            List<Npgsql_Schema_fields> schema = new List<Npgsql_Schema_fields>();
            using (NpgsqlConnection conO = new NpgsqlConnection(npC))
            {
                await conO.OpenAsync();
                var Tmp = conO.GetSchema("Columns", new string[] { null, null, Table_name });
                foreach (DataRow row in Tmp.Rows)
                {
                    Npgsql_Schema_fields rw = new Npgsql_Schema_fields
                    {
                        Field_name = row["column_name"].ToString(),
                        DB_Col_number = Convert.ToInt32(row["ordinal_position"]) - 1,
                        Field_type = Postgres_helpers.PostegresTyp[row["data_type"].ToString()],
                        Dtst_col = P_columns.ContainsKey(row["column_name"].ToString().ToLower()) ? P_columns[row["column_name"].ToString().ToLower()] : 10000,
                        char_max_len = (int)row["character_maximum_length"]                      
                };
                    schema.Add(rw);
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
        public async Task<int> PSTRG_Changes_to_dataTable(Changes_List<T> _list, string name_table, string guid_col, string[] query_before, string[] query_after)
        {

            try
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
                List<Npgsql_Schema_fields> Schema = await Get_shema(name_table, P_columns);
                using (NpgsqlConnection conO = new NpgsqlConnection(npC))
                {
                    await conO.OpenAsync();
                    using (NpgsqlTransaction npgsqlTransaction = conO.BeginTransaction())
                    {
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                        @"UPDATE public.datatbles 
                           SET start_update=current_timestamp, in_progress=true,updt_errors=false 
                           WHERE table_name=@table_name", conO))
                        {
                            cmd.Parameters.Add("table_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = name_table;
                            await cmd.PrepareAsync();
                            await cmd.ExecuteNonQueryAsync();
                        }
                        if (query_before != null)
                        {
                            foreach (string comm in query_before)
                            {
                                using (NpgsqlCommand cmd = new NpgsqlCommand(comm, conO))
                                {
                                    await cmd.ExecuteNonQueryAsync();
                                }
                            }
                        }
                        if (_list.Delete.Count > 0)
                        {
                            string comand = "DELETE FROM " + name_table;
                            string tbl_values = " WHERE ";
                            string param_values = "=";
                            using (NpgsqlCommand cmd = new NpgsqlCommand())
                            {
                                cmd.Connection = conO;
                                foreach (Npgsql_Schema_fields _Fields in Schema)
                                {
                                    string nam = _Fields.Field_name;
                                    if (guid_col == nam && _Fields.Dtst_col != 10000)
                                    {
                                        tbl_values += nam;
                                        param_values = "=@" + nam.ToLower();
                                        cmd.Parameters.Add("@" + nam.ToLower(), _Fields.Field_type);
                                    }
                                }
                                cmd.CommandText = comand + tbl_values + param_values;
                                await cmd.PrepareAsync();
                                foreach (T row in _list.Delete)
                                {
                                    cmd.Parameters[0].Value = Accessors[P_columns[guid_col]].GetValue(row);
                                    await cmd.ExecuteNonQueryAsync();
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
                                        if (nam.ToLower() == guid_col.ToLower())
                                        {
                                            param_values = param_values + nam + "=@" + nam.ToLower();
                                        }
                                        else
                                        {
                                            tbl_values = tbl_values + nam + "=@" + nam.ToLower() + ",";
                                        }
                                        cmd.Parameters.Add("@" + nam.ToLower(), _Fields.Field_type);
                                    }
                                }
                                cmd.CommandText = comand + tbl_values.Substring(0, tbl_values.Length - 1) + " " + param_values;
                                await cmd.PrepareAsync();
                                foreach (T row in _list.Update)
                                {
                                    foreach (Npgsql_Schema_fields _field in Schema)
                                    {
                                        if (_field.Dtst_col != 10000)
                                        {
                                            cmd.Parameters[_field.DB_Col_number].Value = Accessors[_field.Dtst_col].GetValue(row) ?? DBNull.Value;
                                        }
                                    }
                                    await cmd.ExecuteNonQueryAsync();
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
                                await cmd.PrepareAsync();
                                foreach (T row in _list.Insert)
                                {
                                    foreach (Npgsql_Schema_fields _field in Schema)
                                    {
                                        if (_field.Field_name.ToLower() == guid_col.ToLower())
                                        {
                                            if (_field.Field_type == NpgsqlTypes.NpgsqlDbType.Uuid)
                                            {
                                                cmd.Parameters[_field.DB_Col_number].Value = Guid.NewGuid();
                                            }
                                            else
                                            {
                                                cmd.Parameters[_field.DB_Col_number].Value = Accessors[_field.Dtst_col].GetValue(row) ?? DBNull.Value;
                                            }
                                        }
                                        else if (_field.Dtst_col != 10000)
                                        {
                                            cmd.Parameters[_field.DB_Col_number].Value = Accessors[_field.Dtst_col].GetValue(row) ?? DBNull.Value;
                                        }
                                    }
                                    await cmd.ExecuteNonQueryAsync();
                                }
                            }
                        }
                        if (query_after != null)
                        {
                            foreach (string comm in query_after)
                            {
                                using (NpgsqlCommand cmd = new NpgsqlCommand(comm, conO))
                                {
                                    await cmd.ExecuteNonQueryAsync();
                                }
                            }
                        }
                        using (NpgsqlCommand cmd = new NpgsqlCommand("" +
                                                @"UPDATE public.datatbles
                                                    SET last_modify=current_timestamp, in_progress=false 
                                                    WHERE table_name=@table_name", conO))
                        {
                            cmd.Parameters.Add("table_name", NpgsqlTypes.NpgsqlDbType.Varchar).Value = name_table;
                            await cmd.PrepareAsync();
                            await cmd.ExecuteNonQueryAsync();
                        }
                        npgsqlTransaction.Commit();
                    }
                }
                return 0;
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
                Loger.Log(String.Format("Error in update table : {0} =>  {1}",name_table , e.StackTrace));
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
        public int char_max_len {get; set;} 
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
        private static ConcurrentDictionary<PropertyInfo, IPropertyAccessor> _cache =
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
