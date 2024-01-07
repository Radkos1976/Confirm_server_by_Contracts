using DB_Conect;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Confirm_server_by_Contracts
{
    internal class Program
    {

        static  void Main(string[] args)
        {
            Serv_instance serv_Instance = new Serv_instance();
            serv_Instance.Start_calc();
        }
    }
}
