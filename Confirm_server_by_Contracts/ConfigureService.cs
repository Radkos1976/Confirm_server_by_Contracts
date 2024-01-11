using System.Diagnostics;
using System;
using System.IO;
using Topshelf;


namespace Purch_Confirm_server
{
    internal static class ConfigureService
    {
        internal static void Configure()
        {
            string log(string txt)
            {
                return String.Format("{0} => {1}", DateTime.Now, txt);
            }
            HostFactory.Run(configure =>
            {
                configure.Service<Conf_serv>(service =>
                {
                    service.ConstructUsing(s => new Conf_serv());
                    service.WhenStarted(s => s.Start());
                    service.WhenStopped(s => s.Stop());                    
                });
                //Setup Account that window service use to run.  
                configure.RunAsLocalSystem();
                configure.OnException(ex => {
                    using (StreamWriter outputFile = new StreamWriter("C:\\serv\\serv_errors.txt", true))
                    {
                        outputFile.WriteLine("Found Errors");
                        outputFile.WriteLine(log(ex.Message));
                        outputFile.WriteLine(log(ex.StackTrace));
                        outputFile.WriteLine(log(ex.HelpLink));
                    }                      
                                               
                    
                });
                configure.EnableServiceRecovery(r => { r.RestartService(1); });
                configure.SetServiceName("Confirm_serv");
                configure.SetDisplayName("Confirm_serv");
                configure.SetDescription("Braki materiałowe i potwierdzenia dla klientów");
            });
        }       
        
    }
    
}
