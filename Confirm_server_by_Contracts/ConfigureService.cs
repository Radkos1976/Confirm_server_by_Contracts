using Topshelf;


namespace Purch_Confirm_server
{
    internal static class ConfigureService
    {
        internal static void Configure()
        {
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
                configure.SetServiceName("Confirm_serv");
                configure.SetDisplayName("Confirm_serv");
                configure.SetDescription("Braki materiałowe i potwierdzenia dla klientów");
            });
        }
    }
}
