using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(Conductor.Webrole.Startup))]
 namespace Conductor.Webrole

{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
            app.MapSignalR();

        }
    }
}
