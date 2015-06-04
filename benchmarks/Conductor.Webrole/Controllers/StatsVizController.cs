using Azure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Conductor.Webrole.Controllers
{
    public class StatsVizController : Controller
    {
        // GET: StatsViz
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult VizBenchmark(string benchmark = "hello")
        {
            var tableClient = Common.AzureUtils.getTableClient("DataConnectionString");
            var entity = Common.AzureUtils.findEntitiesInPartition<StatEntity>(tableClient, "results", benchmark);
            
            return View(entity);
        }
    }
}