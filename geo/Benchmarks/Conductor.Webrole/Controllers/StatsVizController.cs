using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using GeoOrleans.Runtime.Common;
using GeoOrleans.Benchmarks.Common;

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
            var tableClient = AzureUtils.getTableClient("DataConnectionString");
            var entity = AzureUtils.findEntitiesInPartition<StatEntity>(tableClient, "results", benchmark);
            
            return View(entity);
        }
    }
}