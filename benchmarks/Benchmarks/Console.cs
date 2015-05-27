using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmarks
{
    public class Console 
    {
        BenchmarkList benchmarks = new BenchmarkList();

        private void PrintUsage(Action<string> wl)
        {
            wl("Usage:");
            wl("'list'                    to list all benchmarks");
            wl("'list <bmark>'            to list all scenarios for <bmark>");
            wl("'run <bmark>.<scenario>'  to run a scenario");
            wl("'quit'  to exit");
            wl("");
        }

        Action<string> wl;
        Func<string> rl;
 

        public Console(Action<string> wl, Func<string> rl)
        {
            this.wl = wl;
            this.rl = rl;
        }

        public void Welcome()
        {
            wl("Welcome to the Benchmark Console.");
            PrintUsage(wl);
            wl("");
        }

        public KeyValuePair<IBenchmark, IScenario>?  SelectScenario()
        {
            while (true)
            {
                var command = rl();

                if (command == "quit" || command == "exit")
                    return null;

                else if (command == "list") {
                   foreach (var bm in benchmarks.Benchmarks)
                      wl(bm.Name);
                   wl("");
                }

                else
                {
                    var pos = command.LastIndexOf(" ");
                    if (pos == -1)
                        PrintUsage(wl);
                    else
                    {
                        var dotpos = command.LastIndexOf(".");
                        var bmname = (dotpos == -1 ? command.Substring(pos + 1) : command.Substring(pos + 1, dotpos - pos - 1));
                        var bm = benchmarks.ByName(bmname);
                        if (bm == null)
                            PrintUsage(wl);
                        else
                        {
                            if (command.StartsWith("list"))
                            {
                                foreach (var s in bm.Scenarios)
                                    wl(s.Name);
                                wl("");
                            }
                            else if (command.StartsWith("run"))
                            {
                                var scenarioname = command.Substring(dotpos + 1);
                                var sc = bm.Scenarios.FirstOrDefault(s => s.Name == scenarioname);
                                if (sc == null)
                                    PrintUsage(wl);
                                else
                                {
                                    // run the scenario
                                    return new KeyValuePair<IBenchmark, IScenario>(bm, sc);
                                }
                            }
                            else
                                PrintUsage(wl);
                        }
                    }
                }
            }
        }
    }
}
