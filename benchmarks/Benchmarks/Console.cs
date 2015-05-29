using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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

        public KeyValuePair<IBenchmark, IEnumerable<IScenario>>? SelectScenario()
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

                                if (scenarioname == null || scenarioname.Length == 0)
                                {
                                    PrintUsage(wl);
                                }
                                else
                                {                                    
                                    //support for basic globbing pattern matching (e.g. A* will select all scenarios beginning with A)
                                    //supports * and ? operators
                                    string pattern = string.Format("^{0}$", Regex.Escape(scenarioname).Replace(@"\*", ".*").Replace(@"\?", "."));
                                    Regex scenarioRegex = new Regex(scenarioname);
                                    var scenarios = bm.Scenarios.Where((s) => scenarioRegex.IsMatch(s.Name));
                                    
                                    if (scenarios == null || !scenarios.Any())
                                    {
                                        PrintUsage(wl);
                                    }
                                    else
                                    {
                                        return new KeyValuePair<IBenchmark, IEnumerable<IScenario>>(bm, scenarios);
                                    }
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
