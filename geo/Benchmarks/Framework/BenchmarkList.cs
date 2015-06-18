using GeoOrleans.Runtime.Common;
using GeoOrleans.Benchmarks.Common;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeoOrleans.Benchmarks
{
    public class BenchmarkList : IRequestDispatcher
    {

        public BenchmarkList()
        {
            //---------------------------------------------------------------------------------
            // To add a new benchmark:
            // - add it to this list
            // - add a reference for the benchmark dll to this project
            // - add a reference for the grain dll to the LocalDeployment project
            // - add a reference for both interface and grain dll to the Orleans.Silos project

            Register(new Hello.Benchmark.Benchmark());
            Register(new Leaderboard.Benchmark.Benchmark());
            Register(new Size.Benchmark.Benchmark());

            Register(new Computation.Benchmark.Benchmark());
            Register(new Azure.Benchmark());
            Register(new LeaderboardPileus.Benchmark.Benchmark());

            Register(new Computation.Benchmark.Benchmark());

            //----------------------------------------------------------------------------------
        }

        private Dictionary<string, IBenchmark> benchmarks = new Dictionary<string, IBenchmark>();

        public IEnumerable<IBenchmark> Benchmarks { get { return benchmarks.Values;  } }

        public IBenchmark ByName(string name) {
            IBenchmark bm = null;
            benchmarks.TryGetValue(name, out bm);
            return bm;
        }

        private void Register(IBenchmark benchmark)
        {
            var name = benchmark.Name;

            if (benchmarks.ContainsKey(name))
                throw new ArgumentException("duplicate benchmark name");

             for (int i = 0; i < name.Length; i++)
                if (!  (char.IsLower(name, i) || (char.IsDigit(name, i))))
                    throw new ArgumentException("benchmark name must contain lowercase letters and digits only");

            benchmarks.Add(benchmark.Name, benchmark);
        }


        public IRequest     ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body = null)
        {
            var benchmark = benchmarks[urlpath.ElementAt(0)];
            return benchmark.ParseRequest(verb, urlpath, arguments, body);

        }
    }
}
