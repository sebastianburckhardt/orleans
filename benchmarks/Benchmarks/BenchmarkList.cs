using Common;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmarks
{
    public class BenchmarkList : IRequestDispatcher
    {

        public BenchmarkList()
        {
            //---------------------------------------------------------------------------------
            // To add a new benchmark:
            // - add it to this list
            // - add a project reference for the interface dll and the implementation dll to this project

            Register(new Hello.Benchmark.Benchmark());
            Register(new Leaderboard.Benchmark.Benchmark());
            Register(new Size.Benchmark.Benchmark());

            Register(new Computation.Benchmark.Benchmark());
            Register(new Azure.Storage.Benchmark());

            //Register(new Computation.Benchmark.Benchmark());

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


        public IRequest ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body = null)
        {
            var benchmark = benchmarks[urlpath.ElementAt(0)];
            return benchmark.ParseRequest(verb, urlpath, arguments, body);

        }
    }
}
