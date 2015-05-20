using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class TimeStatRule : StatRule
    {
        public TimeStatRule()
        {
            ViolatedInfo = new List<Tuple<string, DateTime, double>>();    
        }

        public override void Evaluate(PerfCounterData counterData, string counterName)
        {
            if (Statistic == "Any")
            {
                EvaluateAny(counterData, counterName);
                return;
            }
            else if (Statistic == "Zscore")
            {
                EvaluateZscore(counterData, counterName);
                return;
            }

            foreach (var pair in counterData.CounterTimeData[counterName].SiloVals)
            {
                List<double> data = new List<double>();
                foreach (var t in pair.Value)
                {
                    data.Add(t.Item2);
                }
                
                double val = GetStatValue(data);

                if (IsPercentageRule)
                {
                    foreach (var d in data)
                    {
                        if (!op.operateforpercentage(d, ExpectedVal, val))
                        {
                            string s = "Failing at ";
                            s = s + pair.Key;
                            ViolatedInfo.Add(new Tuple<string, DateTime, double>(s, DateTime.Now, d));   
                        }
                    }

                    return;
                }

                if (!op.operate(val, ExpectedVal))
                {
                    string s = "Failing at ";
                    s = s + pair.Key;
                    ViolatedInfo.Add(new Tuple<string, DateTime, double>(s , DateTime.Now, val));
                }
            }
        }

        public void EvaluateZscore(PerfCounterData counterData, string counterName)
        {
            foreach (var pair in counterData.CounterTimeData[counterName].SiloVals)
            {
                List<double> data = new List<double>();
                foreach (var t in pair.Value)
                {
                    data.Add(t.Item2);
                }

                List<double> zcores = NumericalAnalyzer.Zscore(data);

                int count = zcores.Count();
                for(int i=0; i < count; i++)
                {
                    if (!op.operate(zcores[i], ExpectedVal))
                    {
                        string siloName = pair.Value[i].Item1;
                        DateTime dt = counterData.SiloData[siloName].TracePoints.ElementAt(pair.Key);
                        ViolatedInfo.Add(new Tuple<string, DateTime, double>(siloName, dt, zcores[i]));
                    }       
                }
            }         
        }
    }
}
