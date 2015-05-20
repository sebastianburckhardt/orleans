using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class SiloStatRule : StatRule
    {
        public SiloStatRule()
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

            if (Statistic == "Zscore")
            {
                EvaluateZscore(counterData, counterName);
                return;
            }

            foreach (var pair in counterData.SiloData)
            {
                List<double> data = new List<double>();
                try
                {
                    foreach (var counterVal in pair.Value.SiloStatistics[counterName].TimeVals)
                    {
                        data.Add(counterVal.Value);
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
                        ViolatedInfo.Add(new Tuple<string, DateTime, double>(pair.Key, DateTime.Now, val));
                    }
                }
                catch (KeyNotFoundException)
                {

                }
            }
        }


        public void EvaluateZscore(PerfCounterData counterData, string counterName)
        {
            foreach (var pair in counterData.SiloData)
            {
                List<double> data = new List<double>();
                try
                {
                    foreach (var counterVal in pair.Value.SiloStatistics[counterName].TimeVals)
                    {
                        data.Add(counterVal.Value);
                    }

                    List<double> zcores = NumericalAnalyzer.Zscore(data);

                    int count = zcores.Count();
                    for (int i = 0; i < count; i++)
                    {
                        if (!op.operate(zcores[i], ExpectedVal))
                        {
                            ViolatedInfo.Add(new Tuple<string, DateTime, double>(pair.Key, pair.Value.TracePoints[i], zcores[i]));
                        }
                    }
                }
                catch (KeyNotFoundException)
                {

                }
            }
         }
    }
}
