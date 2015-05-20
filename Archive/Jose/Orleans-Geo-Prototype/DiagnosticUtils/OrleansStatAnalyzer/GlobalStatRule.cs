using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class GlobalStatRule : StatRule
    {
        public GlobalStatRule()
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
            
            List<double> data = new List<double>();

            foreach (var pair in counterData.SiloData)
            {
                try
                {
                    foreach (var counterVal in pair.Value.SiloStatistics[counterName].TimeVals)
                    {
                        data.Add(counterVal.Value);
                    }
                }
                catch (KeyNotFoundException)
                {

                }
            }
            
            double val = GetStatValue(data);

            if (IsPercentageRule)
            {
                foreach (var d in data)
                {
                    if (!op.operateforpercentage(d, ExpectedVal, val))
                    {
                        ViolatedInfo.Add(new Tuple<string, DateTime, double>("Global Stat", DateTime.Now, val)); ;
                    }
                }

                return;
            }

            if (!op.operate(val, ExpectedVal))
            {
                ViolatedInfo.Add(new Tuple<string, DateTime, double>("Global Stat", DateTime.Now, val));
            }
        }

        public void EvaluateZscore(PerfCounterData counterData, string counterName)
        {
            List<double> data = new List<double>();

            foreach (var pair in counterData.SiloData)
            {
                try
                {
                    foreach (var counterVal in pair.Value.SiloStatistics[counterName].TimeVals)
                    {
                        data.Add(counterVal.Value);
                    }
                }
                catch (KeyNotFoundException)
                {

                }
            }

            List<double> zcores = NumericalAnalyzer.Zscore(data);
            int i = 0;

            foreach (var pair in counterData.SiloData)
            {
                try
                {
                    foreach (var counterVal in pair.Value.SiloStatistics[counterName].TimeVals)
                    {
                        if (!op.operate(zcores[i], ExpectedVal))
                        {
                            ViolatedInfo.Add(new Tuple<string, DateTime, double>(pair.Key, counterVal.Key,
                                                                                zcores[i]));
                        }
                        i++;
                    }
                }
                catch (KeyNotFoundException)
                {

                }
            }
        }
    }
}
