using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace OrleansStatAnalyzer
{
    public class PcaBasedStatAnalyzer
    {
        public void Analyze(Dictionary<string, SiloInstance> silos)
        {
            foreach (KeyValuePair<string, SiloInstance> spair in silos)
            {
                StreamWriter file = new StreamWriter(spair.Key);
               
                foreach (DateTime dt in spair.Value.TracePoints)
                {
                    string line = "";
                    foreach (KeyValuePair<string, OrleanStatistic> stpair in spair.Value.SiloStatistics)
                    {
                        if(stpair.Value.TimeVals.ContainsKey(dt))
                        {
                            line += stpair.Value.TimeVals[dt];
                            line += " ";
                        }
                        else
                        {
                            line += "0 ";
                        }
                    }
                    file.WriteLine(line);
                }

                file.Close();
            }
        }
    }
}
