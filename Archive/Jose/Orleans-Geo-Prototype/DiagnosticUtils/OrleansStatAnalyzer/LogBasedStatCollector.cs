using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class LogBasedStatCollector : StatCollector
    {
        public string LogDir { get; set; }
        public string FilePrefix { get; set; }
        public int Iterations { get; set; }
        
        public LogBasedStatCollector()
        {
            NewCounterList = new HashSet<string>();
        }

        public override Dictionary<string, SiloInstance> RetreiveData(HashSet<string> counters)
        {
            // Open the directory containing logs

            try
            {
                DirectoryInfo diInfo = new DirectoryInfo(LogDir);
                FileInfo[] fiInfo = diInfo.GetFiles("*.log");
                List<Task<SiloInstance>> parsingThreads = new List<Task<SiloInstance>>();

                foreach (var fi in fiInfo)
                {
                    if (fi.Name.StartsWith(FilePrefix))
                    {
                        var thread = Task<SiloInstance>.Factory.StartNew(() =>
                            {
                                return ParseOneFile(fi, counters);
                            });
                        parsingThreads.Add(thread);
                    }
                }
                Task.WaitAll(parsingThreads.ToArray());

                Dictionary<string, SiloInstance> silos = new Dictionary<string, SiloInstance>();
                foreach (Task<SiloInstance> thread in parsingThreads)
                {
                    SiloInstance silo = thread.Result;
                    silos.Add(silo.Name, silo);
                }

                // This is required to plot the surface graphs
                foreach (KeyValuePair<string, SiloInstance> pair in silos)
                {
                    pair.Value.SetEarliestTime();
                    pair.Value.SetLatestTime();
                }
                return silos;
            }
            catch (DirectoryNotFoundException ex)
            {
                throw ex;
            }
            catch (FileNotFoundException ex)
            {
                throw ex;
            }
        }

        private SiloInstance ParseOneFile(FileInfo fileInfo, HashSet<string> counters)
        {
            Console.WriteLine("Processing file: {0}", fileInfo.FullName);
            string siloName = ExtratctSiloName(fileInfo);
            SiloInstance silo = new SiloInstance(siloName);

            StreamReader logReader = fileInfo.OpenText();

            string line;
            int i = 0;
            while ((line = logReader.ReadLine()) != null)
            {
                if (line.Contains("Statistics: ^^^"))
                {
                    i++;
                    if (Iterations > 0 && i > Iterations)
                    {
                        break;
                    }

                    DateTime dt = ExtractTime(line);
                    silo.TracePoints.Add(dt);
                    line = logReader.ReadLine();

                    try
                    {
                        while (line != null && !line.StartsWith("\t"))
                        {
                            if (line.StartsWith("^^^"))
                            {
                                    while (!line.Contains("Statistics: ^^^"))
                                    {
                                        line = logReader.ReadLine();
                                    }
                                line = logReader.ReadLine();
                            }
                            string counterName = GetStatValue(line, silo, dt, counters);
                            line = logReader.ReadLine();
                        }
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            logReader.Close();
            Console.WriteLine("Processing of file: {0} is done.", fileInfo.FullName);
            return silo;
        }

        private static DateTime ExtractTime(string logmessage)
        {
            int startIndex = logmessage.IndexOf('[') + 1;
            int endIndex = logmessage.IndexOf("GMT") - 1;
            string datetime = logmessage.Substring(startIndex, endIndex - startIndex);
            return Convert.ToDateTime(datetime);
        }

        private static string ExtratctSiloName(FileInfo fi)
        {
            StreamReader fileReader = fi.OpenText();
            string line;
            while ((line = fileReader.ReadLine()) != null)
            {
                if (line.Contains("Silo Name:") || line.Contains("Client Name:"))
                {
                    int index = line.IndexOf(":");
                    index = index + 2;
                    string siloName = line.Substring(index);
                    fileReader.Close();
                    fi.Refresh();
                    return siloName;
                }
            }
            fileReader.Close();
            fi.Refresh();
            return null;
        }

        
        private string GetStatValue(string statentry, SiloInstance silo, DateTime dt, HashSet<string> counters)
        {
            // Seperate the Name part and Value part from the stat
            string namePart = statentry.Substring(0, statentry.IndexOf('='));
            string valPart = statentry.Substring(statentry.IndexOf('=') + 1);
            string statName = "";

            // Compare this stat line in the log with all the user provided stat names.
            
            char[] splitChars1 = new char[] { ',', '\t' };
            char[] splitChars2 = new char[] { '=' };
            
            string[] split = valPart.Split(splitChars1);
            List<string> tempSplit = new List<string>();
            foreach (string s in split)
            {
                string x = s.Trim();
                if (x.EndsWith("Secs"))
                {
                    Console.WriteLine(x);
                    x.Substring(0, x.Length - 4);
                    Console.WriteLine(x);
                    x = x.Trim();
                }
                if (x != "")
                {
                    tempSplit.Add(x);
                }
            }
                    
            int n = tempSplit.Count();

            try
            {

                string statEntryName;

                // Remove the ".Current" if there is any
                if (namePart.EndsWith(".Current"))
                {
                    int indexCurrent = namePart.IndexOf(".Current");
                    statEntryName = namePart.Substring(0, indexCurrent);
                }
                else
                {
                    statEntryName = namePart;
                }
                
                
                if (n > 2)
                {
                    //Console.WriteLine("Does not understand the format for stat {0}", namePart);
                    return null;
                }
                else if (n == 1)
                {
                    statName = statEntryName;
                    InsertStatValue(silo, statName, tempSplit[0], dt, counters);
                }
                else if (n == 0)
                {
                    statName = statEntryName;
                    InsertStatValue(silo, statName, "0.0", dt, counters);
                }
                else
                {
                    statName = statEntryName;
                    InsertStatValue(silo, statName, tempSplit[0], dt, counters);
                    string[] temp = tempSplit[1].Split(splitChars2);

                    string statNameDelta = statEntryName + ".Delta";
                    statName = statNameDelta;
                    InsertStatValue(silo, statNameDelta, temp[1], dt, counters);
                }    
            }
            catch (FormatException)
            {
                //Console.WriteLine("Does not understand the format for stat {0}", statName);
                return null;
            }

            return statName;
        }

        
        private void InsertStatValue(SiloInstance silo, string statName, string val, DateTime dt, HashSet<string> counters)
        {
            InsertStatValue(silo, statName, val, dt);
            lock (counters)
            {
                counters.Add(statName);    
            }
        }

        private void InsertStatValue(SiloInstance silo, string statName, string val, DateTime dt)
        {
            double statVal = 0.0;

            if (val == "NaN")
            {
                statVal = 0.0;
            }
            else
            {
                statVal = Convert.ToDouble(val);
            }

            if (!silo.SiloStatistics.ContainsKey(statName))
            {
                OrleanStatistic orleanSiloStat = new OrleanStatistic(statName);
                silo.SiloStatistics.Add(statName, orleanSiloStat);
            }
            silo.SiloStatistics[statName].TimeVals.Add(dt, statVal);           
        }
    }
}
