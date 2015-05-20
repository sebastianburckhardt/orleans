using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Office.Interop.Excel;


namespace OrleansStatAnalyzer
{
    public class CorrelationAnalyzer : PerformanceCounterAnalyzer
    {
        public bool Pearson { get; set; }
        public bool Spearman { get; set; }
        public bool HistogramExplanation { get; set; }
        public bool HighLowAnalysis { get; set; }

        public CorrelationAnalyzer()
        {
            Pearson = false;
            Spearman = false;
            HistogramExplanation = false;
            HighLowAnalysis = false;
        }
        
        
        public override void Analyze(PerfCounterData counterData, HashSet<string> counters)
        {
            Console.WriteLine();
            Console.WriteLine("Correlation Analysis");
            Console.WriteLine("=====================");
            
            foreach (var kpi in counters)
            {
                Dictionary<string, Tuple<double, double>> ciResults = new Dictionary<string, Tuple<double, double>>();
                foreach (var explainCounter in counterData.AllCounters)
                {
                    if (!counters.Contains(explainCounter))
                    {
                        DoCorrelationAnalysis(explainCounter, kpi, counterData.SiloData, ciResults);
                    }
                }
                foreach (var pair in ciResults)
                {
                    Console.WriteLine("{0} : {1}, {2}", pair.Key, pair.Value.Item1, pair.Value.Item2);
                }
            }
        }

        
        public void DoCorrelationAnalysis(string explainCounter, string kpiCounter, Dictionary<string, SiloInstance> silos, Dictionary<string, Tuple<double, double>> ciResults)
        {
            List<double> explainVals = new List<double>();
            List<double> kpiVals = new List<double>();

            Prepare(silos, kpiCounter, explainCounter, explainVals, kpiVals);

            Console.WriteLine("KPI : {0}     Explain: {1}", kpiCounter, explainCounter);
            Console.WriteLine("==========================================================");

            if (Pearson)
            {
                double pearsonCoefficient = CalculatePearson(explainVals, kpiVals);

                if (pearsonCoefficient > 0.9)
                {
                    Console.WriteLine("Peareson Coefficient : {0}", pearsonCoefficient);    
                }
            }

            if (Spearman)
            {
                double spearmanCoefficient = CalculateSpearman(explainVals, kpiVals);

                if (spearmanCoefficient > 0.9)
                {
                    Console.WriteLine("Spearman Coefficient : {0}", spearmanCoefficient);
                }
            }

            if (HistogramExplanation)
            {
                CorrelationDetection.HistogramExplanation explain = CalculateHistogramExplanations(explainVals, kpiVals);

                Console.WriteLine("Histrogram Explanations");
                Console.WriteLine("=========================");

                if (explain != null)
                {
                    explain.Print();
                }
                else
                {
                    Console.WriteLine("Histrogram Explanation is null");
                }
            }

            // This analysis tries to divided the explanatory counter dataset into an optimal two clusters,
            // based on a given threshold value in the kpi. It does this using maximum probability difference 
            // after calculating all the values which can be used to seperate the dataset.

            // The basic functionality work, but need to do some improvments like considering the number of instances 
            // in a bucket in addition to the probability.

            if (HighLowAnalysis)
            {
                Tuple<double, double> result = GetTransitThreshold(kpiVals, explainVals, 0);
                ciResults.Add(explainCounter, result);    
            }

            Console.WriteLine();
        
        }

        public double CalculatePearson(List<double> explainCounterValues, List<double> kpiValues)
        {
            if ((explainCounterValues.Count() > 0) || (kpiValues.Count() > 0))
            {
                CorrelationDetection.StatisticalFunctions SF = new CorrelationDetection.StatisticalFunctions();
                return SF.Pearsons(explainCounterValues, kpiValues);
            }
            else
            {
                return 0.0;
            }
        }

        public double CalculateSpearman(List<double> explainCounterValues, List<double> kpiValues)
        {
            if ((explainCounterValues.Count() > 0) || (kpiValues.Count() > 0))
            {
                CorrelationDetection.StatisticalFunctions SF = new CorrelationDetection.StatisticalFunctions();
                return SF.Spearman(explainCounterValues, kpiValues);
            }
            else
            {
                return 0.0;
            }
        }

        public CorrelationDetection.HistogramExplanation CalculateHistogramExplanations(List<double> explainCounterValues, List<double> kpiValues)
        {
            List<object> explanations = new List<object>();
            List<object> cois = new List<object>();

            foreach (var v in explainCounterValues)
            {
                explanations.Add(v);    
            }

            foreach (var v in kpiValues)
            {
                cois.Add(v);
            }

            CorrelationDetection.StatisticalFunctions SF = new CorrelationDetection.StatisticalFunctions();
            CorrelationDetection.HistogramExplanation explain = SF.GenerateMinVarianceHistogram(
                cois, explanations, 2, true, CorrelationDetection.DependencyType.Default, CorrelationDetection.CorrelationType.Either);

            return explain;
        }

        public void Plot(List<double> explainCounterValues, List<double> kpiValues, string counter)
        {
            Application xlApp = new Application();
            xlApp.Visible = true;
            Workbook wb = xlApp.Workbooks.Add(XlWBATemplate.xlWBATWorksheet);

            Worksheet ws = wb.Worksheets.Add();

            if (ws == null)
            {
                Console.WriteLine("Worksheet could not be created. Check that your office installation and project references are correct.");
                return;
            }

            ws.Name = counter;

            int size1 = explainCounterValues.Count();
            int size2 = kpiValues.Count();

            for (int i = 0; i < size1; i++)
            {
                ws.Cells[i + 1, 1] = explainCounterValues.ElementAt(i);
                ws.Cells[i + 1, 2] = kpiValues.ElementAt(i);
            }


            Range startCell = ws.Cells[1, 1];
            Range endCell = ws.Cells[size1, 2];
            Range dataRange = ws.Range[startCell, endCell];
            ChartObjects chartobjs = ws.ChartObjects();
            ChartObject charobj = chartobjs.Add(0, 100, 600, 600);

            charobj.Chart.ChartWizard(dataRange, XlChartType.xlXYScatter);
            
            string filename =  counter + ".xlsx";
            Console.WriteLine("Saving {0}", filename);
            wb.SaveAs(filename);
            wb.Close(true);
            xlApp.Quit();
        }

        
        public void Prepare(Dictionary<string, SiloInstance> silos, string ci, string cx, List<double> explainVals, List<double> ciVals)
        {
            foreach (var s in silos)
            {
                Dictionary<DateTime, double> siloExplainStats;
                Dictionary<DateTime, double> silociStats;
                try
                {
                    siloExplainStats = s.Value.SiloStatistics[cx].TimeVals;
                    silociStats = s.Value.SiloStatistics[ci].TimeVals;
                }
                catch (KeyNotFoundException)
                {
                    continue;
                }
                foreach (var pair in siloExplainStats)
                {
                    if (silociStats.ContainsKey(pair.Key))
                    {
                        explainVals.Add(pair.Value);
                        ciVals.Add(silociStats[pair.Key]);
                    }
                }
            }            
        }
        
        
        public Tuple<double, double> GetTransitThreshold(List<double> ciVals, List<double> cexpcandval, double ciThreshold)
        {
            int ciSize = ciVals.Count();
            int cexpSize = cexpcandval.Count();

            double maxProbdiff = 0.0;
            double maxProbdiffVal = cexpcandval[0];

            foreach (double x in cexpcandval)
            {
                int c1 = 0;
                int d1 = 0;
                int c2 = 0;
                int d2 = 0;
                double probDiff = 0.0;
                
                for (int i = 0; i < cexpSize; i++)
                {
                    if (cexpcandval[i] < x)
                    {
                        c1++;
                        if (ciVals[i] >= ciThreshold)
                        {
                            c2++;
                        }
                    }
                    else
                    {
                        d1++;
                        if (ciVals[i] >= ciThreshold)
                        {
                            d2++;
                        }
                    }
                }

                double p1 = 0.0;
                double p2 = 0.0;

                if (c1 > 0.0)
                {
                    p1 = c2/c1;
                }

                if (d1 > 0.0)
                {
                    p2 = d2/d1;
                }

                probDiff = Math.Abs(p1 - p2);

                if (probDiff > maxProbdiff)
                {
                    maxProbdiff = probDiff;
                    maxProbdiffVal = x;
                }
            }

            Tuple<double, double> result = new Tuple<double, double>(maxProbdiff, maxProbdiffVal);
            return result;
        }
    }
}
