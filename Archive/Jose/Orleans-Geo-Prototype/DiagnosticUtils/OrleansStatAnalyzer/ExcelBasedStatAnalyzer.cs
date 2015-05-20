using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Office.Interop.Excel;
using System.Runtime.InteropServices;

namespace OrleansStatAnalyzer
{
    public class ExcelBasedStatAnalyzer : PerformanceCounterAnalyzer
    {
        public string ChartDir { get; set; }
        public bool IsVisible { get; set; }

        public ExcelBasedStatAnalyzer()
        {
            ChartDir = null;
            IsVisible = false;
        }



        public override void Analyze(PerfCounterData counterData, HashSet<string> counters)
        {
            foreach (var counter in counters)
            {
                try
                {
                    Application xlApp = new Application();
                    xlApp.Visible = IsVisible;
                    Workbook wb = xlApp.Workbooks.Add(XlWBATemplate.xlWBATWorksheet);

                    Console.WriteLine();
                    Console.WriteLine("Creating visualizations using Excel");
                    PlotSummaryChartAcrossSilo(counterData.SummaryStatistics, counter, xlApp, wb);
                    PlotSummaryChartAcrossTime(counterData.SummaryStatistics, counter, xlApp, wb);
                    CalculateSummaryStatsGlobally(counterData.SummaryStatistics, counter, xlApp, wb);
                    PlotSurfaceGraph(counterData.CounterTimeData, counter, xlApp, wb);

                    if (ChartDir == null)
                    {
                        ChartDir = "Charts";
                    }

                    string dir = ChartDir + Path.DirectorySeparatorChar + counterData.Name;

                    if (!Directory.Exists(dir))
                    {
                        Directory.CreateDirectory(dir);
                    }

                    string temp = RefineFileName(counter);
                    string filename = Path.Combine(dir, temp + ".xlsx");
                    string fullFileName = Path.GetFullPath(filename);
                    Console.WriteLine("Saving {0}", fullFileName);

                    //// Gabi, in the documentation for the following method there is a way to 
                    //// tell excel not to ask from the user, to replace it.
                    //// please see this, http://msdn.microsoft.com/en-us/library/microsoft.office.tools.excel.workbook.saveas.aspx
                    //// The parameter ConflictResolution is the one we have to set. Before setting that we have to set all the otehrs 
                    //// to default. There may be an easier way to do it, but I couldn't figured out.
                    
                    wb.SaveAs(fullFileName);
                    wb.Close(true);
                    Marshal.FinalReleaseComObject(wb);
                    xlApp.Quit();
                    Marshal.ReleaseComObject(xlApp);
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
                catch (KeyNotFoundException)
                {
                    System.Console.WriteLine("Cannot find the counter for analysis {0}", counter);
                }
            }    
        }

        private void PlotSummaryChartAcrossSilo(Dictionary<string, CounterSummaryStat> summaryData, string counter, Application xlApp, Workbook wb)
        {
            Worksheet ws = wb.Worksheets.Add();

            if (ws == null)
            {
                Console.WriteLine("Worksheet could not be created. Check that your office installation and project references are correct.");
                return;
            }

            ws.Name = "AcroosSilo";

            int k = 1;

            try
            {
                foreach (var spair in summaryData[counter].SiloSummaryStat)
                {
                    k++;
                    ws.Cells[1, k] = spair.Key;
                }  
            }
            catch (KeyNotFoundException)
            {
                Console.WriteLine("Silos Summary Srats for counter {0} not found");
            }

            ws.Cells[2, 1] = "Max";
            ws.Cells[3, 1] = "Min";
            ws.Cells[4, 1] = "Avg";
            ws.Cells[5, 1] = "Median";
            ws.Cells[6, 1] = "Sdev";

            int i = 1;

            foreach (var pair in summaryData[counter].SiloSummaryStat)
            {
                 i++;
                 ws.Cells[2, i] = pair.Value.Max;
                 ws.Cells[3, i] = pair.Value.Min;
                 ws.Cells[4, i] = pair.Value.Avg;
                 ws.Cells[5, i] = pair.Value.Median;
                 ws.Cells[6, i] = pair.Value.Sdev;
            }

            Range startCell = ws.Cells[1, 1];
            Range endCell = ws.Cells[4, i];
            Range dataRange = ws.Range[startCell, endCell];
            
            if (i > 3)
            {
                ChartObjects chartobjs = ws.ChartObjects();
                ChartObject charobj = chartobjs.Add(0, 100, 300, 300);
                charobj.Chart.ChartWizard(dataRange, XlChartType.xlStockHLC);   
            }
        }

        private void PlotSummaryChartAcrossTime(Dictionary<string, CounterSummaryStat> summaryData, string counter, Application xlApp, Workbook wb)
        {
            Worksheet ws = wb.Worksheets.Add();

            if (ws == null)
            {
                Console.WriteLine("Worksheet could not be created. Check that your office installation and project references are correct.");
                return;
            }

            ws.Name = "AcrossTime";

            int k = 1;
            foreach (var tpair in summaryData[counter].TimeSummaryStat)
            {
                k++;
                string s = "Time " + tpair.Key;
                ws.Cells[1, k] = s;
            }

            ws.Cells[2, 1] = "Max";
            ws.Cells[3, 1] = "Min";
            ws.Cells[4, 1] = "Avg";
            ws.Cells[5, 1] = "Median";
            ws.Cells[6, 1] = "Sdev";
            
            int i = 1;

            foreach (var pair in summaryData[counter].TimeSummaryStat)
            {
                i++;
                ws.Cells[2, i] = pair.Value.Max;
                ws.Cells[3, i] = pair.Value.Min;
                ws.Cells[4, i] = pair.Value.Avg;
                ws.Cells[5, i] = pair.Value.Median;
                ws.Cells[6, i] = pair.Value.Sdev;
            }

            Range startCell = ws.Cells[1, 1];
            Range endCell = ws.Cells[4, i];
            Range dataRange = ws.Range[startCell, endCell];
            
            if (i > 3)
            {
                ChartObjects chartobjs = ws.ChartObjects();
                ChartObject charobj = chartobjs.Add(0, 100, 300, 300);
                charobj.Chart.ChartWizard(dataRange, XlChartType.xlStockHLC);
            }
        }


        private void PlotSurfaceGraph(Dictionary<string, OrleanStatistic> PerformanceCounters, string counter, Application xlApp, Workbook wb)
        {
            Worksheet ws = wb.Worksheets.Add();

            if (ws == null)
            {
                Console.WriteLine("Worksheet could not be created. Check that your office installation and project references are correct.");
                return;
            }

            ws.Name = "Detail";

            int i = 1;
            int j = 1;
            int n = 0;

            try
            {
                foreach (var pair in PerformanceCounters[counter].SiloVals)
                {
                    i++;
                    string s = "Time" + pair.Key;
                    ws.Cells[i, j] = s;
                }
                i = 1;

                n = PerformanceCounters[counter].SiloVals[0].Count();
            }
            catch (KeyNotFoundException)
            {
                System.Console.WriteLine("No values for counter {0}", counter);
                return;
            }

            for (int m = 0; m < n; m++)
            {
                j++;
                ws.Cells[i, j] = PerformanceCounters[counter].SiloVals[0].ElementAt(m).Item1;
            }
            j = 1;

            foreach (var pair in PerformanceCounters[counter].SiloVals)
            {
                i++;
                foreach(var val in pair.Value)
                {
                    j++;
                    ws.Cells[i,j]=val.Item2;
                }
                j = 1;
            }
           
            j = n+1;
            
            Range startCell = ws.Cells[1, 1];
            Range endCell = ws.Cells[i, j];
            Range dataRange = ws.Range[startCell, endCell];

            if (j > 2 && i > 3)
            {
                ChartObjects chartobjs = ws.ChartObjects();
                ChartObject charobj = chartobjs.Add(0, 100, 600, 600);

                charobj.Chart.ChartWizard(dataRange, XlChartType.xlSurface);    
            }
        }


        private void CalculateSummaryStatsGlobally(Dictionary<string, CounterSummaryStat> summaryData, string counter, Application xlApp, Workbook wb)
        {
            Worksheet ws = wb.Worksheets.Add();

            if (ws == null)
            {
                Console.WriteLine("Worksheet could not be created. Check that your office installation and project references are correct.");
                return;
            }

            ws.Name = "Global";

            ws.Cells[1, 1] = "Max";
            ws.Cells[2, 1] = "Min";
            ws.Cells[3, 1] = "Avg";
            ws.Cells[4, 1] = "Median";
            ws.Cells[5, 1] = "Sdev";

            ws.Cells[1, 2] = summaryData[counter].GlobalStat.Max;
            ws.Cells[2, 2] = summaryData[counter].GlobalStat.Min;
            ws.Cells[3, 2] = summaryData[counter].GlobalStat.Avg;
            ws.Cells[4, 2] = summaryData[counter].GlobalStat.Median;
            ws.Cells[5, 2] = summaryData[counter].GlobalStat.Sdev;    
        }

        
        string RefineFileName(string counter)
        {
            string newFileName = string.Copy(counter);
            char[] invalid = Path.GetInvalidFileNameChars();
            foreach (char item in invalid)
            {
                newFileName = newFileName.Replace(item.ToString(), ".");
            }
            return newFileName;
        }
    }
}
