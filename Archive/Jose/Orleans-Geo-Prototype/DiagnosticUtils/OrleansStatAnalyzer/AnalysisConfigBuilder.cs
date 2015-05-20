using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace OrleansStatAnalyzer
{
    public class AnalysisConfigBuilder
    {
        public ComparativeAnalyzer CompAnalyzer { get; set; }

        public AnalysisConfigBuilder()
        {
            CompAnalyzer = null;
        }

        public List<PerfCounterData> ProcessConfig(string fileName)
        {
            
            XmlDocument configDoc = new XmlDocument();
            configDoc.Load(fileName);
            XmlElement root = configDoc.DocumentElement;
            List<PerfCounterData> datasets = new List<PerfCounterData>();

            foreach (XmlNode dataNode in root.ChildNodes)
            {
                if (dataNode.NodeType == XmlNodeType.Element)
                {
                    XmlElement dataElement = (XmlElement) dataNode;
                    if (dataElement.Name == "Dataset")
                    {
                        PerfCounterData counterData = new PerfCounterData();

                        if (dataElement.HasAttribute("Reference"))
                        {
                            counterData.IsRef = Boolean.Parse(dataElement.GetAttribute("Reference"));
                        }
                        else
                        {
                            counterData.IsRef = false;
                        }

                        foreach (XmlNode childNode in dataElement.ChildNodes)
                        {
                            if (childNode.NodeType == XmlNodeType.Element)
                            {
                                XmlElement childElement = (XmlElement) childNode;
                                if (childElement.Name == "Name")
                                {
                                    counterData.Name = childElement.InnerText;
                                }
                                else if (childElement.Name == "StorageType")
                                {
                                    StatCollector sc = ProcessStorageTypeElement(childNode);
                                    if (sc == null)
                                    {
                                        return null;
                                    }
                                    counterData.DataSource = sc;
                                }
                                else if (childElement.Name == "UserAnalysis")
                                {
                                    ProcessUserAnalysisElement(childNode, counterData);
                                }
                                else if (childElement.Name == "DataAnalysis")
                                {
                                    ProcessDataAnalysisElement(childNode, counterData);
                                }
                                else
                                {
                                    System.Console.WriteLine("Unknown configuration element");
                                    return null;
                                }
                            }
                        }
                        datasets.Add(counterData);
                    }

                    if (dataElement.Name == "ComparativeAnalysis")
                    {
                        CompAnalyzer = new ComparativeAnalyzer();
                        
                        if (dataElement.HasAttribute("Silo"))
                        {
                            CompAnalyzer.IsSilo = Boolean.Parse(dataElement.GetAttribute("Silo"));
                        }
                        
                        if(dataElement.HasAttribute("Time"))
                        {
                            CompAnalyzer.IsTime = Boolean.Parse(dataElement.GetAttribute("Time"));
                        }
                        
                        ProcessComparativeAnalysisElement(dataNode, CompAnalyzer);
                    }
                }

            }
            return datasets;
        }

        private StatCollector ProcessStorageTypeElement(XmlNode storageNode)
        {
            XmlElement storageElment = (XmlElement) storageNode;
            string attrName = storageElment.GetAttribute("Name");

            if (attrName == "Azure")
            {
                XmlNode azureNode = storageNode.SelectSingleNode("Azure");
                if (azureNode == null)
                {
                    Console.WriteLine("Wrong storage type");
                    return null;
                }

                AzureStorageBasedStatCollector azureCollector = new AzureStorageBasedStatCollector();
                XmlElement azureElement = (XmlElement) azureNode;
                
                azureCollector.ConnectionString = azureElement.GetAttribute("ConnectionString");
                azureCollector.TableName = azureElement.GetAttribute("TableName");
                azureCollector.PartitionKey = azureElement.GetAttribute("PartitionKey");
                azureCollector.TimeGap = Convert.ToDouble(azureElement.GetAttribute("LogWriteInterval"));
                return azureCollector;
            }
            else if (attrName == "Log")
            {
                XmlNode logNode = storageNode.SelectSingleNode("Log");
                if (logNode == null)
                {
                    Console.WriteLine("Wrong storage type");
                    return null;
                }

                LogBasedStatCollector logCollector = new LogBasedStatCollector();
                XmlElement logElement = (XmlElement) logNode;

                logCollector.LogDir = logElement.GetAttribute("LogDir");
                logCollector.FilePrefix = logElement.GetAttribute("FilePrefix");
                logCollector.TimeGap = Convert.ToDouble(logElement.GetAttribute("LogWriteInterval"));
                if (logElement.HasAttribute("Iterations"))
                {
                    logCollector.Iterations = Convert.ToInt32(logElement.GetAttribute("Iterations"));   
                }
                else
                {
                    logCollector.Iterations = 0;
                }
                
                return logCollector;
            }
            else
            {
                Console.WriteLine("Unknown Attribute");
                return null;
            }
        }

        private void ProcessUserAnalysisElement(XmlNode analysisNode, PerfCounterData counterData)
        {
            counterData.UAnalyzer = new UserDrivenAnalyzer();

            foreach (XmlNode childNode in analysisNode.ChildNodes)
            {
                if (childNode.NodeType == XmlNodeType.Element)
                {
                    XmlElement childElement = (XmlElement)childNode;

                    if (childNode.Name == "Actions")
                    {
                        ProcessAnalysisElement(childNode, counterData.UAnalyzer.Analyzers, true);
                    }
                    
                    else if (childNode.Name == "Counters")
                    {
                        ProcessCounterElements(childNode, counterData.UAnalyzer.UserCounters);    
                    }
                    else
                    {
                        Console.WriteLine("Unknown Element Type");
                        return;
                    }   
                }
            }
        }


        private void ProcessDataAnalysisElement(XmlNode analysisNode, PerfCounterData counterData)
        {
            counterData.DAnalyzer = new DataAnalyzer();

            foreach (XmlNode childNode in analysisNode.ChildNodes)
            {
                if (childNode.NodeType == XmlNodeType.Element)
                {
                    XmlElement childElement = (XmlElement)childNode;

                    if (childNode.Name == "Actions")
                    {
                        ProcessAnalysisElement(childNode, counterData.DAnalyzer.Analyzers, false);
                    }
                    else
                    {
                        Console.WriteLine("Unknown Element Type");
                        return;
                    }
                }
            }
        }

        private void ProcessComparativeAnalysisElement(XmlNode analysisNode, ComparativeAnalyzer compAnalyzer)
        {
            foreach (XmlNode childNode in analysisNode.ChildNodes)
            {
                if (childNode.NodeType == XmlNodeType.Element)
                {
                    XmlElement childElement = (XmlElement)childNode;

                    if (childNode.Name == "Actions")
                    {
                        ProcessAnalysisElement(childNode, compAnalyzer.Analyzers, false);
                    }
                    else
                    {
                        Console.WriteLine("Unknown Element Type");
                        return;
                    }
                }
            }
        }



        private void ProcessAnalysisElement(XmlNode analysisNode, List<PerformanceCounterAnalyzer> analyzers, bool IsUserAnalysis)
        {
            foreach (XmlNode childNode in analysisNode.ChildNodes)
            {
                if (childNode.NodeType == XmlNodeType.Element)
                {
                    XmlElement childElement = (XmlElement) childNode;

                    if (childNode.Name == "Visualization")
                    {
                        ExcelBasedStatAnalyzer excelBasedStatAnalyzer = new ExcelBasedStatAnalyzer();
                        if (childElement.HasAttribute("ChartDir"))
                        {
                            excelBasedStatAnalyzer.ChartDir = childElement.GetAttribute("ChartDir");
                        }
                        if (childElement.HasAttribute("Visible"))
                        {
                            excelBasedStatAnalyzer.IsVisible = Boolean.Parse(childElement.GetAttribute("Visible"));
                        }
                        analyzers.Add(excelBasedStatAnalyzer);
                    }
                    else if (childNode.Name == "ThresholdAnalysis")
                    {
                        if (!IsUserAnalysis)
                        {
                            Console.WriteLine("ThresholdAnalysis can only be specified with User analyis.. so ignoring");
                            continue;
                        }
                        ThresholdAnalyzer thresholdAnalyzer = new ThresholdAnalyzer();
                        if (childElement.HasAttribute("RuleConfigFile"))
                        {
                            thresholdAnalyzer.RuleFile = childElement.GetAttribute("RuleConfigFile");
                            analyzers.Add(thresholdAnalyzer);
                        }
                        else
                        {
                            Console.WriteLine("Please specify the Threshold Configuration file for Threshold analysis.");
                        }
                    }
                    else if (childNode.Name == "CorrelationAnalysis")
                    {
                        CorrelationAnalyzer correlationAnalyzer = new CorrelationAnalyzer();
                        if (childElement.HasAttribute("Pearson"))
                        {
                            correlationAnalyzer.Pearson = Boolean.Parse(childElement.GetAttribute("Pearson"));
                        }
                        if (childElement.HasAttribute("Spearman"))
                        {
                            correlationAnalyzer.Spearman = Boolean.Parse(childElement.GetAttribute("Spearman"));
                        }
                        if (childElement.HasAttribute("HistogramExplanation"))
                        {
                            correlationAnalyzer.HistogramExplanation =
                                Boolean.Parse(childElement.GetAttribute("HistogramExplanation"));
                        }
                        
                        analyzers.Add(correlationAnalyzer);
                    }
                    else if (childNode.Name == "VarianceAnalysis")
                    {
                        VarianceBasedAnalyzer varianceBasedAnalyzer = new VarianceBasedAnalyzer();
                        analyzers.Add(varianceBasedAnalyzer);
                    }
                    else if (childNode.Name == "PerformanceTuning")
                    {
                        PerformanceTuningAnalyzer performanceTuningAnalyzer = new PerformanceTuningAnalyzer();
                        analyzers.Add(performanceTuningAnalyzer);
                    }
                    else
                    {
                        Console.WriteLine("Unknown Analysis type");
                        return;
                    }
                }
            }
        }


        void ProcessCounterElements(XmlNode countersNode, HashSet<string> counters)
        {
            foreach (XmlNode childNode in countersNode.ChildNodes)
            {
                if (childNode.NodeType == XmlNodeType.Element)
                {
                    if (childNode.Name == "Name")
                    {
                        counters.Add(childNode.InnerText);
                    }    
                }
            }
        }
    }
}
