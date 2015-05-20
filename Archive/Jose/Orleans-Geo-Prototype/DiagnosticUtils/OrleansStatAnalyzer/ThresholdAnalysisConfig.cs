using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace OrleansStatAnalyzer
{
    public class ThresholdAnalysisConfig
    {
  // <PerformanceCounter Name="Runtime.GC.PercentOfTimeInGC">
  //  <Rules>
  //    <Rule>
  //      <AppliesTo>Global</AppliesTo>
  //      <Statistic>Any</Statistic>
  //      <ExpectedValue>10</ExpectedValue>
  //      <ComparisonOperator>LessThan</ComparisonOperator>
  //    </Rule>
  //  </Rules>
  //</PerformanceCounter>
  //<PerformanceCounter Name="Messaging.SendMessages.Delta">
  //  <Rules>
  //    <Rule>
  //      <AppliesTo>Silo</AppliesTo>
  //      <Statistic>Average</Statistic>
  //      <ExpectedValue>500</ExpectedValue>
  //      <ComparisonOpearator>LessThan</ComparisonOpearator>
  //    </Rule>
  //    <Rule>
  //      <AppliesTo>Time</AppliesTo>
  //      <Statistic>Median</Statistic>
  //      <ExpectedValue>800</ExpectedValue>
  //      <ComparisonOpearator>LessThan</ComparisonOpearator>
  //    </Rule>
  //  </Rules>
  //</PerformanceCounter>
  //<PerformanceCounter Name="Runtime.CPUUsage">
  //  <Rules>
  //    <Rule>
  //      <AppliesTo>Time</AppliesTo>
  //      <Statistic>Average</Statistic>
  //      <ExpectedValue>80</ExpectedValue>
  //      <ComparisonOpearator>GreaterThan</ComparisonOpearator>
  //    </Rule>
  //  </Rules>
  //</PerformanceCounter>


        public void ProcessConfig(string configFile, Dictionary<string, OrleanStatistic> counterData)
        {
            XmlDocument configDoc = new XmlDocument();
            configDoc.Load(configFile);
            XmlElement root = configDoc.DocumentElement;

            foreach (XmlNode perfNode in root.ChildNodes)
            {
                if (perfNode.NodeType == XmlNodeType.Element)
                {
                    XmlElement perfElement = (XmlElement)perfNode;
                    if (perfElement.Name == "PerformanceCounter")
                    {
                        string counterNames = perfElement.GetAttribute("Names");
                        HashSet<string> temp = null;

                        if (!(counterNames == "All"))
                        {
                            temp = GetCounterNames(counterNames);   
                        }
                        
                        XmlElement rulesElement = (XmlElement)perfElement.FirstChild;

                        foreach (XmlNode ruleNode in rulesElement.ChildNodes)
                        {
                            XmlElement ruleElement = (XmlElement)ruleNode;

                            StatRule rule = CreateStatRule(ruleElement.GetAttribute("AppliesTo"));

                            if (rule == null)
                            {
                                Console.WriteLine("Unknown applies to type for the rule");
                            }

                            foreach (XmlNode rulePropertyNode in ruleElement.ChildNodes)
                            {
                                if (rulePropertyNode.NodeType == XmlNodeType.Element)
                                {
                                    XmlElement rulePropertyElement = (XmlElement)rulePropertyNode;
                                    if (rulePropertyElement.Name == "Name")
                                    {
                                        rule.Name = rulePropertyElement.InnerText;
                                    }
                                    if (rulePropertyElement.Name == "Statistic")
                                    {
                                        rule.Statistic = rulePropertyElement.InnerText;
                                    }
                                    else if (rulePropertyElement.Name == "ExpectedValue")
                                    {
                                        rule.ExpectedVal = Convert.ToDouble(rulePropertyElement.InnerText);
                                        rule.IsPercentageRule = false;
                                    }
                                    else if (rulePropertyElement.Name == "ExpectedPercentageValue")
                                    {
                                        rule.ExpectedVal = Convert.ToDouble(rulePropertyElement.InnerText);
                                        rule.IsPercentageRule = true;
                                    }
                                    else if (rulePropertyElement.Name == "ComparisonOperator")
                                    {
                                        rule.op = CreateOperator(rulePropertyElement.InnerText);
                                    }    
                                }
                            }

                            if (temp == null)
                            {
                                foreach (var pair in counterData)
                                {
                                    pair.Value.Rules.Add(rule);
                                }
                            }
                            else
                            {
                                foreach (string s in temp)
                                {
                                    try
                                    {
                                        counterData[s].Rules.Add(rule);
                                    }
                                    catch (KeyNotFoundException)
                                    {
                                        Console.WriteLine("The counter is not present in the dataset {0}", s);   
                                    }
                                 }
                            }
                        }
                    }    
                }
            }
        }

        private HashSet<string> GetCounterNames(string counterNames)
        {
            string [] split = counterNames.Split(new char []{','});
            double n = split.Count();

            HashSet<string> names = null;
            if (n > 0)
            {
                names = new HashSet<string>();

                for (int i = 0; i < n; i++ )
                {
                    names.Add(split[i]);
                }
            }
            return names;
        }


        private StatRule CreateStatRule (string appliesTo)
        {
            if (appliesTo == "Global")
            {
                return new GlobalStatRule();    
            }
            else if (appliesTo == "Time")
            {
                return new TimeStatRule();
            }
            else if (appliesTo == "Silo")
            {
                return new SiloStatRule();
            }
            else 
            {
                return null;
            }
        }

        private RuleOperator CreateOperator (string opName)
        {
            if (opName == "EqualsTo")
            {
                return new RuleEqualOperator();
            }
            else if (opName == "GreaterThan")
            {
                return new RuleGraterThanOperator();
            }
            else if (opName == "LessThan")
            {
                return new RuleLessThanOperator();
            }
            else
            {
                return null;
            }
        }
    }
}
