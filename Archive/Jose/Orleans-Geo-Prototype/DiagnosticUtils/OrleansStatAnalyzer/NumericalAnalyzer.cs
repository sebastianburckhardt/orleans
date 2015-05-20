using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrleansStatAnalyzer
{
    public class NumericalAnalyzer
    {
        public static double Average(List<double> data)
        {
            return data.Average();
        }

        public static double Min(List<double> data)
        {
            return data.Min();
        }

        public static double Max(List<double> data)
        {
            return data.Max();
        }

        public static double Variance(List<double> data)
        {
            double var = 0.0;
            double avg = data.Average();
            double d = 0.0;
            double sum1 = 0.0;
            double sum2 = 0.0;
            double n = data.Count();
            foreach (double x in data)
            {
                d = x - avg;
                sum1 += d*d;
                sum2 += d;
            }

            var = (sum1 - sum2*sum2/n)/(n - 1);

            return var;
        }

        public static double Sdev(List<double> data)
        {
            double var = Variance(data);
            return Math.Sqrt(var);
        }

        public static double Median(List<double> data)
        {
            List<double> temp = new List<double>();

            foreach (var x in data)
            {
                temp.Add(x);
            }

            int n = temp.Count();
            temp.Sort();
            
            if (n == 1)
            {
                return temp[0];
            }

            if ((n % 2) != 0)
            {
                return temp.ElementAt((n + 1)/2 - 1);
            }
            else
            {
                double x1 = temp.ElementAt(n/2 - 1);
                double x2 = temp.ElementAt(n/2);
                return (x1 + x2)/2;
            }
         }

        public static double Percentile(List<double> data, double percentage)
        {
            List<double> temp = new List<double>();

            foreach (var x in data)
            {
                temp.Add(x);
            }

            int n = temp.Count();
            temp.Sort();

            if (n == 1)
            {
                return temp[0];
            }

            int position = (int)Math.Round(n*percentage/100 + 0.5);

            if (position > n)
            {
                return temp.ElementAt(n - 1);    
            }
            else
            {
                return temp.ElementAt(position - 1);
            }
        }

        public static List<double> Normalize(List<double> data)
        {
            List<double> normalizedData = new List<double>();

            double max = data.Max();
            double min = data.Min();

            foreach (double x in data)
            {
                double z = 0.0;
                if (max == min)
                {
                    z = 1.0;
                }
                else
                {
                    z = (x - min)/(max - min);     
                }
                
                normalizedData.Add(z);
            }
            return normalizedData;
        }

        public static List<double> Zscore(List<double> data)
        {
            List<double> zscores = new List<double>();

            double mean = NumericalAnalyzer.Average(data);
            double sdev = NumericalAnalyzer.Sdev(data);

            foreach (double x in data)
            {
                if (sdev != 0.0)
                {
                    double z = Math.Abs((x - mean)) / sdev;
                    zscores.Add(z);   
                }
                else
                {
                    zscores.Add(0.0);
                }
            }
            return zscores;
        }

        public static double Range(List<double> data)
        {
            return NumericalAnalyzer.Max(data) - NumericalAnalyzer.Min(data);
        }
    }
}
