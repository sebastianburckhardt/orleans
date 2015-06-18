using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;

namespace PileusApp.Utils
{
    public delegate string AdditionalPeriodicStat();

    public delegate int EmulationTime();

    /// <summary>
    /// Keeps track of average of added samples in a dictionary where the keys are seconds. 
    /// Hence, if two samples are added in a second, their average is recorded. 
    /// </summary>
    public class Sampler
    {
        public enum OutputType{
            Average,
            Total
        } 

        

        private ConcurrentDictionary<long, ConcurrentDictionary<string, MeasurementType>> samples;

        private Dictionary<string, OutputType> sampleNames;

        private EmulationTime emulationTime;


        /// <summary>
        /// 
        /// </summary>
        /// <param name="periodicOutput">true if periodic outputs on Console is needed.</param>
        /// <param name="sampleNames">List of all sample names along with their types.</param>
        /// <param name="emulationTime">Pointer to a function returning the emulation time. </param>
        public Sampler(bool periodicOutput, Dictionary<string, OutputType> sampleNames, EmulationTime emulationTime)
        {
            samples = new ConcurrentDictionary<long, ConcurrentDictionary<string, MeasurementType>>();

            if (periodicOutput)
            {
                new Thread(PeriodicStat).Start();
            }

            this.sampleNames = sampleNames;

            this.emulationTime = emulationTime;
        }

        /// <summary>
        /// Add a new sample to the sampler.
        /// </summary>
        /// <param name="sampleName">Name of the sample</param>
        /// <param name="sampleValue">Value of the Sample</param>
        public void AddSample(string sampleName , float sampleValue)
        {
            int currentEmulationHour = emulationTime.Invoke();

            if (samples.ContainsKey(currentEmulationHour))
            {
                ConcurrentDictionary<string, MeasurementType> tmp = samples[currentEmulationHour];
                if (tmp.ContainsKey(sampleName))
                {
                    tmp[sampleName].Values.Add(sampleValue);
                    samples[currentEmulationHour] = tmp;
                }
                else
                {
                    MeasurementType m = new MeasurementType();
                    m.Values.Add(sampleValue);
                    tmp[sampleName] = m;
                    samples[currentEmulationHour] = tmp;
                }
                
            }
            else
            {
                ConcurrentDictionary<string, MeasurementType> tmp = new ConcurrentDictionary<string, MeasurementType>();
                MeasurementType m = new MeasurementType();
                m.Values.Add(sampleValue);
                tmp[sampleName] = m;
                samples[currentEmulationHour] = tmp;
            }

        }

        public void AddSampleName(string sampleName, OutputType totalOrAve)
        {
            sampleNames[sampleName] = totalOrAve;
        }

        public float GetSampleValue(string sampleName)
        {
            int currentEmulationHour = emulationTime.Invoke();
            float result = 0;
            if (samples.ContainsKey(currentEmulationHour))
            {
                ConcurrentDictionary<string, MeasurementType> tmp = samples[currentEmulationHour];
                if (tmp.ContainsKey(sampleName) && sampleNames[sampleName] == OutputType.Total)
                {
                    result = tmp[sampleName].GetTotal();
                }
                else if (tmp.ContainsKey(sampleName) && sampleNames[sampleName] == OutputType.Average)
                {
                    result = tmp[sampleName].GetAverage();
                }
            }
            return result;
        }

        public ConcurrentBag<float> GetAllSampleValues(string sampleName)
        {
            int currentEmulationHour = emulationTime.Invoke();
            ConcurrentBag<float> result = null;
            if (samples.ContainsKey(currentEmulationHour))
            {
                ConcurrentDictionary<string, MeasurementType> tmp = samples[currentEmulationHour];
                if (tmp.ContainsKey(sampleName))
                {
                    result = tmp[sampleName].Values;
                }
            }
            return result;
        }

        public override string ToString()
        {
            string result="utchour,";
            foreach (string name in sampleNames.Keys)
            {
                result += name + ",";
            }
            result += "\n";
            
            List<long> sortedTicks=samples.Keys.ToList();
            sortedTicks.Sort();

            foreach (long tick in sortedTicks)
            {
                ConcurrentDictionary<string, MeasurementType> tmp = samples[tick];
                string s = "";
                foreach (string sampleName in sampleNames.Keys)
                {
                    if (sampleNames[sampleName] == OutputType.Total && tmp.ContainsKey(sampleName))
                    {
                        s += tmp[sampleName].GetTotal() + ",";
                    }
                    else if (sampleNames[sampleName] == OutputType.Average && tmp.ContainsKey(sampleName))
                    {
                        s += tmp[sampleName].GetAverage() + ",";
                    }
                    else if (!tmp.ContainsKey(sampleName))
                    {
                        //this sample is not taken at this particular time. 
                        //thus we put zero.
                        s += "0,";
                    }
                    
                }
                result +=tick + "," + s + "\n"; 
            }

            return result;
        }

        /// <summary>
        /// Periodically prints to the console the sampler's state. 
        /// </summary>
        private void PeriodicStat()
        {
            while (true)
            {
                int currentEmulationHour = emulationTime.Invoke();

                string result="";
                if (samples.ContainsKey(currentEmulationHour))
                {
                    result = "";
                    ConcurrentDictionary<string, MeasurementType> tmp = samples[currentEmulationHour];
                    foreach (string sampleName in sampleNames.Keys)
                    {
                        if (tmp.ContainsKey(sampleName) && sampleNames[sampleName] == OutputType.Total)
                        {
                            result += sampleName + ":" + tmp[sampleName].GetTotal() + "  ,  ";
                        }
                        else if (tmp.ContainsKey(sampleName) && sampleNames[sampleName] == OutputType.Average)
                        {
                            result += sampleName + ":" + tmp[sampleName].GetAverage() + "  ,  ";
                        }
                    }
                    Console.WriteLine(result + "\n");
                }
                else
                {
                    Console.WriteLine(result + "\n");
                }

                Thread.Sleep(PileusAppConstPool.SAMPLER_STAT_INTERVAL);
            }
        }
    }
}
