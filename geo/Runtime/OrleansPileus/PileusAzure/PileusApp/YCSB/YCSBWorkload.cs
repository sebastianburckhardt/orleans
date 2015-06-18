using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System.Threading;
using PileusApp.Utils;

namespace PileusApp.YCSB
{

    public class YCSBConst
    {
        public static readonly string YCSB_KEY_PREFIX = "user";

        public static readonly int MAXIMUM_NUMBER_OF_YCSB_OPERATIONS = 100;

        public static readonly int NUMBER_OF_AVAILABLE_TRACES = 300;
    }

    public enum YCSBWorkloadType
    {
        Workload_a,
        Workload_b,
        Workload_c
    }

    public class YCSBWorkload
    {
        private static int seed=DateTime.Now.Millisecond;
        

        private List<YCSBOperation> operations;
        private int returnedOperationIndex=0;

        public YCSBWorkload(YCSBWorkloadType type, int numberOfObjects)
        {


            string traceDir=Directory.GetCurrentDirectory() + "\\YCSB\\";

            if (type == YCSBWorkloadType.Workload_a)
                traceDir += "workloada\\";
            else if (type == YCSBWorkloadType.Workload_b)
                traceDir += "workloadb\\";
            else if (type == YCSBWorkloadType.Workload_c)
                traceDir += "workloadb\\";

            Random rand = new Random(Interlocked.Increment(ref seed));
            int traceNumber = rand.Next(1, YCSBConst.NUMBER_OF_AVAILABLE_TRACES);
            traceDir += traceNumber;

            operations = new List<YCSBOperation>();

            string[] lines= File.ReadAllLines(traceDir);

            foreach (string line in lines)
            {
                string command=line.Split(' ')[2];
                command = command.Replace(YCSBConst.YCSB_KEY_PREFIX, "");
                if (Convert.ToInt32(command) > numberOfObjects || command.Equals(""))
                {
                    continue;
                }

                if (line.Contains("READ"))
                {
                    operations.Add(new YCSBOperation(line.Split(' ')[2], YCSBOperationType.READ));
                }
                else if (line.Contains("UPDATE"))
                {
                    operations.Add(new YCSBOperation(line.Split(' ')[2], YCSBOperationType.UPDATE));
                }
            }
        }

        public static List<string> GetAllKeys(int numberOfObjects)
        {
            List<string> result = new List<string>();
            for (int i = 0; i <= numberOfObjects; i++)
                result.Add(YCSBConst.YCSB_KEY_PREFIX + i);

            return result;
        }

        Random rand = new Random();

        public YCSBOperation GetNextOperation()
        {
            if (YCSBConst.MAXIMUM_NUMBER_OF_YCSB_OPERATIONS == returnedOperationIndex)
                return null;

            if (operations.Count > 0 && operations.Count > returnedOperationIndex)
            {
                return operations[returnedOperationIndex++];
            }
            else
            {
                return null;
            }
        }

    }


}
