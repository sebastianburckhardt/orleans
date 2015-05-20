using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans
{
    internal static class ConsoleText
    {
        public static void WriteError(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(msg);
            Console.ResetColor();
        }

        public static void WriteError(string msg, Exception exc)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(msg);
            Console.WriteLine("Exception = " + exc);
            Console.ResetColor();
        }

        public static void WriteStatus(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(msg);
            Console.ResetColor();
        }

        public static void WriteStatus(string format, params object[] args)
        {
            WriteStatus(string.Format(format, args));
        }

        public static void WriteUsage(string msg)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(msg);
            Console.ResetColor();
        }

        public static void WriteLine(string msg)
        {
            Console.WriteLine(msg);
        }

        public static void WriteLine(string format, params object[] args)
        {
            Console.WriteLine(format,args);
        }
    }
}
