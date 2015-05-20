using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Reflection;

namespace Orleans.Counter.Control
{
    class Program
    {
        static int Main(string[] args)
        {
            var prog = new CounterControl();

            // Program ident
            AssemblyName thisProg = Assembly.GetExecutingAssembly().GetName();
            string progTitle = string.Format("{0} v{1}",
                thisProg.Name,
                thisProg.Version.ToString());
            ConsoleText.WriteStatus(progTitle);
            Console.Title = progTitle;

            int result;
            if (!prog.ParseArguments(args))
            {
                prog.PrintUsage();
                result = -1;
            }
            else
            {
                result = prog.Run();
            }

            if (prog.PauseAtEnd)
            {
                Console.WriteLine("Press any key to exit");
                Console.ReadKey();
            }

            return result;
        }
    }
}
