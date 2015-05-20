//*********************************************************//
//    Copyright (c) Microsoft. All rights reserved.
//    
//    Apache 2.0 License
//    
//    You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//    
//    Unless required by applicable law or agreed to in writing, software 
//    distributed under the License is distributed on an "AS IS" BASIS, 
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
//    implied. See the License for the specific language governing 
//    permissions and limitations under the License.
//
//*********************************************************
using System;
using System.Threading;
using System.Windows.Forms;
using Examples.Interfaces;
using Orleans;

namespace Examples.Test
{
    /// <summary>
    /// Orleans test silo host
    /// </summary>
    public class Program
    {
        public static AppDomain hostDomain;

        static void Main(string[] args)
        {
            // The Orleans silo environment is initialized in its own app domain in order to more
            // closely emulate the distributed situation, when the client and the server cannot
            // pass data via shared memory.
            hostDomain = AppDomain.CreateDomain("OrleansHost", null, new AppDomainSetup
            {
                AppDomainInitializer = InitSilo,
                AppDomainInitializerArguments = args,
            });

            GrainClient.Initialize("DevTestClientConfiguration.xml");

            /* var grain = PersonFactory.GetGrain(0);

            // If the name is set, we've run this code before.
            var name = grain.FirstName.Result;

            if (name != null)
            {
                Console.WriteLine("\n\nThis was found in the persistent store: {0}, {1}, {2}\n\n",
                    name,
                    grain.LastName.Result,
                    grain.Gender.Result.ToString());
            }
            else
            {
                grain.SetPersonalAttributes(new PersonalAttributes { FirstName = "John", LastName = "Doe", Gender = GenderType.Male }).Wait();
                Console.WriteLine("\n\nWe just wrote something to the persistent store. Please verify!\n\n");
            }
            */

            //var acc = Examples.Interfaces.AccountGrainFactory.GetGrain(0);
            //Console.WriteLine("\n\nbalance = {0}\n\n", acc.Balance().Result);


            BootUserInterface();
            Console.WriteLine("Orleans Silo is running. Press enter to terminate.");
            
           
            Console.ReadLine();

            hostDomain.DoCallBack(ShutdownSilo);
        }

        private static void BootUserInterface()
        {
            Thread t = new Thread(UserInterfaceThread);
            t.IsBackground = true;
            t.SetApartmentState(ApartmentState.STA);
            t.Start();
        }
        private static void UserInterfaceThread(object arg)
        {
            TestButtonsForm frm = new TestButtonsForm();  // use your own
            Application.Run(frm);
        }

        public void Menu()
        {
        }

        static void InitSilo(string[] args)
        {
            hostWrapper = new OrleansHostWrapper(args);

            if (!hostWrapper.Run())
            {
                Console.Error.WriteLine("Failed to initialize Orleans silo");
            }
        }

        static void ShutdownSilo()
        {
            if (hostWrapper != null)
            {
                hostWrapper.Dispose();
                GC.SuppressFinalize(hostWrapper);
            }
        }

        private static OrleansHostWrapper hostWrapper;
    }
}
