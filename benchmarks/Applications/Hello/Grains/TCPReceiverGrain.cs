using Orleans;
using Hello.Interfaces;
using System.Threading.Tasks;
using Common;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System;

namespace Hello.Grains
{
    /// <summary>
    /// Grain implementation class Grain1.
    /// </summary>
    /// 
    public class TCPReceiverGrain : Grain, ITCPReceiverGrain
    {

        public override Task OnActivateAsync()
        {

            IPHostEntry host;
            string localIp = "?";
            host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily.ToString() == "InterNetwork")
                {
                    localIp = ip.ToString();
                    Console.WriteLine("IP is {0} ", localIp);
                }
            }
            Console.WriteLine("IPAddress.Any {0} ",IPAddress.Any);
            
            tcpListener = new TcpListener(IPAddress.Any, 3000);
            tcpClient = tcpListener.AcceptTcpClient();
           
            return base.OnActivateAsync();
        }

        public async Task<string> listenMessages()
        {
            NetworkStream clientStream = tcpClient.GetStream();

            byte[] message = new byte[4096];
            int bytesRead;

            bytesRead = 0;

            int messagesReceived = 0;
            while (messagesReceived++ < 5) { 
            try
            {
                bytesRead = await clientStream.ReadAsync(message, 0 , 4096 );
                Console.WriteLine("Echo {0} ", message);
            }
            catch (Exception e)
            {
                throw new Exception(e.ToString());
            }

            if (bytesRead == 0)
            {
                break;
            }

            //message has successfully been received
            ASCIIEncoding encoder = new ASCIIEncoding();

            }
            return "Done";
        }

        private TcpListener tcpListener;
        private TcpClient tcpClient;


    }


}
