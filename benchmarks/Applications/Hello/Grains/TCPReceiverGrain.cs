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

        bool tcpActive = false;

        public override Task OnActivateAsync()
        {

            Console.WriteLine("OnActivateAsync");
            if (!tcpActive)
            {
                IPHostEntry host;
                IPAddress localIp = null;
                host = Dns.GetHostEntry(Dns.GetHostName());
                foreach (IPAddress ip in host.AddressList)
                {
                    if (ip.AddressFamily.ToString() == "InterNetwork")
                    {
                        localIp = ip;
                        Console.WriteLine("IP is {0} ", localIp);
                    }
                }

                tcpListener = new TcpListener(localIp, 15001);
                tcpListener.Start();
                Util.register(this, 15001, "mygrain");
                tcpActive = true;
                tcpClient = tcpListener.AcceptTcpClient();


            }

            return base.OnActivateAsync();
        }

        public async Task<string> listenMessages()
        {
            NetworkStream clientStream = tcpClient.GetStream();

            byte[] message = new byte[4096];
            int bytesRead;

            bytesRead = 0;



            bytesRead = await clientStream.ReadAsync(message, 0, 4096);
            Console.WriteLine("Echo {0} ", message);



            //message has successfully been received
            ASCIIEncoding encoder = new ASCIIEncoding();


            return encoder.GetString(message);
        }

        private TcpListener tcpListener;
        private TcpClient tcpClient;


        public override Task OnDeactivateAsync()
        {
            tcpListener.Stop();
            return base.OnDeactivateAsync();
        }
    }


}
