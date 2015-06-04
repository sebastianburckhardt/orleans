using Orleans;
using Hello.Interfaces;
using System.Threading.Tasks;
using Common;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Hello.Grains
{
    /// <summary>
    /// Grain implementation class Grain1.
    /// </summary>
    public class TCPSenderGrain : Grain, ITCPSenderGrain
    {
        public async Task<string> SayHello(string arg)
        {

            TcpClient client = new TcpClient();

            IPEndPoint serverEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 3000);

            client.Connect(serverEndPoint);

            NetworkStream clientStream = client.GetStream();

            ASCIIEncoding encoder = new ASCIIEncoding();
            byte[] buffer = encoder.GetBytes(arg);

            await clientStream.WriteAsync(buffer, 0, buffer.Length);
            clientStream.Flush();

            return "sent";
        }



    }
}
