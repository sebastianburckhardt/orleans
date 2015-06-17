using Orleans;
using GeoOrleans.Benchmarks.Hello.Interfaces;
using System.Threading.Tasks;
using GeoOrleans.Runtime.Common;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System;

namespace GeoOrleans.Benchmarks.Hello.Grains
{
    /// <summary>
    /// Grain implementation class Grain1.
    /// </summary>
    public class TCPSenderGrain : Grain, ITCPSenderGrain
    {
        public async Task<string> SayHello(string arg)
        {

            TcpClient client = new TcpClient();

            int retries = 100;
            while (retries-- > 0)
            {
                try
                {

                    AzureEndpoints.ServiceDeployments myRegion = GeoOrleans.Runtime.Common.Util.GetRegion();

                    AzureEndpoints.ServiceDeployments otherRegion = myRegion == AzureEndpoints.ServiceDeployments.OrleansGeoUsWest ?
                        AzureEndpoints.ServiceDeployments.OrleansGeoEuropeWest :
                        (myRegion == AzureEndpoints.ServiceDeployments.OrleansGeoEuropeWest ?
                            AzureEndpoints.ServiceDeployments.OrleansGeoUsWest : AzureEndpoints.ServiceDeployments.Simulator);
                    //          Endpoints.ServiceDeployments otherRegion = myRegion;

                    Tuple<IPAddress, int> address = await GeoOrleans.Benchmarks.Common.Util.getGrainAddress(otherRegion, typeof(TCPReceiverGrain), "mygrain");

                    IPEndPoint serverEndPoint = new IPEndPoint(address.Item1, address.Item2);

                    //    IPEndPoint serverEndPoint = new IPEndPoint(IPAddress.Parse("172.31.46.27"), 15000);

                    client.Connect(serverEndPoint);

                    NetworkStream clientStream = client.GetStream();

                    ASCIIEncoding encoder = new ASCIIEncoding();
                    byte[] buffer = encoder.GetBytes(arg);

                    await clientStream.WriteAsync(buffer, 0, buffer.Length);
                    clientStream.Flush();
                    retries = 0;
                }
                catch (Exception e)
                {

                    if (retries == 0) throw e;
                }
            }
            return "sent";
        }



    }
}
