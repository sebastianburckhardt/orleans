using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.WebSockets;
using System.Diagnostics;
using System.Web;
using Benchmarks;
using Newtonsoft.Json.Linq;
using System.Collections.Specialized;
using Common;
using ClusterProtocol.Interfaces;
using Newtonsoft.Json;

namespace Orleans.Frontend
{
    /// <summary>
    /// An HttpListener for receiving requests (both regular http requests and websocket connections).
    /// Used both for running locally during simulation, or in a worker role in Azure for the Orleans front end.
    /// </summary>
    public class ClusterProtocolServer
    {

        public ClusterProtocolServer(string deployment, string instanceid, bool runningincloud,
            bool securehttp, Action<string> tracer, Action<string> diag)
        {
            this.deployment = deployment;
            this.instanceid = instanceid;
            this.runningincloud = runningincloud;
            this.securehttp = securehttp;
            this.tracer = tracer;
            this.diag = diag;
        }

        internal string deployment;
        internal string instanceid;
        internal bool runningincloud;
        internal bool securehttp;
        internal Action<string> tracer;
        internal Action<string> diag;

        internal HttpListener listener;

        public void Start(string listenerPrefix)
        {

            listener = new HttpListener();
            listener.Prefixes.Add(listenerPrefix);
            listener.AuthenticationSchemes = AuthenticationSchemes.Anonymous;

            listener.Start();

            servertask = Serve();
        }

        private Task servertask;

        public async Task Serve()
        {
            try
            {
                while (true)
                {
                    HttpListenerContext listenerContext = await listener.GetContextAsync();

                    var request = listenerContext.Request;
                    var response = listenerContext.Response;

                    var url = new Uri("http://" + request.UserHostName + request.RawUrl);
                    var verb = request.IsWebSocketRequest ? "WS" : request.HttpMethod;

                    string body = null;
                    if (request.HasEntityBody)
                    {
                        System.IO.Stream bodystr = request.InputStream;
                        System.IO.StreamReader reader = new System.IO.StreamReader(bodystr, request.ContentEncoding);
                        body = await reader.ReadToEndAsync();
                    }

                    //Potential bug. Ask Sebastian 
                    var urlpath = url.AbsolutePath.Split('/').Select(s => HttpUtility.UrlDecode(s)).Skip(1).ToArray();
                    //var urlpath = new String[] { url.AbsolutePath.Split('/').Last() };
                    var arguments = HttpUtility.ParseQueryString(url.Query);

                    tracer(string.Format("---> {0} {1}\n{2}", verb, request.RawUrl, body));


                    try
                    {
                        string responsestring = null;

                        if (urlpath[0] == "mgt")
                        {
                            var clusterrep = GrainFactory.GetGrain<IClusterRep>(0);

                            var cmd = urlpath[1].ToLower();

                            if (verb == "GET" && cmd == "info")
                            {
                                var info = await clusterrep.GetGlobalInfo();
                                responsestring = JsonConvert.SerializeObject(info).ToString();
                            }
                            else if (verb == "POST" && cmd == "info")
                            {
                                var info = JsonConvert.DeserializeObject<Dictionary<string, DeploymentInfo>>(body);
                                info = await clusterrep.PostInfo(info);
                                responsestring = JsonConvert.SerializeObject(info).ToString();
                            }
                            else
                            {
                                throw new HttpException((int)HttpStatusCode.BadRequest, "invalid request");
                            }
                        }
                        else
                        {
                            throw new HttpException((int)HttpStatusCode.BadRequest, "invalid request");
                        }


                        // prevent caching of responses
                        response.Headers.Add("Cache-Control", "no-cache");

                        if (responsestring != null)
                            EncodeJsonResponse(response, responsestring);
                        else 
                            throw new Exception("response string should not be null");

                        response.Close();

                        tracer("<- HttpResponse " + (responsestring.Length > 100 ? ("(" + responsestring.Length + " characters)") : responsestring));
                    }
                    catch (Exception ee)
                    {
                        while (ee is AggregateException)
                            ee = ee.InnerException;

                        if (ee is HttpException)
                        {
                            var he = (HttpException)ee;
                            response.StatusCode = he.GetHttpCode();
                            response.StatusDescription = he.Message;
                        }
                        else
                        {
                            response.StatusCode = (int)HttpStatusCode.InternalServerError;
                            response.StatusDescription = "Server Error";
                        }

                        // send Json with detailed error description to Client
                        var json = JObject.FromObject(new
                        {
                            code = response.StatusCode,
                            error = response.StatusDescription,
                            exception = new
                            {
                                type = ee.GetType().Name,
                                message = ee.Message,
                                stacktrace = ee.StackTrace,
                            }
                        });
                        EncodeJsonResponse(response, json.ToString());
                        response.Close();

                        tracer("<- HttpErrorResponse: " + ee.GetType().Name + ": " + ee.Message + "\n" + ee.StackTrace);
                    }
                }
            }
            catch (HttpListenerException e)
            {
                diag("HttpListenerException caught: " + e.Message);
            }
        }
        


        public void Stop()
        {

            try
            {
                diag("Stopping ClusterProtocol Listener...");
                listener.Stop();
                diag("OK.");

            }
            catch (Exception e)
            {
                diag("Exception caught while stopping ClusterProtocol listener:" + e);
            }

        }

        public string GetIdentity()
        {
            return instanceid;
        }
 

        private static void EncodeJsonResponse(HttpListenerResponse response, string responsestring)
        {
            System.Text.Encoding encoding = response.ContentEncoding;
            if (encoding == null)
            {
                encoding = System.Text.Encoding.UTF8;
                response.ContentEncoding = encoding;
            }
            byte[] buffer = encoding.GetBytes(responsestring);
            // Get a response stream and write the response to it.
            response.ContentType = "application/json";
            response.ContentLength64 = buffer.Length;
            System.IO.Stream output = response.OutputStream;
            System.Console.WriteLine(response.ToString());
            output.Write(buffer, 0, buffer.Length);
            // Send the response
            output.Close();
        }




  
    }

}