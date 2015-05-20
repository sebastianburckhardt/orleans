using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using Orleans.Messaging;

using System.Reflection;

namespace Orleans.WFUtils
{
#if !DISABLE_WF_INTEGRATION
    internal class WFGatewayListProvider : IGatewayListProvider, IGatewayListObserverable
    {
        private readonly IGatewayNamingService gwProvider;
        private readonly ClientConfiguration config;

        public WFGatewayListProvider(ClientConfiguration conf)
        {
            config = conf;

            string assemblyName = "OrleansWFHost";
            string className = "Orleans.WindowsFabric.Host.WFNamingServiceProvider";
            
            Type type = OrleansClient.LoadTypeThroughReflection(
                assemblyName,
                className);

            ConstructorInfo ctor = type.GetConstructor(new[] { typeof(Uri), typeof(List <IPEndPoint>) });            
            gwProvider = (IGatewayNamingService)ctor.Invoke(new object[] { config.WindowsFabricServiceName, config.Gateways });
        }

        #region Implementation of IGatewayListProvider

        public List<IPEndPoint> GetGateways()
        {
            return gwProvider.GetGateways();
        }

        public TimeSpan MaxStaleness 
        {
            get { return gwProvider.MaxStaleness; }
        }

        public bool IsUpdatable
        {
            get { return gwProvider.IsUpdatable; }
        }

        #endregion

        #region Implementation of IGatewayListObserverable

        public bool SubscribeToGatewayNotificationEvents(IGatewayListListener listener)
        {
            return gwProvider.SubscribeToGatewayNotificationEvents(listener);
        }

        public bool UnSubscribeFromGatewayNotificationEvents(IGatewayListListener listener)
        {
            return gwProvider.UnSubscribeFromGatewayNotificationEvents(listener);
        }

        #endregion
    }
#endif
}

//string methodName = "Start";
//MethodInfo method = type.GetMethod(methodName);
//if (method == null) throw new InvalidOperationException(string.Format("Cannot find method {0} of class {1} in assembly {2}", methodName, className, assemblyName));
//method.Invoke(gwProvider, new object[0]);
//gwProvider.Start();