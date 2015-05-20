﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans;
using Orleans.Management;
using Echo;


namespace Echo.Grains
{
    public interface IEchoTaskGrainState : IGrainState
    {
        int MyId { get; set; }
        string LastEcho { get; set; }
    }

    public class EchoGrain : GrainBase<IEchoTaskGrainState>, IEchoGrain
    {
        private readonly OrleansLogger logger;

        public EchoGrain()
        {
            this.logger = base.GetLogger();
            logger.Info(GetType().FullName + " created");
        }

        public Task<string> LastEcho
        {
            get { return Task.FromResult(State.LastEcho); }
        }

        public Task<string> Echo(string data)
        {
            logger.Info("IEchoGrain.Echo=" + data);
            State.LastEcho = data;
            return Task.FromResult(data);
        }

        public Task<string> EchoError(string data)
        {
            logger.Info("IEchoGrain.EchoError=" + data);
            State.LastEcho = data;
            throw new Exception(data);
        }
    }

    public class EchoTaskGrain : GrainBase<IEchoTaskGrainState>, IEchoTaskGrain
    {
        private readonly OrleansLogger logger;

        public Task<int> MyIdAsync { get { return Task.FromResult(State.MyId); } }
        public Task<string> LastEchoAsync { get { return Task.FromResult(State.LastEcho); } }

        public EchoTaskGrain()
        {
            this.logger = base.GetLogger();
            logger.Info(GetType().FullName + " created");
        }

        public Task<string> EchoAsync(string data)
        {
            logger.Info("IEchoGrainAsync.Echo=" + data);
            State.LastEcho = data;
            return Task.FromResult(data);
        }

        public Task<string> EchoErrorAsync(string data)
        {
            logger.Info("IEchoGrainAsync.EchoError=" + data);
            State.LastEcho = data;
            throw new Exception(data);
        }

        private AsyncValue<string> EchoErrorAV(string data)
        {
            logger.Info("IEchoGrainAsync.EchoErrorAV=" + data);
            State.LastEcho = data;
            throw new Exception(data);
        }

        public async Task<string> AwaitMethodErrorAsync(string data)
        {
            logger.Info("IEchoGrainAsync.CallMethodErrorAsync=" + data);
            return await EchoErrorAsync(data);
        }

        public async Task<string> AwaitAVMethodErrorAsync(string data)
        {
            logger.Info("IEchoGrainAsync.CallMethodErrorAsync=" + data);
            return await EchoErrorAV(data).AsTask();
        }

        public async Task<string> AwaitAVGrainCallErrorAsync(string data)
        {
            logger.Info("IEchoGrainAsync.AwaitAVGrainErrorAsync=" + data);
            IEchoGrain avGrain = EchoGrainFactory.GetGrain(this.GetPrimaryKeyLong());
            //return await avGrain.EchoError(data).AsTask();
            return await avGrain.EchoError(data);
        }

        public Task<int> BlockingCallTimeoutAsync(TimeSpan delay)
        {
            logger.Info("IEchoGrainAsync.BlockingCallTimeout Delay={0}", delay);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            Thread.Sleep(delay);
            logger.Info("IEchoGrainAsync.BlockingCallTimeout Awoke from sleep after {0}", sw.Elapsed);
            throw new InvalidOperationException("Timeout should have been returned to caller before " + delay);
        }

        public Task PingAsync()
        {
            logger.Info("IEchoGrainAsync.Ping");
            return TaskDone.Done;
        }

        public Task PingLocalSiloAsync()
        {
            logger.Info("IEchoGrainAsync.PingLocal");
            SiloAddress mySilo = _Data.Address.Silo;
            return GetSiloControlReference(mySilo).Ping("PingLocal");
        }

        public Task PingRemoteSiloAsync(SiloAddress siloAddress)
        {
            logger.Info("IEchoGrainAsync.PingRemote");
            return GetSiloControlReference(siloAddress).Ping("PingRemote");
        }

        public async Task PingOtherSiloAsync()
        {
            logger.Info("IEchoGrainAsync.PingOtherSilo");
            SiloAddress mySilo = this._Data.Silo;

            IOrleansManagementGrain mgmtGrain = OrleansManagementGrainFactory.GetGrain(RuntimeInterfaceConstants.SystemManagementId);
            var silos = await mgmtGrain.GetHosts();

            SiloAddress siloAddress = silos.Where(pair => !pair.Key.Equals(mySilo)).Select(pair => pair.Key).First();
            logger.Info("Sending Ping to remote silo {0}", siloAddress);

            await GetSiloControlReference(siloAddress).Ping("PingOtherSilo-" + siloAddress);
            logger.Info("Ping reply received for {0}", siloAddress);
        }

        public async Task PingClusterMemberAsync()
        {
            logger.Info("IEchoGrainAsync.PingClusterMemberAsync");
            SiloAddress mySilo = this._Data.Silo;

            IOrleansManagementGrain mgmtGrain = OrleansManagementGrainFactory.GetGrain(RuntimeInterfaceConstants.SystemManagementId);
            var silos = await mgmtGrain.GetHosts();

            SiloAddress siloAddress = silos.Where(pair => !pair.Key.Equals(mySilo)).Select(pair => pair.Key).First();
            logger.Info("Sending Ping to remote silo {0}", siloAddress);

            IRemoteSiloStatusOracle oracle = RemoteSiloStatusOracleFactory.GetSystemTarget(Constants.MembershipOracleId, siloAddress);
            await oracle.Ping(1);
            logger.Info("Ping reply received for {0}", siloAddress);
        }

        private ISiloControl GetSiloControlReference(SiloAddress silo)
        {
            return SiloControlFactory.GetSystemTarget(Constants.SiloControlId, silo);
        }
    }

    public class BlockingEchoTaskGrain : GrainBase<IEchoTaskGrainState>, IBlockingEchoTaskGrain
    {
        private readonly OrleansLogger logger;

        public Task<int> MyId
        {
            get { return Task.FromResult(State.MyId); }
        }

        public Task<string> LastEcho
        {
            get { return Task.FromResult(State.LastEcho); }
        }

        public BlockingEchoTaskGrain()
        {
            this.logger = base.GetLogger();
            logger.Info(GetType().FullName + " created");
        }

        public Task<string> Echo(string data)
        {
            string name = GetType().Name + ".Echo";

            logger.Info(name + " Data=" + data);
            State.LastEcho = data;
            var result = Task.FromResult(data);
            logger.Info(name + " Result=" + result);
            return result;
        }

        public async Task<string> CallMethodTask_Await(string data)
        {
            string name = GetType().Name + ".CallMethodTask_Await";

            logger.Info(name + " Data=" + data);
            IEchoTaskGrain avGrain = EchoTaskGrainFactory.GetGrain(this.GetPrimaryKeyLong());
            var result = await avGrain.EchoAsync(data);
            logger.Info(name + " Result=" + result);
            return result;
        }

        public async Task<string> CallMethodAV_Await(string data)
        {
            string name = GetType().Name + ".CallMethodAV_Await";

            logger.Info(name + " Data=" + data);
            IEchoGrain avGrain = EchoGrainFactory.GetGrain(this.GetPrimaryKeyLong());
            var result = await avGrain.Echo(data);
            logger.Info(name + " Result=" + result);
            return result;
        }

        #pragma warning disable 1998
        public async Task<string> CallMethodTask_Block(string data)
        {
            string name = GetType().Name + ".CallMethodTask_Block";

            logger.Info(name + " Data=" + data);
            IEchoTaskGrain avGrain = EchoTaskGrainFactory.GetGrain(this.GetPrimaryKeyLong());
            var result = avGrain.EchoAsync(data).Result;
            logger.Info(name + " Result=" + result);
            return result;
        }
        #pragma warning restore 1998

        #pragma warning disable 1998
        public async Task<string> CallMethodAV_Block(string data)
        {
            string name = GetType().Name + ".CallMethodAV_Block";

            logger.Info(name + " Data=" + data);
            IEchoGrain avGrain = EchoGrainFactory.GetGrain(this.GetPrimaryKeyLong());
            var result = avGrain.Echo(data).Result;
            logger.Info(name + " Result=" + result);
            return result;
        }
        #pragma warning restore 1998
    }

    [Reentrant]
    public class ReentrantBlockingEchoTaskGrain : GrainBase<IEchoTaskGrainState>, IReentrantBlockingEchoTaskGrain
    {
        private readonly OrleansLogger logger;

        public Task<int> MyId
        {
            get { return Task.FromResult(State.MyId); }
        }

        public Task<string> LastEcho
        {
            get { return Task.FromResult(State.LastEcho); }
        }

        public ReentrantBlockingEchoTaskGrain()
        {
            this.logger = base.GetLogger();
            logger.Info(GetType().FullName + " created");
        }

        public Task<string> Echo(string data)
        {
            string name = GetType().Name + ".Echo";

            logger.Info(name + " Data=" + data);
            State.LastEcho = data;
            var result = Task.FromResult(data);
            logger.Info(name + " Result=" + result);
            return result;
        }

        public async Task<string> CallMethodTask_Await(string data)
        {
            string name = GetType().Name + ".CallMethodTask_Await";

            logger.Info(name + " Data=" + data);
            IEchoTaskGrain avGrain = EchoTaskGrainFactory.GetGrain(this.GetPrimaryKeyLong());
            var result = await avGrain.EchoAsync(data);
            logger.Info(name + " Result=" + result);
            return result;
        }

        public async Task<string> CallMethodAV_Await(string data)
        {
            string name = GetType().Name + ".CallMethodAV_Await";

            logger.Info(name + " Data=" + data);
            IEchoGrain avGrain = EchoGrainFactory.GetGrain(this.GetPrimaryKeyLong());
            var result = await avGrain.Echo(data);
            logger.Info(name + " Result=" + result);
            return result;
        }

#pragma warning disable 1998
        public async Task<string> CallMethodTask_Block(string data)
        {
            string name = GetType().Name + ".CallMethodTask_Block";

            logger.Info(name + " Data=" + data);
IEchoTaskGrain avGrain = EchoTaskGrainFactory.GetGrain(this.GetPrimaryKeyLong());
            var result = avGrain.EchoAsync(data).Result;
            logger.Info(name + " Result=" + result);
            return result;
        }
#pragma warning restore 1998

#pragma warning disable 1998
        public async Task<string> CallMethodAV_Block(string data)
        {
            string name = GetType().Name + ".CallMethodAV_Block";

            logger.Info(name + " Data=" + data);
            IEchoGrain avGrain = EchoGrainFactory.GetGrain(this.GetPrimaryKeyLong());
            var result = avGrain.Echo(data).Result;
            logger.Info(name + " Result=" + result);
            return result;
        }
#pragma warning restore 1998
    }
}
