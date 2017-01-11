﻿using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Services;

namespace Tester
{
    public interface ICustomGrainServiceClient : IGrainServiceClient<ICustomGrainService>
    {
        Task<string> GetHelloWorldUsingCustomService();
        Task<string> GetServiceConfigProperty(string propertyName);
    }

    public interface ICustomGrainService : IGrainService
    {
        Task<string> GetHelloWorldUsingCustomService(GrainReference reference);
        Task<string> GetServiceConfigProperty(string propertyName);
    }
}