using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using AppFabricSample;

namespace UnitTests
{
    [TestClass]
    public class AppFabricSampleTests : UnitTestBase
    {
        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Apps")]
        public void AppFabricSample()
        {
            ICustomer customer = CustomerFactory.GetGrain(Guid.NewGuid());
            customer.AddProductToCart(new Product { Name = "Product1", Quantity = 1, UnitPrice = 0.5f }).Wait();
            customer.AddProductToCart(new Product { Name = "Product2", Quantity = 2, UnitPrice = 0.7f }).Wait();

            Order order = customer.Checkout().Result;
        }
    }
}
