using Orleans;
using System;

namespace OrleansCart.Interfaces
{
    /// <summary>
    /// </summary>
    public interface ICustomerGrain : IGrain
    {

        /// <summary>
        /// Unique User Name for Customer
        /// </summary>
        string c_uname { get; set; }
        /// <summary>
        /// User Password for Customer
        /// </summary>
        string c_passwd;
        /// <summary>
        /// First name for Customer
        /// </summary>
        string c_fname;
        /// <summary>
        /// Last Name for Cusomer
        /// </summary>
        string c_lname;
        /// <summary>
        /// Address ID for Custoemr
        /// </summary>
        long c_ddr_id;

        
    }
}
