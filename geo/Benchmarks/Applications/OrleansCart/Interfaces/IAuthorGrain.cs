using Orleans;
using System;

namespace OrleansCart.Interfaces
{
    /// <summary>
    /// </summary>
    public interface IAuthorGrain : IGrain
    {

        string a_fname {get;set;}
        string a_lname {get;set;}
        string a_mname {get;set;}
        DateTime a_dob {get;set;}
        string a_bio {get;set;}


        
    }
}
