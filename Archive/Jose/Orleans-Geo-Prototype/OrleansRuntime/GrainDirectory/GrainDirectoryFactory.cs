using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Orleans.Runtime.GrainDirectory;


namespace Orleans.Runtime.GrainDirectory
{
    internal static class GrainDirectoryFactory
    {
        internal static ILocalGrainDirectory CreateLocalGrainDirectory(Silo silo)
        {
            // return new DirectoryRouter(silo.LocalMessageCenter, silo.GlobalConfig.SeedNodes, silo.LocalScheduler, silo.LocalTypeManager);
            // return new LocalGrainDirectory(silo);
            throw new NotImplementedException();
        }
    }
}
