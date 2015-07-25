 

﻿namespace Orleans.Runtime.GossipNetwork
 {
     // Interface to receive notifications from IGossipOracle about changes in gossip data.
     internal interface IGossipListener
     {
         /// <summary>
         /// Receive notifications about gossip.
         /// </summary>
         void GossipNotification(GossipData changedentries);
     }
 }