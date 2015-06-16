using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    public class ServiceLevelAgreementTableEntity : TableEntity
    {

        private ServiceLevelAgreement _sla;

        public ServiceLevelAgreementTableEntity(){}

        public ServiceLevelAgreementTableEntity(ServiceLevelAgreement sla, string containerName, string epochId, string clientName)
        {
            this.SetSLA(sla);
            this.ClientName = clientName;
            this.PartitionKey = containerName + epochId;
            this.RowKey = clientName + sla.Id;
        }

        public byte[] byteSLA { get; set; }

        public string ClientName { get; set; }

        public void SetSLA(ServiceLevelAgreement sla)
        {
            _sla = sla;
            byteSLA = sla.ToBytes();
        }

        public ServiceLevelAgreement GetSLA()
        {
            if (_sla == null)
                _sla = FromBytes(byteSLA);
            return _sla;
        }

        private ServiceLevelAgreement FromBytes(byte[] slaByte)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                stream.Write(slaByte, 0, slaByte.Count());
                stream.Position = 0;
                BinaryFormatter formatter = new BinaryFormatter();
                return (ServiceLevelAgreement)formatter.Deserialize(stream);
            }
        }

    }

}
