using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Azure.Storage
{
    [Serializable]
    public class ByteEntity : TableEntity
    {
        // assume no . in partition key


        public byte[] payload { get; set; }


        public ByteEntity(string pPartitionKey, string pRowKey, byte[] pPayload)
        {
            this.PartitionKey = pPartitionKey;
            this.RowKey = pRowKey;
            this.payload = pPayload;
        }


        public static ByteEntity FromStringToEntity(string pEntityString)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(pEntityString);
            MemoryStream ms = new MemoryStream(bytes);
            BinaryFormatter bf = new BinaryFormatter();
            return (ByteEntity)bf.Deserialize(ms);
        }

        public static string FromEntityToString(ByteEntity pEntity)
        {
            MemoryStream ms = new MemoryStream();
            BinaryFormatter bf = new BinaryFormatter();
            bf.Serialize(ms, pEntity);
            return Encoding.ASCII.GetString(ms.GetBuffer());
        }

    }

    
   


}
