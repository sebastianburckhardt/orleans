using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Newtonsoft.Json.Linq;


namespace GeoOrleans.Runtime.Common
{
    [Serializable]
    public class TextEntity : TableEntity
    {
        // assume no . in partition key

        public string payload { get; set; }

        public TextEntity()
        {

        }

        public TextEntity(string pPartitionKey, string pRowKey, string  pPayload)
        {
            this.PartitionKey = pPartitionKey;
            this.RowKey = pRowKey;
            this.payload = pPayload;
        }

        /*
        public static ByteEntity FromStringToEntity(string pEntityString)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(pEntityString);
            MemoryStream ms = new MemoryStream(bytes);
            BinaryFormatter bf = new BinaryFormatter();
            return (ByteEntity)bf.Deserialize(ms);
        }

        public static string FromEntityToString(ByteEntity pEntity)
        {
            try
            {
                MemoryStream ms = new MemoryStream();
                BinaryFormatter bf = new BinaryFormatter();
                bf.Serialize(ms, pEntity);
                return Encoding.ASCII.GetString(ms.GetBuffer());
            }
            catch (Exception e)
            {
                Console.Write("Exception " + e.ToString());
            }
            return null; 
       }
*/

    }





}
