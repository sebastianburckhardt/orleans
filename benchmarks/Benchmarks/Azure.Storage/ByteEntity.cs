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

        public ByteEntity()
        {

        }

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

        public override bool Equals(object obj)
        {
            if (!(obj is ByteEntity))
            {
                return false;
            }
            ByteEntity entity = (ByteEntity)obj;
            return entity.PartitionKey == this.PartitionKey &&
                    this.RowKey == entity.RowKey &&
                checkArray(entity.payload, this.payload);
        }

        private bool checkArray(byte[] p1, byte[] p2)
        {
            int sizeP1 = p1.Length;
            int sizeP2 = p2.Length;
            if (sizeP1 != sizeP2) return false;
            else
            {
                for (int i = 0; i < sizeP1; i++)
                {
                    if (p1[i] != p2[i]) return false;
                }
            }
            return true;
        }

    }





}
