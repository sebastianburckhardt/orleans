using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MultiLingualChat.GrainInterfaces;
using System.Security.Cryptography;

namespace MultiLingualChat.Grains
{
    public class Translations : Grain, ITranslations
    {
        Dictionary<string, MessageTranslation> translations;
        private static MD5 md5Hash;

        public override Task OnActivateAsync() {
            translations  = new Dictionary<string, MessageTranslation>();
            md5Hash = MD5.Create();
            return base.OnActivateAsync();
        }

        #region AddTranslation
        public Task<MessageTranslation> addTranslation(string srcLang, string tgtLang, string original, string translated, bool isBing, int rank)
        {
            var translation = new MessageTranslation
            {
                RowKey = _makeHashKey(srcLang, tgtLang, translated),
                Text = translated,
                Rank = rank,
                IsBing = isBing
            };
            translations.Add(translation.RowKey, translation);
            return Task.FromResult(translation);
        }

        private string _makeHashKey(string srcLang, string tgtLang, string original)
        {
            byte[] data = md5Hash.ComputeHash(Encoding.UTF32.GetBytes(string.Format("{0}:{1}:{2}", srcLang, tgtLang, original)));
            StringBuilder sBuilder = new StringBuilder();
            for (int i = 0; i < data.Length; i++)
            {
                sBuilder.Append(data[i].ToString("x2"));
            }
            return sBuilder.ToString();
        }
        #endregion

        public Task<int> voteTranslation(string row, int offset)
        {
            MessageTranslation translation;
            if (translations.TryGetValue(row, out translation))
            {
                translation.Rank = translation.Rank + offset;
                // TODO: must update the structure in dictionary?
                return Task.FromResult(translation.Rank);
            }
            return Task.FromResult(0);
        }

        public Task<List<MessageTranslation>> getAlternativeTranslations()
        {
            return Task.FromResult(new List<MessageTranslation>(translations.Values));
        }

        public Task<List<MessageTranslation>> disputeTranslation()
        {
            return getAlternativeTranslations();
        }
    }
}
