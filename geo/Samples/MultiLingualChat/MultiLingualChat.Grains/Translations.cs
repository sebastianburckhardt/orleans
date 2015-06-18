using Orleans;
using Orleans.Providers;
using System;
using GeoOrleans.Runtime.Strawman.ReplicatedGrains;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MultiLingualChat.GrainInterfaces;
using System.Security.Cryptography;

namespace MultiLingualChat.Grains
{
    [StorageProvider(ProviderName = "AzureStore")]
    public class Translations : SequencedGrain<Translations.State>, ITranslations
    {

        #region State and State Modifiers
        [Serializable]
        public new class State
        {
            public Dictionary<string, MessageTranslation> translations { get; set; }

            public State()
            {
                translations = new Dictionary<string, MessageTranslation>();
            }
        }

        [Serializable]
        public class StateModifier : IAppliesTo<State>
        {
            public string type { get; set; }
            public string rowKey { get; set; }
            public int offset { get; set; }
            public MessageTranslation translation { get; set; }
            public void Update(State state)
            {
                switch (type)
                {
                    case "addTranslation":
                        state.translations.Add(rowKey, translation);
                        break;
                    case "voteTranslation":
                        MessageTranslation t;
                        if (state.translations.TryGetValue(rowKey, out t))
                        {
                            t.Rank = t.Rank + offset;
                            state.translations[rowKey] = t;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        #endregion

        private static MD5 md5Hash;

        public override Task OnActivateAsync() {
            md5Hash = MD5.Create();
            return base.OnActivateAsync();
        }

        #region AddTranslation
        public async Task<MessageTranslation> addTranslation(string srcLang, string tgtLang, string original, string translated, bool isBing, int rank)
        {
            var rowKey = _makeHashKey(srcLang, tgtLang, translated);
            var translation = new MessageTranslation
            {
                RowKey = rowKey,
                Text = translated,
                Rank = rank,
                IsBing = isBing
            };

            await UpdateLocallyAsync(new StateModifier() { type = "addTranslation", rowKey = rowKey, translation = translation }, false);
            return translation;
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

        #region VoteTranslation
        public async Task<int> voteTranslation(string row, int offset)
        {
            await UpdateLocallyAsync(new StateModifier() { type = "voteTranslation", rowKey = row, offset = offset }, false);
            return 0;
        }
        #endregion

        #region GetAlternativeTranslations
        public async Task<List<MessageTranslation>> getAlternativeTranslations()
        {
            return new List<MessageTranslation>((await GetLocalStateAsync()).translations.Values);
        }
        #endregion

        #region DisputeTranslation
        public async Task<List<MessageTranslation>> disputeTranslation()
        {
            return await getAlternativeTranslations();
        }
        #endregion
    }
}
