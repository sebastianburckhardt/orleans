using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace MultiLingualChat.GrainInterfaces
{
    /// <summary>
    /// Grain interface ITranslations
    /// </summary>
    public interface ITranslations : IGrainWithStringKey
    {
        Task<MessageTranslation> addTranslation(string srcLang, string tgtLang, string original, string translated, bool isBing, int rank);
        Task<int> voteTranslation(string row, int offset);
        Task<List<MessageTranslation>> getAlternativeTranslations();
        Task<List<MessageTranslation>> disputeTranslation();
    }

    public struct MessageTranslation {
        public string RowKey { get; set; }
        public string Text { get; set; }
        public int Rank { get; set; }
        public bool IsBing { get; set; }
    }
}
