namespace Orleans.Transactions
{
    //TODO Move to AzureUtils once the config work is done!
    public class AzureTableTransactionLogOptions
    {
        public string ConnectionString { get; set; }
        public string TableName { get; set; }
    }
}
