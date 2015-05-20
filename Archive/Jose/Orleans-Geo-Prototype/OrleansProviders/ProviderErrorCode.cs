
namespace Orleans.Providers
{
    internal enum ProviderErrorCode
    {
        ProvidersBase = 200000,
        
        // Azure storage provider related
        AzureTableProviderBase                      = ProvidersBase + 100,
        AzureTableProvider_DataNotFound             = AzureTableProviderBase + 1,
        AzureTableProvider_ReadingData              = AzureTableProviderBase + 2,
        AzureTableProvider_WritingData              = AzureTableProviderBase + 3,
        AzureTableProvider_Storage_Reading          = AzureTableProviderBase + 4,
        AzureTableProvider_Storage_Writing          = AzureTableProviderBase + 5,
        AzureTableProvider_Storage_DataRead         = AzureTableProviderBase + 6,
        AzureTableProvider_WriteError               = AzureTableProviderBase + 7,
        AzureTableProvider_DeleteError              = AzureTableProviderBase + 8,
        AzureTableProvider_InitProvider             = AzureTableProviderBase + 9,
        AzureTableProvider_ParamConnectionString    = AzureTableProviderBase + 10,

        ShardedStorageProviderBase                  = ProvidersBase + 200,
        ShardedStorageProvider_ProviderName         = ShardedStorageProviderBase + 1,
        ShardedStorageProvider_HashValueOutOfBounds = ShardedStorageProviderBase + 2,
    }
}