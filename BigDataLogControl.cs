using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigDataTiler
{
    public class BigDataLogControl
    {
        private static bool isLocal = Environment.GetEnvironmentVariable("TILER_ENV") == "local";
        private static bool isStaging = Environment.GetEnvironmentVariable("TILER_ENV") == "staging";
        private static string defaultEndPointUri = "https://tiler-event-log.documents.azure.com:443/";
        private static string EndpointUrl = isLocal ? BigDataLogControl.defaultEndPointUri : BigDataLogControl.defaultEndPointUri;
        private static string dbName = isLocal ? "LocalTilerBigDataDB" : isStaging ? "TilerBigDataDB" : "OtherBigDataDB";
        private static string collectionName = "UserLogs";
        private static string partitionKey = "/UserId";
        private static int OfferThroughput = 400;
        private string PrimaryKey = isLocal ? ConfigurationManager.AppSettings["azureDocumentDbKeyLocal"] :ConfigurationManager.AppSettings["azureDocumentDbKey"];
        private DocumentClient Client;
        public LogChange loadedDocument { get; set; }
        public BigDataLogControl()
        {
            Client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
        }

        public async Task createAzureDocumentDatabase()
        {
            await BigDataLogControl.createAzureDocumentDatabase(EndpointUrl, PrimaryKey).ConfigureAwait(false);
        }
        public static async Task createAzureDocumentDatabase(string EndpointUrl, string PrimaryKey)
        {
            DocumentClient client = new DocumentClient(new Uri(EndpointUrl), PrimaryKey);
            Database dataBase = new Database { Id = dbName };
            await client.CreateDatabaseIfNotExistsAsync(dataBase).ConfigureAwait(false);
            DocumentCollection myCollection = new DocumentCollection();
            myCollection.Id = collectionName;
            myCollection.PartitionKey.Paths.Add(partitionKey);
            await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(dbName),
                myCollection,
                new RequestOptions { OfferThroughput = OfferThroughput }).ConfigureAwait(false);
        }
        public async Task AddLogDocument(LogChange log)
        {
            await Client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(dbName, collectionName), log).ConfigureAwait(false);
        }

        async Task<Document> loadDocument(string userId, string documentId)
        {
            RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey(userId) };
            Uri uri = UriFactory.CreateDocumentUri(dbName, collectionName, documentId);
            Document retrievedDocument = await Client.ReadDocumentAsync(uri, options).ConfigureAwait(false);
            return retrievedDocument;
        }

        public async Task<LogChange> getDocument(string userId, string documentId)
        {
            Document loadedDocument = await loadDocument(userId, documentId).ConfigureAwait(false);
            byte[] allBytesbefore = loadedDocument.GetPropertyValue<byte[]>("ZippedLog");
            LogChange retValue = new LogChange();
            retValue.ZippedLog = allBytesbefore;
            retValue.Id = loadedDocument.GetPropertyValue<string>("Id");
            retValue.UserId = loadedDocument.GetPropertyValue<string>("UserId");
            retValue.TypeOfEvent = loadedDocument.GetPropertyValue<string>("TypeOfEvent");
            retValue.TimeOfCreation = loadedDocument.GetPropertyValue<DateTimeOffset>("TimeOfCreation");
            retValue.JsTimeOfCreation = loadedDocument.GetPropertyValue<ulong>("JsTimeOfCreation");
            this.loadedDocument = retValue;
            return retValue;
        }

        public async Task writeDocumentTofile(byte[] bytes, string fullZipPathName)
        {
            byte[] allBytesbefore = bytes;

            string documentZipPathCopy = @fullZipPathName;
            FileStream fs = new FileStream(documentZipPathCopy, FileMode.Create);
            fs.Write(allBytesbefore, 0, allBytesbefore.Length);
        }
    }
}
