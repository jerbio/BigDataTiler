using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TilerElements;
using Newtonsoft;
using static TilerElements.UserActivity;

namespace BigDataTiler
{
    public class BigDataLogControl
    {
        private static bool isLocal = Environment.GetEnvironmentVariable("TILER_ENV") == "local";
        private static string EndpointUrl = "https://tiler-event-log.documents.azure.com:443/";
        private static string dbName = isLocal ? "OtherBigDataDB" : "TilerBigDataDB";
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

        public async Task<IEnumerable<LogChange>> getLogChangesByType(string userId, TimeLine timeLineArg = null, ActivityType typeOfDocument = ActivityType.None, int count = 100)
        {
            FeedOptions options = new FeedOptions { PartitionKey = new PartitionKey(userId) };
            var documentCollectionUri = UriFactory.CreateDocumentCollectionUri(dbName, collectionName);
            var collectionParams = new List<SqlParameter>();
            string sqlQuery = @"SELECT * FROM c";
            bool isWhereAlready = false;
            TimeLine timeline = timeLineArg.StartToEnd;

            if (userId.isNot_NullEmptyOrWhiteSpace())
            {
                if (!isWhereAlready)
                {
                    sqlQuery += @" WHERE ";
                    isWhereAlready = true;
                }
                else
                {
                    sqlQuery += " AND ";
                }
                var sqlParam = new SqlParameter { Name = "@UserId", Value = userId };
                collectionParams.Add(sqlParam);
                sqlQuery += @" c.UserId = @UserId ";
            }

            if (typeOfDocument != ActivityType.None && typeOfDocument.ToString().isNot_NullEmptyOrWhiteSpace())
            {
                if (!isWhereAlready)
                {
                    sqlQuery += " WHERE ";
                    isWhereAlready = true;
                }
                else
                {
                    sqlQuery += " AND ";
                }
                var sqlParam = new SqlParameter { Name = "@TypeOfEvent", Value = typeOfDocument.ToString() };
                collectionParams.Add(sqlParam);
                sqlQuery += " c.TypeOfEvent = @TypeOfEvent ";
            }

            if (timeline != null)
            {
                if (!isWhereAlready)
                {
                    sqlQuery += " WHERE ";
                    isWhereAlready = true;
                }
                else
                {
                    sqlQuery += " AND ";
                }

                var sqlParamStart = new SqlParameter { Name = "@JsTimeOfCreationStart", Value = timeline.Start.ToUnixTimeMilliseconds() };
                collectionParams.Add(sqlParamStart);
                var sqlParamEnd = new SqlParameter { Name = "@JsTimeOfCreationEnd", Value = timeline.End.ToUnixTimeMilliseconds() };
                collectionParams.Add(sqlParamEnd);
                sqlQuery += " c.JsTimeOfCreation >= @JsTimeOfCreationStart and c.JsTimeOfCreation < @JsTimeOfCreationEnd ";
            }

            sqlQuery += @" order by  c.JsTimeOfCreation desc ";
            sqlQuery += @" offset 0 limit "+count +" ";

            var querySpec = new SqlQuerySpec
            {
                QueryText = sqlQuery,
                Parameters = new SqlParameterCollection(
                    collectionParams
                )
            };
           

            var query = Client.CreateDocumentQuery<Document>(documentCollectionUri, querySpec, options).AsDocumentQuery();
            List<LogChange> retValue = new List<LogChange>();

            while (query.HasMoreResults)
            {
                var documents = await query.ExecuteNextAsync<Document>().ConfigureAwait(false);
                foreach (var loadedDocument in documents)
                {
                    LogChange logChange = new LogChange();
                    byte[] allBytesbefore = loadedDocument.GetPropertyValue<byte[]>("ZippedLog");
                    logChange.ZippedLog = allBytesbefore;
                    logChange.Id = loadedDocument.GetPropertyValue<string>("Id");
                    logChange.UserId = loadedDocument.GetPropertyValue<string>("UserId");
                    logChange.TypeOfEvent = loadedDocument.GetPropertyValue<string>("TypeOfEvent");
                    logChange.TimeOfCreation = loadedDocument.GetPropertyValue<DateTimeOffset>("TimeOfCreation");
                    logChange.JsTimeOfCreation = loadedDocument.GetPropertyValue<ulong>("JsTimeOfCreation");
                    retValue.Add(logChange);
                }
            }
            return retValue;
        }

        public LogChange getLogChange(Document loadedDocument)
        {
            LogChange retValue = new LogChange();
            byte[] allBytesbefore = loadedDocument.GetPropertyValue<byte[]>("ZippedLog");
            retValue.ZippedLog = allBytesbefore;
            retValue.Id = loadedDocument.GetPropertyValue<string>("Id");
            retValue.UserId = loadedDocument.GetPropertyValue<string>("UserId");
            retValue.TypeOfEvent = loadedDocument.GetPropertyValue<string>("TypeOfEvent");
            retValue.TimeOfCreation = loadedDocument.GetPropertyValue<DateTimeOffset>("TimeOfCreation");
            retValue.JsTimeOfCreation = loadedDocument.GetPropertyValue<ulong>("JsTimeOfCreation");
            this.loadedDocument = retValue;
            return retValue;
        }

        public async Task<LogChange> getLogChange(string userId, string documentId)
        {
            Document loadedDocument = await loadDocument(userId, documentId).ConfigureAwait(false);
            return getLogChange(loadedDocument);
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
