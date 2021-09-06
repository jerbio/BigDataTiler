using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TilerElements;
using Newtonsoft;
using Microsoft.Azure.Cosmos;
using static TilerElements.UserActivity;


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
        private CosmosClient Client;
        public LogChange loadedDocument { get; set; }
        public BigDataLogControl()
        {
            Client = new CosmosClient(EndpointUrl, PrimaryKey);
        }

        public async Task createAzureDocumentDatabase()
        {
            await BigDataLogControl.createAzureDocumentDatabase(EndpointUrl, PrimaryKey).ConfigureAwait(false);
        }
        public static async Task createAzureDocumentDatabase(string EndpointUrl, string PrimaryKey)
        {
            var client = new CosmosClient(EndpointUrl, PrimaryKey);
            DatabaseResponse databaseResponse = await client.CreateDatabaseIfNotExistsAsync(dbName);
            Database database = databaseResponse;
            DatabaseProperties databaseProperties = databaseResponse;

            // Create a database with a shared manual provisioned throughput
            string databaseIdManual = dbName;
            database = await client.CreateDatabaseIfNotExistsAsync(databaseIdManual, ThroughputProperties.CreateManualThroughput(OfferThroughput));

            ContainerResponse container = await database.CreateContainerIfNotExistsAsync(
                id: collectionName,
                partitionKeyPath: partitionKey,
                throughput: OfferThroughput);




            //Database dataBase = new Database { Id = dbName };
            //await client.CreateDatabaseIfNotExistsAsync(dataBase).ConfigureAwait(false);
            //DocumentCollection myCollection = new DocumentCollection();
            //myCollection.Id = collectionName;
            //myCollection.PartitionKey.Paths.Add(partitionKey);
            //await client.CreateDocumentCollectionIfNotExistsAsync(
            //    UriFactory.CreateDatabaseUri(dbName),
            //    myCollection,
            //    new RequestOptions { OfferThroughput = OfferThroughput }).ConfigureAwait(false);
        }
        public async Task AddLogDocument(LogChange log)
        {
            string cosmosDBName = dbName;
            string containerName = collectionName;
            var database = Client.GetDatabase(cosmosDBName);
            Container container = database.GetContainer(containerName);

            ItemResponse<LogChange> response = await container.CreateItemAsync(log, new PartitionKey(log.UserId)).ConfigureAwait(false);
            //await Client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(dbName, collectionName), log).ConfigureAwait(false);
        }

        async Task<LogChange> loadDocument(string userId, string documentId)
        {
            var database = Client.GetDatabase(dbName);
            Container container = database.GetContainer(collectionName);
            ItemResponse<LogChange> response = await container.ReadItemAsync<LogChange>(documentId, new PartitionKey(userId)).ConfigureAwait(false);
            return response;


            //RequestOptions options = new RequestOptions { PartitionKey = new PartitionKey(userId) };
            //Uri uri = UriFactory.CreateDocumentUri(dbName, collectionName, documentId);
            //Document retrievedDocument = await Client.ReadDocumentAsync(uri, options).ConfigureAwait(false);
            //return retrievedDocument;
        }

        public async Task<IEnumerable<LogChange>> getLogChangesByType(string userId, TimeLine timeLineArg = null, ActivityType typeOfDocument = ActivityType.None, int count = 100)
        {
            var options = new QueryRequestOptions { PartitionKey = new PartitionKey(userId) };
            
            var collectionParams = new List<Tuple<string, object>>();
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
                var sqlParam = new Tuple<string, object> ("@UserId",userId);
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
                var sqlParam = new Tuple<string, object>("@TypeOfEvent", typeOfDocument.ToString());
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

                var sqlParamStart = new Tuple<string, object>("@JsTimeOfCreationStart", timeline.Start.ToUnixTimeMilliseconds());
                collectionParams.Add(sqlParamStart);
                var sqlParamEnd = new Tuple<string, object>("@JsTimeOfCreationEnd", timeline.End.ToUnixTimeMilliseconds());
                collectionParams.Add(sqlParamEnd);
                sqlQuery += " c.JsTimeOfCreation >= @JsTimeOfCreationStart and c.JsTimeOfCreation < @JsTimeOfCreationEnd ";
            }


            sqlQuery += @" order by  c.JsTimeOfCreation desc ";
            sqlQuery += @" offset 0 limit "+count +" ";


            QueryDefinition queryDefinition = new QueryDefinition(sqlQuery);
            collectionParams.ForEach((param) =>
            {
                queryDefinition = queryDefinition.WithParameter(param.Item1, param.Item2);
            });

            var database = Client.GetDatabase(dbName);
            Container container = database.GetContainer(collectionName);

            List<LogChange> retValue = new List<LogChange>();
            using (FeedIterator<LogChange> resultSet = container.GetItemQueryIterator<LogChange>(
            queryDefinition,
            requestOptions: new QueryRequestOptions()
            {
                PartitionKey = new PartitionKey("Account1"),
                MaxItemCount = 1
            }))
                {
                    while (resultSet.HasMoreResults)
                    {
                        FeedResponse<LogChange> response = await resultSet.ReadNextAsync();
                        LogChange logChange = response.First();
                        Console.WriteLine($"\n Log change UserId is : {logChange.UserId}; Id: {logChange.Id};");
                        retValue.AddRange(response);
                    }
                }


            //var querySpec = new SqlQuerySpec
            //{
            //    QueryText = sqlQuery,
            //    Parameters = new SqlParameterCollection(
            //        collectionParams
            //    )
            //};

            

            //var documentCollectionUri = UriFactory.CreateDocumentCollectionUri(dbName, collectionName);
            //var query = Client.CreateDocumentQuery<Document>(documentCollectionUri, querySpec, options).AsDocumentQuery();
            //List<LogChange> retValue = new List<LogChange>();

            //while (query.HasMoreResults)
            //{
            //    var documents = await query.ExecuteNextAsync<Document>().ConfigureAwait(false);
            //    foreach (var loadedDocument in documents)
            //    {
            //        LogChange logChange = new LogChange();
            //        byte[] allBytesbefore = loadedDocument.GetPropertyValue<byte[]>("ZippedLog");
            //        logChange.ZippedLog = allBytesbefore;
            //        logChange.Id = loadedDocument.GetPropertyValue<string>("Id");
            //        logChange.UserId = loadedDocument.GetPropertyValue<string>("UserId");
            //        logChange.TypeOfEvent = loadedDocument.GetPropertyValue<string>("TypeOfEvent");
            //        logChange.TimeOfCreation = loadedDocument.GetPropertyValue<DateTimeOffset>("TimeOfCreation");
            //        logChange.JsTimeOfCreation = loadedDocument.GetPropertyValue<ulong>("JsTimeOfCreation");
            //        retValue.Add(logChange);
            //    }
            //}
            return retValue;
        }

        //public LogChange getLogChange(Document loadedDocument)
        //{
        //    LogChange retValue = new LogChange();
        //    byte[] allBytesbefore = loadedDocument.GetPropertyValue<byte[]>("ZippedLog");
        //    retValue.ZippedLog = allBytesbefore;
        //    retValue.Id = loadedDocument.GetPropertyValue<string>("Id");
        //    retValue.UserId = loadedDocument.GetPropertyValue<string>("UserId");
        //    retValue.TypeOfEvent = loadedDocument.GetPropertyValue<string>("TypeOfEvent");
        //    retValue.TimeOfCreation = loadedDocument.GetPropertyValue<DateTimeOffset>("TimeOfCreation");
        //    retValue.JsTimeOfCreation = loadedDocument.GetPropertyValue<ulong>("JsTimeOfCreation");
        //    this.loadedDocument = retValue;
        //    return retValue;
        //}

        public async Task<LogChange> getLogChange(string userId, string documentId)
        {
            LogChange retValue= await loadDocument(userId, documentId).ConfigureAwait(false);
            return retValue;
            //Document loadedDocument = await loadDocument(userId, documentId).ConfigureAwait(false);
            //return getLogChange(loadedDocument);
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
