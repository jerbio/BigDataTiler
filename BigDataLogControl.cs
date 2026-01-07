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
        private static bool isLocal = false && Environment.GetEnvironmentVariable("TILER_ENV") == "local";
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
        
        /// <summary>
        /// Adds multiple LogChange documents to Cosmos DB (handles split documents)
        /// </summary>
        /// <param name="logDocuments">List of LogChange documents to add</param>
        /// <returns>The ID of the first/parent document</returns>
        public async Task<string> AddLogDocuments(List<LogChange> logDocuments)
        {
            if (logDocuments == null || logDocuments.Count == 0)
            {
                throw new ArgumentException("logDocuments cannot be null or empty");
            }
            
            string cosmosDBName = dbName;
            string containerName = collectionName;
            var database = Client.GetDatabase(cosmosDBName);
            Container container = database.GetContainer(containerName);
            
            string parentId = logDocuments[0].Id;
            
            foreach (var log in logDocuments)
            {
                try
                {
                    ItemResponse<LogChange> response = await container.CreateItemAsync(log, new PartitionKey(log.UserId)).ConfigureAwait(false);
                    System.Diagnostics.Trace.WriteLine($"Added LogChange document {log.Id} (split {log.SplitIndex + 1}/{log.TotalSplits}), size: {log.ZippedLog?.Length ?? 0} bytes");
                }
                catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.RequestEntityTooLarge)
                {
                    System.Diagnostics.Trace.TraceError($"Document {log.Id} still too large ({log.ZippedLog?.Length ?? 0} bytes): {ex.Message}");
                    throw;
                }
            }
            
            return parentId;
        }
        
        /// <summary>
        /// Retrieves all split parts of a LogChange document and combines them
        /// </summary>
        /// <param name="userId">User ID for partition key</param>
        /// <param name="parentDocumentId">ID of the first/parent document</param>
        /// <returns>Combined LogChange with full XML content</returns>
        public async Task<LogChange> GetCombinedLogChange(string userId, string parentDocumentId)
        {
            var database = Client.GetDatabase(dbName);
            Container container = database.GetContainer(collectionName);
            
            // First, get the parent document
            LogChange parentLog = await loadDocument(userId, parentDocumentId).ConfigureAwait(false);
            
            // If it's not split, return as-is
            if (parentLog.TotalSplits <= 1)
            {
                return parentLog;
            }
            
            // Query for all split parts
            string sqlQuery = @"SELECT * FROM c WHERE c.UserId = @UserId AND (c.id = @ParentId OR c.ParentLogId = @ParentId) ORDER BY c.SplitIndex";
            QueryDefinition queryDefinition = new QueryDefinition(sqlQuery)
                .WithParameter("@UserId", userId)
                .WithParameter("@ParentId", parentDocumentId);
            
            List<LogChange> splitParts = new List<LogChange>();
            using (FeedIterator<LogChange> resultSet = container.GetItemQueryIterator<LogChange>(
                queryDefinition,
                requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey(userId) }))
            {
                while (resultSet.HasMoreResults)
                {
                    FeedResponse<LogChange> response = await resultSet.ReadNextAsync().ConfigureAwait(false);
                    splitParts.AddRange(response);
                }
            }
            
            // Use LogChange's simple string concatenation to combine
            string combinedXml = LogChange.CombineSplitLogs(splitParts);
            
            if (string.IsNullOrEmpty(combinedXml))
            {
                return parentLog;
            }
            
            // Create combined LogChange with the full XML
            LogChange combinedLog = new LogChange
            {
                Id = parentLog.Id,
                UserId = parentLog.UserId,
                TypeOfEvent = parentLog.TypeOfEvent,
                Trigger = parentLog.Trigger,
                TimeOfCreation = parentLog.TimeOfCreation,
                JsTimeOfCreation = parentLog.JsTimeOfCreation,
                SplitIndex = 0,
                TotalSplits = 1,
                ParentLogId = null
            };
            
            // Re-zip the combined content
            using (var memoryStream = new MemoryStream())
            {
                using (var archive = new System.IO.Compression.ZipArchive(memoryStream, System.IO.Compression.ZipArchiveMode.Create, true))
                {
                    var zipEntry = archive.CreateEntry(parentLog.TimeOfCreation.toJSMilliseconds().ToString() + ".xml", System.IO.Compression.CompressionLevel.Optimal);
                    using (var entryStream = zipEntry.Open())
                    using (var writer = new StreamWriter(entryStream, Encoding.UTF8))
                    {
                        writer.Write(combinedXml);
                    }
                }
                combinedLog.ZippedLog = memoryStream.ToArray();
            }
            
            return combinedLog;
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
