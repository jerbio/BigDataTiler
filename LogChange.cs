using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using TilerElements;

namespace BigDataTiler
{
    public enum TriggerType
    {
        schedulechange,
        preview,
    }
    public class LogChange:IHasId
    {
        // Max size in bytes for a single document (1.5MB to stay well under 2MB limit with overhead)
        private const int MaxDocumentSizeBytes = 1500000;
        
        [JsonProperty("UserId")]
        public string UserId { get; set; }
        protected string _Id = "";
        [JsonProperty("id")]
        public string Id { 
            get
            {
                if (string.IsNullOrWhiteSpace(_Id) || string.IsNullOrEmpty(_Id))
                {
                    _Id = GenerateId();
                }
                return _Id;
            } 
            set
            {
                _Id = value;
            }
        }
        [JsonProperty("TypeOfEvent")]
        public string TypeOfEvent { get; set; }
        [JsonProperty("Trigger")]
        public string Trigger { get; set; }
        [JsonConverter(typeof(IsoDateTimeConverter))]
        [JsonProperty("TimeOfCreation")]
        public DateTimeOffset TimeOfCreation { get; set; }

        [JsonProperty("JsTimeOfCreation")]
        public ulong JsTimeOfCreation { get; set; }
        [JsonProperty("ZippedLog")]
        public byte[] ZippedLog { get; set; }
        
        /// <summary>
        /// Index of this split (0 for unsplit or first part, 1+ for subsequent parts)
        /// </summary>
        [JsonProperty("SplitIndex")]
        public int SplitIndex { get; set; }
        
        /// <summary>
        /// Total number of splits (1 for unsplit documents, 2+ for split documents)
        /// </summary>
        [JsonProperty("TotalSplits")]
        public int TotalSplits { get; set; }
        
        /// <summary>
        /// ID of the parent/first log entry (null for unsplit or first part, set for subsequent parts)
        /// </summary>
        [JsonProperty("ParentLogId")]
        public string ParentLogId { get; set; }

        public void UpdateTrigger(TriggerType trigger)
        {
            this.Trigger = trigger.ToString();
        }

        public string GenerateId()
        {
            string triggerInfo = this.Trigger != null ? this.Trigger : "NoTrigger";
            string _ulid = Ulid.NewUlid().ToString();
            return (this.UserId.isNot_NullEmptyOrWhiteSpace() ? this.UserId : ExecutionConstants.NoUserId) + "_" + triggerInfo + "_" + _ulid + "_" + this.TimeOfCreation.toJSMilliseconds();
        }
        
        public string GenerateSplitId(int splitIndex)
        {
            string triggerInfo = this.Trigger != null ? this.Trigger : "NoTrigger";
            string _ulid = Ulid.NewUlid().ToString();
            return (this.UserId.isNot_NullEmptyOrWhiteSpace() ? this.UserId : ExecutionConstants.NoUserId) + "_" + triggerInfo + "_" + _ulid + "_" + this.TimeOfCreation.toJSMilliseconds() + "_split" + splitIndex;
        }

        /// <summary>
        /// Loads XML file and splits into multiple LogChange objects if needed to stay under size limits.
        /// Uses simple string splitting - chunks are not valid XML individually but concatenate back to valid XML.
        /// </summary>
        /// <returns>List of LogChange objects (1 if no split needed, multiple if split)</returns>
        public List<LogChange> loadXmlFile(XmlDocument xmlDoc)
        {
            List<LogChange> results = new List<LogChange>();
            
            // Get the XML as a string
            string xmlContent = xmlDoc.OuterXml;
            
            // Create initial zip to check compressed size
            byte[] initialZip = CreateZipFromString(xmlContent);
            
            // If it fits in one document, return as-is
            if (initialZip.Length <= MaxDocumentSizeBytes)
            {
                this.ZippedLog = initialZip;
                this.SplitIndex = 0;
                this.TotalSplits = 1;
                this.ParentLogId = null;
                results.Add(this);
                return results;
            }
            
            // Need to split - estimate uncompressed chunk size based on compression ratio
            // Use 70% safety margin to account for compression variance
            double compressionRatio = (double)initialZip.Length / xmlContent.Length;
            int targetCompressedSize = (int)(MaxDocumentSizeBytes * 0.7);
            int estimatedChunkSize = (int)(targetCompressedSize / compressionRatio);
            
            // Ensure minimum chunk size
            if (estimatedChunkSize < 50000)
            {
                estimatedChunkSize = 50000;
            }
            
            // Split the string into chunks
            List<string> stringChunks = SplitStringIntoChunks(xmlContent, estimatedChunkSize);
            
            // Verify each chunk compresses to under the limit, re-split if needed
            List<byte[]> validatedZippedChunks = new List<byte[]>();
            foreach (string chunk in stringChunks)
            {
                List<byte[]> zippedChunks = CompressAndValidateChunk(chunk, estimatedChunkSize);
                validatedZippedChunks.AddRange(zippedChunks);
            }
            
            string parentId = this.Id;
            
            for (int i = 0; i < validatedZippedChunks.Count; i++)
            {
                LogChange splitLog = new LogChange
                {
                    UserId = this.UserId,
                    TypeOfEvent = this.TypeOfEvent,
                    Trigger = this.Trigger,
                    TimeOfCreation = this.TimeOfCreation,
                    JsTimeOfCreation = this.JsTimeOfCreation,
                    SplitIndex = i,
                    TotalSplits = validatedZippedChunks.Count,
                    ParentLogId = i == 0 ? null : parentId,
                    ZippedLog = validatedZippedChunks[i]
                };
                
                if (i == 0)
                {
                    splitLog._Id = parentId;
                }
                else
                {
                    splitLog._Id = this.GenerateSplitId(i);
                }
                
                results.Add(splitLog);
            }
            
            System.Diagnostics.Trace.WriteLine($"LogChange: Split XML ({xmlContent.Length} chars, {initialZip.Length} bytes compressed) into {results.Count} chunks");
            
            return results;
        }
        
        /// <summary>
        /// Splits a string into chunks of approximately the target size.
        /// Simple linear operation with no recursion.
        /// </summary>
        private List<string> SplitStringIntoChunks(string content, int chunkSize)
        {
            List<string> chunks = new List<string>();
            
            if (string.IsNullOrEmpty(content))
            {
                return chunks;
            }
            
            if (content.Length <= chunkSize)
            {
                chunks.Add(content);
                return chunks;
            }
            
            int position = 0;
            while (position < content.Length)
            {
                int remainingLength = content.Length - position;
                int currentChunkSize = Math.Min(chunkSize, remainingLength);
                
                string chunk = content.Substring(position, currentChunkSize);
                chunks.Add(chunk);
                position += currentChunkSize;
            }
            
            return chunks;
        }
        
        /// <summary>
        /// Compresses a chunk and validates it's under the size limit.
        /// If too large, splits further and returns multiple compressed chunks.
        /// </summary>
        private List<byte[]> CompressAndValidateChunk(string chunk, int targetUncompressedSize)
        {
            List<byte[]> results = new List<byte[]>();
            
            byte[] zipped = CreateZipFromString(chunk);
            
            if (zipped.Length <= MaxDocumentSizeBytes)
            {
                results.Add(zipped);
                return results;
            }
            
            // Chunk is still too large after compression
            // Calculate new smaller chunk size based on how much we're over
            double overageRatio = (double)zipped.Length / MaxDocumentSizeBytes;
            int newChunkSize = (int)(targetUncompressedSize / (overageRatio * 1.3)); // 30% extra margin
            
            // Ensure minimum chunk size to prevent too many splits
            if (newChunkSize < 10000)
            {
                newChunkSize = 10000;
            }
            
            // Re-split this chunk into smaller pieces
            List<string> smallerChunks = SplitStringIntoChunks(chunk, newChunkSize);
            
            // Recursively validate each smaller chunk (simple iteration, not deep recursion)
            foreach (string smallerChunk in smallerChunks)
            {
                byte[] smallerZipped = CreateZipFromString(smallerChunk);
                
                if (smallerZipped.Length <= MaxDocumentSizeBytes)
                {
                    results.Add(smallerZipped);
                }
                else
                {
                    // Still too large, split again (will converge since we're halving each time)
                    results.AddRange(CompressAndValidateChunk(smallerChunk, newChunkSize));
                }
            }
            
            return results;
        }
        
        /// <summary>
        /// Creates a zip archive containing the string content
        /// </summary>
        private byte[] CreateZipFromString(string content)
        {
            using (var memoryStream = new MemoryStream())
            {
                using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Create, true))
                {
                    var zipEntry = archive.CreateEntry(this.TimeOfCreation.toJSMilliseconds().ToString() + ".xml", CompressionLevel.Optimal);

                    using (var entryStream = zipEntry.Open())
                    using (var writer = new StreamWriter(entryStream, Encoding.UTF8))
                    {
                        writer.Write(content);
                    }
                }

                return memoryStream.ToArray();
            }
        }
        
        /// <summary>
        /// Extracts string content from a zipped log
        /// </summary>
        public static string ExtractStringFromZip(byte[] zippedLog)
        {
            if (zippedLog == null || zippedLog.Length == 0)
            {
                return null;
            }
            
            using (var memoryStream = new MemoryStream(zippedLog))
            using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Read))
            {
                var entry = archive.Entries.FirstOrDefault();
                if (entry != null)
                {
                    using (var entryStream = entry.Open())
                    using (var reader = new StreamReader(entryStream, Encoding.UTF8))
                    {
                        return reader.ReadToEnd();
                    }
                }
            }
            
            return null;
        }
        
        /// <summary>
        /// Combines multiple split LogChange objects back into a single XML string.
        /// Simply concatenates the string content from each split in order.
        /// </summary>
        public static string CombineSplitLogs(IEnumerable<LogChange> splitLogs)
        {
            if (splitLogs == null)
            {
                return null;
            }
            
            var orderedLogs = splitLogs.OrderBy(l => l.SplitIndex).ToList();
            
            if (orderedLogs.Count == 0)
            {
                return null;
            }
            
            if (orderedLogs.Count == 1)
            {
                return ExtractStringFromZip(orderedLogs[0].ZippedLog);
            }
            
            StringBuilder combined = new StringBuilder();
            foreach (var log in orderedLogs)
            {
                string content = ExtractStringFromZip(log.ZippedLog);
                if (content != null)
                {
                    combined.Append(content);
                }
            }
            
            return combined.ToString();
        }
    }
}
