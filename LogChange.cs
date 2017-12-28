using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using TilerElements;

namespace BigDataTiler
{
    public class LogChange
    {
        [JsonProperty("UserId")]
        public string UserId { get; set; }
        [JsonProperty("id")]
        public string Id { get; set; } = Guid.NewGuid().ToString();
        [JsonProperty("TypeOfEvent")]
        public string TypeOfEvent { get; set; }
        [JsonConverter(typeof(IsoDateTimeConverter))]
        [JsonProperty("TimeOfCreation")]
        public DateTimeOffset TimeOfCreation { get; set; }

        [JsonProperty("JsTimeOfCreation")]
        public ulong JsTimeOfCreation { get; set; }
        [JsonProperty("ZippedLog")]
        public byte[] ZippedLog { get; set; }
        public void loadXmlFile(XmlDocument xmlDoc)
        {
            byte[] retValue = null;
            MemoryStream ms;

            //MemoryStream textFileStream = new MemoryStream();
            

            {
                using (var memoryStream = new MemoryStream())
                {
                    using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Create, true))
                    {
                        var demoFile = archive.CreateEntry(this.TimeOfCreation.toJSMilliseconds().ToString() + ".xml", CompressionLevel.Optimal);

                        using (var entryStream = demoFile.Open())
                        {
                            //textFileStream.CopyTo(entryStream);
                            xmlDoc.Save(entryStream);
                        }
                    }

                    retValue = memoryStream.ToArray();
                }
            }
            ZippedLog = retValue;
        }
    }
}
