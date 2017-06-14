using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.IO;
using Newtonsoft.Json.Linq;

namespace DocDb.DotNetSdk
{
	public static class Program
	{
		private static void Main(string[] args)
		{

            StoredProceduresDemo.ExecuteServerSide().Wait();

            Console.WriteLine();
			Console.Write("Done. Press any key to continue...");
			Console.ReadKey(true);
			Console.Clear();
		}
        public static class StoredProceduresDemo
        {
            private static DocumentCollection _collection;

            public async static Task ExecuteServerSide()
            {
               

                var endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
                var masterKey = ConfigurationManager.AppSettings["DocDbMasterKey"];

                using (var client = new DocumentClient(new Uri(endpoint), masterKey))
                {
                    Database database = client.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'sampleDB'").AsEnumerable().First();
                    _collection = client.CreateDocumentCollectionQuery(database.CollectionsLink, "SELECT * FROM c WHERE c.id = 'sampleColl'").AsEnumerable().First();

                    await ExecuteStoredProcedures(client);

                }
            }

            private async static Task ExecuteStoredProcedures(DocumentClient client)
            {
                await Execute_spBulkInsert(client);
            }

            private async static Task Execute_spBulkInsert(DocumentClient client)
            {
                string path = ConfigurationManager.AppSettings["txtFolderPath"];
                string[] files = Directory.GetFiles(path);
                int totalNewDocCount = files.Length;
                var rawjson = "";
                var fixedjson = "";
                JObject jsonobj = null;
                var docs = new List<dynamic>();
                
                foreach ( var f in files)
                {
                    rawjson = File.ReadAllText(f.ToString());
                    fixedjson = rawjson.Replace("\"hash\":", "\"id\":");
                    jsonobj = JObject.Parse(fixedjson);
                    docs.Add(jsonobj);
                }
               
            
                var totalInsertedCount = 0;
                while (totalInsertedCount < totalNewDocCount)
                {
                    var insertedCount = await ExecuteStoredProcedure<int>(client, "bulkImport", docs);
                    totalInsertedCount += insertedCount;
                    Console.WriteLine("Inserted {0} documents ({1} total, {2} remaining)", insertedCount, totalInsertedCount, totalNewDocCount - totalInsertedCount);
                    docs = docs.GetRange(insertedCount, docs.Count - insertedCount);
                }
            }

            private async static Task<T> ExecuteStoredProcedure<T>(DocumentClient client, string sprocId, params dynamic[] sprocParams)
            {
                var query = new SqlQuerySpec
                {
                    QueryText = "SELECT * FROM c WHERE c.id = @id",
                    Parameters = new SqlParameterCollection { new SqlParameter { Name = "@id", Value = sprocId } }
                };

                StoredProcedure sproc = client
                    .CreateStoredProcedureQuery(_collection.StoredProceduresLink, query)
                    .AsEnumerable()
                    .First();

                while (true)
                {
                    try
                    {
                        var result = await client.ExecuteStoredProcedureAsync<T>(sproc.SelfLink, sprocParams);

                        Console.WriteLine("Executed stored procedure: {0}", sprocId);
                        return result;
                    }
                    catch (DocumentClientException ex)
                    {
                        if ((int)ex.StatusCode == 429)
                        {
                            Console.WriteLine("  ...retry in {0}", ex.RetryAfter);
                            Thread.Sleep(ex.RetryAfter);
                            continue;
                        }
                        throw ex;
                    }
                }
            }


        }


    }
}
