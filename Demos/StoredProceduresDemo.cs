using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

namespace DocDb.DotNetSdk.Demos
{
	public static class StoredProceduresDemo
	{
		private static DocumentCollection _collection;

		public async static Task Run()
		{
			Debugger.Break();

			var endpoint = ConfigurationManager.AppSettings["DocDbEndpoint"];
			var masterKey = ConfigurationManager.AppSettings["DocDbMasterKey"];

			using (var client = new DocumentClient(new Uri(endpoint), masterKey))
			{
				Database database = client.CreateDatabaseQuery("SELECT * FROM c WHERE c.id = 'bitcoindb'").AsEnumerable().First();
				_collection = client.CreateDocumentCollectionQuery(database.CollectionsLink, "SELECT * FROM c WHERE c.id = 'bitcoincoll'").AsEnumerable().First();

				await ExecuteStoredProcedures(client);

			}
		}

		private async static Task ExecuteStoredProcedures(DocumentClient client)
		{
			await Execute_spBulkInsert(client);
		}

		private async static Task Execute_spBulkInsert(DocumentClient client)
		{
			Console.WriteLine();
			Console.WriteLine("Execute bulkImport");

			var docs = new List<dynamic>();
			var totalNewDocCount = 3;
			for (var i = 1; i <= totalNewDocCount; i++)
			{
				dynamic newDoc = new
				{
					name = string.Format("Bulk inserted doc {0}", i)
				};
				docs.Add(newDoc);
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
