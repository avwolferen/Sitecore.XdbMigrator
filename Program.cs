using Sitecore.XConnect;
using Sitecore.XConnect.Client;
using Sitecore.XConnect.Client.WebApi;
using Sitecore.XConnect.Collection.Model;
using Sitecore.XConnect.Schema;
using Sitecore.XConnect.Serialization;
using Sitecore.XConnect.Operations;
using Sitecore.ContentTesting.Model.xConnect;
using Sitecore.Xdb.Common.Web;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using static Sitecore.XConnect.Collection.Model.CollectionModel;
using Newtonsoft.Json;
using Sitecore.XConnect.Client.Serialization;
using System.Security;
using System.Configuration;

namespace AlexVanWolferen.SitecoreXdbMigrator
{
    class Program
    {
        private static void Main(string[] args)
        {
            bool identityContacts = true;
            bool transferContacts = true;
            // If you know the passwords to the PFX you can use this. This doesn't work for all types of certificates.
            bool usePfx = false;
            bool skipInteractions = false;
            // If you use a custom model please add the references and run through the entire code to uncomment things
            bool useCustomModel = false;
            int batchSize = 100;

            var history = DateTime.UtcNow.AddDays(-14);

            XConnectConnector xConnectConnector = new XConnectConnector(history, DateTime.MaxValue, skipInteractions, batchSize, usePfx, useCustomModel);

            Task.WaitAll(xConnectConnector.Init());

            Task.WaitAll(xConnectConnector.GetInfo(xConnectConnector.sourceClient));
            Task.WaitAll(xConnectConnector.GetInfo(xConnectConnector.targetClient));

            // If you want to identify contacts please make a SQL export with
            /*
             * 
             * Run this in both (all) shards of the source and store it in files according to the filepath format in the app.config (appsetting shard_csv)
                SELECT [ContactId]
                      ,[Source]
                FROM [xdb_collection].[ContactIdentifiers]
                WHERE Source = 'xDB.Tracker' and IdentifierType = 0
                
             * 
             */
            if (identityContacts)
            {
                IdentifyUnidentifiedContacts(args, xConnectConnector).ConfigureAwait(false).GetAwaiter().GetResult();
                Console.WriteLine("Done identifying contacts, press any key to go to the contact transfer process");
                Console.ReadKey();
            }

            if (transferContacts)
            {
                TransferAsync(args, xConnectConnector).ConfigureAwait(false).GetAwaiter().GetResult();
            }

            Console.ForegroundColor = ConsoleColor.DarkGreen;
            Console.WriteLine("");
            Console.WriteLine("END OF PROGRAM.");
            Console.ReadKey();
        }

        private static async Task TransferAsync(string[] args, XConnectConnector xConnectConnector)
        {
            Console.WriteLine("Press enter to start migrating data");
            Console.ReadKey();

            await xConnectConnector.TransferContacts();

            Console.WriteLine("Done, press any key to get the results");
            Console.ReadKey();

            //Task.WaitAll(xConnectConnector.GetInfo(xConnectConnector.sourceClient));
            Task.WaitAll(xConnectConnector.GetInfo(xConnectConnector.targetClient));
        }

        private static async Task IdentifyUnidentifiedContacts(string[] args, XConnectConnector xConnectConnector)
        {
            Console.WriteLine("Loading shard CSV files");

            for (int shard = 0; shard < xConnectConnector.numSourceShards; shard++)
            {
                var sharddata = File.ReadAllLines(string.Format(ConfigurationManager.AppSettings["shard_csv"], shard));

                Console.WriteLine($"Shard{shard} contains {sharddata.Length - 1} contacts");

                xConnectConnector.IdentifyContacts(xConnectConnector.sourceClient, sharddata);
            }

            Console.WriteLine("Done, press any key to get the results");
            Console.ReadKey();

            Task.WaitAll(xConnectConnector.GetInfo(xConnectConnector.sourceClient));
        }
    }
}
