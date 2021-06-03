using Newtonsoft.Json;
using Sitecore.ContentTesting.Model.xConnect;
using Sitecore.XConnect;
using Sitecore.XConnect.Client;
using Sitecore.XConnect.Client.Serialization;
using Sitecore.XConnect.Client.WebApi;
using Sitecore.XConnect.Collection.Model;
using Sitecore.XConnect.Operations;
using Sitecore.XConnect.Schema;
using Sitecore.XConnect.Serialization;
using Sitecore.Xdb.Common.Web;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using static Sitecore.XConnect.Collection.Model.CollectionModel;

namespace AlexVanWolferen.SitecoreXdbMigrator
{
    public class XConnectConnector
    {
        public XConnectClient sourceClient { get; private set; }
        public XConnectClient targetClient { get; private set; }
        public DateTime startDate { get; private set; } = DateTime.UtcNow.AddDays(-7);
        public DateTime endDate { get; private set; } = DateTime.MaxValue;
        public int batchSize { get; private set; } = 1000;

        public bool skipInteractions { get; private set; } = true;

        public string LogFile { get; private set; }

        public bool usePfx { get; set; }

        public bool useCustomModel { get; set; }

        public int numSourceShards { get; set; } = 2;

        public XConnectConnector(DateTime startDate, DateTime endDate, bool skipInteractions, int batchSize, bool usePfx, bool useCustomModel)
            : this()
        {
            this.startDate = startDate;
            this.endDate = endDate;
            this.batchSize = batchSize;
            this.skipInteractions = skipInteractions;
            this.usePfx = usePfx;
            this.useCustomModel = useCustomModel;
        }

        public XConnectConnector()
        {
            LogFile = string.Format(ConfigurationManager.AppSettings["logfileformat"], DateTime.Now.ToString("yyyy-MM-dd HHmmss"));
        }

        internal async Task Init()
        {
            string sourceCollectEndpoint = ConfigurationManager.AppSettings["sourceCollectEndpoint"];
            string sourceSearchEndpoint = ConfigurationManager.AppSettings["sourceSearchEndpoint"];
            ModelVersion sourceModelVersion = ModelVersion.Model901;
            object sourceModel;
            string sourceCertificateFile = ConfigurationManager.AppSettings["sourceCertificate"];
            SecureString sourceCertificatePassword;

            Console.Write($"Source collect endpoint (default: {sourceCollectEndpoint}): ");
            var scep = Console.ReadLine();
            if (!string.IsNullOrEmpty(scep) && Uri.IsWellFormedUriString(scep, UriKind.Absolute))
            {
                sourceCollectEndpoint = scep;
            }

            Console.Write($"Source search endpoint (default: {sourceCollectEndpoint}): ");
            var ssep = Console.ReadLine();
            if (!string.IsNullOrEmpty(ssep) && Uri.IsWellFormedUriString(ssep, UriKind.Absolute))
            {
                sourceCollectEndpoint = ssep;
            }

            Console.Write("Source model version 901, 100, 101, (default: 901): ");
            var smv = Console.ReadLine();
            if (!string.IsNullOrEmpty(smv))
            {
                if (smv == "901")
                {
                    sourceModelVersion = ModelVersion.Model901;
                }
                else if (smv == "100")
                {
                    sourceModelVersion = ModelVersion.Model100;
                }
                else if (smv == "101")
                {
                    sourceModelVersion = ModelVersion.Model101;
                }
            }
            sourceModel = GetModel(sourceModelVersion, useCustomModel);

            Console.Write($"Source endpoint certificate (default: {sourceCertificateFile}): ");
            var scf = Console.ReadLine();
            if (!string.IsNullOrEmpty(scf) && File.Exists(scf))
            {
                sourceCertificateFile = scf;
            }

            #region Source Certificate Password
            sourceCertificatePassword = new SecureString();

            Console.Write("Password for the source pfx or press enter to use the default password: ");
            ConsoleKeyInfo key;
            do
            {
                key = Console.ReadKey(true);

                // Ignore any key out of range.
                if (((int)key.Key) >= 65 && ((int)key.Key <= 90))
                {
                    // Append the character to the password.
                    sourceCertificatePassword.AppendChar(key.KeyChar);
                    Console.Write("*");
                }
                // Exit if Enter key is pressed.
            }
            while (key.Key != ConsoleKey.Enter);
            if (sourceCertificatePassword.Length == 0)
            {
                "P@ssw0rd".ToList()
                    .ForEach(c => { sourceCertificatePassword.AppendChar(c); });
            }
            Console.WriteLine();
            #endregion

            Console.WriteLine();

            string targetCollectEndpoint = ConfigurationManager.AppSettings["targetCollectEndpoint"];
            string targetSearchEndpoint = ConfigurationManager.AppSettings["targetSearchEndpoint"];
            ModelVersion targetModelVersion = ModelVersion.Model101;
            object targetModel;
            string targetCertificateFile = ConfigurationManager.AppSettings["targetCertificate"];
            SecureString targetCertificatePassword;

            Console.Write($"Target collect endpoint (default: {targetCollectEndpoint}): ");
            var dcep = Console.ReadLine();
            if (!string.IsNullOrEmpty(dcep) && Uri.IsWellFormedUriString(dcep, UriKind.Absolute))
            {
                targetCollectEndpoint = dcep;
            }

            Console.Write($"Target search endpoint (default: {targetSearchEndpoint}): ");
            var dsep = Console.ReadLine();
            if (!string.IsNullOrEmpty(dsep) && Uri.IsWellFormedUriString(dsep, UriKind.Absolute))
            {
                targetSearchEndpoint = dsep;
            }

            Console.Write("Target model version 901, 100 or 101, (default: 101): ");
            var dmv = Console.ReadLine();
            if (!string.IsNullOrEmpty(dmv))
            {
                if (dmv == "901")
                {
                    targetModelVersion = ModelVersion.Model901;
                }
                else if (dmv == "100")
                {
                    targetModelVersion = ModelVersion.Model100;
                }
                else if (dmv == "101")
                {
                    targetModelVersion = ModelVersion.Model101;
                }
            }

            targetModel = GetModel(targetModelVersion, useCustomModel);

            Console.Write($"Target endpoint certificate (default: {targetCertificateFile}): ");
            var dcf = Console.ReadLine();
            if (!string.IsNullOrEmpty(dcf) && File.Exists(dcf))
            {
                targetCertificateFile = dcf;
            }

            #region Destination Certificate Password
            targetCertificatePassword = new SecureString();
            Console.Write("Password for the target pfx or press enter to use the default password: ");
            do
            {
                key = Console.ReadKey(true);

                // Ignore any key out of range.
                if (((int)key.Key) >= 65 && ((int)key.Key <= 90))
                {
                    // Append the character to the password.
                    targetCertificatePassword.AppendChar(key.KeyChar);
                    Console.Write("*");
                }
                // Exit if Enter key is pressed.
            }
            while (key.Key != ConsoleKey.Enter);
            if (targetCertificatePassword.Length == 0)
            {
                "P@ssw0rd".ToList().ForEach(c => { targetCertificatePassword.AppendChar(c); });
            }
            Console.WriteLine();
            #endregion

            string sourceCertificateStoreLocation = ConfigurationManager.AppSettings["sourceCertificateStore"];
            string targetCertificateStoreLocation = ConfigurationManager.AppSettings["targetCertificateStore"];

            sourceClient = await GetClient(sourceModel, sourceCollectEndpoint, sourceSearchEndpoint, sourceCertificateStoreLocation, sourceCertificateFile, sourceCertificatePassword);
            targetClient = await GetClient(targetModel, targetCollectEndpoint, targetSearchEndpoint, targetCertificateStoreLocation, targetCertificateFile, targetCertificatePassword);
        }

        /// This is only applicable if you have anonymous contact indexing turned off in the IndexWorker.
        internal void IdentifyContacts(XConnectClient client, string[] contacts)
        {
            List<Guid> contactIds = new List<Guid>();

            foreach (var contact in contacts)
            {
                Guid.TryParse(contact.Split(',')[0], out Guid contactId);
                if (contactId != Guid.Empty)
                {
                    contactIds.Add(contactId);
                }
            }

            Log($"Ready to identify {contactIds.Count} contacts.", addNewLine: true);

            int contactsIdentified = 0;

            /// If you have a specific facet that is really important to you and that contact is NOT identified yet
            /// you will not be able to get them out of xConnect if you search for that facet. For these scenario's
            /// I made a list of contacts based on raw SQL to get a list of contacts I wanted to identify first so I could get them from xConnectSearch.
            /// 
            foreach (var contactId in contactIds)
            {
                try
                {
                    var contactIdIdentifier = new ContactReference(contactId);
                    var contact = client.Get(contactIdIdentifier,
                        // If you want to filter on contacts with only a specific facet
                        //new ContactExecutionOptions(new ContactExpandOptions(MyCustomFacet.DefaultFacetKey))
                        new ContactExecutionOptions(new ContactExpandOptions())
                        );
                    if (contact != null)
                    {
                        // If you want to filter on contacts with only a specific facet
                        //if (!contact.Facets.ContainsKey(MyCustomFacet.DefaultFacetKey))
                        //{
                        //    continue;
                        //}

                        if (!contact.Identifiers.Any(i => i.Source == "mycustomidentifier"))
                        {
                            Console.Write("*");
                            client.AddContactIdentifier(contact, new ContactIdentifier("mycustomidentifier", $"mycustomidentifier-{Guid.NewGuid()}", ContactIdentifierType.Known));

                            client.Submit();
                            contactsIdentified++;
                        }
                        if (contactsIdentified % 1000 == 0)
                        {
                            Console.Write("#");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log(ex.Message);
                }
            }

            Console.WriteLine();
            Log($"Identified {contactsIdentified} contacts");

        }

        public async Task GetInfo(XConnectClient client)
        {
            try
            {
                PrintXConnect();

                Log($"Endpoint: {client.Configuration.SearchClient.ServiceBaseAddress.AbsoluteUri}");
                Log($"XConnectClient CombinedModel: {client.Model}");
                Log($"Modelversion: {client.Model.Version}");
                foreach (var model in client.Model.ReferencedModels)
                {
                    Log($"ReferencedModel => {model.Name} : {model.Version}");
                }

                Log("Connection verified!", ConsoleColor.Green);
            }
            catch (XdbModelConflictException ce)
            {
                Log("ERROR:" + ce.Message, ConsoleColor.Red);
                return;
            }

            try
            {
                var results0 = await client.Contacts.CountAsync();

                Log("Total contacts: " + results0.ToString());

                // Use InteractionsCache instead of client.Contacts.Where(x => x.Interactions.Any()) as not all search providers support joins
                var results = await client.Contacts
                    .Where(c => c.InteractionsCache().InteractionCaches.Any())
                    .GetBatchEnumerator();

                Log("Total contacts with interactions: " + results.TotalCount.ToString());

                var contactExecutionOptions = GetContactExecutionOptions();
                var results1 = await client.Contacts
                        .Where(c => c.InteractionsCache().InteractionCaches.Any())
                        .WithExpandOptions(contactExecutionOptions.ExpandOptions)
                        .GetBatchEnumerator();

                Log($"Total contacts with interactions after {startDate} : {results1.TotalCount}");

                Console.WriteLine();
            }
            catch (XdbExecutionException ex)
            {
                Console.WriteLine(ex.Message);
                // Deal with exception
            }

        }

        public async Task TransferContacts()
        {
            var contactExecutionOptions = GetContactExecutionOptions();

            int contactsProcessed = 0, contactsCreated = 0, interactionsProcessed = 0;

            var sourceSerializerSettings = new JsonSerializerSettings
            {
                ContractResolver = new XdbJsonContractResolver(sourceClient.Model,
                serializeFacets: true,
                serializeContactInteractions: true),
                DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                DefaultValueHandling = DefaultValueHandling.Ignore,
            };

            var targetSerializerSettings = new JsonSerializerSettings
            {
                ContractResolver = new XdbJsonContractResolver(targetClient.Model,
                serializeFacets: true,
                serializeContactInteractions: true),
                DateTimeZoneHandling = DateTimeZoneHandling.Utc,
                DefaultValueHandling = DefaultValueHandling.Ignore,                
            };

            try
            {
                byte[] contactBookmark = null;
                var contactCursor = await sourceClient.Contacts
                    .Where(c => c.InteractionsCache().InteractionCaches.Any())
                    .WithExpandOptions(contactExecutionOptions.ExpandOptions)
                    .GetBatchEnumerator(contactBookmark, batchSize);

                var totalContactsCount = contactCursor.TotalCount;
                while (await contactCursor.MoveNextAsync())
                {
                    contactBookmark = contactCursor.GetBookmark();

                    // Batch of x
                    var currentBatch = contactCursor.Current;
                    int interactionsInBatch = currentBatch.Sum(c => c.Interactions.Count);
                    //interactionsProcessed = 0;
                    contactsCreated = 0;
                    //contactsProcessed = 0;

                    Log($"Contacts in batch: {currentBatch.Count}");
                    Log($"Interactions in batch: {interactionsInBatch}");

                    #region Process new contacts first so they exist!
                    foreach (var c in currentBatch.ToList())
                    {
                        Contact contact = null;

                        var identifiers = c.Identifiers.Where(i => i.Source != "Alias").Select(ci => new ContactIdentifier(ci.Source, ci.Identifier, ci.IdentifierType)).ToArray();
                        var xdbIdentifier = identifiers.FirstOrDefault(i => i.Source == "xDB.Tracker");

                        var existingContactTask = targetClient.GetContactAsync(new IdentifiedContactReference(xdbIdentifier.Source, xdbIdentifier.Identifier), contactExecutionOptions);
                        var existingContact = await existingContactTask;
                        if (existingContact == null)
                        {
                            contact = new Contact(identifiers);
                            targetClient.AddContact(contact);
                            contactsCreated++;
                        }
                    }

                    try
                    {
                        Log($"Submitting '{targetClient.DirectOperations.Count}' operations to the destinationClient");
                        await targetClient.SubmitAsync();
                        Log($"Submit done to the destinationClient");
                    }
                    catch (XdbExecutionException ex)
                    {
                        Log(ex.Message);
                    }

                    Log($"Contacts created '{contactsCreated}'");

                    #endregion

                    foreach (var c in currentBatch.ToList())
                    {
                        contactsProcessed++;

                        if (c.LastModified < startDate)
                        {
                            continue;
                        }

                        Contact contact = null;

                        var identifiers = c.Identifiers.Where(i => i.Source != "Alias").Select(ci => new ContactIdentifier(ci.Source, ci.Identifier, ci.IdentifierType)).ToList();
                        var alias = c.Identifiers.FirstOrDefault(i => i.Source == "Alias");
                        if (alias != null)
                        {
                            identifiers.Add(new ContactIdentifier("Alias_Sitecore_9", alias.Identifier, alias.IdentifierType));
                        }

                        var xdbIdentifier = identifiers.FirstOrDefault(i => i.Source == "xDB.Tracker");

                        var existingContactTask = targetClient.GetContactAsync(new IdentifiedContactReference(xdbIdentifier.Source, xdbIdentifier.Identifier), contactExecutionOptions);
                        var existingContact = await existingContactTask;
                        if (existingContact != null)
                        {
                            contact = existingContact;
                        }
                        else
                        {
                            // Process contact
                            contact = new Contact(identifiers.ToArray());
                            //destinationClient.AddContact(contact);
                        }

                        Log($"Processing contact {contactsProcessed} of total {totalContactsCount} contacts :'{contact.Id}'");

                        // Settings all facets
                        #region Contact Facets                       

                        if (c.Facets.ContainsKey(FacetKeys.AddressList))
                        {
                            var serialized = JsonConvert.SerializeObject(c.Facets[FacetKeys.AddressList], sourceSerializerSettings);
                            AddressList sourceFacet = JsonConvert.DeserializeObject<AddressList>(serialized, targetSerializerSettings);
                            var facet = contact.GetFacet<AddressList>(FacetKeys.AddressList);
                            if (facet != null)
                            {
                                facet.PreferredAddress = sourceFacet.PreferredAddress;
                                facet.PreferredKey = sourceFacet.PreferredKey;
                                facet.Others = sourceFacet.Others;
                            }
                            else
                            {
                                facet = new AddressList(sourceFacet.PreferredAddress, sourceFacet.PreferredKey);
                                facet.Others = sourceFacet.Others;
                            }

                            targetClient.SetFacet(contact, FacetKeys.AddressList, facet);
                        }

                        // TODO classification gives errors
                        //if (c.Facets.ContainsKey(FacetKeys.Classification))
                        //{
                        //    var serialized = JsonConvert.SerializeObject(c.Facets[FacetKeys.Classification], sourceSerializerSettings);
                        //    Classification sourceFacet = JsonConvert.DeserializeObject<Classification>(serialized, targetSerializerSettings);
                        //    var facet = contact.GetFacet<Classification>(FacetKeys.Classification) ?? new Classification();
                        //    if (facet != null)
                        //    {
                        //        facet.ClassificationLevel = sourceFacet.ClassificationLevel;
                        //        facet.OverrideClassificationLevel = sourceFacet.OverrideClassificationLevel;
                        //    }

                        //    targetClient.SetFacet(new FacetReference(contact, FacetKeys.Classification), facet);
                        //}

                        if (c.Facets.ContainsKey(FacetKeys.EmailAddressList))
                        {
                            var serialized = JsonConvert.SerializeObject(c.Facets[FacetKeys.EmailAddressList], sourceSerializerSettings);
                            EmailAddressList sourceFacet = JsonConvert.DeserializeObject<EmailAddressList>(serialized, targetSerializerSettings);
                            var facet = contact.GetFacet<EmailAddressList>(FacetKeys.EmailAddressList);
                            if (facet != null)
                            {
                                facet.PreferredEmail = sourceFacet.PreferredEmail;
                                facet.PreferredKey = sourceFacet.PreferredKey;
                                facet.Others = sourceFacet.Others;
                            }
                            else
                            {
                                facet = new EmailAddressList(sourceFacet.PreferredEmail, sourceFacet.PreferredKey);
                                facet.Others = sourceFacet.Others;
                            }

                            targetClient.SetFacet(new FacetReference(contact, FacetKeys.EmailAddressList), facet);
                        }

                        if (c.Facets.ContainsKey(FacetKeys.PhoneNumberList))
                        {
                            var serialized = JsonConvert.SerializeObject(c.Facets[FacetKeys.PhoneNumberList], sourceSerializerSettings);
                            PhoneNumberList sourceFacet = JsonConvert.DeserializeObject<PhoneNumberList>(serialized, targetSerializerSettings);
                            if (sourceFacet.PreferredPhoneNumber != null && !string.IsNullOrWhiteSpace(sourceFacet.PreferredPhoneNumber.Number))
                            {
                                var facet = contact.GetFacet<PhoneNumberList>(FacetKeys.PhoneNumberList);
                                if (facet != null)
                                {
                                    facet.PreferredPhoneNumber = new PhoneNumber(sourceFacet.PreferredPhoneNumber.CountryCode ?? string.Empty, sourceFacet.PreferredPhoneNumber.Number);
                                    facet.PreferredKey = sourceFacet.PreferredKey;
                                    facet.Others = sourceFacet.Others;
                                }
                                else
                                {
                                    facet = new PhoneNumberList(new PhoneNumber(sourceFacet.PreferredPhoneNumber.CountryCode ?? string.Empty, sourceFacet.PreferredPhoneNumber.Number), sourceFacet.PreferredKey);
                                    facet.Others = sourceFacet.Others;
                                }

                                targetClient.SetFacet(contact, FacetKeys.PhoneNumberList, facet);
                            }
                        }

                        if (c.Facets.ContainsKey(FacetKeys.PersonalInformation))
                        {
                            var serialized = JsonConvert.SerializeObject(c.Facets[FacetKeys.PersonalInformation], sourceSerializerSettings);
                            PersonalInformation sourceFacet = JsonConvert.DeserializeObject<PersonalInformation>(serialized, targetSerializerSettings);
                            var facet = contact.GetFacet<PersonalInformation>(FacetKeys.PersonalInformation) ?? new PersonalInformation();
                            if (facet != null)
                            {
                                facet.Birthdate = sourceFacet.Birthdate;
                                facet.FirstName = sourceFacet.FirstName;
                                facet.Gender = sourceFacet.Gender;
                                facet.JobTitle = sourceFacet.JobTitle;
                                facet.LastName = sourceFacet.LastName;
                                facet.MiddleName = sourceFacet.MiddleName;
                                facet.Nickname = sourceFacet.Nickname;
                                facet.PreferredLanguage = sourceFacet.PreferredLanguage;
                                facet.Suffix = sourceFacet.Suffix;
                                facet.Title = sourceFacet.Title;
                            }

                            targetClient.SetFacet(new FacetReference(contact, FacetKeys.PersonalInformation), facet);
                        }

                        #endregion

                        #region Custom Contact Facets

                        //if (c.Facets.ContainsKey(MyCustomFacet.DefaultFacetKey))
                        //{
                        //    var xobject = c.Facets[MyCustomFacet.DefaultFacetKey];
                        //    var serialized = JsonConvert.SerializeObject(c.Facets[MyCustomFacet.DefaultFacetKey], sourceSerializerSettings);
                        //    MyCustomFacet sourceFacet = JsonConvert.DeserializeObject<MyCustomFacet>(serialized, targetSerializerSettings);
                        //    var facet = contact.GetFacet<MyCustomFacet>(MyCustomFacet.DefaultFacetKey);
                        //    if (facet == null)
                        //    {
                        //        facet = new MyCustomFacet();
                        //    }

                        //    facet.PropertyX = sourceFacet.PropertyX;
                        //    facet.PropertyY = sourceFacet.PropertyY;

                        //    targetClient.SetFacet<MyCustomFacet>(contact, MyCustomFacet.DefaultFacetKey, facet);

                        //}

                        #endregion

                        if (!skipInteractions)
                        {
                            foreach (var sourceInteraction in c.Interactions)
                            {
                                interactionsProcessed++;

                                Interaction addOrUpdateInteraction = contact.Interactions?.FirstOrDefault(oi => oi.StartDateTime == sourceInteraction.StartDateTime && oi.EndDateTime == sourceInteraction.EndDateTime);

                                if (addOrUpdateInteraction != null)
                                {
                                    Log($"Processing interaction {interactionsProcessed} of total {interactionsInBatch} interactions in batch : interaction already exists ('{sourceInteraction.Id}' -> '{addOrUpdateInteraction.Id}'", color: ConsoleColor.Yellow);
                                    // Update interaction?
                                }
                                else
                                {
                                    addOrUpdateInteraction = new Interaction(contact, InteractionInitiator.Brand, sourceInteraction.ChannelId, sourceInteraction.UserAgent);
                                    if (sourceInteraction.Events != null)
                                    {
                                        sourceInteraction.Events.ToList().ForEach(sourceEvent =>
                                        {
                                            Event targetEvent;

                                            if (sourceEvent is CampaignEvent ce)
                                            {
                                                CampaignEvent newEvent = new CampaignEvent(ce.CampaignDefinitionId, ce.Timestamp);
                                                targetEvent = newEvent;
                                            }
                                            else if (sourceEvent is DownloadEvent de)
                                            {
                                                DownloadEvent newEvent = new DownloadEvent(de.Timestamp, de.ItemId);
                                                targetEvent = newEvent;
                                            }
                                            else if (sourceEvent is Goal g)
                                            {
                                                Goal newEvent = new Goal(g.DefinitionId, g.Timestamp);
                                                targetEvent = newEvent;
                                            }
                                            else if (sourceEvent is MVTestTriggered mvt)
                                            {
                                                MVTestTriggered newEvent = new MVTestTriggered(mvt.Timestamp);
                                                newEvent.Combination = mvt.Combination;
                                                newEvent.EligibleRules = mvt.EligibleRules;
                                                newEvent.ExposureTime = mvt.ExposureTime;
                                                newEvent.FirstExposure = mvt.FirstExposure;
                                                newEvent.IsSuspended = mvt.IsSuspended;
                                                newEvent.ValueAtExposure = mvt.ValueAtExposure;
                                                targetEvent = newEvent;
                                            }
                                            else if (sourceEvent is Outcome o)
                                            {
                                                Outcome newEvent = new Outcome(o.DefinitionId, o.Timestamp, o.CurrencyCode, o.MonetaryValue);
                                                targetEvent = newEvent;
                                            }
                                            else if (sourceEvent is PageViewEvent pve)
                                            {
                                                PageViewEvent newEvent = new PageViewEvent(
                                                    pve.Timestamp,
                                                    pve.ItemId,
                                                    pve.ItemVersion,
                                                    pve.ItemLanguage);
                                                newEvent.SitecoreRenderingDevice = pve.SitecoreRenderingDevice;
                                                newEvent.Url = pve.Url;
                                                targetEvent = newEvent;
                                            }
                                            else if (sourceEvent is PersonalizationEvent pe)
                                            {
                                                PersonalizationEvent newEvent = new PersonalizationEvent(pe.Timestamp);
                                                newEvent.ExposedRules = pe.ExposedRules;
                                                targetEvent = newEvent;
                                            }
                                            else if (sourceEvent is SearchEvent se)
                                            {
                                                SearchEvent newEvent = new SearchEvent(se.Timestamp);
                                                newEvent.Keywords = se.Keywords;
                                                targetEvent = newEvent;
                                            }
                                            #region EXM related, disabled those
                                            //else if (sourceEvent is BounceEvent be)
                                            //{
                                            //    BounceEvent newEvent = new BounceEvent(be.Timestamp);
                                            //    newEvent.BounceReason = be.BounceReason;
                                            //    newEvent.BounceType = be.BounceType;
                                            //    targetEvent = newEvent;
                                            //}
                                            //else if (sourceEvent is DispatchFailedEvent dfe)
                                            //{
                                            //    DispatchFailedEvent newEvent = new DispatchFailedEvent(dfe.Timestamp);
                                            //    newEvent.FailureReason = dfe.FailureReason;
                                            //    targetEvent = newEvent;
                                            //}
                                            //else if (sourceEvent is EmailClickedEvent ece)
                                            //{
                                            //    EmailClickedEvent newEvent = new EmailClickedEvent(ece.Timestamp);
                                            //    newEvent.Url = ece.Url;
                                            //    targetEvent = newEvent;
                                            //}
                                            //else if (sourceEvent is EmailOpenedEvent eoe)
                                            //{
                                            //    EmailOpenedEvent newEvent = new EmailOpenedEvent(eoe.Timestamp);
                                            //    targetEvent = newEvent;
                                            //}
                                            //else if (sourceEvent is EmailSentEvent ese)
                                            //{
                                            //    EmailSentEvent newEvent = new EmailSentEvent(ese.Timestamp);
                                            //    targetEvent = newEvent;
                                            //}
                                            //else if (sourceEvent is SpamComplaintEvent sce)
                                            //{
                                            //    SpamComplaintEvent newEvent = new SpamComplaintEvent(sce.Timestamp);
                                            //    targetEvent = newEvent;
                                            //}
                                            //else if (sourceEvent is UnsubscribedFromEmailEvent uee)
                                            //{
                                            //    UnsubscribedFromEmailEvent newEvent = new UnsubscribedFromEmailEvent(uee.Timestamp);
                                            //    targetEvent = newEvent;
                                            //}
                                            #endregion
                                            else
                                            {
                                                targetEvent = new Event(sourceEvent.DefinitionId, sourceEvent.Timestamp);
                                            }

                                            targetEvent.Data = sourceEvent.Data;
                                            targetEvent.Id = sourceEvent.Id;
                                            targetEvent.Duration = sourceEvent.Duration;
                                            targetEvent.ParentEventId = sourceEvent.ParentEventId;
                                            targetEvent.EngagementValue = sourceEvent.EngagementValue;
                                            targetEvent.DataKey = sourceEvent.DataKey;
                                            targetEvent.Text = sourceEvent.Text;

                                            if (sourceEvent.CustomValues != null)
                                            {
                                                foreach (var key in sourceEvent.CustomValues.Keys)
                                                {
                                                    targetEvent.CustomValues.Add(key, sourceEvent.CustomValues[key]);
                                                }
                                            }

                                            addOrUpdateInteraction.Events.Add(targetEvent);
                                        });
                                    }

                                    targetClient.AddInteraction(addOrUpdateInteraction);
                                    Log($"Processing interaction {interactionsProcessed} of total {interactionsInBatch} interactions in batch : interaction added ('{sourceInteraction.Id}' -> '{addOrUpdateInteraction.Id}'", color: ConsoleColor.Green);

                                }

                                if (sourceInteraction.Facets.ContainsKey(UserAgentInfo.DefaultFacetKey))
                                {
                                    var serialized = JsonConvert.SerializeObject(sourceInteraction.Facets[UserAgentInfo.DefaultFacetKey], sourceSerializerSettings);
                                    UserAgentInfo sourceFacet = JsonConvert.DeserializeObject<UserAgentInfo>(serialized, targetSerializerSettings);
                                    var facet = addOrUpdateInteraction.GetFacet<UserAgentInfo>(FacetKeys.UserAgentInfo);
                                    if (facet == null)
                                    {
                                        facet = new UserAgentInfo();
                                    }

                                    facet.CanSupportTouchScreen = sourceFacet.CanSupportTouchScreen;
                                    facet.DeviceType = sourceFacet.DeviceType;
                                    facet.DeviceVendor = sourceFacet.DeviceVendor;
                                    facet.DeviceVendorHardwareModel = sourceFacet.DeviceVendorHardwareModel;

                                    targetClient.SetFacet(addOrUpdateInteraction, FacetKeys.UserAgentInfo, facet);
                                }

                                // todo get original types from source
                                //if (sourceInteraction.Facets.ContainsKey(IpInfo.DefaultFacetKey))
                                //{
                                //    var serialized = JsonConvert.SerializeObject(sourceInteraction.Facets[IpInfo.DefaultFacetKey], sourceSerializerSettings);
                                //    IpInfo sourceFacet = JsonConvert.DeserializeObject<IpInfo>(serialized, targetSerializerSettings);
                                //    var facet = addOrUpdateInteraction.GetFacet<IpInfo>(FacetKeys.IpInfo);
                                //    if (facet == null)
                                //    {
                                //        facet = new IpInfo(sourceFacet.IpAddress);
                                //    }

                                //    facet.AreaCode = sourceFacet.AreaCode ?? string.Empty;
                                //    facet.BusinessName = sourceFacet.BusinessName ?? string.Empty;
                                //    facet.City = sourceFacet.City ?? string.Empty;
                                //    facet.Country = sourceFacet.Country ?? string.Empty;
                                //    facet.Isp = sourceFacet.Isp ?? string.Empty;
                                //    facet.Latitude = sourceFacet.Latitude;
                                //    facet.Longitude = sourceFacet.Longitude;
                                //    facet.LocationId = sourceFacet.LocationId;
                                //    facet.MetroCode = sourceFacet.MetroCode ?? string.Empty;
                                //    facet.PostalCode = sourceFacet.PostalCode ?? string.Empty;
                                //    facet.Region = sourceFacet.Region ?? string.Empty;
                                //    facet.Url = sourceFacet.Url ?? string.Empty;
                                //    facet.Dns = sourceFacet.Dns ?? string.Empty;

                                //    targetClient.SetFacet(addOrUpdateInteraction, FacetKeys.IpInfo, facet);
                                //}

                                //if (sourceInteraction.Facets.ContainsKey(ProfileScores.DefaultFacetKey))
                                //{
                                //    //TODO implement
                                //    //xConnectConnector.targetClient.SetFacet(
                                //    //    addOrUpdateInteraction,
                                //    //    ProfileScores.DefaultFacetKey,
                                //    //    addOrUpdateInteraction.ProfileScores().WithClearedConcurrency());
                                //}


                                // todo get original types from source
                                //if (sourceInteraction.Facets.ContainsKey(WebVisit.DefaultFacetKey))
                                //{
                                //    var serialized = JsonConvert.SerializeObject(sourceInteraction.Facets[WebVisit.DefaultFacetKey], sourceSerializerSettings);
                                //    WebVisit sourceFacet = JsonConvert.DeserializeObject<WebVisit>(serialized, targetSerializerSettings);
                                //    var facet = addOrUpdateInteraction.GetFacet<WebVisit>(FacetKeys.WebVisit);
                                //    if (facet == null)
                                //    {
                                //        facet = new WebVisit();
                                //    }

                                //    facet.Browser = new BrowserData();
                                //    if (sourceFacet.Browser != null)
                                //    {
                                //        facet.Browser.BrowserMajorName = sourceFacet.Browser.BrowserMajorName;
                                //        facet.Browser.BrowserMinorName = sourceFacet.Browser.BrowserMinorName;
                                //        facet.Browser.BrowserVersion = sourceFacet.Browser.BrowserVersion;
                                //    }
                                //    facet.IsSelfReferrer = sourceFacet.IsSelfReferrer;
                                //    facet.Language = sourceFacet.Language;
                                //    facet.OperatingSystem = sourceFacet.OperatingSystem;
                                //    facet.Referrer = sourceFacet.Referrer;
                                //    facet.Screen = new ScreenData();
                                //    if (sourceFacet.Screen != null)
                                //    {
                                //        facet.Screen.ScreenHeight = sourceFacet.Screen.ScreenHeight;
                                //        facet.Screen.ScreenWidth = sourceFacet.Screen.ScreenWidth;
                                //    }

                                //    facet.SearchKeywords = sourceFacet.SearchKeywords;
                                //    facet.SiteName = sourceFacet.SiteName;

                                //    targetClient.SetFacet(addOrUpdateInteraction, FacetKeys.WebVisit, facet);
                                //}

                            }
                        }

                        if (contactsProcessed % 50 == 0 || interactionsProcessed % 50 == 0)
                        {
                            // Submit every now and then
                            try
                            {
                                Log($"Submitting '{targetClient.DirectOperations.Count}' operations to the destinationClient");
                                await targetClient.SubmitAsync();
                                Log($"Submit done to the destinationClient");
                            }

                            catch (XdbExecutionException ex)
                            {
                                Console.WriteLine(ex.Message);

                                //// Handle conflicts that may have happened when updating existing contacts
                                //var setPersonalOperations = ex.GetOperations(targetClient)
                                //    .OfType<SetFacetOperation<PersonalInformation>>()
                                //    .Where(x => x.Result.Status == SaveResultStatus.Conflict);

                                //var setPhoneNumberListOperations = ex.GetOperations(targetClient)
                                //    .OfType<SetFacetOperation<PhoneNumberList>>();

                                //var setIpInfoOperations = ex.GetOperations(targetClient)
                                //    .OfType<SetFacetOperation<IpInfo>>();

                                //// Handle contacts that already exist
                                //var contactExists = ex.GetOperations(targetClient).OfType<AddContactOperation>()
                                //    .Where(x => x.Result.Status == SaveResultStatus.AlreadyExists);
                            }
                        }
                    }

                    try
                    {
                        Log($"Submitting '{targetClient.DirectOperations.Count}' operations to the destinationClient");
                        await targetClient.SubmitAsync();
                        Log($"Submit done to the destinationClient");
                    }
                    catch (XdbExecutionException ex)
                    {
                        Log(ex.Message);

                        // Handle conflicts that may have happened when updating existing contacts
                        //var setPersonalOperations = ex.GetOperations(targetClient)
                        //    .OfType<SetFacetOperation<PersonalInformation>>()
                        //    .Where(x => x.Result.Status == SaveResultStatus.Conflict);

                        //var setWebVisitOperations = ex.GetOperations(targetClient)
                        //    .OfType<SetFacetOperation<WebVisit>>()
                        //    .Where(x => x.Result.Status == SaveResultStatus.Conflict);

                        //var setPhoneNumberListOperations = ex.GetOperations(targetClient)
                        //    .OfType<SetFacetOperation<PhoneNumberList>>()
                        //    .Where(x => x.Result.Status == SaveResultStatus.Conflict);

                        //var setIpInfoOperations = ex.GetOperations(targetClient)
                        //    .OfType<SetFacetOperation<IpInfo>>()
                        //    .Where(x => x.Result.Status == SaveResultStatus.Conflict);

                        //// Handle contacts that already exist
                        //var contactExists = ex.GetOperations(targetClient).OfType<AddContactOperation>()
                        //    .Where(x => x.Result.Status == SaveResultStatus.AlreadyExists);

                    }
                }
            }
            catch (XdbExecutionException ex)
            {
                Log(ex.Message);

                // Handle conflicts that may have happened when updating existing contacts
                //var setPersonalOperations = ex.GetOperations(xConnectConnector.targetClient)
                //    .OfType<SetFacetOperation<PersonalInformation>>()
                //    .Where(x => x.Result.Status == SaveResultStatus.Conflict);

                //var setPhoneNumberListOperations = ex.GetOperations(targetClient)
                //    .OfType<SetFacetOperation<PhoneNumberList>>();

                //var setIpInfoOperations = ex.GetOperations(targetClient)
                //    .OfType<SetFacetOperation<IpInfo>>();

                //// Handle contacts that already exist
                //var contactExists = ex.GetOperations(targetClient).OfType<AddContactOperation>()
                //    .Where(x => x.Result.Status == SaveResultStatus.AlreadyExists);

            }
            catch (Exception ex)
            {
                Log(ex.Message);
            }
        }

        private async Task<XConnectClient> GetClient(object model, string collectionEndpoint, string searchEndpoint, string certificateStoreLocation, string certificateFile, SecureString certificatePassword)
        {
            XdbModel modelType = model as XdbModel;
            XConnectClientConfiguration config = GetXconnectClientConfiguration(modelType, collectionEndpoint, searchEndpoint, certificateStoreLocation, certificateFile, certificatePassword);

            try
            {
                await config.InitializeAsync();
            }
            catch (XdbCollectionUnavailableException xdbEx)
            {
                Log(xdbEx.Message);
            }
            catch (Exception ex)
            {
                Log(ex.Message);
            }

            var client = new XConnectClient(config);
            return client;
        }

        private void Log(string message, ConsoleColor color = ConsoleColor.White, bool addNewLine = false)
        {
            string logMessage = $"{DateTime.Now.ToString("HH:mm:ss.fff")} {message}";

            using (var fw = File.AppendText(LogFile))
            {
                fw.WriteLine(logMessage);
            }

            var prevColor = Console.ForegroundColor;

            Console.ForegroundColor = color;

            Console.WriteLine(logMessage);
            if (addNewLine)
            {
                Console.WriteLine();
            }

            Console.ForegroundColor = prevColor;
        }

        private static void PrintXConnect()
        {
            // Print xConnect if configuration is valid
            var arr = new[]
            {
                        @"            ______                                                       __     ",
                        @"           /      \                                                     |  \    ",
                        @" __    __ |  $$$$$$\  ______   _______   _______    ______    _______  _| $$_   ",
                        @"|  \  /  \| $$   \$$ /      \ |       \ |       \  /      \  /       \|   $$ \  ",
                        @"\$$\/  $$| $$      |  $$$$$$\| $$$$$$$\| $$$$$$$\|  $$$$$$\|  $$$$$$$ \$$$$$$   ",
                        @" >$$  $$ | $$   __ | $$  | $$| $$  | $$| $$  | $$| $$    $$| $$        | $$ __  ",
                        @" /  $$$$\ | $$__/  \| $$__/ $$| $$  | $$| $$  | $$| $$$$$$$$| $$_____   | $$|  \",
                        @"|  $$ \$$\ \$$    $$ \$$    $$| $$  | $$| $$  | $$ \$$     \ \$$     \   \$$  $$",
                        @" \$$   \$$  \$$$$$$   \$$$$$$  \$$   \$$ \$$   \$$  \$$$$$$$  \$$$$$$$    \$$$$ "
                    };
            Console.WindowWidth = 160;
            foreach (string line in arr)
            {
                Console.WriteLine(line);
            }
            Console.WriteLine();
            Console.WriteLine();
        }

        private XConnectClientConfiguration GetXconnectClientConfiguration(object model, string collectionEndpoint, string searchEndpoint, string certificateStoreLocation, string certificateFile, SecureString certificatePassword)
        {
            XdbModel modelType = model as XdbModel;
            WriteJsonModel(modelType);

            List<IHttpClientModifier> clientModifiers = new List<IHttpClientModifier>();

            clientModifiers.Add(new TimeoutHttpClientModifier(new TimeSpan(0, 0, 20)));

            CollectionWebApiClient collectionClient;
            SearchWebApiClient searchClient;
            ConfigurationWebApiClient configurationClient;

            if (usePfx)
            {
                var pfxCertificateModifier = certificatePassword.Length == 0 ? new PfxCertificateHttpClientHandlerModifier(certificateFile) : new PfxCertificateHttpClientHandlerModifier(certificateFile, certificatePassword);

                collectionClient = new CollectionWebApiClient(new Uri($"{collectionEndpoint}/odata"), clientModifiers, new[] { pfxCertificateModifier });
                searchClient = new SearchWebApiClient(new Uri($"{searchEndpoint}/odata"), clientModifiers, new[] { pfxCertificateModifier });
                configurationClient = new ConfigurationWebApiClient(new Uri($"{collectionEndpoint}/configuration"), clientModifiers, new[] { pfxCertificateModifier });
            }
            else
            {
                CertificateHttpClientHandlerModifierOptions options = CertificateHttpClientHandlerModifierOptions.Parse(certificateStoreLocation);
                var certificateModifier = new CertificateHttpClientHandlerModifier(options);

                collectionClient = new CollectionWebApiClient(new Uri($"{collectionEndpoint}/odata"), clientModifiers, new[] { certificateModifier });
                searchClient = new SearchWebApiClient(new Uri($"{searchEndpoint}/odata"), clientModifiers, new[] { certificateModifier });
                configurationClient = new ConfigurationWebApiClient(new Uri($"{collectionEndpoint}/configuration"), clientModifiers, new[] { certificateModifier });
            }

            bool dataExtract = true;

            var cfg = new XConnectClientConfiguration(
                new XdbRuntimeModel(modelType),
                collectionClient,
                searchClient,
                configurationClient,
                dataExtract);

            return cfg;
        }

        private static object GetModel(ModelVersion version, bool customModel = false)
        {
            XdbModel requestedModel = null;
            try
            {
                var callingAssemblyPath = new DirectoryInfo(Assembly.GetCallingAssembly().Location.Substring(0, Assembly.GetCallingAssembly().Location.LastIndexOf('\\')));

                if (customModel)
                {
                    var assmCustomModel = Assembly.LoadFile($@"{callingAssemblyPath.FullName}\..\..\lib\AlexVanWolferen.XConnect.Models.{version}.dll");
                    var custommodeltype = assmCustomModel.GetTypes().FirstOrDefault(t => t.FullName == "AlexVanWolferen.XConnect.Models.MyCustomModel");
                    MethodInfo staticModel = custommodeltype.GetMethod("get_Model");

                    requestedModel = staticModel.Invoke(null, null) as XdbModel;
                }
                else
                {
                    var assm = Assembly.LoadFile($@"{callingAssemblyPath.FullName}\..\..\lib\Sitecore.XConnect.Collection.{version}.dll");
                    var modtype = assm.GetTypes().FirstOrDefault(t => t.FullName == "Sitecore.XConnect.Collection.Model.CollectionModel");
                    MethodInfo staticModel = modtype.GetMethod("get_Model");

                    requestedModel = staticModel.Invoke(null, null) as XdbModel;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("There are issues finding the xConnect model.");
                Console.WriteLine(ex.Message);
            }

            return requestedModel;
        }

        private void WriteJsonModel(XdbModel modelType)
        {
            var json = XdbModelWriter.Serialize(modelType, false);
            File.WriteAllText(modelType + ".json", json);
        }

        private ContactExecutionOptions GetContactExecutionOptions()
        {
            // Include all facets that you want to migrate
            var ceo = new ContactExecutionOptions(new ContactExpandOptions(new[]
            {
                FacetKeys.PersonalInformation
                , FacetKeys.EmailAddressList
                , FacetKeys.Classification
                , FacetKeys.PhoneNumberList
                , FacetKeys.AddressList
                //, MyCustomFacet.DefaultFacetKey
            })
            {
                Interactions = new RelatedInteractionsExpandOptions(new[] { FacetKeys.IpInfo, FacetKeys.UserAgentInfo, FacetKeys.WebVisit })
                {
                    StartDateTime = startDate
                }
            });

            return ceo;
        }

        //private InteractionExecutionOptions GetInteractionExecutionOptions()
        //{
        //    // Include all facets that you want to migrate
        //    return new InteractionExecutionOptions(new InteractionExpandOptions(FacetKeys.IpInfo)
        //    {
        //        Contact = new RelatedContactExpandOptions(
        //            FacetKeys.PersonalInformation,
        //            FacetKeys.EmailAddressList,
        //            FacetKeys.Classification,
        //            FacetKeys.PhoneNumberList,
        //            FacetKeys.UserAgentInfo,
        //            FacetKeys.WebVisit,
        //            FacetKeys.IpInfo,
        //            FacetKeys.AddressList)
        //    });
        //}
    }
}
