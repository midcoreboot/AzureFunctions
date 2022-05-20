using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net.Http;
using System;
using System.Text;
using System.Linq;
using System.Text.Json;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Azure.Core.Pipeline;
using Azure;

namespace JTHSmartSpace.AzureFunctions
{
    public static class PropagateStateChange
    { 

        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly string adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");

        //private static readonly string azureMapsSubscriptionKey = Environment.GetEnvironmentVariable("AZURE_MAPS_SUB_KEY");
        //private static readonly string azureMapsStatesetsVariable = Environment.GetEnvironmentVariable("AZURE_MAPS_STATESETS");
        //private static readonly JObject azureMapsStateSets = JObject.Parse(azureMapsStatesetsVariable);

        private static DigitalTwinsClient dtClient;

        [FunctionName("PropagateStateChange")]
        public static async Task Run(
            [EventHubTrigger("twins-event-hub", Connection = "EventHubAppSetting-Twins")]EventData myEventHubMessage,
            [EventHub("tsi-event-hub", Connection = "EventHubAppSetting-TSI")]IAsyncCollector<string> outputEvents,
            ILogger log)
        {

            if (adtServiceUrl == null) {
                log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
                return;
            }

            DefaultAzureCredential credentials = new DefaultAzureCredential();
            dtClient = new DigitalTwinsClient(new Uri(adtServiceUrl), credentials, new DigitalTwinsClientOptions{
                Transport = new HttpClientTransport(httpClient)
            });

            string capabilityTwinId = myEventHubMessage.Properties["cloudEvents:subject"].ToString();
            JObject message = (JObject)JsonConvert.DeserializeObject(Encoding.UTF8.GetString(myEventHubMessage.Body));
            log.LogInformation($"Reading event: '{message}' from twin '{capabilityTwinId}'.");

            // Get the twin from ADT that was updated to cause the incoming message
            // Query verifies that the twin is an instance of REC capability subclass
            List<BasicDigitalTwin> capabilityTwins = new List<BasicDigitalTwin>();
            string capabilityTwinQuery = $"SELECT * FROM DIGITALTWINS DT WHERE IS_OF_MODEL(DT, 'dtmi:digitaltwins:rec_3_3:core:Capability;1') AND DT.$dtId = '{capabilityTwinId}'";
            await foreach (BasicDigitalTwin returnedTwin in dtClient.QueryAsync<BasicDigitalTwin>(capabilityTwinQuery))
            {
                capabilityTwins.Add(returnedTwin);
                break;
            }
            // If we get anything but a single result back from the above, something has gone wrong; the twin does not exist or is the wrong type
            if (capabilityTwins.Count != 1) {
                log.LogInformation($"The modified twin {capabilityTwinId} is not defined as a REC Capability in ADT.");
                return;
            }
            BasicDigitalTwin capabilityTwin = capabilityTwins.First();

            // Extract name of capability twin. Only proceed if such a name exists.
            string capabilityName = "";
            if (capabilityTwin.Contents.ContainsKey("name") && capabilityTwin.Contents["name"] is JsonElement) {
                capabilityName = ((JsonElement)capabilityTwin.Contents["name"]).ToString().Trim();
            }
            if (capabilityName == "") {
                log.LogError($"Capability twin {capabilityTwinId} does not have a name assigned.");
                log.LogError(capabilityTwin.ToString());
                return;
            }
            
            // Extract value from incoming message JSON patch. Only proceed if the patch assigns a value to /lastValue
            JToken capabilityValue = message["patch"].Children<JObject>()
                .Where(
                    patch => 
                        patch.ContainsKey("path") && patch["path"].ToString() == "/lastValue" &&
                        patch.ContainsKey("op") && (patch["op"].ToString() == "replace" || patch["op"].ToString() == "add") &&
                        patch.ContainsKey("value"))
                .Select(patch => patch["value"]).FirstOrDefault();
            if (capabilityValue == null) {
                log.LogInformation($"Incoming event does not contain a JSON patch for /lastValue.");
                return;
            }

            // At this stage, we have enough information to propagate the state to TSI
            await PropagateStateToTSI(capabilityTwinId, capabilityName, capabilityValue, outputEvents, log);

            // And also to Azure Maps
            // await PropagateStateToAzureMapsAsync(capabilityTwinId, capabilityName, capabilityValue, log);
        }

        /*
        private static async Task PropagateStateToAzureMapsAsync(string capabilityTwinId, string capabilityName, JToken capabilityValue, ILogger log) {
            
            // Find parent to which the capability applies, using isCapabilityOf relationships
            List<string> parentTwinIds = new List<string>();
            AsyncPageable<BasicRelationship> rels = dtClient.GetRelationshipsAsync<BasicRelationship>(capabilityTwinId, "isCapabilityOf");
            await foreach (BasicRelationship relationship in rels)
            {
                parentTwinIds.Add(relationship.TargetId);
            }
            if (parentTwinIds.Count != 1) {
                log.LogInformation($"Capability twin {capabilityTwinId} isn't assigned to exactly one parent.");
                return;
            }
            string parentTwinId = parentTwinIds.First();
            Response<BasicDigitalTwin> parentTwinResponse = await dtClient.GetDigitalTwinAsync<BasicDigitalTwin>(parentTwinId);
            BasicDigitalTwin parentTwin = parentTwinResponse.Value;
            
            // Based on parent twin, get Azure Maps Feature ID (from ADT model using externalIds property w/ AzureMaps key)
            string AzureMapsFeatureID = "";
            if (parentTwin.Contents.ContainsKey("externalIds") && parentTwin.Contents["externalIds"] is JsonElement) {
                JsonElement externalIds = (JsonElement)parentTwin.Contents["externalIds"];
                if (externalIds.TryGetProperty("AzureMaps", out JsonElement AzureMapsIdElement)) {
                    AzureMapsFeatureID = AzureMapsIdElement.GetString();
                }
            }
            if (string.IsNullOrEmpty(AzureMapsFeatureID)) {
                log.LogError($"Parent twin {parentTwin} does not have externalIds/AzureMaps set.");
                return;
            }

            // Load the Azure Maps stateset ID (configured in an env. variable JSON fragment) that corresponds to this capability name
            if (!azureMapsStateSets.ContainsKey(capabilityName)) {
                log.LogInformation($"There is no configured Azure Maps stateset supporting the capability type {capabilityName}.");
                return;
            }
            string statesetID = azureMapsStateSets[capabilityName].ToString();

            // Update the maps feature stateset
            JObject postcontent = new JObject(
                new JProperty(
                    "States",
                    new JArray(
                        new JObject(
                            new JProperty("keyName", capabilityName),
                            new JProperty("value", capabilityValue.ToString()),
                            new JProperty("eventTimestamp", DateTime.UtcNow.ToString("s"))))));

            var response = await httpClient.PutAsync(
                $"https://eu.atlas.microsoft.com/featurestatesets/{statesetID}/featureStates/{AzureMapsFeatureID}?api-version=2.0&subscription-key={azureMapsSubscriptionKey}",
                new StringContent(postcontent.ToString())
            );
            log.LogInformation($"Azure Maps response:\n\t" + await response.Content.ReadAsStringAsync());
        }
        */

        private static async Task PropagateStateToTSI(string capabilityTwinId, string capabilityName, JToken capabilityValue, IAsyncCollector<string> outputEvents, ILogger log) {
            var tsiUpdate = new Dictionary<string, object>();
            tsiUpdate.Add("$dtId", capabilityTwinId);
            tsiUpdate.Add(capabilityName, capabilityValue);
            log.LogInformation($"SENDING: {JsonConvert.SerializeObject(tsiUpdate)}");
            await outputEvents.AddAsync(JsonConvert.SerializeObject(tsiUpdate));
        }
    }
}