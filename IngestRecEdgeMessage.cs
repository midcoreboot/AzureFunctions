// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.DigitalTwins.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Azure.Core.Pipeline;
using Newtonsoft.Json.Linq;
using Azure;
using Newtonsoft.Json;
using RealEstateCore;

namespace JTHSmartSpace.AzureFunctions
{
    public class IngestRecEdgeMessage
    {

        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly string adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");

        [FunctionName("IngestRecEdgeMessage")]
        public async Task Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            if (adtServiceUrl == null) {
                log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
                return;
            }

            DefaultAzureCredential credentials = new DefaultAzureCredential();
            DigitalTwinsClient dtClient = new DigitalTwinsClient(new Uri(adtServiceUrl), credentials, new DigitalTwinsClientOptions{
                Transport = new HttpClientTransport(httpClient)
            });

            JObject payload = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());
            if (payload.ContainsKey("body")) {
                JObject payloadBody = (JObject)payload["body"];

                if (payloadBody.ContainsKey("format") && ((string)payloadBody["format"]).StartsWith("rec3.2")) {
                    RecEdgeMessage recMessage = JsonConvert.DeserializeObject<RecEdgeMessage>(payloadBody.ToString());
                    foreach (Observation observation in recMessage.observations)
                    {
                        string twinId = observation.sensorId.Replace(":","");

                        var updateTwinData = new JsonPatchDocument();
                        if (observation.numericValue.HasValue) {
                            updateTwinData.AppendAdd("/lastValue", observation.numericValue.Value);
                        }
                        else if (observation.booleanValue.HasValue) {
                            updateTwinData.AppendAdd("/lastValue", observation.booleanValue.Value);
                        }
                        else {
                            updateTwinData.AppendAdd("/lastValue", observation.stringValue);
                        }

                        try {
                            log.LogInformation($"Updating twin '{twinId}' with operation '{updateTwinData.ToString()}'");
                            await dtClient.UpdateDigitalTwinAsync(twinId, updateTwinData);
                        }
                        catch (Exception ex) {
                            log.LogError(ex, ex.Message);
                        }
                    }
                }
            }
        }
    }
}