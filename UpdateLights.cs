using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.Devices;
using Microsoft.Extensions.Logging;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Azure.Core.Pipeline;
using Azure;
using RealEstateCore;

namespace JTH_Smart_Space_AzureFunctions
{
    public class UpdateLights
    {
        private static readonly HttpClient httpClient = new HttpClient();
        private static readonly string adtServiceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static DigitalTwinsClient dtClient;
        private static readonly string prefix = "[UpdateLights]: ";

        [FunctionName("UpdateLights")]
        public async Task Run([EventHubTrigger("devicemessage-event-hub", Connection = "EventHubAppSetting-Devicemessage")] EventData myEventHubMessage, ILogger log)
        {

            if (adtServiceUrl == null)
            {
                log.LogError("Application setting \"ADT_SERVICE_URL\" not set");
                return;
            }

            DefaultAzureCredential credentials = new DefaultAzureCredential();
            dtClient = new DigitalTwinsClient(new Uri(adtServiceUrl), credentials, new DigitalTwinsClientOptions
            {
                Transport = new HttpClientTransport(httpClient)
            });

            string capabilityTwinId = myEventHubMessage.Properties["cloudEvents:subject"].ToString();
            JObject message = (JObject)JsonConvert.DeserializeObject(Encoding.UTF8.GetString(myEventHubMessage.Body));
            log.LogInformation($"{prefix}Reading event: '{message}' from twin '{capabilityTwinId}'.");

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
            if (capabilityTwins.Count != 1)
            {
                log.LogInformation($"{prefix}The modified twin {capabilityTwinId} is not defined as a REC Capability in ADT.");
                return;
            }
            BasicDigitalTwin capabilityTwin = capabilityTwins.First();

            // Extract name of capability twin. Only proceed if such a name exists.
            string capabilityName = "";
            if (capabilityTwin.Contents.ContainsKey("name") && capabilityTwin.Contents["name"] is JsonElement)
            {
                capabilityName = ((JsonElement)capabilityTwin.Contents["name"]).ToString().Trim();
            }
            if (capabilityName == "")
            {
                log.LogError($"{prefix}Capability twin {capabilityTwinId} does not have a name assigned.");
                log.LogError(prefix + capabilityTwin.ToString());
                return;
            }
            log.LogInformation($"{prefix}THIS IS THE CAPABILITY NAME: {capabilityName} for id : {capabilityTwinId}");
            //{"modelId":"dtmi:digitaltwins:rec_3_3:device:Switch;1","patch":[{"value":true,"path":"/lastValue","op":"add"}]}
            // Extract value from incoming message JSON patch. Only proceed if the patch assigns a value to /lastValue
            JToken modelId = message["modelId"];
            if(modelId.ToString() == "dtmi:digitaltwins:rec_3_3:device:Switch;1")
            {
                log.LogInformation($"{prefix}Found switch model with name: {capabilityName}");
                JToken capabilityValue = message["patch"].Children<JObject>()
                    .Where(patch =>
                        patch.ContainsKey("path") && patch["path"].ToString() == "/lastValue" &&
                        patch.ContainsKey("op") && (patch["op"].ToString() == "replace" || patch["op"].ToString() == "add") &&
                        patch.ContainsKey("value"))
                   .Select(patch => patch["value"]).FirstOrDefault();
                bool lightOn = (bool)capabilityValue;
                var updateTwinData = new JsonPatchDocument();
                updateTwinData.AppendAdd("/lastValue", lightOn);
                log.LogInformation($"{prefix}Patching digital twin with patch: {updateTwinData}, capabilityValue = {capabilityValue}, bool = {lightOn}");
                //string lightName = capabilityName + "Light";


                List<String> lights = new List<String>();
                //Get this digital twin's relationships 
                AsyncPageable<IncomingRelationship> rel = dtClient.GetIncomingRelationshipsAsync(capabilityTwinId);
                //Loop through all of the relationships
                await foreach(IncomingRelationship relationship in rel)
                {
                    //log.LogInformation($"{prefix}Found relationship from {capabilityTwinId}, relationship : : : : : {relationship.RelationshipName} from {relationship.SourceId} id: {relationship.RelationshipId}");
                    if(relationship.RelationshipName == "controlledBy")
                    {
                        lights.Add(relationship.SourceId);
                    }
                }
                if(lights.Count > 0)
                {
                    try
                    {
                        foreach(string light in lights)
                        {
                            //string connectionStringIoT = "LIGHTS_CONNECTIONSTRING_" + light;
                            string iotConnectionStr = Environment.GetEnvironmentVariable("IOTHUB_CONNECTION_STRING");
                            var serviceClient = ServiceClient.CreateFromConnectionString(iotConnectionStr);

                            string twinQuery = $"SELECT * FROM DIGITALTWINS DT WHERE DT.$dtId = '{light}'";
                            List<BasicDigitalTwin> twins = new List<BasicDigitalTwin>();
                            await foreach (BasicDigitalTwin returnedTwin in dtClient.QueryAsync<BasicDigitalTwin>(twinQuery))
                            {
                                twins.Add(returnedTwin);
                                break;
                            }
                            // If we get anything but a single result back from the above, something has gone wrong; the twin does not exist or is the wrong type
                            if (twins.Count != 1)
                            {
                                log.LogInformation($"{prefix}The modified twin {light} is not defined as a REC Capability in ADT.");
                                return;
                            }
                            BasicDigitalTwin lightTwin = twins.First();
                            // Extract name of capability twin. Only proceed if such a name exists.
                            // The name of the lights digital twin must be the same as the device id on the iot hub.
                            string lightName = "";
                            if (lightTwin.Contents.ContainsKey("name") && lightTwin.Contents["name"] is JsonElement)
                            {
                                lightName = ((JsonElement)lightTwin.Contents["name"]).ToString().Trim();
                            }
                            if (lightName == "")
                            {
                                log.LogError($"{prefix}Capability twin {light} does not have a name assigned.");
                                log.LogError(lightTwin.ToString());
                                return;
                            }
                            var messageContent = lightOn.ToString();
                            //var commandMessage = new Message(Encoding.ASCII.GetBytes(messageContent));
                            var commandMessage = new Message(Encoding.ASCII.GetBytes(messageContent));
                            serviceClient.SendAsync(lightName, commandMessage).GetAwaiter().GetResult();
                            await dtClient.UpdateDigitalTwinAsync(light, updateTwinData);
                        }
                    }
                    catch (Exception ex)
                    {
                        log.LogError(ex, ex.Message);
                    }
                }
            }
        }
    }
}
