using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace RealEstateCore
{
    public class Observation
    {
        public DateTime observationTime { get; set; }
        [JsonProperty("value")]
        public double? numericValue { get; set; }
        [JsonProperty("valueString")]
        public string stringValue { get; set; }
        [JsonProperty("valueBoolean")]
        public bool? booleanValue { get; set; }
        public Uri quantityKind { get; set; }
        public string sensorId { get; set; }
    }

    public class RecEdgeMessage
    {
        public string format { get { return "rec3.2"; } }
        public string deviceId { get; set; }
        public List<Observation> observations { get; set; }
    }
}