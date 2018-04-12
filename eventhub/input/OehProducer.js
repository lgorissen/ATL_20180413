// before running, either globally install kafka-node  (npm install kafka-node)
// or add kafka-node to the dependencies of the local application

var kafka = require('kafka-node')
var Producer = kafka.Producer
KeyedMessage = kafka.KeyedMessage;

var client;

// from the Oracle Event Hub - Platform Cluster Connect Descriptor
var topicName = "a516817-AtlTopicLgo";
var EVENT_HUB_PUBLIC_IP = "129.150.77.116";

console.log("Starting Oracle Event Hub - message producer");

function initializeKafkaProducer(attempt) {
    try {
  
      console.log(`Try to initialize Kafka Client and Producer at ${EVENT_HUB_PUBLIC_IP} , attempt ${attempt}`);

      client = new kafka.Client(EVENT_HUB_PUBLIC_IP);
      console.log("created client");

      producer = new Producer(client);
      console.log("submitted async producer creation request");

      producer.on('ready', function () {
        console.log("Producer is ready");
      });
      producer.on('error', function (err) {
        console.log("failed to create the client or the producer " + JSON.stringify(err));
      })
    }
    catch (e) {
      console.log("Exception in initializeKafkaProducer" + e);
      console.log("Exception in initializeKafkaProducer" + JSON.stringify(e));
      console.log("Try again in 5 seconds");
      setTimeout(initializeKafkaProducer, 5000, ++attempt);
    }
}


var eventPublisher = module.exports;


eventPublisher.publishEvent = function (eventKey, event) {
  km = new KeyedMessage(eventKey, JSON.stringify(event));
  payloads = [
    { topic: topicName, messages: [km], partition: 0 }
  ];
  producer.send(payloads, function (err, data) {
    if (err) {
      console.error("Failed to publish event with key " + eventKey + " to topic " + topicName + " :" + JSON.stringify(err));
    }
    console.log("Published event with key " + eventKey + " to topic " + topicName + " :" + JSON.stringify(data));
  });

}


// initialize the Producer
initializeKafkaProducer(1);

// wait for three seconds to give the producer time to initialize - then send the message
setTimeout( function() {
eventPublisher.publishEvent("mykey",{"kenteken":"56-LGO-3","country":"nl"})}
, 3000)

// wait for then seconds then exit  - this may be tricky if the Producer can not be created at first attempt..
setTimeout( function() {
  process.exit() }
  , 10000)
