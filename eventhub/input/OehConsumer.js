// before running, either globally install kafka-node  (npm install kafka-node)
// or add kafka-node to the dependencies of the local application

var kafka = require('kafka-node');
var Consumer = kafka.Consumer

var async = require('async');

var client;


// from the Oracle Event Hub - Platform Cluster Connect Descriptor
var topicName = "a516817-AtlTopicLgo";
var EVENT_HUB_PUBLIC_IP = "129.150.77.116";

console.log("Event Hub Host " + EVENT_HUB_PUBLIC_IP);
console.log("Event Hub Topic " + topicName);

var subscribers = [];

var eventListenerAPI = module.exports;

eventListenerAPI.subscribeToEvents = function (callback) {
    subscribers.push(callback);
}


var consumerOptions = {
    host: EVENT_HUB_PUBLIC_IP,
    groupId: "atl-event-hub-consumer-application",
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};


var topics = [topicName];
var consumerGroup = new kafka.ConsumerGroup(Object.assign({ id: 'consumerLocal' }, consumerOptions), topics);

consumerGroup.on('error', onError);
consumerGroup.on('message', onMessage);
consumerGroup.on('connect', function () {
    console.log('connected to ' + topicName + " at " + consumerOptions.host);
})


function onMessage(message) {
    console.log('%s read msg Topic="%s" Partition=%s Offset=%d'
    , this.client.clientId, message.topic, message.partition, message.offset);
    //    console.log("Message Value " + message.value)

    subscribers.forEach((subscriber) => {
        subscriber(message.value);

    })
}

function onError(error) {
    console.error(error);
    console.error(error.stack);
}

// when SIGINT is received, close consumers
process.once('SIGINT', function () {
    async.each([consumerGroup], function (consumer, callback) {
        consumer.close(true, callback);
    });
});

// add a subscriber that will print the event to the console
eventListenerAPI.subscribeToEvents( function(event){
  console.log("Received an event from the Kafka Topic " + event)
})
