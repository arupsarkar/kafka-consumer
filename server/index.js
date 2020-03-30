const express = require("express");
const port = process.env.PORT || 5000;
const cors = require("cors");
const Kafka = require('no-kafka');
const Promise = require('bluebird');

const app = express();

//middleware
app.use(cors());
// request data from req.body from the client
app.use(express.json());

// kafka integration
let consumer = new Kafka.SimpleConsumer();
// data handler function can return a Promise
let dataHandler = function (messageSet, topic, partition) {
    return Promise.each(messageSet, function (m){
        console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
        // commit offset
        return consumer.commitOffset({topic: topic, partition: partition, offset: m.offset, metadata: 'optional'});
    });
};

let strategies = [{
    subscriptions: ['apalachicola-477.interactions'],
    handler: dataHandler
}];

consumer.init(strategies);

app.listen(port, () => {
    console.log("Node server started on port " + port);
});