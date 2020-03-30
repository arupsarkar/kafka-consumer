const express = require("express");
const port = process.env.PORT || 5000;
const cors = require("cors");
var Kafka = require('no-kafka');
const app = express();

//middleware
app.use(cors());
// request data from req.body from the client
app.use(express.json());

// kafka integration
var consumer = new Kafka.SimpleConsumer();
// data handler function can return a Promise
var dataHandler = function (messageSet, topic, partition) {
    messageSet.forEach(function (m) {
        console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
    });
};

consumer.init().then(function () {
    // Subscribe all partitons
    consumer.subscribe('apalachicola-477.interactions', dataHandler);
});

app.listen(port, () => {
    console.log("Node server started on port " + port);
});