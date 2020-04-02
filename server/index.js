const express = require('express');
const bodyParser = require('body-parser')
const port = process.env.PORT || 5000;
const cors = require("cors");
const Promise = require('bluebird');
const Kafka = require('no-kafka');
const { pool } = require('./config')
const consumer = new Kafka.SimpleConsumer();
consumer.init();
let kafkaPrefix = process.env.KAFKA_PREFIX;
if (kafkaPrefix === undefined) {
  kafkaPrefix = '';
}
// const kafka = require('kafka-node'),
//     Producer = kafka.Producer,
//     KeyedMessage = kafka.KeyedMessage,
//     client = new kafka.KafkaClient(),
//     producer = new Producer(client),
//     km = new KeyedMessage('key', 'message');

const app = express();

//middleware
app.use(cors());
// request data from req.body from the client
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.get('/', async(req, res, next) => {

    const data = { topic: 'key', messages: 'hi'};
    res.send(data);
});

// kafka integration - start

// data handler function can return a Promise
let dataHandler = function (messageSet, topic, partition ) {
    console.log(new Date(), topic);
    console.log(new Date(), partition);
    console.log(new Date(), messageSet);
    // check for null
    if(messageSet) {
      messageSet.forEach(function (m) {
        //console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
        let data = m.message.value;
        console.log(new Date(), '---> Tweet data - start ') ;        
        console.log(Date.now(), JSON.stringify(data));
        console.log(new Date(), '---> Tweet data - end ') ;                
        //console.log(JSON.stringify(m.message.value.toString('utf8')));
        console.log(new Date(), '---> save to db - start ') ;
        console.log(new Date(), '---> save to db - end ') ;        
      });
    }
  
  };

  consumer.subscribe(kafkaPrefix + 'interactions', dataHandler).then(r => {
    if(r) {
      console.log(new Date(), '---> consumer result ' + JSON.stringify(r) ) ;

    }else {
      console.log(new Date(), '---> consumer result is null ') ;
    }
  });

// kafka integration - end


app.listen(port, () => {
    console.log("Node server started on port " + port);
});