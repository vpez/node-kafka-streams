const {KafkaStreams} = require("kafka-streams");
const config = require("./config.js").nativeConfig;
const factory = require('./streamUtils');
const {avg, sum} = require('./aggregators');

const INPUT = "input_1";
const OUTPUT = "output_1";

const kafkaStreams = new KafkaStreams(config);

const stream_a = factory.create(INPUT, OUTPUT, avg, 5);
stream_a.start().then(() => {
    console.log(`stream_a is running: (${INPUT}) => (${OUTPUT})`)
});

// Simulate input
const inputStream = kafkaStreams.getKStream();
inputStream.to(INPUT);
inputStream.start();

for (let i = 0; i < 3; i++) {
    setInterval(() => {
        let char = String.fromCharCode(97 + i);
        let value = Math.floor(Math.random() * 100 + 1);
        inputStream.writeToStream(`{"name": "${char}", "value": "${value}"}`);
    }, 1000);
}

// Consume output
// const outputStream = kafkaStreams.getKStream();
// outputStream.from(OUTPUT)
//     .mapBufferValueToString()
//     .map(message => {
//         return JSON.parse(message.value)
//     })
//     .forEach(v => console.log(v));
// outputStream.start();