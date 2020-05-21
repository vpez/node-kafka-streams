const {KafkaStreams} = require("kafka-streams");
const config = require("./config.js").nativeConfig;
const kafkaStreams = new KafkaStreams(config);

const mapper = (message) => {
    let obj = JSON.parse(message.value);
    return {
        timestamp: message.timestamp,
        name: obj.name,
        value: obj.value
    }
};

const init = (window) => {
    return {
        items: [],
        createdAt: undefined,
        updatedAt: undefined,
        addItem: function (item) {
            this.items.push(item);
            if (this.createdAt === undefined) {
                this.createdAt = item.timestamp;
            }
            this.updatedAt = item.timestamp;
        },
        isFull: function () {
            return this.updatedAt - this.createdAt >= window * 1000;
        }
    }
};

const createStreamWithBuckets = (input, output, aggregator, seconds) => {
    const bucketsHolder = {};

    const stream = kafkaStreams.getKStream(input);
    stream
        .map(mapper)
        .map(item => {
            const key = item.name;

            if (bucketsHolder[key] === undefined) {
                bucketsHolder[key] = init(seconds);
            }

            let bucket = bucketsHolder[key];
            if (bucket.isFull()) {
                bucket = init(seconds);
                bucketsHolder[key] = bucket;
            }

            bucket.addItem(item);

            return bucket;
        })
        .filter(bucket => bucket.isFull())
        .map(bucket => {

            const key = bucket.items[0].name;

            return {
                name: key,
                timestamp: bucket.updatedAt,
                value: aggregator(bucket.items)
            }
        })
        .mapStringify()
        .to(output);

    return stream;
};

const createStream = (input, output, filter, aggregator, seconds) => {
    const stream = kafkaStreams.getKStream(input);
    stream.map(mapper)
        .filter(obj => obj.name === filter)
        .scan((bucket, item) => {

            if (bucket.isFull()) {
                bucket = init(seconds)
            }

            bucket.addItem(item);
            return bucket;
        }, init(seconds))
        .filter(bucket => bucket.isFull())
        .map(bucket => {
            return {
                filter: filter,
                timestamp: bucket.updatedAt,
                value: aggregator(bucket.items)
            }
        })
        .mapStringify()
        .to(output);

    return stream;
};

module.exports = {
    //create: createStream
    create: createStreamWithBuckets
};