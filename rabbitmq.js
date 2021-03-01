/*
 Rabbitmq wrapper for promise based connection

 */
const amqp = require('amqplib/callback_api');
let exchange = '';
let exchangeFanout = '';
let channel;
let connection;

let mappingCorrelationIdConsumerTag = [];

const connect = (config) => {
    return new Promise((resolve, reject) => {
        try {
            exchange = config.exchange;
            exchangeFanout = config.exchange + ".fanout";
            amqp.connect(config.url,  (err, conn) => {
                connection = conn;
                if (err) {
                    console.error("[AMQP]", err);
                    reject(err);
                }
                console.log("[AMQP] connected");

                conn.on("error", (err) => {
                    throw err;
                });
                conn.on("close", (err) => {
                    throw err;
                });

                conn.createConfirmChannel(function (err, ch) {
                    console.log("Connected to rabbit");
                    channel = ch;
                    channel.assertExchange(exchange, 'direct', {durable: true});
                    channel.assertExchange(exchangeFanout, 'fanout', {durable: true});
                    resolve(conn);
                });
            });
        } catch (e) {
            reject(e.message);
            console.error("[AMQP] connect", e.message);
        }
    });
};

const publish = (msg, key) => {
    if(typeof msg === 'object') {
        // Json object, must be stringified
        msg = JSON.stringify(msg);
    }
    return new Promise((resolve, reject) =>  {
        try {
            channel.publish(exchange, key, new Buffer(msg), {persistent: true}, (err, ok) => {
                    if (err !== null) {
                        reject(err);
                        console.warn(' [*] Message nacked');
                    } else {
                        console.log(' [*] Message acked');
                        resolve(msg);
                    }
                }
            );
        } catch (e) {
            reject(e.message);
            console.error("[AMQP] publish", e.message);
        }
    });
};

// Egen variant for å sende array med (msg,key)-par.
// Kan da sende til forskjellige routing keys per msg.
const publishEvents = (events) => {
    if (!Array.isArray(events))
    {
        let event = events;
        events = [];
        events.push(event);
    }

    return Promise.all(events.map((event) => new Promise((resolve, reject) => {
        try {
            if(typeof event.msg === 'object') {
                // Json object, must be stringified
                event.msg = JSON.stringify(event.msg);
            }
            channel.publish(exchange, event.key, new Buffer(event.msg), {persistent: true}, function (err, ok) {
                    if (err !== null) {
                        reject(err);
                        console.warn(' [*] Message nacked');
                    } else {
                        console.log(' [*] Message acked');
                        resolve("Message sendt");
                    }
                }
            );
        } catch (e) {
            reject(e.message);
            console.error("[AMQP] publish", e.message);
        }
    })));
};

const fanout = (sendMsg,key) => {
    return new Promise(function(resolve, reject) {
        try {
            if (typeof sendMsg !== 'string') {
                sendMsg = JSON.stringify(sendMsg);
            }
            channel.publish(exchangeFanout, key, new Buffer(sendMsg), {persistent: true}, function (err, ok) {
                    if (err !== null) {
                        reject(err);
                        console.warn(' [*] Message nacked');
                    } else {
                        console.log(' [*] Message acked');
                        resolve("Message sendt");
                    }
                }
            );
        } catch (e) {
            reject(e.message);
            console.error("[AMQP] publish", e.message);
        }
    });
};


const RPC = (queue,service,sendMsg) =>{
    return new Promise(function(resolve, reject) {
        try {
            connection.createChannel((err, ch) => {
                ch.assertQueue('', {exclusive: true,autoDelete:true}, (err, q) => {
                    const corr = generateUuid();
                    ch.consume(q.queue, (msg) => {
                        if (msg.properties.correlationId === corr) {
                            if (msg.content.toString() === 'Unknown service') {
                                reject(new Error("Unknown service: " + service));
                            }
                            else if (typeof msg.content === 'object') {
                                let parsed = JSON.parse(msg.content.toString());

                                // Check for errors
                                if (parsed.errors) {
                                    // This is the correct way to throw Error from promise
                                    reject(new Error(parsed.errors[0].message));
                                }
                                else
                                {
                                    resolve(parsed);
                                }
                            }
                            ch.close();
                        }
                    }, {noAck: true});

                    if (typeof sendMsg !== 'string') {
                        sendMsg = JSON.stringify(sendMsg);
                    }
                    ch.sendToQueue("RPC." + queue,
                        new Buffer(sendMsg),
                        {correlationId: corr, replyTo: q.queue, type: service}
                    );
                });
            })
        } catch(e) {
            console.error(e);
            reject(e.message);
        }
    });
};

// Egen variant for å sende array med (msg,service)-par.
// Kan da sende til forskjellige services per msg.
const RPCMany = (queue, services) => {
    return Promise.all(services.map((service) => new Promise((resolve, reject) => {
        try {
            connection.createChannel((err, ch) => {
                ch.assertQueue('', {exclusive: true,autoDelete:true}, (err, q) => {
                    const corr = generateUuid();
                    ch.consume(q.queue, (msg) => {
                        if (msg.properties.correlationId === corr) {
                            if (msg.content.toString() === 'Unknown service') {
                                reject(new Error("Unknown service: " + service.service));
                            }
                            else if (typeof msg.content === 'object') {
                                let parsed = JSON.parse(msg.content.toString());

                                // Check for errors
                                if (parsed.errors) {
                                    // This is the correct way to throw Error from promise
                                    reject(new Error(parsed.errors[0].message));
                                }
                                else
                                {
                                    resolve(parsed);
                                }
                            }
                            ch.close();
                        }
                    }, {noAck: true});

                    if (typeof service.msg !== 'string') {
                        service.msg = JSON.stringify(service.msg);
                    }
                    ch.sendToQueue("RPC." + queue,
                        new Buffer(service.msg),
                        {correlationId: corr, replyTo: q.queue, type: service.service}
                    );
                });
            })
        } catch(e) {
            console.error(e);
            reject(e.message);
        }
    })));
};

const generateUuid = () => {
    return Math.random().toString() +
        Math.random().toString() +
        Math.random().toString();
};

const RPCListen = (queue,cb, ...args) => {
    const q = "RPC." + queue;
    channel.assertQueue(q, {durable: false});
    channel.prefetch(1);
    console.log(' [x] Awaiting RPC requests on queue:' + q);
    channel.consume(q, (msg) => {
        (function(msg){
            new Promise((resolve, reject) => {
                let services = cb();
                if (services[msg.properties.type] === undefined) {
                    channel.sendToQueue(msg.properties.replyTo,
                        new Buffer("Unknown service"),
                        {correlationId: msg.properties.correlationId});
                    channel.ack(msg);
                }
                else {
                    services[msg.properties.type](msg.content.toString(), resolve, args);
                }
            }).then((sendMsg) => {
                if (typeof sendMsg !== 'string') {
                    sendMsg = JSON.stringify(sendMsg);
                }

                channel.sendToQueue(msg.properties.replyTo,
                    new Buffer(sendMsg),
                    {correlationId: msg.properties.correlationId});
                channel.ack(msg);
            }).catch((msg) => {
                console.error(msg);
            });
        }(msg))
    });
};
const listen = (queue,key,cb) => {
    channel.assertQueue(queue, {durable:true},(err, q) => {
        console.log(' [*] Waiting for data on'+q.queue);
        channel.bindQueue(q.queue, exchange, key);
        //Fetch 5 messages in a time and wait for ack on those
        channel.prefetch(5);
        channel.consume(q.queue, (msg) => {
            cb(() => {channel.ack(msg);},() => {channel.nack(msg);},msg.content.toString());
        }, {noAck: false});
    });
}

// listen promise-based with consumer canceling
const listenp = (queue, key, correlationId, cb) => {
// NB! CorrelationId er direkte knyttet til consumerTag, dvs. det er en request/correlationId per consumer.
    return new Promise((resolve, reject) =>  {
        try {
            channel.assertQueue(queue, {durable:true}, (err, q) => {
                if (err !== null) {
                    reject(err);
                    console.warn(' [*] Listen rejected');
                } else {
                    console.log(' [*] Waiting for data on ' + q.queue);
                    channel.bindQueue(q.queue, exchange, key);
                    channel.prefetch(5);
                    //https://www.squaremobius.net/amqp.node/channel_api.html#channel_consume
                    channel.consume(q.queue, (msg) => {
                        //console.log('msg.content:');
                        //console.log(msg.content.toString());
                        let s = msg.content.toString();
                        let parsed = JSON.parse(s);

                        // Finner mappingen for denne consumeren
                        const mapping = mappingCorrelationIdConsumerTag.filter(x => x.consumerTag === msg.fields.consumerTag);

                        // Vi må sjekke på length fordi det kan tenkes at den andre exporten kommer inn på samme
                        // consumer som den første (og denne consumeren kan allerede være terminert). Selv om
                        // consumeren er terminert, så vil de prefetchede meldinge allerede trigge callbacken.
                        // Er det samsvar mellom svarets correlationId og opprinnelig request sin correlationId?
                        if (mapping.length > 0 && parsed.securityContext.correlationId === mapping[0].correlationId)
                        {
                            // Vi har mottatt riktig svar. Vi kan kansellere og resolve.
                            channel.ack(msg);
                            console.log(`Reply message consumed successfully with consumerTag ${msg.fields.consumerTag}.`);

                            // Vi kan slette mappingen, siden den ikke har noen funksjon lenger...
                            // (vi har mottatt svaret og tilhørende consumer blir terminert)
                            mappingCorrelationIdConsumerTag = mappingCorrelationIdConsumerTag.filter(x => x.consumerTag !== msg.fields.consumerTag);

                            // Cancel consuming (terminate consumer)
                            channel.cancel(msg.fields.consumerTag, (err, ok)  => {
                                if (err != null)
                                {
                                    console.log('Cancelling consumer failed with error message: ');
                                    console.log(err.message);
                                    reject(err.message);
                                }
                                else
                                {
                                    console.log('Canceled consumer successfully with consumerTag: ', ok.consumerTag);
                                }
                            });
                            resolve(msg.content.toString());
                            //cb(msg.content.toString());
                        }
                        else
                        {
                            // Vi legger svaret tilbake i køa slik at en annen (korrekt) konsumer kan plukke og sjekke
                            channel.nack(msg);
                        }

                    }, {noAck: false}, (err, ok)  => {
                        if (err != null)
                        {
                            console.log('Starting consumer failed with error message: ');
                            console.log(err.message);
                            reject(err.message);
                        }
                        else
                        {
                            console.log('Started consumer successfully with consumerTag: ', ok.consumerTag);
                            // Sjekker om correlationid ligger i array fra før...
                            const mapping = mappingCorrelationIdConsumerTag.filter(x => x.correlationId === correlationId);
                            // Ligger allerede i array
                            if (mapping.length > 0)
                            {
                                console.log("Relation is already in the array");
                            }
                            else {
                                mappingCorrelationIdConsumerTag.push({correlationId: correlationId, consumerTag: ok.consumerTag});
                            }
                        }
                    });
                }
            });
        } catch (e) {
            reject(e.message);
            console.error("[AMQP] listen", e.message);
        }
    });
}

// Set up services (event receivers) server-side
// Use triggers before and after service execution
const useEvents = (queue, services, beforeTrigger, afterTrigger, ...params) => {
    channel.assertQueue(queue, {durable: true});

    // Bind all routing keys
    Object.keys(services.EventTaskMapping).forEach(function(key)
    {
        channel.bindQueue(queue, exchange, key);
    });
    channel.prefetch(5);
    console.log(' [x] Awaiting events on queue:' + queue);
    channel.consume(queue, (msg) => {
        (function(msg){
            if (services.EventTaskMapping[msg.fields.routingKey] === undefined) {
                channel.sendToQueue(msg.properties.replyTo,
                    new Buffer("Unknown event"),
                    {correlationId: msg.properties.correlationId});
            }
            else {

                let task = services.EventTaskMapping[msg.fields.routingKey];
                let service = services.TaskServiceMapping[task];

                if (beforeTrigger)
                {
                    Promise.resolve(beforeTrigger.resolve(task, msg.content.toString(),...params))
                        .then((sendMsg) => {
                        })
                        .catch((err) => console.error(err));
                }

                Promise.resolve(service.resolve(msg.content.toString(), msg.fields.routingKey, ...params))
                    .then((sendMsg) => {
                        if (!sendMsg) {
                            throw new Error("Internal service must return some value");
                        }
                        if (typeof sendMsg !== 'string') {
                            sendMsg = JSON.stringify(sendMsg);
                        }
                        if (afterTrigger)
                        {
                            Promise.resolve(afterTrigger.resolve(task, msg.content.toString(), sendMsg, ...params))
                                .then((sendMsg) => {
                                })
                                .catch((err) => console.error(err));
                        }

                        channel.sendToQueue(msg.properties.replyTo,
                            new Buffer(sendMsg),
                            {correlationId: msg.properties.correlationId});
                    })
                    .catch((err) => {
                        console.log(err);

                        // Returns error message
                        let errors = { errors: [
                                { message: err.message,
                                    detail: err
                                }]};

                        if (afterTrigger)
                        {
                            Promise.resolve(afterTrigger.resolve(task, msg.content.toString(), JSON.stringify(errors), ...params))
                                .then((sendMsg) => {
                                })
                                .catch((err) => console.error(err));
                        }

                        channel.sendToQueue(msg.properties.replyTo,
                            new Buffer(JSON.stringify(errors)),
                            {correlationId: msg.properties.correlationId});
                    });
            }
        }(msg))
        channel.ack(msg);
    });
};
const listenFanout = (queue,key,cb) => {
    channel.assertQueue(queue, {durable:true},function(err, q) {
        console.log(' [*] Waiting for data on'+q.queue);

        channel.bindQueue(q.queue, exchangeFanout, key);
        //Fetch 5 messages in a time and wait for ack on those
        channel.prefetch(5);
        channel.consume(q.queue, function(msg) {
            cb(() => {channel.ack(msg)},msg.content.toString());
        }, {noAck: false});
    });
};


// Set up services (rpc receivers) server-side
// Use triggers before and after service execution
const useRPC = (queue,services, beforeTrigger, afterTrigger, ...params) => {
    const q = "RPC." + queue;
    channel.assertQueue(q, {durable: false});
    channel.prefetch(1);
    console.log(' [x] Awaiting RPC requests on queue:' + q);
    channel.consume(q, (msg) => {
        (function(msg){
            if (services[msg.properties.type] === undefined) {
                channel.sendToQueue(msg.properties.replyTo,
                    new Buffer("Unknown service"),
                    {correlationId: msg.properties.correlationId});
            }
            else {
                if (beforeTrigger)
                {
                    Promise.resolve(beforeTrigger.resolve(msg.properties.type, msg.content.toString(),...params))
                        .then((sendMsg) => {
                        })
                        .catch((err) => console.error(err));
                }

                Promise.resolve(services[msg.properties.type].resolve(msg.content.toString(),
                    msg.properties.type,
                    ...params))
                    .then((sendMsg) => {
                        if (!sendMsg) {
                            throw new Error("Internal service must return some value");
                        }
                        if (typeof sendMsg !== 'string') {
                            sendMsg = JSON.stringify(sendMsg);
                        }
                        if (afterTrigger)
                        {
                            Promise.resolve(afterTrigger.resolve(msg.properties.type, msg.content.toString(), sendMsg, ...params))
                                .then((sendMsg) => {
                                })
                                .catch((err) => console.error(err));
                        }

                        channel.sendToQueue(msg.properties.replyTo,
                            new Buffer(sendMsg),
                            {correlationId: msg.properties.correlationId});
                    })
                    .catch((err) => {
                        console.log(err);

                        // Returns error message
                        let errors = { errors: [
                                { message: err.message,
                                    detail: err
                                }]};

                        if (afterTrigger)
                        {
                            Promise.resolve(afterTrigger.resolve(msg.properties.type, msg.content.toString(), JSON.stringify(errors), ...params))
                                .then((sendMsg) => {
                                })
                                .catch((err) => console.error(err));
                        }

                        channel.sendToQueue(msg.properties.replyTo,
                            new Buffer(JSON.stringify(errors)),
                            {correlationId: msg.properties.correlationId});
                    });
            }
        }(msg))
        channel.ack(msg);
    });
};

module.exports = {
    connect:connect,
    publish:publish,
    publishEvents:publishEvents,
    listen:listen,
    listenp:listenp,
    RPCListen:RPCListen,
    RPC:RPC,
    RPCMany:RPCMany,
    fanout:fanout,
    listenFanout:listenFanout,
    disconnect: (cb) => {
        if(connection) {
            connection.close(() => {
                cb();
            })
        } else {cb();}},
    useRPC:useRPC,
    useEvents:useEvents,
    generateUuid:generateUuid
};