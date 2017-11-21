/*
Rabbitmq wrapper for promise based connection

 */
const amqp = require('amqplib/callback_api');
let exchange = '';
let exchangeFanout = '';
let channel;
let connection;

/*const connect = (config) => {
    return new Promise(function (resolve, reject) {
        try {
            exchange = config.exchange;
            exchangeFanout = config.exchange + ".fanout";
            amqp.connect(config.url, function (err, conn) {
                connection = conn;
                if (err) {
                    console.error("[AMQP]", err);
                    reject(err);
                }
                console.log("[AMQP] connected");
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
};*/
const connect = (config,cb) => {

    exchange = config.exchange;
    exchangeFanout = config.exchange+".fanout";
    amqp.connect(config.url, function (err, conn) {
        connection = conn;
        if (err) {
            console.error("[AMQP]", err);
            return setTimeout(connect.bind(this,config,cb), 1000);
        }
        conn.on("error", function(err) {
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err);
                return setTimeout(connect.bind(this,config,cb), 1000);
            }
        });
        conn.on("close", function(err) {
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err);
                return setTimeout(connect.bind(this,config,cb), 1000);
            }
        });

        console.log("[AMQP] connected");
        conn.createConfirmChannel(function (err, ch) {
            console.log("Connected to rabbit");
            channel = ch;
            channel.assertExchange(exchange, 'direct', {durable: true});
            channel.assertExchange(exchangeFanout, 'fanout', {durable: true});
            if(cb)
                cb(ch);

        });

    });
}

const publish = (msg, key) => {
    return new Promise(function(resolve, reject) {
        try {

            channel.publish(exchange, key, new Buffer(msg), {persistent: true}, function (err, ok) {
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
                            resolve(msg.content.toString());
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
    channel.assertQueue(queue, {durable:true},function(err, q) {
        console.log(' [*] Waiting for data on'+q.queue);
        channel.bindQueue(q.queue, exchange, key);
        //  channel.bindQueue(q.queue, exchangeFanout, key);
        //Fetch 5 messages in a time and wait for ack on those
        channel.prefetch(5);
        channel.consume(q.queue, function(msg) {
            cb(() => {channel.ack(msg);},() => {channel.nack();},msg.content.toString());
        }, {noAck: false});
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



const use = (queue,services, ...params) => {
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

                Promise.resolve(services[msg.properties.type].resolve(msg.content.toString(),...params))
                    .then((sendMsg) => {
                        if (typeof sendMsg !== 'string') {
                            sendMsg = JSON.stringify(sendMsg);
                        }
                        channel.sendToQueue(msg.properties.replyTo,
                            new Buffer(sendMsg),
                            {correlationId: msg.properties.correlationId});
                    })
                    .catch((err) => console.error(err));
            }
        }(msg))
        channel.ack(msg);
    });
};


module.exports = {
    connect:connect,
    publish:publish,
    listen:listen,
    RPCListen:RPCListen,
    RPC:RPC,
    fanout:fanout,
    listenFanout,listenFanout,
    disconnect: (cb) => {
        if(connection) {
            connection.close(() => {
                cb();
            })
        } else {cb();}},
    use:use
};