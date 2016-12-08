var util = require('util');
var EventEmitter = require('events').EventEmitter;
var uuid = require('uuid').v4;

function Producer(queueName, options){
    EventEmitter.call(this);

    this._name = queueName;
    this._pubName = "queue.req." + this._name;
    this._subName = "queue.res." + this._name;
    this._queueName = "queue." + this._name;
    this._options = options || {};
    this._client = this._options.client;
    this._pub = this._options.pub;
    this._sub = this._options.sub;

    this._callbacks = {};

    this._onMessage = this._onMessage.bind(this);
    this._init();
}

util.inherits(Producer, EventEmitter);

Producer.prototype._init = function (callback) {
    callback = callback || function(){};
    var self = this;
    this._sub.on("message", this._onMessage);
    this._sub.subscribe(this._subName, function(err){
        if (err) return callback(err);
        self.emit('connected');
    });
};

Producer.prototype.add = function (data, callback) {
    var id = uuid();
    var message = {data, id};
    if (typeof callback == 'function') {
        this._callbacks[id] = callback;
        Object.assign(message, {ack: true});
    }
    var self = this;
    this._client.lpush(this._queueName, JSON.stringify(message), function(err, len){
        if (err) return self._callCallback(id, err);

        self._pub.publish(self._pubName, "new", function(err){
            // return self._callCallback(id, err);
        });
    });
};

Producer.prototype._callCallback = function (id, err, data) {
    if (!this._callbacks[id]) return;
    this._callbacks[id].call(this, err, data);
};

Producer.prototype._onMessage = function (channel, message) {
    if (channel != this._subName) return;
    try {
        message = JSON.parse(message);
    } catch (e) {
        console.error("Can't parse response json", message, channel);
        return;
    }
    var id = message.id;
    this._callCallback(id, null, message.data);
};

module.exports = Producer;