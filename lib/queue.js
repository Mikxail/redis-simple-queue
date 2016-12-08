var redis = require('redis');
var Producer = require('./producer');
var Consumer = require('./consumer');

function Queue(name, redisOptions){
    this._name = name;
    this._redisOptions = redisOptions || {};

    this._client = null;
    this._pub = null;
    this._sub = null;
}

Queue.prototype.producer = function () {
    return new Producer(this._name, {
        client: this._getClient(),
        pub: this._getPub(),
        sub: this._getSub()
    });
};

Queue.prototype.consumer = function () {
    return new Consumer(this._name, {
        client: this._getClient(),
        pub: this._getPub(),
        sub: this._getSub()
    });
};

Queue.prototype._getClient = function () {
    this._client = this._client || redis.createClient(this._redisOptions);
    return this._client;
};

Queue.prototype._getPub = function () {
    this._pub = this._pub || redis.createClient(this._redisOptions);
    return this._pub;
};

Queue.prototype._getSub = function () {
    this._sub = this._sub || redis.createClient(this._redisOptions);
    return this._sub;
};

module.exports = Queue;