var util = require('util');
var EventEmitter = require('events').EventEmitter;
var uuid = require('uuid').v4;

function crc1(str) {
    var crc = 0;
    str = String(str);
    var accum = 0;

    var byte;
    for (var index = 0; index < str.length; index++) {
        byte = str.charCodeAt(index);
        accum += byte
    }

    crc += accum % 256;
    return crc % 256;
}

// TODO: can be refactored and moved to separate module
function ConsumerTopology(name, options){
    EventEmitter.call(this);

    this._uuid = uuid();
    this._index = null;
    this._name = name;
    this._topologyListName = "queue.ctop." + this._name;

    this._options = options || {};
    this._client = this._options.client;

    this._pingTimeout = this._options.pingTimeout || 500;
    this._renewTimeout = (this._pingTimeout * 0.7)|0;
    this._renewTimeoutOnUpdError = 100;

    this._timer = null;

    this._checkTopologyTimer = setInterval(function(){
        this.emit('check');
    }.bind(this), this._pingTimeout);
}

util.inherits(ConsumerTopology, EventEmitter);

ConsumerTopology.prototype.uuid = function () {
    return this._uuid;
};

ConsumerTopology.prototype.register = function (callback) {
    var self = this;

    this.renew(function(err){
        if (err) return callback(err);

        self._client.lpush(self._topologyListName, self._uuid, function(err, index){
            if (err) return callback(err);
            self._index = index - 1;
            self._startTimer(self._renewTimeout);
            callback(null, self._uuid);
        });
    });
};

ConsumerTopology.prototype.renew = function (callback) {
    var self = this;
    var aliveKey = this._aliveKeyByUUID(this._uuid);
    this._client.set(aliveKey, this._uuid, 'PX', this._pingTimeout, function(err){
        if (err) return callback(err, self._renewTimeoutOnUpdError);

        callback(null, self._renewTimeout);
    });
};

ConsumerTopology.prototype.stop = function (callback) {
    // TODO: should remove node from topology list
    this._clearTimer();
    callback();
};

ConsumerTopology.prototype.addToUnregisterByUUID = function (multi, id) {
    multi.lrem(this._topologyListName, 1, id);
};

ConsumerTopology.prototype.check = function (fn, callback) {
    var self = this;
    this._client.watch(this._topologyListName, function(err){
        if (err) return callback(err);
        self._client.lrange(self._topologyListName, 0, -1, function(err, results){
            if (err) return callback(err);
            var aliveIds = results.map(id => self._aliveKeyByUUID(id));
            self._client.mget(aliveIds, function(err, aliveResults){
                aliveResults = aliveResults.filter(Boolean);
                var c = aliveResults.length;
                var index = aliveResults.indexOf(self._uuid);
                var watchFor = results.filter(id => {
                    var crc = crc1(id);
                    return crc % c == index;
                });
                var dead = watchFor.filter(id => aliveResults.indexOf(id) == -1);
                var leave = watchFor.filter(id => aliveResults.indexOf(id) != -1);
                var multi = self._client.multi();
                fn(multi, watchFor, leave, dead, function(err){
                    if (err) {
                        multi.discard(function(err2){
                            callback(err)
                        });
                    } else {
                        multi.exec(function(err){
                            callback(err);
                        });
                    }
                });
            });
        });
    });
};


ConsumerTopology.prototype._startTimer = function (timeout) {
    var self = this;
    this._clearTimer();
    this._timer = setTimeout(function(){
        self.renew(function(err, timeout2){
            self._startTimer(timeout2 || timeout);
        });
    }, timeout);
};

ConsumerTopology.prototype._clearTimer = function () {
    if (this._timer) clearTimeout(this._timer);
    this._timer = null;
};

ConsumerTopology.prototype._aliveKeyByUUID = function (uuid) {
    return "queue.calive." + this._name + "." + uuid;
};

module.exports = ConsumerTopology;