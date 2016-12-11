var util = require('util');
var EventEmitter = require('events').EventEmitter;
var ConsumerTopology = require('./consumer-topology');

function Consumer(name, options){
    EventEmitter.call(this);

    this._options = options || {};
    this._client = this._options.client;
    this._pub = this._options.pub;
    this._sub = this._options.sub;

    this._name = name;
    this._topology = new ConsumerTopology(this._name, {client: this._client});
    this._pubName = "queue.req." + this._name;
    this._subName = "queue.res." + this._name;
    this._queueName = "queue." + this._name;
    this._consumerQueue = this._consumerQueueNameByUUID(this._topology.uuid());

    this._callbacks = {};

    this._processMessage = null;
    this._processMessageId = null;
    this._processMessageNeedAck = null;

    this._inProgress = false;

    this._queueIsEmpty = false;

    this._onMessage = this._onMessage.bind(this);
    this._onFree = this._onFree.bind(this);
    this.checkTopology = this.checkTopology.bind(this, function(err){
        if (err) {
            console.error(err);
        }
    });
    this._init();
}

util.inherits(Consumer, EventEmitter);

Consumer.prototype.stop = function (callback) {
    var self = this;
    this._topology.stop(function(err){
        if (err) return callback(err);
        self._topology.removeAllListeners('check');
        self._sub.removeListener('message', self._onMessage);
    });
};

Consumer.prototype._init = function (callback) {
    callback = callback || function(){};
    var self = this;

    this._sub.on('message', this._onMessage);
    this.once('connected', this._onFree);
    this._topology.on('check', this.checkTopology);

    this._topology.register(function(err){
        if (err) return callback(err);
        self._sub.subscribe(self._pubName, function(err){
            if (err) return callback(err);
            self.emit('connected');
        });
    });
};

Consumer.prototype._onMessage = function (channel, message) {
    if (channel != this._pubName) return;
    if (message === "new") {
        this._onNewTask();
    }
};

Consumer.prototype._onNewTask = function () {
    var self = this;
    if (this._inProgress) {
        self._registerOnFree();
        return;
    }
    this._inProgress = true;
    var callback = function(err, task){
        if (err) {
            console.error(err, task);
            self.ack();
        }
    };
    this._client.rpoplpush(this._queueName, this._consumerQueue, function(err, taskStr){
        if (err) return callback(err);
        if (taskStr === null) {
            self._queueIsEmpty = true;
            self._unlockQueue(null, callback);
            return;
        }
        var task;
        try {
            task = JSON.parse(taskStr);
            self._processMessage = taskStr;
            self._processMessageId = task.id;
            self._processMessageNeedAck = !!task.ack;
            if (!task.data) throw new Error("bad task data");
        } catch (e) {
            callback(new Error("can't parse task"), taskStr);
            return;
        }

        self.emit('message', task.data);
        if (!self._processMessageNeedAck) {
            self.ack(function(err){
                callback(null);
            });
        } else {
            callback(null);
        }
    });
};

Consumer.prototype._onFree = function () {
    this._onNewTask(this._pubName, "new");
};

Consumer.prototype._registerOnFree = function () {
    this.removeListener('free', this._onFree);
    this.once('free', this._onFree);
};

Consumer.prototype.ack = function (data, callback) {
    if (typeof data == 'function') {
        callback = data;
        data = null;
    }
    callback = callback || function(){};
    var self = this;

    var cb = function(err){
        self._unlockQueue(err, callback);
    };

    this._removeTaskFromQueue(function(err, isOk){
        if (err) return cb(err);
        if (!isOk) return cb(new Error("Already removed from ack queue"));

        if (!self._processMessageNeedAck) {
            cb();
        } else {
            var message = {data, id: self._processMessageId};
            self._pub.publish(self._subName, JSON.stringify(message), function(err){
                if (err) return cb(err);
                cb(null);
            });
        }
    });
};

Consumer.prototype._unlockQueue = function (err, callback) {
    this._inProgress = false;
    if (!this._queueIsEmpty) {
        this._registerOnFree();
    }
    this.emit('free');
    return callback(err);
};

Consumer.prototype._removeTaskFromQueue = function (callback) {
    var task = this._processMessage;
    if (typeof task == 'undefined') return callback();

    this._client.lrem(this._consumerQueue, 1, task, callback);
};

Consumer.prototype.checkTopology = function (callback) {
    var self = this;
    // TODO: check tasks for leave nodes and process it
    this._topology.check(function(multi, allNodes, leaveNodes, deadNodes, done){
        deadNodes.forEach(function(id){
            self._topology.addToUnregisterByUUID(multi, id);
        });
        Promise.all(
            deadNodes.map(id => {
                return new Promise((resolve, reject) => {
                    self.getCountTasksForNode(id, function(err, len, qName){
                        if (err) return reject(err);
                        for (var i=0; i<len; i++) {
                            multi.rpoplpush(qName, self._queueName);
                        }
                        resolve();
                    });
                });
            })
        ).then(() => {
            done();
        }).catch(done);
    }, function(err){
        callback(err);
    });
};

Consumer.prototype.getCountTasksForNode = function (id, callback) {
    var qName = this._consumerQueueNameByUUID(id);
    this._client.lrange(qName, 0, -1, function(err, tasks){
        if (err) return callback(err);
        if (tasks === null || !tasks.length) return callback(null, 0, qName);
        return callback(null, tasks.length, qName);
    });
};

Consumer.prototype._consumerQueueNameByUUID = function (uuid) {
    return "queue.cq." + uuid;
};

module.exports = Consumer;