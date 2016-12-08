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

    this._redisMulti = null;
    this._processMessage = null;
    this._processMessageId = null;
    this._processMessageNeedAck = null;

    this._inProgress = false;

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
        this._onNewTask(channel, message);
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
        }
        self._inProgress = false;
        self.emit('free');
    };
    this._client.rpoplpush(this._queueName, this._consumerQueue, function(err, taskStr){
        if (err) return callback(err);
        if (taskStr === null) return callback();
        var task;
        try {
            task = JSON.parse(taskStr);
            self._processMessage = taskStr;
            self._processMessageId = task.id;
            self._processMessageNeedAck = !!task.ack;
            if (!task.data) throw new Error("bad task data");
        } catch (e) {
            self._removeTaskFromQueue(function(err){
                if (err) return callback(err);
                callback(new Error("can't parse task"), taskStr);
            });
            return;
        }

        if (!self._processMessageNeedAck) {
            self._removeTaskFromQueue(function(err, isOk){
                if (err) return callback(err);
                if (isOk) {
                    self.emit('message', task.data);
                }
                self.ack();
                callback(null);
            });
        } else {
            self.emit('message', task.data);
            callback(null);
        }
    });
};

Consumer.prototype._onFree = function () {
    this._onNewTask(this._pubName, "new");
};

Consumer.prototype._registerOnFree = function () {
    this.removeListener('free', this._onFree);
    this.on('free', this._onFree);
};

Consumer.prototype.ack = function (data, callback) {
    if (typeof data == 'function') {
        callback = data;
        data = null;
    }
    callback = callback || function(){};
    var self = this;
    var cb = function(err){
        self._inProgress = false;
        self.emit('free');
        return callback(err);
    };
    if (!this._processMessageNeedAck) {
        cb();
    }

    this._removeTaskFromQueue(function(err, isOk){
        if (err) return cb(err);
        if (!isOk) return cb(new Error("Already removed from ack queue"));

        var message = {data, id: self._processMessageId};
        self._pub.publish(self._subName, JSON.stringify(message), function(err){
            if (err) return cb(err);
            cb(null);
        });
    });
};

Consumer.prototype._removeTaskFromQueue = function (callback) {
    var task = this._processMessage;
    if (typeof task == 'undefined') return callback();

    this._client.lrem(this._consumerQueue, 1, task, callback);
};

Consumer.prototype.startTransaction = function (callback) {
    if (this._redisMulti) return callback(new Error("Only one transaction per consumer"));

    var self = this;
    this._client.watch(this._consumerQueue, function(err){
        if (err) return callback(err);
        self._redisMulti = self._client.multi();
        callback(null, self._redisMulti);
    });
};

Consumer.prototype.commitTransaction = function (callback) {
    if (!this._redisMulti) return callback(new Error("No one transaction started"));

    var self = this;
    this._redisMulti.exec(function(err, results){
        self._redisMulti = null;
        callback(null, results);
    });
};

Consumer.prototype.rollbackTransaction = function (callback) {
    if (!this._redisMulti) return callback(new Error("No one transaction started"));

    var self = this;
    this._redisMulti.discard(function(err, results){
        self._redisMulti = null;
        callback(err, results);
    });
};

Consumer.prototype.checkTopology = function (callback) {
    var self = this;
    this._topology.check(function(multi, allNodes, leaveNodes, deadNodes, done){
        deadNodes.forEach(function(id){
            self._topology.addToUnregisterByUUID(multi, id);
        });
        Promise.all(
            leaveNodes.map(id => {
                return new Promise((resolve, reject) => {
                    self.getTasksForQueueByNode(id, function(err, qName, tasks){
                        if (err) return reject(err);
                        resolve({qName, tasks});
                    });
                })
            })
        ).then(results => {
            results.forEach(batch => {
                batch.tasks.forEach(task => {
                    multi.lrem(batch.qName, 1, task);
                    var updatedTask = self._updateTaskTtl(task);
                    if (updatedTask) {
                        multi.lpush(self._queueName, updatedTask);
                    }
                });
            });
            done();
        }).catch(err => {
            done(err);
        })
    }, function(err){
        callback(err);
    });
};

Consumer.prototype._updateTaskTtl = function (taskStr) {
    var task;
    try {
        task = JSON.parse(taskStr);
        if (!task.ttl) throw new Error("bad ttl")
    } catch (e) {
        return null;
    }
    task.ttl = Date.now() + 3000; // TODO
    return JSON.stringify(task);
};

Consumer.prototype.getTasksForQueueByNode = function (id, callback) {
    var qName = this._consumerQueueNameByUUID(id);
    this._client.lrange(qName, 0, -1, function(err, tasks){
        if (err) return callback(err);
        if (tasks === null || !tasks.length) return callback(null, qName, []);
        var tasksForQueue = tasks.filter((taskStr, idx) => {
            var task;
            try {
                task = JSON.parse(taskStr);
                if (!task.ttl) throw new Error("bad ttl")
            } catch (e) {
                return false;
            }
            if (task.ttl > Date.now()) {
                return true;
            }
            return false;
        });
        callback(null, qName, tasksForQueue);
    });
};

Consumer.prototype._consumerQueueNameByUUID = function (uuid) {
    return "queue.cq." + uuid;
};

module.exports = Consumer;