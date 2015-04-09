var fs = require('fs');
var os = require('os');
var path = require('path');
var crypto = require('crypto');
var zlib = require('zlib');
var AWS = require('aws-sdk');
var async = require('async');

var Recv = module.exports = function(opts) {
  if (!(this instanceof Recv)) {
    return new Recv(opts);
  }

  if (!opts) {
    opts = { };
  }

  this.random = crypto.randomBytes(6).toString('hex');
  this.s3 = new AWS.S3(opts.awsConfig);
  this.sqs = new AWS.SQS(opts.awsConfig);
};

Recv.prototype.from = function(queueUrl) {
  this.queueUrl = queueUrl;
  return this;
};

Recv.prototype.to = function(bucketName) {
  this.bucket = bucketName;
  return this;
};

Recv.prototype.map = function(func) {
  this._map = func;
  return this;
};

Recv.prototype.limit = function(limit) {
  this.limit = limit;
  return this;
};

Recv.prototype._map = function(msg) {
  return msg;
};

Recv.prototype.finish = function(callback) {
  var self = this;
  var params = { QueueUrl: self.queueUrl, AttributeNames: ['ApproximateNumberOfMessages']};
  self.sqs.getQueueAttributes(params, function(err, data) {
    if (err) {
      return callback(err);
    }
    
    var totalMessages = data.Attributes.ApproximateNumberOfMessages;

    if (self.limit) {
      totalMessages = (self.limit > data.Attributes.ApproximateNumberOfMessages) ? data.Attributes.ApproximateNumberOfMessages : self.limit;
    }

    var recievedMessages = [];
    var files = {};

    function recv() {
      if (recievedMessages.length >= totalMessages) {
        finish();
        return;
      }

      var params = {
        QueueUrl: self.queueUrl,
        MaxNumberOfMessages: 10
      };
      self.sqs.receiveMessage(params, function(err, data) {
        if (err) {
          return callback(err);
        }

        if (data.Messages.length === 0) {
          return finish();
        }
        
        var all = [];
        data.Messages.forEach(function(msg) {
          recievedMessages.push(msg.ReceiptHandle);
          all = all.concat(JSON.parse(msg.Body));
        });

        all = all.map(self._map);

        async.eachSeries(all, writeToFile, function(err) {
          if (err) {
            return callback(err);
          }
          recv();
        });
      });
    }
    recv();

    function getFileHash(entry) {
      var tenant = entry.tenant;
      var dateString = new Date(entry.timestamp).toDateString();
      return tenant + '-' + dateString.replace(/ /g, '-');
    }

    function createFile(tenant, hash, cb) {
      var filePath = path.join(os.tmpdir(), hash + '-' + self.random);
      var gzip = zlib.createGzip();
      var stream = fs.createWriteStream(filePath);
      //    gzip.pipe(stream);
      stream.on('open', function() {
        files[hash] = {
          stream: stream,
          path: filePath,
          tenant: tenant,
          startTime: null,
          endTime: null
        };
        cb();
      });
    }

    function writeToFile(entry, cb) {
      var fileHash = getFileHash(entry);

      function addToFile(cb) {
        var file = files[fileHash];
        if (!file.startTime || file.startTime > entry.timestamp) {
          file.startTime = entry.timestamp;
        }

        if (!file.endTime || file.endTime < entry.timestamp) {
          file.endTime = entry.timestamp;
        }
        file.stream.write(JSON.stringify(entry) + '\n', cb);
      }

      if (!files[fileHash]) {
        createFile(entry.tenant, fileHash, function(err) {
          if (err) {
            return cb(err);
          }
          addToFile(cb);
        });
      } else {
        addToFile(cb);
      }
    }

    function finish() {
      
      async.each(Object.keys(files), function(k, next) {
        var file = files[k];
        
        file.stream.end(function() {
          var d = new Date(file.startTime);
          var Key = file.tenant + '/' + d.getFullYear() + '/' + (d.getMonth()+1) + '/' + d.getDate() + '/' + file.startTime + '-' + file.endTime + '-' + self.random + '.jsonl';

          var params = {
            Bucket: self.bucket,
            Key: Key,
            Body: fs.createReadStream(file.path),
            Metadata: {
              tenant: file.tenant,
              startTime: file.startTime + '',
              endTime: file.endTime + ''
            }
          };

          self.s3.putObject(params, function(err) {
            if (err) {
              return next(err);
            }
            
            fs.unlink(file.path, function() {
              next();
            });
          });
        });
      }, function(err) {
        if (err) {
          return callback(err);
        }
        removeMessagesFromQueue();
      });
    }

    function removeMessagesFromQueue() {
      async.each(recievedMessages, function(handle, next) {
        var params = {
          QueueUrl: self.queueUrl,
          ReceiptHandle: handle
        };
        self.sqs.deleteMessage(params, next);
      }, callback);
    }
  });

  return this;
};

