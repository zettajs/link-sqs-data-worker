var fs = require('fs');
var os = require('os');
var path = require('path');
var crypto = require('crypto');
var zlib = require('zlib');
var AWS = require('aws-sdk');
var program = require('commander');
var async = require('async');

program
  .version('0.0.0')
  .option('-r, --region [region]', 'AWS Region', 'us-east-1')
  .parse(process.argv);

var s3 = new AWS.S3();
var sqs = new AWS.SQS({
  region: program.region
});

var QueueUrl = program.args[0];
if (!QueueUrl) {
  console.error('Provide queue url');
  program.help();
  process.exit(1);
}

var Bucket = program.args[1];
if (!Bucket) {
  console.error('Bucket queue url');
  program.help();
  process.exit(1);
}

var random = crypto.randomBytes(6).toString('hex');

var params = { QueueUrl: QueueUrl, AttributeNames: ['ApproximateNumberOfMessages']};
sqs.getQueueAttributes(params, function(err, data) {
  if (err) {
    console.error(err);
    process.exit(1);
  }

  var totalMessages = data.Attributes.ApproximateNumberOfMessages;
  var recievedMessages = [];
  var files = {};

  function recv() {
    if (recievedMessages.length >= totalMessages) {
      finish();
      return;
    }

    var params = {
      QueueUrl: QueueUrl,
      MaxNumberOfMessages: 10
    };
    sqs.receiveMessage(params, function(err, data) {
      if (err) {
        console.error(err);
        process.exit(1);
      }

      if (data.Messages.length === 0) {
        return finish();
      }

      async.map(data.Messages, function(msg, next) {
        recievedMessages.push(msg.ReceiptHandle);
        next(null, JSON.parse(msg.Body));
      }, function(err, results) {
        if (err) {
          console.error(err);
          process.exit(1);
        }
        var all = [];
        results.forEach(function(arr) {
          all = all.concat(arr);
        });
        
        async.eachSeries(all, writeToFile, function(err) {
          if (err) {
            console.error(err);
            process.exit(1);
          }
          recv();
        });
      });
    });
  }
  recv();

  function getFileHash(entry) {
    var tenant = entry.tags['req-header-x-apigee-iot-tenant-id'] || 'default';
    var dateString = new Date(entry.timestamp).toDateString();
    return tenant + '-' + dateString.replace(/ /g, '-');
  }

  function createFile(tenant, hash, cb) {
    var filePath = path.join(os.tmpdir(), hash + '-' + random);
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
      createFile((entry.tags['req-header-x-apigee-iot-tenant-id'] || 'default'), fileHash, function(err) {
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
        var Key = file.tenant + '/' + d.getFullYear() + '/' + (d.getMonth()+1) + '/' + d.getDate() + '/' + file.startTime + '-' + file.endTime + '-' + random + '.jsonl';

        var params = {
          Bucket: Bucket,
          Key: Key,
          Body: fs.createReadStream(file.path),
          Metadata: {
            tenant: file.tenant,
            startTime: file.startTime + '',
            endTime: file.endTime + ''
          }
        };

        s3.putObject(params, function(err) {
          if (err) {
            return next(err);
          }
          
          fs.unlink(file.path, function() {
            next();
          });
        });
      });
    }, function(err) {
      removeMessagesFromQueue();      
    });
  }

  function removeMessagesFromQueue() {
    async.each(recievedMessages, function(handle, next) {
      var params = {
        QueueUrl: QueueUrl,
        ReceiptHandle: handle
      };
      sqs.deleteMessage(params, next);
    }, function(err) {
      if (err) {
        console.error(err);
        process.exit(1);
      }
    });
  }
});

