var program = require('commander');
var ReceiveSqs = require('./lib/recv-sqs');

program
  .version('0.0.0')
  .option('-r, --region [region]', 'AWS Region', 'us-east-1')
  .option('-l, --limit [number]', 'Limit the number of message processed to a fixed number')
  .parse(process.argv);

var maps = {
  device: function(msg) {
    msg.tenant = msg.tags['req-header-x-apigee-iot-tenant-id'] || 'default';
    return msg;
  },
  usage: function(msg) {
    msg.tenant = msg.tenant || 'default';
    msg.timestamp = msg.upload;
    delete msg.upload;
    return msg;
  }
};

var Type = program.args[0];
if (!Type || Object.keys(maps).indexOf(Type) < 0) {
  console.error('Provide queue url');
  program.help();
  process.exit(1);
}

var QueueUrl = program.args[1];
if (!QueueUrl) {
  console.error('Provide queue url');
  program.help();
  process.exit(1);
}

var Bucket = program.args[2];
if (!Bucket) {
  console.error('Bucket queue url');
  program.help();
  process.exit(1);
}

ReceiveSqs({ awsConfig: { region: program.region } })
  .from(QueueUrl)
  .to(Bucket)
  .limit(program.limit)
  .map(maps[Type])
  .finish(function(err) {
    if (err) {
      console.error(err);
      process.exit(1);
    }
  });

