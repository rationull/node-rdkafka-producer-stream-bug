"use strict";

const kafka = require('node-rdkafka');
const stream = require('stream');

const host = 'kafka';
const port = 9092;
const topic = 'test-topic';
const partition = 0;

const globalConfig = {
   'metadata.broker.list': `${host}:${port}`
};
const topicConfig = {};
const streamConfig = { topic };

function getMessages() {
   let msgSize = 10;
   let msgCount = 50;
   
   function getMsg(msgSize) {
      let msg = [];
      while(msgSize--) {
         msg += 'a'
      }
      return msg;
   }

   let messages = [];
   while (msgCount--) {
      messages.push(getMsg(msgSize))
   }
   return messages;
}

// Simply piping from an input stream to a Kafka producer stream does not flush the messages
// before the output stream closes due to the input stream being exhausted.
function pipe() {
   const messages = getMessages();
   const msgStream = new stream.Readable({ objectMode: true });
   messages.forEach(m => msgStream.push(new Buffer(m)));
   msgStream.push(null);

   let processed = 0;
   const countStream = new stream.Transform({ objectMode: true,
      transform: (chunk, encoding, callback) => {
         processed++;
         callback(null, chunk);
      }
   });
   
   const out = kafka.Producer.createWriteStream(globalConfig, topicConfig, streamConfig);
   out.on('error', e => console.error('Producer stream error: ' + e));
   out.on('finish', () => {
      console.log(`Out stream finished, ${processed} items run through the pipe`);
   });

   msgStream.pipe(countStream).pipe(out);
}

// This is not good stream code; it ignores backpressure. It may help illustrate the problem, though.
// By flushing before closing the output stream, we can ensure that the messages are all written out.
function write() {
   const messages = getMessages();

   const out = kafka.Producer.createWriteStream(globalConfig, topicConfig, streamConfig);
   out.on('error', e => console.error('Producer stream error: ' + e));

   out.producer.once('ready', () => {
      for (let msg of messages) {
         out.write(new Buffer(msg));
      }

      // Ending the stream right away causes the messages to be dropped.
      //out.end()

      // Need to flush it first.
      out.producer.flush(5000, () => {
         out.end();
      })
   });
}

//write();
pipe();