import express from 'express';
import {Kafka, logLevel} from 'kafkajs'

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3001;

const kafka = new Kafka({
    clientId: 'addHeartBear',
    brokers: ["broker:9092"]
})

const topic = 'build-bears'
const consumer = kafka.consumer({groupId: "addheart"})

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({topic: topic, fromBeginning: true})
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`${message.value.toString()}`)
            console.log("Hey there, we consumed?")
            await addHeart(message.value.toString());
          },
    })
    
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const producer = kafka.producer();

const addHeart = async (name) => {
    await producer.connect();
    await producer.send({
        topic: "bear-hearts",
        messages: [{key: "Added hearts", value:`${name}`}]
    })
    console.log('New message to bear hearts sent!')
}


const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect();
      await producer.disconnect();
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect();
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type)
    }
  })
})





app.listen(PORT, () => {
    console.log("Bear heart service running at PORT 3001");
})