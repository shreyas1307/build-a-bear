import express from 'express';
import { Kafka, logLevel } from 'kafkajs'
import pg from "pg"

const { Client } = pg

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3002;

const dbClient = new Client({
    user: "admin",
    password: "root",
    database: "bears",
    host: "db",
    port: 5432
})

const testDB = async () => {
    try {
        await dbClient.connect();
        console.log("Client connnected!");
        await dbClient.end();
    } catch (error) {
        console.error("ERROR--->", error)
    }

}

const kafka = new Kafka({
    clientId: 'addStoreBear',
    brokers: ["broker:9092"]
})

const topic = 'bear-hearts'
const consumer = kafka.consumer({ groupId: "storeheart" })

const run = async () => {
    await dbClient.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: topic, fromBeginning: true })
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`${message.value.toString()}`)
            console.log("Hey there, we consumed? - STORE BEAR SERVICE")
            await createBear(message.value.toString());
        },
    })

}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))
// testDB().catch(e => console.error("ERROR", e))

const createBear = async (name) => {
    try {
        await dbClient.query(`INSERT INTO bear_names(name) VALUES('${name}');`)
        console.log("Bear has been stored!");
        await storeHeart(name);
    } catch (err) {
        console.error(err)
    }
}

const producer = kafka.producer();

const storeHeart = async (name) => {
    await producer.connect();
    await producer.send({
        topic: "bear-stored",
        messages: [{ key: "Added hearts", value: `${name}` }]
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
            await dbClient.end();
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
            await dbClient.end();
        } finally {
            process.kill(process.pid, type)
        }
    })
})





app.listen(PORT, () => {
    console.log("Bear heart service running at PORT 3002");
})