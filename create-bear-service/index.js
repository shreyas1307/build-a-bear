import express from "express";
import { Kafka, logLevel } from "kafkajs";
import {v4 as uuidv4 } from "uuid"
import 'dotenv/config'
import { createClient} from 'redis'
import pg from 'pg';
const { Client } = pg;

const app = express();
app.use(express.json());
const PORT = process.env.PORT;

const redisClient = createClient({url: `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`});

const dbClient = new Client({
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_DB,
    host: process.env.DATABASE_HOST,
    port: process.env.DATABASE_PORT
})

const kafkaPORT = process.env.KAFKA_PORT

const kafka = new Kafka({
    clientId: "build-bear",
    brokers: [`${kafkaPORT}`],
    logLevel: logLevel.ERROR
});

const testRedis = async () => {
    await redisClient.connect();
    if(redisClient.connected) {
        console.log("REDIS CLIENT CONNECTED" + redisClient.host + " " + redisClient.port);
    }
}


const testDB = async () => {
    try {
        await dbClient.connect();
        console.log("Client connnected!");
        await dbClient.end();
    } catch (error) {
        console.error("ERROR--->", error)
    }

}

const testKafka = async () => {
    try {
        await producer.connect()
        console.log('Producer connected')
        await producer.disconnect();
    } catch (error) {
        console.error('ERROR CONNECTING KAFKA --->', error)
    }
}

const producer = kafka.producer({allowAutoTopicCreation: true});

app.get('/health', async (req, res) => {
    // await testDB();
    // await testKafka();
    await testRedis();
    res.send("DB, Redis and Kafka Connected")
    

})

/**
 * TODO: Make uuid for bear
 * JSON Stringify the bear name and id, protocol buffers or use a Kafkajs lib
 * next services need to destringify and read. and push
 */

app.post("/build-bear", async (req, res) => {
    const { name } = req.body

    if (name) {
        try {
            await producer.connect();
            await producer.send({
                topic: "build-bears",
                messages: [{ key: "New bear order", value: name }],
            });
            await producer.disconnect();
            res.status(200).send("Order received");
            console.log(`Order received ${name}`)
        } catch (error) {
            console.error(`Error: ${error}`)
        }
    } else {
        res.send("Please send a valid name")
    }
});

/**
 * Get bear by the ID
 * if bear not created, return an appropriate response (it is in progress)
 * Store the state of this creation somewhere like redis? */ 

app.get('/get-bears', async (req, res) => {
    await dbClient.connect()
    try {
        const {rows} = await dbClient.query('SELECT * FROM bear_names;');
        res.status(200).send({message: "Bear names", bears: rows})
        await dbClient.end();
    } catch(err) {
        console.error(err)
    }
    
})

app.listen(PORT, () => {
    console.log(`App started in PORT ${PORT}`);
});

