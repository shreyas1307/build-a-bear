import express from "express";
import { Kafka, logLevel } from "kafkajs";
import pg from 'pg';
const {Client} = pg;

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3100;

const dbClient = new Client({
    user: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASSWORD,
    database: process.env.DATABASE_DB,
    host: process.env.DATABASE,
    port: process.env.DATABASE_PORT
})

const kafkaPORT = process.env.KAFKA_PORT

const kafka = new Kafka({
    clientId: "build-bear",
    brokers: [`${kafkaPORT}`],
    logLevel: logLevel.ERROR
});


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

app.get('/db', async (req, res) => {
    await testDB();
    await testKafka();
    res.send("DB and KAFKA Connected")
    

})

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

