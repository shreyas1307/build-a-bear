import express from "express";
import { Kafka, logLevel } from "kafkajs";
import pg from 'pg';
const {Client} = pg;

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 3000;

const dbClient = new Client({
    user: "admin",
    password: "root",
    database: "bears",
    host: "db",
    port: 5432
})

const kafka = new Kafka({
    clientId: "build-bear",
    brokers: ["broker:9092"],
    logLevel: logLevel.ERROR
});


const producer = kafka.producer();

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

