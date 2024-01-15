import { Kafka, Producer } from "kafkajs";
import fs from 'fs'
import path from "path";
import prismaClient from "./prisma";

const kafka = new Kafka({
    brokers: ["kafka-24911c14-yashuofficial7-fb0e.a.aivencloud.com:28828"],
    ssl:{
        ca: [fs.readFileSync(path.resolve('./ca.pem'), "utf-8")],
    },
    sasl:{
        username:'avnadmin',
        password:'AVNS_CB8PG4rkvJIgexWSuYC',
        mechanism:'plain',
    }
})

let producer: null | Producer = null

export async function createProducer(){
    if(producer) return producer;
    const _producer = kafka.producer()
    await _producer.connect();
    producer = _producer;
    return producer;
}

export async function produceMessage(message: string){
    const producer = await createProducer();
    producer.send({
        messages: [{key: `message-${Date.now()}`, value:message}],
        topic: "MESSAGES",
    });
    return true;
}

export async function startMessageConsumer() {
    console.log("consumer is running")
    const consumer = kafka.consumer({groupId: 'default'});
    await consumer.connect();
    await consumer.subscribe({topic:"MESSAGES", fromBeginning:true})

    await consumer.run({
        autoCommit:true,
        eachMessage: async({message,pause}) => {
            if(!message.value) return;
            console.log(`new msg rec...`)
            try{
            await prismaClient.message.create({
                data: {
                    text:message.value?.toString(),
                }
            })
        }
            catch(err){
                console.log("Something is worng")
                pause()
                setTimeout(() => { consumer.resume([{topic: "MESSAGES"}]) 
                }, 60*1000)
            }
        }
    })
}

export default kafka;