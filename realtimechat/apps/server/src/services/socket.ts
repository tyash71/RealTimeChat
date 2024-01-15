import { Server } from "socket.io";
import { Redis } from "ioredis";
import prismaClient from "./prisma";
import { produceMessage } from "./kafka";

const pub = new Redis({
    host: 'redis-1a7458c8-yashuofficial7-fb0e.a.aivencloud.com',
    port: 28815,
    username: 'default',
    password: 'AVNS_awFDEgyZmx5RAV0ZLPx'
});
const sub = new Redis({
    host: 'redis-1a7458c8-yashuofficial7-fb0e.a.aivencloud.com',
    port: 28815,
    username: 'default',
    password: 'AVNS_awFDEgyZmx5RAV0ZLPx'
});

class SocketService {
    private _io: Server;

    constructor() {
        console.log('Init Socket Service...');
        this._io = new Server({
            cors: {
                allowedHeaders: ["*"],
                origin: "*",
            },
        });
        sub.subscribe("MESSAGES")
    }

    get io() {
        return this._io;
    }

    public initListeners() {
        const io = this._io;
        console.log('Init Socket Listeners...');
        
        io.on("connect", (socket) => {
            console.log(`New Socket Connected`, socket.id);

            socket.on('event:message', async ({message}: {message: string}) => {
                console.log("New Message Recieved", message);
                //publish this message to redis 
                await pub.publish("MESSAGES", JSON.stringify({message}));
            })
        });
        sub.on('message', async (channel, message) => {
            if( channel === "MESSAGES") {
                console.log("new msg from redis",message)
                io.emit("message",message);
                await produceMessage(message);
                console.log("Message produced to kafka broker")
                
            }
        })
    }
}

export default SocketService;