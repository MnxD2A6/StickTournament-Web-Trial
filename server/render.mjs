import {startServer} from './server.mjs';
const port=Number(process.env.PORT??10000);
if(!Number.isInteger(port)||port<1||port>65535)throw new Error('Invalid deployment port');
const server=await startServer({host:'0.0.0.0',port,publicOrigin:process.env.RENDER_EXTERNAL_URL});
console.log('Render experiment listening; public multiplayer acceptance remains unverified.');
const stop=async()=>{await server.close();process.exit(0);};
process.once('SIGINT',stop);process.once('SIGTERM',stop);
