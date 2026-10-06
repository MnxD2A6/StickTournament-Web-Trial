import http from 'node:http';
import {readFile,lstat,realpath} from 'node:fs/promises';
import {resolve,join,relative,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import {WebSocketServer,WebSocket} from 'ws';
import {RoomRegistry} from './room_registry.mjs';
const files=new Map([
  ['index.html','text/html; charset=utf-8'],['index.js','text/javascript; charset=utf-8'],['index.wasm','application/wasm'],
  ['index.pck','application/octet-stream'],['index.png','image/png'],['index.icon.png','image/png'],['index.apple-touch-icon.png','image/png'],
  ['index.audio.worklet.js','text/javascript; charset=utf-8'],['index.audio.position.worklet.js','text/javascript; charset=utf-8'],['FONT_LICENSE.txt','text/plain; charset=utf-8']
]);
for(const [name,mime] of [...files])files.set('probe/'+name,mime);
export async function startServer({host='127.0.0.1',port=8768,webRoot,registry=new RoomRegistry(),allowedOrigins=[],publicOrigin}={}){
  const publicMode=host==='0.0.0.0';
  if(host!=='127.0.0.1'&&!publicMode)throw new Error('Unsupported listener');
  if(publicMode&&!/^https:\/\/[a-z0-9][a-z0-9-]*\.onrender\.com$/.test(publicOrigin??''))throw new Error('Public listening requires exact HTTPS Render origin');
  if(!publicMode&&publicOrigin!==undefined)throw new Error('Public origin requires deployment listener');
  const publicFiles=new Set(['index.html','index.js','index.wasm','index.pck','index.png','index.audio.worklet.js','index.audio.position.worklet.js','FONT_LICENSE.txt']);
  const root=resolve(webRoot??join(dirname(fileURLToPath(import.meta.url)),'../web'));
  if((await lstat(root)).isSymbolicLink()||await realpath(root)!==root)throw new Error('Web root must be a real directory, not a link');
  let url;const clients=new Map();
  const server=http.createServer(async(req,res)=>{
    res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('Cache-Control','no-store');
    const reject=(status=404)=>{res.writeHead(status);res.end('Not found');};
    if(req.method!=='GET'&&req.method!=='HEAD'){reject(405);return;}
    const raw=(req.url??'').split('?')[0];
    if(raw==='/health'){res.setHeader('Content-Type','application/json');res.end(JSON.stringify({experiment:'stick-web-multiplayer-20261006',protocol:1}));return;}
    if(raw.includes('%')||raw.includes('\\')||raw.includes('\0')){reject();return;}
    const name=raw==='/'?'index.html':raw.slice(1);if(!files.has(name)||(publicMode&&!publicFiles.has(name))){reject();return;}
    try {
      const file=join(root,name);if((await lstat(file)).isSymbolicLink()) {reject();return;}
      const actual=await realpath(file);if(relative(root,actual)!==name){reject();return;}
      const body=await readFile(actual);res.setHeader('Content-Type',files.get(name));res.setHeader('Content-Length',body.length);
      res.end(req.method==='HEAD'?undefined:body);
    } catch {reject();}
  });
  const wss=new WebSocketServer({noServer:true,maxPayload:65536,perMessageDeflate:false});
  const deliver=deliveries=>{for(const d of deliveries){const ws=clients.get(d.to);if(!ws||ws.readyState!==WebSocket.OPEN)continue;if(ws.bufferedAmount>1048576){ws.close(1008,'send backlog');continue;}ws.send(JSON.stringify(d.message));}};
  server.on('upgrade',(req,socket,head)=>{
    const origins=publicMode?[publicOrigin]:[url,...allowedOrigins];
    if(req.url!=='/signal'||!origins.includes(req.headers.origin)){socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');return;}
    wss.handleUpgrade(req,socket,head,ws=>wss.emit('connection',ws,req));
  });
  wss.on('connection',ws=>{
    const id=randomUUID();clients.set(id,ws);let windowStart=Date.now(),count=0;
    ws.on('error',()=>{});
    ws.on('message',(bytes,isBinary)=>{
      const now=Date.now();if(now-windowStart>=1000){windowStart=now;count=0;}if(++count>60){ws.close(1008,'rate limit');return;}
      if(isBinary){ws.close(1008,'JSON only');return;}
      let m;try{m=JSON.parse(bytes.toString('utf8'));}catch{deliver([{to:id,message:{v:1,type:'error',code:'BAD_JSON',request_id:''}}]);return;}
      deliver(registry.handle(id,m));
    });
    ws.on('close',()=>{clients.delete(id);deliver(registry.disconnect(id));});
  });
  await new Promise((ready,reject)=>{server.once('error',reject);server.listen(port,host,ready);});
  url=`http://${host}:${server.address().port}`;
  const interval=setInterval(()=>deliver(registry.sweep()),1000);interval.unref();
  let closed=false;
  return {url,close:async()=>{if(closed)return;closed=true;clearInterval(interval);for(const ws of clients.values())ws.terminate();await new Promise(done=>wss.close(done));server.closeAllConnections();await new Promise(done=>server.close(done));}};
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const port=Number(process.argv[2]??8768);if(!Number.isInteger(port)||port<1||port>65535)throw new Error('Invalid port');
  const server=await startServer({port});console.log(`LOCAL ONLY ${server.url} — not remote multiplayer validation`);
  const stop=async()=>{await server.close();process.exit(0);};process.once('SIGINT',stop);process.once('SIGTERM',stop);
}
