import {randomBytes as cryptoRandomBytes} from 'node:crypto';
import {validate,event} from './protocol.mjs';
const alphabet='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const clone=value=>structuredClone(value);
export class RoomRegistry {
  constructor({now=()=>Date.now(),randomBytes=cryptoRandomBytes}={}) {
    this.now=now;this.randomBytes=randomBytes;this.rooms=new Map();this.connections=new Map();this.replays=new Map();
  }
  publicRoom(code) {
    const r=this.rooms.get(code);if(!r)return null;
    return {code:r.code,room_id:r.room_id,signal_available:r.signal_available!==false,expires_at_ms:r.created+3600000,phase:r.phase,match_id:r.match?.match_id??null,
      members:[...r.members.values()].map(({peer_id,slot,ready,rtc_ready,build_hash})=>({peer_id,slot,ready,rtc_ready,build_hash}))};
  }
  broadcast(r,message) {return [...r.members.keys()].map(to=>({to,message:clone(message)}));}
  state(r) {return this.broadcast(r,event('room',{room:this.publicRoom(r.code)}));}
  joined(r,id) {const m=r.members.get(id);return {to:id,message:event('joined',{token:m.token,peer_id:m.peer_id,slot:m.slot,room:this.publicRoom(r.code)})};}
  error(id,request,code) {return [{to:id,message:event('error',{request_id:request?.request_id??'',code})}];}
  handle(id,message) {
    if(!validate(message))return this.error(id,message,'BAD_MESSAGE');
    const cache=this.replays.get(id);const prior=cache?.get(message.request_id);
    if(prior) return prior.fingerprint===JSON.stringify(message)?clone(prior.deliveries):this.error(id,message,'REQUEST_CONFLICT');
    const result=this.dispatch(id,message);
    const saved=cache??new Map();saved.set(message.request_id,{fingerprint:JSON.stringify(message),deliveries:result.filter(d=>d.to===id)});
    if(saved.size>128)saved.delete(saved.keys().next().value);this.replays.set(id,saved);return result;
  }
  dispatch(id,m) {
    const fail=code=>this.error(id,m,code);const now=this.now();
    let r=this.rooms.get(this.connections.get(id));
    if(m.type==='create'||m.type==='join') {
      if(r) return m.type==='join'&&m.code===r.code?[this.joined(r,id)]:fail('ALREADY_JOINED');
      if(m.type==='create') {
        if(this.rooms.size>=100)return fail('ROOM_LIMIT');
        let code;for(let attempt=0;attempt<32;attempt++) {code=[...this.randomBytes(8)].map(b=>alphabet[b%32]).join('');if(!this.rooms.has(code))break;code=null;}
        if(!code)return fail('ROOM_LIMIT');
        r={code,room_id:this.randomBytes(16).toString('hex'),created:now,activity:now,phase:'lobby',members:new Map(),nextPeer:2,build_hash:m.build_hash.toLowerCase(),match:null};
        this.rooms.set(code,r);
      } else {
        r=this.rooms.get(m.code);if(!r)return fail('ROOM_NOT_FOUND');
        if(r.signal_available===false)return fail('SIGNAL_LOST');
        if(now>=r.created+3600000)return fail('ROOM_EXPIRED');
        if(r.phase!=='lobby')return fail('MATCH_LOCKED');if(r.members.size>=4)return fail('ROOM_FULL');
        if(r.build_hash!==m.build_hash.toLowerCase())return fail('BUILD_MISMATCH');
      }
      const occupied=new Set([...r.members.values()].map(member=>member.slot));const slot=[0,1,2,3].find(s=>!occupied.has(s));
      r.members.set(id,{peer_id:slot===0?1:r.nextPeer++,slot,ready:false,rtc_ready:false,build_hash:r.build_hash,token:this.randomBytes(16).toString('hex'),lastSeen:now});
      this.connections.set(id,r.code);r.activity=now;this.resetReady(r);
      return [this.joined(r,id),...this.state(r)];
    }
    if(!r)return fail('NOT_JOINED');const member=r.members.get(id);member.lastSeen=now;
    if(m.type==='heartbeat')return [{to:id,message:event('heartbeat',{request_id:m.request_id})}];
    if(m.type==='leave')return this.disconnect(id,'PEER_LEFT',true);
    if(m.type==='signal') {
      const target=[...r.members.entries()].find(([,other])=>other.peer_id===m.to);if(!target)return fail('UNKNOWN_PEER');
      if(m.to===member.peer_id||member.peer_id!==1&&m.to!==1)return fail('STAR_ONLY');
      return [{to:target[0],message:event('signal',{from:member.peer_id,payload:clone(m.payload)})}];
    }
    if(m.type==='ready') {
      if(r.signal_available===false)return fail('SIGNAL_LOST');
      if(r.phase!=='lobby')return fail('MATCH_LOCKED');if(m.build_hash.toLowerCase()!==r.build_hash)return fail('BUILD_MISMATCH');
      member.ready=m.ready;member.rtc_ready=m.rtc_ready;r.activity=now;return this.state(r);
    }
    if(m.type==='loaded') {
      if(r.phase!=='starting'||m.match_id!==r.match?.match_id)return fail('STALE_MATCH');
      r.loaded.add(id);if(r.loaded.size!==4)return this.state(r);
      r.phase='fight';return [...this.broadcast(r,event('begin',{config:clone(r.match)})),...this.state(r)];
    }
    if(member.peer_id!==1)return fail('HOST_ONLY');
    if(m.type==='start') {
      if(r.signal_available===false)return fail('SIGNAL_LOST');
      if(r.phase!=='lobby')return this.state(r);
      if(r.created+3600000-now<300000)return fail('ROOM_EXPIRING');
      if(r.members.size!==4||[...r.members.values()].some(p=>!p.ready||!p.rtc_ready))return fail('NOT_READY');
      r.match={match_id:this.randomBytes(16).toString('hex'),build_hash:r.build_hash,map_id:1,character_id:'hunter',members:this.publicRoom(r.code).members,initial_health:1000,countdown_s:2,duration_s:180};
      r.phase='starting';r.started=now;r.loaded=new Set();r.activity=now;
      return [...this.broadcast(r,event('starting',{config:clone(r.match)})),...this.state(r)];
    }
    if(m.match_id!==r.match?.match_id)return fail('STALE_MATCH');
    if(m.type==='finish'&&r.phase!=='fight')return fail('NOT_FIGHTING');
    return this.endMatch(r,m.type==='abort'?m.reason:'FINISHED');
  }
  resetReady(r) {for(const member of r.members.values())member.ready=false;}
  endMatch(r,reason) {
    const result=this.broadcast(r,event('ended',{match_id:r.match?.match_id,reason}));r.match=null;r.phase='lobby';r.loaded=null;r.activity=this.now();this.resetReady(r);
    return [...result,...this.state(r)];
  }
  closeRoom(r,reason) {
    const result=this.broadcast(r,event('closed',{reason}));for(const id of r.members.keys()){this.connections.delete(id);this.replays.delete(id);}
    this.rooms.delete(r.code);return result;
  }
  disconnect(id,reason='PEER_LEFT',explicit=false) {
    const r=this.rooms.get(this.connections.get(id));this.replays.delete(id);if(!r)return [];
    const member=r.members.get(id);
    if(r.phase==='fight'&&!explicit) {
      member.signal_lost=true;r.signal_available=false;this.connections.delete(id);
      return this.state(r);
    }
    if(member.peer_id===1&&reason==='HEARTBEAT_TIMEOUT')return this.closeRoom(r,reason);
    r.members.delete(id);this.connections.delete(id);
    if(member.peer_id===1)return this.closeRoom(r,reason==='PEER_LEFT'?'HOST_LEFT':reason);
    const result=r.phase!=='lobby'?this.endMatch(r,reason):[];r.activity=this.now();this.resetReady(r);
    return [...result,...this.state(r)];
  }
  sweep() {
    const result=[];const now=this.now();
    for(const r of [...this.rooms.values()]) {
      if(now>=r.created+3600000){result.push(...this.closeRoom(r,'ROOM_EXPIRED'));continue;}
      if(r.phase==='lobby'&&now-r.activity>=600000){result.push(...this.closeRoom(r,'IDLE_EXPIRED'));continue;}
      if(r.phase==='starting'&&now-r.started>=20000)result.push(...this.endMatch(r,'LOAD_TIMEOUT'));
      for(const [id,m] of [...r.members])if(!m.signal_lost&&now-m.lastSeen>=15000)result.push(...this.disconnect(id,'HEARTBEAT_TIMEOUT'));
    }
    return result;
  }
}
