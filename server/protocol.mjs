const shapes={create:['build_hash'],join:['code','build_hash'],ready:['ready','rtc_ready','build_hash'],start:[],loaded:['match_id'],signal:['to','payload'],leave:[],heartbeat:[],finish:['match_id'],abort:['match_id','reason']};
export function validate(message) {
  if(!message || typeof message!=='object' || Array.isArray(message) || message.v!==1 || !Object.hasOwn(shapes,message.type))return false;
  if(typeof message.request_id!=='string'||! /^[A-Za-z0-9_-]{1,64}$/.test(message.request_id))return false;
  const fields=shapes[message.type]; if(Object.keys(message).some(k=>!['v','type','request_id',...fields].includes(k)))return false;
  if(fields.some(k=>!Object.hasOwn(message,k)))return false;
  if('build_hash' in message && (typeof message.build_hash!=='string'||!/^[a-fA-F0-9]{64}$/.test(message.build_hash)))return false;
  if('code' in message && (typeof message.code!=='string'||! /^[ABCDEFGHJKLMNPQRSTUVWXYZ23456789]{8}$/.test(message.code)))return false;
  if('match_id' in message && (typeof message.match_id!=='string'||message.match_id.length<1||Buffer.byteLength(message.match_id)>64))return false;
  if(message.type==='ready' && (typeof message.ready!=='boolean'||typeof message.rtc_ready!=='boolean'))return false;
  if(message.type==='signal' && (!Number.isSafeInteger(message.to)||message.to<1||!message.payload||typeof message.payload!=='object'||Array.isArray(message.payload)))return false;
  if(message.type==='abort' && (typeof message.reason!=='string'||! /^[A-Z_]{1,40}$/.test(message.reason)))return false;
  return Buffer.byteLength(JSON.stringify(message))<=65536;
}
export const event=(type,fields={})=>({v:1,type,...fields});
