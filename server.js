"use strict";
/* PIXEL ROYALE — online server: static hosting + dependency-free WebSocket
   + public matchmaking queue + authoritative matches (bots backfill). */
const http=require('http'), fs=require('fs'), path=require('path'), crypto=require('crypto');
const DIR=__dirname, PORT=process.env.PORT||8000;
const MIME={'.html':'text/html','.js':'text/javascript','.png':'image/png','.css':'text/css','.json':'application/json','.md':'text/plain'};

const server=http.createServer((req,res)=>{
  let urlPath=decodeURIComponent((req.url||'/').split('?')[0]);
  if(urlPath==='/')urlPath='/online.html';
  const file=path.join(DIR,path.normalize(urlPath));
  if(!file.startsWith(DIR)){ res.writeHead(403); return res.end('forbidden'); }
  fs.readFile(file,(err,data)=>{
    if(err){ res.writeHead(404,{'Content-Type':'text/plain'}); return res.end('not found'); }
    res.writeHead(200,{'Content-Type':MIME[path.extname(file)]||'application/octet-stream','Cache-Control':'no-cache'});
    res.end(data);
  });
});

/* ---------- minimal WebSocket (RFC6455, text frames) ---------- */
const clients=new Set();
function wsSend(c,obj){ if(c.dead)return; const data=Buffer.from(JSON.stringify(obj)); const len=data.length;
  let head; if(len<126){ head=Buffer.from([0x81,len]); } else if(len<65536){ head=Buffer.alloc(4); head[0]=0x81; head[1]=126; head.writeUInt16BE(len,2); }
  else { head=Buffer.alloc(10); head[0]=0x81; head[1]=127; head.writeBigUInt64BE(BigInt(len),2); }
  try{ c.sock.write(Buffer.concat([head,data])); }catch(e){ c.dead=true; }
}
server.on('upgrade',(req,sock)=>{
  const key=req.headers['sec-websocket-key']; if(!key){ sock.destroy(); return; }
  const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  const c={sock,buf:Buffer.alloc(0),dead:false,id:'p'+(++pidSeq),match:null,pid:null,queued:false,name:'',pinpoint:false};
  clients.add(c);
  sock.on('data',chunk=>{
    c.buf=Buffer.concat([c.buf,chunk]);
    while(true){
      if(c.buf.length<2)return;
      const fin=c.buf[0]&0x80, op=c.buf[0]&0x0f, masked=c.buf[1]&0x80; let len=c.buf[1]&0x7f, off=2;
      if(len===126){ if(c.buf.length<4)return; len=c.buf.readUInt16BE(2); off=4; }
      else if(len===127){ if(c.buf.length<10)return; len=Number(c.buf.readBigUInt64BE(2)); off=10; }
      const maskOff=off; if(masked)off+=4;
      if(c.buf.length<off+len)return;
      let payload=Buffer.from(c.buf.slice(off,off+len));
      if(masked){ const mask=c.buf.slice(maskOff,maskOff+4); for(let i=0;i<payload.length;i++)payload[i]^=mask[i%4]; }
      c.buf=c.buf.slice(off+len);
      if(op===8){ c.dead=true; sock.end(); return; }
      if(op===9){ try{ sock.write(Buffer.from([0x8a,0])); }catch(e){} continue; }
      if(op!==1)continue;
      let msg; try{ msg=JSON.parse(payload.toString()); }catch(e){ continue; }
      handleMsg(c,msg);
      if(!fin)continue;
    }
  });
  const gone=()=>{ c.dead=true; clients.delete(c); removeFromQueue(c); if(c.match&&c.pid){ const m=matches.get(c.match); if(m){ const p=m.players.get(c.pid); if(p&&p.alive){ p.alive=false; p.hp=0; mEvent(m,{ev:'left',victim:p.name}); checkEnd(m); } } } };
  sock.on('close',gone); sock.on('error',gone);
});

/* ---------- game data ---------- */
const TAU=Math.PI*2, clamp=(v,a,b)=>v<a?a:v>b?b:v, rnd=(a,b)=>a+Math.random()*(b-a), choice=a=>a[Math.floor(Math.random()*a.length)];
const C={x:1600,y:1600,r:2850};
const WEAPONS={
  pistol:{dmg:11,rate:.21,mag:14,reload:.7,speed:1350,spread:.014,pellets:1,range:820},
  shotgun:{dmg:8,rate:.78,mag:6,reload:1.3,speed:1180,spread:.11,pellets:7,range:430},
  uzi:{dmg:6,rate:.048,mag:42,reload:.9,speed:1330,spread:.052,pellets:1,range:780},
  m4:{dmg:10,rate:.082,mag:32,reload:.95,speed:1480,spread:.016,pellets:1,range:980},
  ak:{dmg:13,rate:.105,mag:30,reload:1.1,speed:1400,spread:.032,pellets:1,range:940},
  lmg:{dmg:9,rate:.057,mag:90,reload:2,speed:1420,spread:.05,pellets:1,range:960},
  rpg:{dmg:20,rate:.95,mag:4,reload:1.8,speed:820,spread:.006,pellets:1,range:1050,explosive:true,blast:104},
};
const WBLOOM={pistol:.006,shotgun:.02,m4:.005,ak:.008,lmg:.007,uzi:.006,rpg:.03};
const FALLOFF={pistol:[430,.55],uzi:[300,.5],shotgun:[165,.3],m4:[580,.6],ak:[540,.55],lmg:[560,.55],rpg:[1e9,1]};
const BOTNAMES=['影','虚無','零','悪魔','cøld','kxrma','v o i d','zxro','.exe','lucid','aimbot','百殺','虐殺','もう無理','寝不足','助けて','kage.exe','hyakusatsu','shinigami.ql','zxro影','akuma 999','yaiba_qt','kyomu.ttv','kijin diff','arashi fn','100kills.ex','i miss her','whenwin','coldwiner','breather','weter','残響','夜桜','無音','ツキ','s1lent','n0va','r e q u i e m','gl1tch','yurei.ql','oni mask','hakai','sora.exe','mugetsu','j1nx','wr3ck','ketsueki','dr1ft','shinobi.7','bl00d','kurayami','asagiri','phant0m','setsuna','oboro','hollow.ttv','negai','munou','hikari','kuromi.exe','abyss 999','neko diff','tsukuyomi','raven.ql','sa1koro','vex.exe','nukumori','shikaku','aokage','melt','yokai_99','genjitsu','cr0w','satsujin','yoru.exe','w o l f','kodoku','zangetsu','himawari','d0ze','unmei','ame.exe','月蝕','神楽','ロンリ','yuurei','s0rrow','n e o n','hakai.exe','ketsugai','yami.ttv','mugen','rengoku','shinra','oborozuki','wasure','k1tsune','aoi.exe','hannya','tsukiyo','guren','s4kurai','mirage','zenith.ql','kamikaze 0','shiranai','usotsuki','hibana','kagerou','y0kai','seishin','hikikomori','0uter','phantasm','ketsumei','ryu.exe','tengoku','jigoku','sakusei','nemui','kawaii.exe','monsutaa','zaregoto','inori','kessoku','yabai','senpai.ql','ouka','fuyu.exe','akatsuki','ketsubou','hitori','TenZ','aspas','yay','Derke','Chronicle','ScreaM','Asuna','Hiko','WARDELL','ShahZaM','zombs','leaf','s1mple','ZywOo','NiKo','donk','m0NESY','sh1ro','Ax1Le','ropz','Twistzz','dev1ce','karrigan','Faker','Caps','Rekkles','Perkz','prodigy','wunder','clutchgod','headshot','TenZ fan','f0rest','vanity','zekken','Less','cauanzin','jawgemo','Boostio','bang','Cryo','sScary','Jinggg','f0rsaken','something','mindfreak'];
const COLORS=['#ff5c7a','#ffb02e','#39a7ff','#c05cff','#3dff73','#ff7847','#4de8e0','#f2f2f2','#9dff57','#ff8ad8','#ffd52e','#6fc4ff'];
const SPRITES=['woman','manblue','hero'];

function buildObstacles(){
  const obs=[];
  const houseAt=[[-980,-660],[1020,-760],[820,720],[-920,820],[90,-1280],[-160,1240],[1380,80]];
  houseAt.forEach(([x,y],i)=>obs.push({kind:'house',img:'house'+((i%3)+1),x:C.x+x,y:C.y+y,w:195,h:118}));
  let tries=0;
  while(obs.filter(o=>o.kind==='tree'||o.kind==='rock').length<95&&tries++<500){
    const a=rnd(0,TAU), d=180+Math.random()*(C.r-240), x=C.x+Math.cos(a)*d, y=C.y+Math.sin(a)*d;
    if(obs.some(o=>o.kind==='house'&&Math.abs(x-o.x)<190&&Math.abs(y-o.y)<160))continue;
    obs.push(Math.random()<.55?{kind:'tree',img:choice(['tree1','tree2']),x,y,r:26+rnd(0,8)}:{kind:'rock',x,y,r:20+rnd(0,14)});
  }
  const bushes=[];
  for(let i=0;i<29;i++){ const a=rnd(0,TAU), d=160+Math.random()*(C.r-300); bushes.push({x:C.x+Math.cos(a)*d,y:C.y+Math.sin(a)*d,r:46}); }
  return {obs,bushes};
}
function pointBlocked(obs,x,y,pad=2){
  for(const o of obs){
    if(o.kind==='house'){ if(x>o.x-o.w/2-pad&&x<o.x+o.w/2+pad&&y>o.y-o.h/2-pad&&y<o.y+o.h/2+pad)return o; }
    else if(Math.hypot(x-o.x,y-o.y)<(o.kind==='tree'?9:o.r)+pad)return o;
  } return null;
}
function clearOf(obs,x,y,pad){
  if(Math.hypot(x-C.x,y-C.y)>C.r-70)return false;
  for(const o of obs){ if(o.kind==='house'){ if(x>o.x-o.w/2-pad&&x<o.x+o.w/2+pad&&y>o.y-o.h/2-pad&&y<o.y+o.h/2+pad)return false; } else if(Math.hypot(x-o.x,y-o.y)<o.r+pad)return false; }
  return true;
}
function collide(obs,f){
  for(const o of obs){
    if(o.kind==='house'){ const nx=clamp(f.x,o.x-o.w/2,o.x+o.w/2), ny=clamp(f.y,o.y-o.h/2,o.y+o.h/2), dx=f.x-nx, dy=f.y-ny, d2=dx*dx+dy*dy;
      if(d2<f.r*f.r){ if(d2<.01)f.y=o.y-o.h/2-f.r; else { const d=Math.sqrt(d2); f.x=nx+dx/d*f.r; f.y=ny+dy/d*f.r; } } }
    else { const dx=f.x-o.x, dy=f.y-o.y, rr=f.r+o.r, d2=dx*dx+dy*dy; if(d2<rr*rr&&d2>.01){ const d=Math.sqrt(d2); f.x=o.x+dx/d*rr; f.y=o.y+dy/d*rr; } }
  }
  const dx=f.x-C.x, dy=f.y-C.y, d=Math.hypot(dx,dy), max=C.r-26;
  if(d>max){ f.x=C.x+dx/d*max; f.y=C.y+dy/d*max; }
}
function distSeg(px,py,x1,y1,x2,y2){ const dx=x2-x1, dy=y2-y1, L=dx*dx+dy*dy||1; let t=((px-x1)*dx+(py-y1)*dy)/L; t=clamp(t,0,1); return Math.hypot(px-(x1+dx*t),py-(y1+dy*t)); }
function losBlocked(obs,x1,y1,x2,y2){ const d=Math.hypot(x2-x1,y2-y1), st=Math.max(1,d/38|0); for(let i=1;i<st;i++){ const t=i/st; if(pointBlocked(obs,x1+(x2-x1)*t,y1+(y2-y1)*t,4))return true; } return false; }
function openCrate(m,f,cr){ cr.open=true; f.crateCD=1; const key=rollForTier(cr.tier);
  m.pending.push({t:1.1,wkey:key,x:cr.x,y:cr.y+18,name:f.name}); // lands when the roll animation ends
  mEvent(m,{ev:'loot',name:f.name,wkey:key}); }
function rollForTier(tier){ const r=Math.random();
  if(tier==='Neon'){ if(r<.07)return 'rpg'; if(r<.27)return 'lmg'; return choice(['ak','m4']); }
  if(tier==='Camo'){ if(r<.05)return 'lmg'; return choice(['uzi','m4','ak','shotgun']); }
  if(r<.015)return 'lmg'; return choice(['shotgun','shotgun','uzi','m4','m4','ak']);
}

/* ---------- matchmaking ---------- */
const matches=new Map(); let matchSeq=1; const LOBBY_TARGET=12;
const MODES={solo:1,duo:2,squad:4}; // fighters per team
const lobbies={};
function getLobby(mode){ if(!lobbies[mode])lobbies[mode]={queue:[],fakes:[],createdAt:0,startAt:0,nextFakeAt:0,leaveAt:0,leaveDone:false,pace:1,lullAt:0}; return lobbies[mode]; }
function resetLobbySim(L){ L.fakes=[]; L.nextFakeAt=0; L.leaveAt=0; L.leaveDone=false; }
function pushFake(L){ if(lobbyCount(L)>=LOBBY_TARGET)return false;
  const used=new Set([...L.queue.map(c=>c.name),...L.fakes.map(f=>f.name)]);
  let bn=''; do{ bn=BOTNAMES[Math.floor(Math.random()*BOTNAMES.length)]; }while(used.has(bn));
  L.fakes.push({name:bn,id:'u'+(100000+Math.floor(Math.random()*899999))}); return true; }
function lobbyCount(L){ return L.queue.length+L.fakes.length; }
function fakeGap(L){ // messy on purpose: quick bursts, uneven gaps, and that one mid-fill breather — different shape every lobby
  if(L.lullAt&&lobbyCount(L)>=L.lullAt){ L.lullAt=0; return 2000+Math.random()*1800; }
  const r=Math.random();
  return (480+(r<.45?Math.random()*300:(r<.75?300+Math.random()*600:900+Math.random()*1700)))*(L.pace||1);
}
function planQueueStart(L){
  const n=L.queue.length, now=Date.now();
  if(!n){ L.createdAt=0; L.startAt=0; return; }
  if(!L.createdAt){ L.createdAt=now; L.pace=.56+Math.random()*.26; L.lullAt=Math.random()<.3?0:2+Math.floor(Math.random()*8); } // every lobby gets its own tempo + one breather moment
  L.startAt=(n>=LOBBY_TARGET||lobbyCount(L)>=LOBBY_TARGET)?now+400:0; // no countdown, no auto-launch: a full 12 is the only trigger
}
function removeFromQueue(c){ const L=getLobby(c.mode||'solo'); const i=L.queue.indexOf(c); if(i>=0)L.queue.splice(i,1); c.queued=false; if(!L.queue.length){ L.createdAt=0; L.startAt=0; resetLobbySim(L); } else planQueueStart(L); }
function queueStatus(L){ const startsIn=L.startAt?Math.max(0,Math.ceil((L.startAt-Date.now())/1000)):30;
  const members=[...L.queue.map(c=>c.name),...L.fakes.map(f=>f.name)]; // everybody in the lobby, in seat order
  L.queue.forEach((c,i)=>wsSend(c,{type:'queue',count:lobbyCount(L),pos:i+1,target:LOBBY_TARGET,members,botsNeeded:Math.max(0,LOBBY_TARGET-lobbyCount(L)),startsIn})); }
function fakeJoinStep(L,now){ // players wander in ONE at a time (never two in the same instant), at irregular human intervals, until the lobby is full
  if(!L.queue.length){ if(L.fakes.length)resetLobbySim(L); return; }
  if(lobbyCount(L)>=LOBBY_TARGET){ if(!L.startAt||L.startAt>now+600)L.startAt=now+600; return; }
  if(now<(L.nextFakeAt||0))return; // engineered human pacing: never two joins in the same instant
  const warm=now-L.createdAt>1200||lobbyCount(L)>1; // a brief beat when you are the first one here, then it flows
  if(Math.random()<(warm?1:.8)&&pushFake(L)){ L.nextFakeAt=now+fakeGap(L); // messy rhythm, ~15s on average
    queueStatus(L); }
}
function launchLobby(L,mode){
  const humans=L.queue.splice(0,LOBBY_TARGET).filter(c=>!c.dead);
  humans.forEach(c=>{ c.queued=false; });
  if(!humans.length){ L.startAt=0; return; }
  const fakes=L.fakes; resetLobbySim(L);
  startMatch(humans,fakes,mode);
  L.createdAt=0; L.startAt=0; planQueueStart(L); // anyone left queued forms the next lobby
}
setInterval(()=>{
  for(const md of Object.keys(MODES)){ const L=getLobby(md); fakeJoinStep(L,Date.now());
    if(L.queue.length&&L.startAt&&Date.now()>=L.startAt)launchLobby(L,md);
    queueStatus(L); }
},500);
function mkPlayer(id,name,sprite,color,client){
  return {id,name,sprite,color,client:client||null,isBot:!client,x:0,y:0,vx:0,vy:0,r:17,aim:0,hp:100,alive:true,
    inv:['pistol'],slot:0,ammos:[WEAPONS.pistol.mag],cool:0,reloadT:0,dashT:0,dashCD:0,dashDX:0,dashDY:0,bloom:0,recoilA:0,
    input:{mx:0,my:0,aim:0,fire:false},pinpoint:client?client.pinpoint:false,kills:0,dmg:0,team:0,place:null,lastHurt:99,hidden:false,inBush:false,revealT:0,
    aiT:0,aiTX:0,aiTY:0,strafe:1,seeT:0,burst:0,skill:.25+Math.pow(Math.random(),1.6)*.65,crateCD:0,swapCD:0,gunCD:0,
    clock:0,memX:0,memY:0,memT:-99,foeId:null,aggro:.85+Math.random()*.3,checkT:2,checkA:0,checkDur:0,hideT:0,hideCD:0,hideTX:0,hideTY:0,holdT:0,investigating:false};
}
function giveGun(m,p,key){
  if(p.inv.includes(key)){ p.slot=p.inv.indexOf(key); p.ammos[p.slot]=WEAPONS[key].mag; }
  else if(p.inv.length<2){ p.inv.push(key); p.ammos.push(WEAPONS[key].mag); p.slot=p.inv.length-1; }
  else { // hands full: drop the gun you're holding on the floor instead of deleting it
    const old=p.inv[p.slot];
    if(old!==key)m.pickups.push({kind:'gun',wkey:old,x:p.x+rnd(-14,14),y:p.y+10,vx:rnd(-50,50),vy:rnd(-50,50),life:60,delay:1.1,ignoreId:p.id,ignoreT:3});
    p.inv[p.slot]=key; p.ammos[p.slot]=WEAPONS[key].mag;
    if(p.client)wsSend(p.client,{type:'event',ev:'dropped',wkey:old});
  }
  p.reloadT=0; p.gunCD=.35;
}
function startMatch(humans,fakes,mode){
  fakes=fakes||[]; mode=MODES[mode]?mode:'solo'; const teamSize=MODES[mode];
  const id='m'+(matchSeq++);
  const {obs,bushes}=buildObstacles();
  const m={id,mode,players:new Map(),bullets:[],pickups:[],crates:[],obs,bushes,phase:'countdown',t:3.2,tickN:0,
    zone:{x:C.x,y:C.y,r:C.r,fx:C.x,fy:C.y,fr:C.r,tx:C.x,ty:C.y,tr:C.r,phase:0,wait:26,shrinking:false,prog:0},events:[],pending:[]};
  const total=Math.max(LOBBY_TARGET,humans.length);
  const pads=[];
  if(teamSize>1){ const teams=Math.ceil(total/teamSize); for(let ti=0;ti<teams;ti++){ const ba=(ti/teams)*TAU+rnd(-.1,.1), bd=rnd(2250,2620);
      for(let k=0;k<teamSize&&pads.length<total;k++){ const a=ba+rnd(-.03,.03), d=bd+rnd(-120,120); pads.push([C.x+Math.cos(a)*d,C.y+Math.sin(a)*d]); } } }
  else { for(let i=0;i<total;i++){ const a=(i/total)*TAU+rnd(-.06,.06), d=rnd(2250,2620); pads.push([C.x+Math.cos(a)*d,C.y+Math.sin(a)*d]); }
    for(let i=pads.length-1;i>0;i--){ const j=Math.floor(Math.random()*(i+1)); [pads[i],pads[j]]=[pads[j],pads[i]]; } }
  humans.forEach((c,i)=>{ const p=mkPlayer(c.id,c.name,choice(SPRITES),COLORS[i%COLORS.length],c); p.x=pads[i][0]; p.y=pads[i][1]; p.aim=Math.atan2(C.y-p.y,C.x-p.x);
    c.match=id; c.pid=p.id; m.players.set(p.id,p); });
  const avgLv=humans.reduce((a2,c2)=>a2+(c2.level||1),0)/Math.max(1,humans.length), LF=Math.max(0,Math.min(1,(avgLv-1)/29));
  const usedNames=new Set(humans.map(c=>c.name));
  for(let i=humans.length;i<total;i++){ const fk=fakes[i-humans.length];
    let bn, bid; if(fk&&!usedNames.has(fk.name)){ bn=fk.name; bid=fk.id; } else { bn=choice(BOTNAMES); while(usedNames.has(bn))bn=choice(BOTNAMES); bid='u'+(100000+Math.floor(Math.random()*899999)); }
    usedNames.add(bn);
    const b=mkPlayer(bid,bn,choice(SPRITES),COLORS[i%COLORS.length],null); b.skill=Math.max(.08,Math.min(.85,.14+Math.pow(Math.random(),1.6)*(.36+.26*LF)+LF*.08)); // level-scaled, softened: online bots are meant to be beatable
    b.x=pads[i][0]; b.y=pads[i][1]; b.aim=Math.atan2(C.y-b.y,C.x-b.x); m.players.set(b.id,b); }
  for(let i=0;i<23;i++){ let x,y,n=0; const a=rnd(0,TAU), d=rnd(150,C.r-250); x=C.x+Math.cos(a)*d; y=C.y+Math.sin(a)*d;
    while(!clearOf(obs,x,y,44)&&n++<24){ x+=rnd(-60,60); y+=rnd(-60,60); }
    const tr=Math.random(), tier=i===0?'Camo':(tr<.6?'Wood':tr<.95?'Camo':'Neon');
    m.crates.push({x,y,open:false,tier}); }
  matches.set(id,m);
  [...m.players.values()].forEach((p,idx)=>{ p.team=Math.floor(idx/teamSize); }); // teams follow roster order: your queue seat decides your team
  m.players.forEach(p=>{ if(p.client)wsSend(p.client,{type:'match_start',id:p.id,mode:m.mode,map:{cx:C.x,cy:C.y,r:C.r},obs,bushes,crates:m.crates,players:[...m.players.values()].map(q=>({id:q.id,name:q.name,color:q.color,sprite:q.sprite,team:q.team}))}); });
  return m;
}
function mEvent(m,ev){ m.players.forEach(p=>{ if(p.client)wsSend(p.client,{type:'event',...ev}); }); }
function aliveCount(m){ let n=0; m.players.forEach(p=>{ if(p.alive)n++; }); return n; }
function checkEnd(m){
  if(m.phase!=='live')return;
  const alive=[...m.players.values()].filter(p=>p.alive);
  m.players.forEach(p=>{ if(!p.alive&&p.place==null)p.place=alive.length+1; }); // TRUE placement: recorded the moment you drop = how long you survived
  const aliveTeams=new Set(alive.map(p=>p.team));
  if(aliveTeams.size<=1){ m.phase='ended'; m.t=12;
    alive.forEach(p=>{ if(p.place==null)p.place=1; }); // the last one standing takes #1
    const used=new Set([...m.players.values()].map(p=>p.place).filter(x=>x!=null));
    let slot=m.players.size; // anyone unplaced (left during the countdown, etc.) takes a leftover slot, best kills first
    [...m.players.values()].filter(p=>p.place==null).sort((a,b)=>(b.kills-a.kills)||(b.dmg-a.dmg)).forEach(p=>{ while(used.has(slot))slot--; p.place=slot; used.add(slot); });
    const winTeam=alive.length?alive[0].team:-1;
    const standings=[...m.players.values()].map(p=>({place:p.place,name:p.name,kills:p.kills,dmg:Math.round(p.dmg),win:p.team===winTeam,team:p.team,id:p.id}))
      .sort((a,b)=>a.place-b.place); // leaderboard = survival order, full stop
    m.players.forEach(p=>{ if(p.client){ wsSend(p.client,{type:'match_end',standings,you:p.id,mode:m.mode}); p.client.match=null; p.client.pid=null; } }); // free the player NOW, not at cleanup 12s later
  }
}
function explode(m,x,y,owner){
  mEvent(m,{ev:'boom',x,y});
  m.players.forEach(f=>{ if(!f.alive)return; const d=Math.hypot(f.x-x,(f.y-28)-y);
    if(d<118&&!(owner&&f!==owner&&f.team===owner.team)){ const dmg=Math.max(14,58*(1-d/130))*(f===owner?.45:1); hurt(m,f,dmg,owner); const ka=Math.atan2((f.y-28)-y,f.x-x); f.kvx=(f.kvx||0)+Math.cos(ka)*300; f.kvy=(f.kvy||0)+Math.sin(ka)*300; } });
}
function hurt(m,victim,dmg,attacker,crit){
  if(attacker&&victim!==attacker&&attacker.team===victim.team)return; // friendly fire is OFF
  if(attacker&&victim!==attacker)mEvent(m,{ev:'hit',attacker:attacker.id,victim:victim.id,dmg:Math.round(dmg),crit:!!crit,x:Math.round(victim.x),y:Math.round(victim.y-40)});
  if(!victim.alive||m.phase!=='live')return;
  victim.hp-=dmg; victim.lastHurt=0; victim.revealT=.8;
  if(victim.isBot&&attacker&&attacker!==victim){ victim.memX=attacker.x; victim.memY=attacker.y; victim.memT=victim.clock||0; if(Math.random()<.5)victim.foeId=attacker.id; }
  if(attacker&&attacker!==victim)attacker.dmg+=dmg;
  if(victim.hp<=0){ victim.alive=false; victim.hp=0; if(attacker&&attacker!==victim)attacker.kills++;
    mEvent(m,{ev:'kill',killer:attacker?attacker.name:null,victim:victim.name,killerId:attacker?attacker.id:null,victimId:victim.id,wkey:attacker?attacker.inv[attacker.slot]:null});
    if(Math.random()<.5)m.pickups.push({kind:'med',x:victim.x,y:victim.y,vx:rnd(-60,60),vy:rnd(-60,60),life:16});
    checkEnd(m);
  }
}
function tryFire(m,f){
  const key=f.inv[f.slot], wp=WEAPONS[key];
  if(f.reloadT>0||f.cool>0||!f.alive)return;
  if(f.ammos[f.slot]<=0){ startReload(f); return; }
  f.cool=wp.rate; f.ammos[f.slot]--; f.revealT=.8;
  const moveF=clamp(Math.hypot(f.vx,f.vy)/260,0,1);
  const eff=f.aim, mx=f.x+Math.cos(eff)*38, my=(f.y-28)+Math.sin(eff)*38; // f.aim already includes the shooter's visible recoil (sent by the client)
  const spread=f.pinpoint?0:(f.client? wp.spread*.35+f.bloom+moveF*moveF*.14 : wp.spread*(.92-.4*f.skill)+f.bloom+moveF*.03); // human-ish: worse than you, better than target dummies
  for(let p=0;p<wp.pellets;p++){
    const a=wp.pellets>1?eff+((p/(wp.pellets-1))-.5)*wp.spread*1.7+rnd(-(.014+moveF*.034),.014+moveF*.034):eff+rnd(-spread,spread);
    m.bullets.push({x:mx,y:my,vx:Math.cos(a)*wp.speed,vy:Math.sin(a)*wp.speed,dmg:wp.dmg,owner:f,travel:0,wkey:key,explosive:!!wp.explosive,life:wp.range/wp.speed});
    if(m.bullets.length>240)m.bullets.shift();
  }
  if(f.pinpoint)f.bloom=0; else f.bloom=Math.min(.085,f.bloom+(WBLOOM[key]||.006)*(1+moveF*3.4));
  const push=(wp.kick||2)*9*(1+moveF*1.1); f.kvx=(f.kvx||0)-Math.cos(eff)*push; f.kvy=(f.kvy||0)-Math.sin(eff)*push; // gun shoves the shooter, like solo
  mEvent(m,{ev:'shot',wkey:key,x:mx,y:my});
  if(f.ammos[f.slot]===0)startReload(f);
}
function startReload(p){ const wp=WEAPONS[p.inv[p.slot]]; if(p.reloadT<=0&&p.ammos[p.slot]<wp.mag)p.reloadT=wp.reload; }

function botThink(m,f,dt){
  f.clock+=dt;
  const see=(o,d)=>d<680&&!(o.hidden&&d>115)&&!losBlocked(m.obs,f.x,f.y-26,o.x,o.y-26);
  // ---- perception: nearest visible fighter + committed 1v1 target with memory ----
  let vis=null,vd=1e9,near=null,nd=1e9;
  m.players.forEach(o=>{ if(o!==f&&o.alive&&o.team!==f.team){ const d=Math.hypot(o.x-f.x,o.y-f.y); if(d<nd){nd=d;near=o;} if(see(o,d)&&d<vd){vd=d;vis=o;} } });
  if(vis){ f.memX=vis.x; f.memY=vis.y; f.memT=f.clock; }
  const _lk=f.foeId?m.players.get(f.foeId):null; const locked=(_lk&&_lk.alive)?_lk:null;
  let foe=null;
  if(locked){ const ld=Math.hypot(locked.x-f.x,locked.y-f.y);
    if(see(locked,ld)||f.clock-f.memT<2.8)foe=locked;                       // stay in the 1v1 while it's alive/recent
    if(vis&&vis!==locked&&vd<ld*.55)foe=vis;                               // ...unless someone much closer is right on me
  }
  if(!foe)foe=vis;
  f.foeId=foe?foe.id:null;
  const fd=foe?Math.hypot(foe.x-f.x,foe.y-f.y):1e9;
  const canSee=!!(foe&&see(foe,fd));
  const outZ=Math.hypot(f.x-m.zone.x,f.y-m.zone.y)>m.zone.r;
  // ---- hide when hurt or caught reloading: break for the nearest bush ----
  if(f.hideCD>0)f.hideCD-=dt;
  if(f.hideT>0){ f.hideT-=dt; if(f.hp>64)f.hideT=0; }
  if(f.hideT<=0){
    const threat=canSee||f.lastHurt<3;
    if((f.hp<38&&threat)||(f.reloadT>.3&&canSee&&fd<250)){
      let bb=null,bdd=560; m.bushes.forEach(b=>{ const d=Math.hypot(b.x-f.x,b.y-f.y); if(d<bdd){bdd=d;bb=b;} });
      if(bb){ f.hideTX=bb.x; f.hideTY=bb.y; }
      else if(foe){ const a=Math.atan2(f.y-foe.y,f.x-foe.x); f.hideTX=f.x+Math.cos(a)*280; f.hideTY=f.y+Math.sin(a)*280; }
      if(bb||foe){ f.hideT=f.reloadT>.3?Math.min(2.4,f.reloadT+.9):rnd(2.6,4.6); f.hideCD=7; }
    }
  }
  f.aiT-=dt;
  if(f.aiT<=0){ f.aiT=.15;
    if(outZ){ f.aiTX=m.zone.x+rnd(-80,80); f.aiTY=m.zone.y+rnd(-80,80); f.investigating=false; }
    else if(f.hideT>0){ f.aiTX=f.hideTX; f.aiTY=f.hideTY; f.investigating=false; }
    else if(canSee){ const ideal=(f.inv[f.slot]==='shotgun'?150:260)*(1.15-(f.aggro-.85)), a=Math.atan2(foe.y-f.y,foe.x-f.x); f.aiTX=foe.x-Math.cos(a)*ideal; f.aiTY=foe.y-Math.sin(a)*ideal; f.investigating=false; }
    else if(f.clock-f.memT<6){ f.aiTX=f.memX+rnd(-40,40); f.aiTY=f.memY+rnd(-40,40); f.investigating=true; } // hunt the last place I saw/heard them
    else { let best=null,bd=2400; m.crates.forEach(cr=>{ if(!cr.open){ const d=Math.hypot(cr.x-f.x,cr.y-f.y); if(d<bd){bd=d;best=cr;} } });
      const WRANK={pistol:0,shotgun:1,uzi:2,m4:3,ak:4,lmg:5,rpg:6}; let bg=null,bd2=820;
      m.pickups.forEach(pk=>{ if(pk.kind==='gun'&&!(pk.delay>0)){ const d=Math.hypot(pk.x-f.x,pk.y-f.y); if(d<bd2&&(WRANK[pk.wkey]||0)>(WRANK[f.inv[f.slot]]||0)){ bd2=d; bg=pk; } } });
      let md=null,mdd=950; if(f.hp<62)m.pickups.forEach(pk=>{ if(pk.kind==='med'){ const d=Math.hypot(pk.x-f.x,pk.y-f.y); if(d<mdd){ mdd=d; md=pk; } } });
      f.investigating=false;
      if(md){ f.aiTX=md.x; f.aiTY=md.y; }
      else if(bg){ f.aiTX=bg.x; f.aiTY=bg.y; }
      else if(best){ f.aiTX=best.x; f.aiTY=best.y; }
      else { f.roamT=(f.roamT||0)-.15; if(f.roamX===undefined||f.roamT<=0||Math.hypot(f.roamX-f.x,f.roamY-f.y)<70){ const a=rnd(0,TAU), d=rnd(0,m.zone.r*.6); f.roamX=m.zone.x+Math.cos(a)*d; f.roamY=m.zone.y+Math.sin(a)*d; f.roamT=rnd(7,13); } f.aiTX=f.roamX; f.aiTY=f.roamY; } } // hold ONE roam target (re-rolling every tick made bots spin)
    // stick with the team: drifted away from every living teammate (and not hiding/hunting)? regroup
    { let tm=null,td2=1e9; m.players.forEach(o=>{ if(o!==f&&o.alive&&o.team===f.team){ const d2=Math.hypot(o.x-f.x,o.y-f.y); if(d2<td2){td2=d2;tm=o;} } });
      if(tm&&td2>480&&!outZ&&f.hideT<=0&&f.clock-f.memT>=6){ f.aiTX=tm.x+rnd(-130,130); f.aiTY=tm.y+rnd(-130,130); f.investigating=false; } }
    if(Math.random()<.18)f.strafe*=-1;
  }
  let tx=f.aiTX, ty=f.aiTY;
  if(f.detourT>0){ f.detourT-=dt; if(f.detourT>0){ tx=f.detourX; ty=f.detourY; } } // on a stuck-detour: walk the waypoint, not the wall
  let mx=0,my=0; const dx=tx-f.x, dy=ty-f.y, dm=Math.hypot(dx,dy);
  if(dm>30){ mx=dx/dm; my=dy/dm; }
  if(f.investigating&&Math.hypot(f.aiTX-f.x,f.aiTY-f.y)<44){ f.investigating=false; f.holdT=.8; f.checkA=Math.PI; f.checkDur=.42; f.memT=f.clock-99; } // arrived: stop, clear behind
  f.stuckChk=(f.stuckChk||0)-dt; // no PROGRESS toward the goal (lapping a wall counts — moving is not progressing)? detour wide around the blocker
  if(f.detCD>0)f.detCD-=dt;
  if(f.stuckChk<=0){ f.stuckChk=.45; const distT=Math.hypot(f.aiTX-f.x,f.aiTY-f.y);
    const trying=(mx||my)&&f.holdT<=0&&!(f.hideT>0&&f.inBush)&&!canSee&&!outZ&&distT>100&&f.detourT<=0&&(f.detCD||0)<=0; // never judge progress mid-detour — that's what made bots yo-yo along walls
    const prog=(f.ldT==null?distT:f.ldT)-distT; f.ldT=distT;
    if(prog>=10)f.detFlip=1; // making progress again: forget the side-switching
    f.stuckT=trying&&prog<10?(f.stuckT||0)+.45:0;
    if(f.stuckT>=1.3){ f.stuckT=0; f.ldT=null; const side=(f.strafe||1)*(f.detFlip||1); f.detFlip=(f.detFlip||1)*-1; // alternate sides: if left didn't work, go right next time
      const sa=Math.atan2(f.aiTY-f.y,f.aiTX-f.x)+Math.PI/2*side; f.detourX=f.x+Math.cos(sa)*rnd(260,420); f.detourY=f.y+Math.sin(sa)*rnd(260,420); f.detourT=2.4; f.detCD=3.6; } }
  if(f.holdT>0){ f.holdT-=dt; mx=0; my=0; }
  if(f.hideT>0&&f.inBush){ mx=0; my=0; } // dug into the bush: stay put
  if((mx||my)&&pointBlocked(m.obs,f.x+mx*56,f.y+my*56,10)){ const ox=mx; mx=-my*f.strafe; my=ox*f.strafe; }
  f.input.mx=mx; f.input.my=my;
  let crateNear=null; m.crates.forEach(cr=>{ if(!cr.open&&Math.hypot(cr.x-f.x,cr.y-f.y)<54)crateNear=cr; });
  let nearGun=null; m.pickups.forEach(pk=>{ if(pk.kind==='gun'&&Math.hypot(pk.x-f.x,pk.y-f.y)<58)nearGun=pk; });
  f.input.interact=!!(nearGun&&f.gunCD<=0);
  if(crateNear&&f.crateCD<=0){ openCrate(m,f,crateNear); }
  let fleeing=false;
  if(canSee&&foe){ // fight the 1v1 like a player: hold range, strafe, dash
    const a=Math.atan2(foe.y-f.y,foe.x-f.x), wp2=f.inv[f.slot], ideal=(wp2==='shotgun'?150:wp2==='rpg'?330:250)*(1.15-(f.aggro-.85));
    f.strafeT=(f.strafeT||0)-dt; if(f.strafeT<=0){ f.strafeT=rnd(.55,1.3); const r2=Math.random(); f.strafe=r2<.24?0:(r2<.7?-f.strafe:f.strafe); } // changes direction or plants more often — no endless orbiting
    fleeing=f.hp<Math.round(34-f.aggro*8)&&fd<320;
    if(!(f.hideT>0&&f.inBush)){ // (if dug into a bush, don't dance out of it)
      f.orbT=(f._orbId===foe.id)?(f.orbT||0)+dt:0; f._orbId=foe.id; // how long have we been dancing with this one fighter?
      if(f.orbT>2.2){ f.orbT=.6; f.strafe=f.strafe===0?(Math.random()<.5?-1:1):-f.strafe; f.orbPush=1.1; } // break the death-spiral: flip side + charge in
      if(f.orbPush>0)f.orbPush-=dt;
      let rad=0; if(fleeing)rad=-1; else if(f.orbPush>0)rad=1; else if(fd>ideal+70)rad=1; else if(fd<ideal-70)rad=-1;
      let smx=Math.cos(a)*rad+Math.cos(a+Math.PI/2)*f.strafe*.72, smy=Math.sin(a)*rad+Math.sin(a+Math.PI/2)*f.strafe*.72;
      const m2=Math.hypot(smx,smy); if(m2>1){ smx/=m2; smy/=m2; }
      f.input.mx=smx; f.input.my=smy;
    }
    if(f.dashCD<=0&&((f.lastHurt<.45&&Math.random()<dt*.9)||Math.random()<dt*.02))f.input.dash=true; // mostly dashes when bullets start flying at them
  }
  f.seeT=canSee?f.seeT+dt:0; f.burst-=dt;
  if(canSee){ f.aimT=(f.aimT||0)-dt;
    if(f.seeT<dt*1.5)f.aimErr=rnd(-(0.19-0.1*f.skill),0.19-0.1*f.skill); // first glance is way off, then settles
    f.aimErr=clamp((f.aimErr||0)+rnd(-1,1)*dt*(0.3-0.15*f.skill),-(0.15-0.08*f.skill),0.15-0.08*f.skill); // hand sway (looser hands)
    if(f.aimT<=0){ f.aimT=rnd(.06,.13); const bs=(WEAPONS[f.inv[f.slot]]||{}).speed||1350, lead=Math.min(.32,fd/bs)*(0.25+0.4*f.skill); // humans under-lead (these under-lead more)
      f.input.aim=Math.atan2((foe.y-28+foe.vy*lead)-(f.y-28),foe.x+foe.vx*lead-f.x)+(f.aimErr||0); }
    if(f.inv.length>1){ const wantClose=fd<200; const wi=f.inv.findIndex(k=>wantClose?(k==='shotgun'||k==='uzi'):['m4','ak','lmg','rpg'].includes(k)); if(wi>=0) { f.slot=wi; } }
    f.input.fire=f.seeT>(.55-.22*f.skill)&&f.burst<=0;
    if(f.hideT>0&&f.inBush&&fd>190)f.input.fire=false; // hiding: don't give the bush away
    if(f.hidden&&fd>205&&f.hp>60)f.input.fire=false;   // ambush: let them walk in closer
    if(fleeing)f.burst=Math.max(f.burst,rnd(.05,.2));
    if(f.input.fire&&Math.random()<.09&&fd>140)f.burst=rnd(.25,.6);
  } else { f.input.fire=false;
    // ---- no target: clear directions like a person (sweep, check flanks/behind, snap to shots) ----
    let base=(mx||my)?Math.atan2(my,mx):f.input.aim;
    f.checkT-=dt;
    if(f.checkT<=0){ f.checkT=rnd(2.1,4.3)*(1.12-.3*f.skill); f.checkDur=.34; const r=Math.random(); f.checkA=r<.45?rnd(1.0,1.7):r<.8?-rnd(1.0,1.7):Math.PI+rnd(-.5,.5); }
    let off=Math.sin(f.clock*.6)*.32;
    if(f.checkDur>0){ f.checkDur-=dt; off=f.checkA; }
    if(f.lastHurt<.5&&(f.memX||f.memY)){ base=Math.atan2(f.memY-f.y,f.memX-f.x); off=0; }
    f.input.aim=base+off;
  }
}
function tickMatch(m,dt){
  m.tickN++;
  if(m.phase==='countdown'){ m.t-=dt; if(m.t<=0){ m.phase='live'; mEvent(m,{ev:'go'}); } }
  else if(m.phase==='ended'){ m.t-=dt; if(m.t<=0){ m.players.forEach(p=>{ if(p.client&&p.client.match===m.id){ p.client.match=null; p.client.pid=null; } }); matches.delete(m.id); } return; } // (delayed cleanup only ever clears bindings that still point at THIS match)
  if(m.phase!=='live'&&m.phase!=='countdown')return;
  const live=m.phase==='live';
  // zone
  if(live){ const z=m.zone; z.wait-=dt;
    if(!z.shrinking&&z.wait<=0&&z.phase<5){ const fr=[.68,.48,.32,.2,.1][z.phase]; z.fx=z.x; z.fy=z.y; z.fr=z.r; z.tr=C.r*fr;
      const maxOff=Math.max(0,(z.r-z.tr)*.7), a=rnd(0,TAU), off=rnd(0,maxOff); z.tx=z.x+Math.cos(a)*off; z.ty=z.y+Math.sin(a)*off; z.shrinking=true; z.prog=0; mEvent(m,{ev:'zone'}); }
    else if(z.shrinking){ z.prog+=dt/38; const k=Math.min(1,z.prog); z.x=z.fx+(z.tx-z.fx)*k; z.y=z.fy+(z.ty-z.fy)*k; z.r=z.fr+(z.tr-z.fr)*k;
      if(k>=1){ z.shrinking=false; z.phase++; z.wait=[26,22,20,18,16][Math.min(z.phase,4)]; } }
  }
  m.players.forEach(f=>{
    if(!f.alive)return;
    if(f.isBot&&live)botThink(m,f,dt);
    const inp=f.input;
    if(live){ f.aim=inp.aim; }
    let mx=live?inp.mx:0, my=live?inp.my:0; const mm=Math.hypot(mx,my); if(mm>1){mx/=mm;my/=mm;}
    f.kvx=(f.kvx||0)*Math.pow(.001,dt); f.kvy=(f.kvy||0)*Math.pow(.001,dt);
    if(f.dashT>0){ f.dashT-=dt; f.vx=f.dashDX*620+f.kvx; f.vy=f.dashDY*620+f.kvy; }
    else { const max=f.inv[f.slot]==='lmg'?238:264; f.vx=mx*max+f.kvx; f.vy=my*max+f.kvy; }
    f.x+=f.vx*dt; f.y+=f.vy*dt; collide(m.obs,f);
    f.inBush=m.bushes.some(b=>Math.hypot(b.x-f.x,b.y-f.y)<b.r);
    f.hidden=f.inBush&&f.revealT<=0;
    f.cool-=dt; f.dashCD-=dt; f.crateCD-=dt; f.swapCD-=dt; f.gunCD-=dt; f.revealT-=dt; f.lastHurt+=dt;
    f.bloom=Math.max(0,f.bloom-dt*(Math.hypot(f.vx,f.vy)>50?.038:.13));
    f.recoilA*=Math.pow(.000001,dt);
    if(f.lastHurt>6&&f.hp<75)f.hp=Math.min(75,f.hp+5*dt);
    if(f.reloadT>0){ f.reloadT-=dt; if(f.reloadT<=0)f.ammos[f.slot]=WEAPONS[f.inv[f.slot]].mag; }
    if(live&&inp.fire)tryFire(m,f);
    if(live&&inp.interact&&f.crateCD<=0){ let cr=null; m.crates.forEach(c2=>{ if(!c2.open&&Math.hypot(c2.x-f.x,c2.y-f.y)<80)cr=c2; });
      if(cr){ openCrate(m,f,cr); } }
    if(live&&inp.swap&&f.swapCD<=0&&f.inv.length>1){ f.slot=(f.slot+1)%f.inv.length; f.reloadT=0; f.swapCD=.35; inp.swap=false; }
    if(live&&inp.reload){ startReload(f); inp.reload=false; }
    if(live&&inp.dash&&f.dashCD<=0){ let dx=f.vx,dy=f.vy; if(Math.hypot(dx,dy)<20){dx=Math.cos(f.aim);dy=Math.sin(f.aim);} const d=Math.hypot(dx,dy); f.dashDX=dx/d; f.dashDY=dy/d; f.dashT=.17; f.dashCD=2; inp.dash=false; }
    if(live&&Math.hypot(f.x-m.zone.x,f.y-m.zone.y)>m.zone.r){ f.zoneAcc=(f.zoneAcc||0)+dt; if(f.zoneAcc>=1){ f.zoneAcc=0; hurt(m,f,[4,7,11,16,24][Math.min(m.zone.phase,4)],null); } }
  });
  if(live){ const arr=[...m.players.values()].filter(p=>p.alive); // soft separation: overlapping fighters shove apart
    for(let i=0;i<arr.length;i++)for(let j=i+1;j<arr.length;j++){ const A=arr[i],B=arr[j],dx=B.x-A.x,dy=B.y-A.y,d=Math.hypot(dx,dy);
      if(d>0.01&&d<30){ const push=(30-d)*.5,ux=dx/d,uy=dy/d; A.x-=ux*push; A.y-=uy*push; B.x+=ux*push; B.y+=uy*push; } } }
  for(let i=m.pending.length-1;i>=0;i--){ const pd=m.pending[i]; pd.t-=dt;
    if(pd.t<=0){ m.pickups.push({kind:'gun',wkey:pd.wkey,x:pd.x+rnd(-8,8),y:pd.y,vx:rnd(-70,70),vy:rnd(30,90),life:60,delay:0});
      mEvent(m,{ev:'landed',name:pd.name,wkey:pd.wkey,x:Math.round(pd.x),y:Math.round(pd.y)}); m.pending.splice(i,1); } }
  // bullets
  for(let i=m.bullets.length-1;i>=0;i--){ const b=m.bullets[i]; let dead=false;
    for(let st=0;st<2&&!dead;st++){ b.x+=b.vx*dt/2; b.y+=b.vy*dt/2; b.travel+=Math.hypot(b.vx,b.vy)*dt/2;
      if(pointBlocked(m.obs,b.x,b.y,1)){ if(b.explosive)explode(m,b.x,b.y,b.owner); dead=true; break; }
      for(const f of m.players.values()){ if(!f.alive||f===b.owner||f.team===b.owner.team)continue; // shots pass through teammates
        if(distSeg(b.x,b.y,f.x,f.y-56,f.x,f.y-6)<21){ if(b.explosive)explode(m,b.x,b.y,b.owner);
          else { const fo=FALLOFF[b.wkey]||[500,.5], fd=b.travel<=fo[0]?1:Math.max(fo[1],1-(b.travel-fo[0])/520), crit=Math.random()<.08; hurt(m,f,b.dmg*fd*(crit?1.5:1),b.owner,crit); }
          dead=true; break; } }
    }
    b.life-=dt; if(dead||b.life<=0){ if(!dead&&b.explosive)explode(m,b.x,b.y,b.owner); m.bullets.splice(i,1); }
  }
  for(let i=m.pickups.length-1;i>=0;i--){ const pk=m.pickups[i]; pk.life-=dt; pk.x+=(pk.vx||0)*dt; pk.y+=(pk.vy||0)*dt; pk.vx*=.9; pk.vy*=.9;
    pk.delay=Math.max(0,(pk.delay||0)-dt); pk.ignoreT=Math.max(0,(pk.ignoreT||0)-dt);
    let taken=false;
    for(const f of m.players.values()){ if(!f.alive)continue;
      if(pk.kind==='med'&&f.hp<100&&Math.hypot(f.x-pk.x,f.y-pk.y)<38){ f.hp=Math.min(100,f.hp+34); taken=true; break; }
      if(pk.kind==='gun'&&f.input.interact&&!pk.delay&&f.gunCD<=0&&!(pk.ignoreT>0&&pk.ignoreId===f.id)&&Math.hypot(f.x-pk.x,f.y-pk.y)<68){
        giveGun(m,f,pk.wkey); mEvent(m,{ev:'pickup',name:f.name,pId:f.id,wkey:pk.wkey}); taken=true; break; }
    }
    if(taken||pk.life<=0)m.pickups.splice(i,1);
  }
  // snapshots at ~15Hz, personalised (hidden enemies' positions are withheld)
  {
    m.players.forEach(viewer=>{
      if(!viewer.client)return;
      const snap={type:'snap',phase:m.phase,t:Math.max(0,m.t),zone:{x:m.zone.x,y:m.zone.y,r:m.zone.r,shrinking:m.zone.shrinking,phase:m.zone.phase,wait:Math.max(0,m.zone.wait)},alive:aliveCount(m),
        players:[...m.players.values()].map(p=>{
          if(p!==viewer&&p.hidden&&p.alive&&p.team!==viewer.team)return {id:p.id,name:p.name,color:p.color,alive:p.alive,hidden:true,hp:Math.ceil(p.hp),team:p.team};
          return {id:p.id,name:p.name,color:p.color,sprite:p.sprite,x:Math.round(p.x),y:Math.round(p.y),aim:p.aim,hp:Math.ceil(p.hp),alive:p.alive,weapon:p.inv[p.slot],ammo:p.ammos[p.slot],kills:p.kills,hidden:p.hidden,dashCD:Math.max(0,p.dashCD),team:p.team};
        }),
        bullets:m.bullets.map(b=>[Math.round(b.x),Math.round(b.y),Math.round(Math.atan2(b.vy,b.vx)*100)/100,b.wkey,b.owner.id]),
        crates:m.crates, pickups:m.pickups,
        self:{hp:Math.ceil(viewer.hp),weapon:viewer.inv[viewer.slot],ammo:viewer.ammos[viewer.slot],inv:viewer.inv,ammos:viewer.ammos,slot:viewer.slot,kills:viewer.kills,dmg:Math.round(viewer.dmg),hidden:viewer.hidden,reload:viewer.reloadT,bloom:viewer.bloom,dashCD:Math.max(0,viewer.dashCD)}};
      wsSend(viewer.client,snap);
    });
  }
}
setInterval(()=>{ matches.forEach(m=>{ try{ tickMatch(m,1/60); }catch(e){ console.error('tick',e); } }); },1000/60);

function handleMsg(c,msg){
  if(msg.type==='queue'){
    if(c.match&&!matches.has(c.match)){ c.match=null; c.pid=null; } // stale binding from a dead match must never block queueing
    if(c.match)return;
    c.name=(msg.name||'PLAYER').toString().replace(/[<>&"']/g,'').slice(0,10).toUpperCase()||'PLAYER';
    c.pinpoint=!!msg.pinpoint; c.level=Math.max(1,Math.min(999,parseInt(msg.level)||1));
    const want='solo'; // duo/squad removed at the player's request: every match is solo again
    if(c.queued&&(c.mode||'solo')!==want)removeFromQueue(c);
    c.mode=want; const L=getLobby(want);
    if(!c.queued){ c.queued=true; L.queue.push(c); planQueueStart(L); }
    wsSend(c,{type:'queued',count:L.queue.length});
    queueStatus(L);
  }
  if(msg.type==='dequeue'){ removeFromQueue(c); }
  if(msg.type==='forfeit'&&c.match&&c.pid){ const m=matches.get(c.match); if(m){ const pl=m.players.get(c.pid); if(pl)pl.client=null;
    if(pl&&pl.alive){ pl.alive=false; pl.hp=0; mEvent(m,{ev:'left',victim:pl.name}); checkEnd(m); } } c.match=null; c.pid=null; }
  if(msg.type==='input'&&c.match&&c.pid){
    const m=matches.get(c.match); if(!m)return; const p=m.players.get(c.pid); if(!p)return;
    const i=msg.input||{};
    p.input.mx=clamp(+i.mx||0,-1,1); p.input.my=clamp(+i.my||0,-1,1);
    if(typeof i.aim==='number'&&isFinite(i.aim))p.input.aim=i.aim;
    p.input.fire=!!i.fire; if(i.dash)p.input.dash=true; if(i.interact)p.input.interact=true; else if(i.interact===false)p.input.interact=false;
    if(i.swap)p.input.swap=true; if(i.reload)p.input.reload=true;
    if(typeof i.pinpoint==='boolean')p.pinpoint=i.pinpoint;
  }
}
let pidSeq=1;
server.on('connection',()=>{});
server.listen(PORT,'0.0.0.0',()=>console.log('Pixel Royale online server on :'+PORT));