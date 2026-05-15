/**
 * KJC Arena — cTrader Proxy + WebSocket Bridge Server
 * Runs on Railway
 */
const tls = require("tls");
const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");

const app = express();
app.use(express.json());
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

const CLIENT_ID = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET;
const PORT = process.env.PORT || 3001;
const CTRADER_HOST = "live.ctraderapi.com";
const CTRADER_PORT = 5035;

const PT = { APP_AUTH_REQ:2100,APP_AUTH_RES:2101,ACCOUNT_AUTH_REQ:2102,ACCOUNT_AUTH_RES:2103,GET_ACCOUNTS_REQ:2149,GET_ACCOUNTS_RES:2150,DEAL_LIST_REQ:2140,DEAL_LIST_RES:2141,ERROR_RES:2142,HEARTBEAT:51 };

// ── WebSocket Bridge ──────────────────────────────────────────────────────────
const wsSessions = new Map();
// Plugin connections (cTrader plugin — no sessionId, just one global plugin slot)
let pluginWs = null;

wss.on("connection", (ws, req) => {
  const isDesktop = req.headers["x-client"]?.includes("KJCArenaDesktop");
  let sessionId = null;
  let isPlugin = false;
  console.log(`WS connect: ${isDesktop ? "desktop" : "browser"}`);

  ws.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());

      // Plugin registration (cTrader plugin — no sessionId needed)
      if (msg.type === "REGISTER_PLUGIN") {
        isPlugin = true;
        pluginWs = ws;
        ws.send(JSON.stringify({ type: "REGISTERED_PLUGIN", version: "1.0" }));
        console.log("Plugin registered");
        // Notify all browser sessions that plugin is connected
        for (const [sid, pair] of wsSessions) {
          if (pair.browser?.readyState === 1) {
            pair.browser.send(JSON.stringify({ type: "DESKTOP_CONNECTED" }));
          }
        }
        return;
      }

      if (msg.type === "REGISTER") {
        sessionId = msg.sessionId;
        if (!wsSessions.has(sessionId)) wsSessions.set(sessionId, {});
        if (isDesktop) wsSessions.get(sessionId).desktop = ws;
        else {
          wsSessions.get(sessionId).browser = ws;
          const p = wsSessions.get(sessionId);
          // Check if desktop OR plugin is connected
          const hasDesktop = p.desktop?.readyState === 1;
          const hasPlugin = pluginWs?.readyState === 1;
          if (hasDesktop || hasPlugin) ws.send(JSON.stringify({ type: "DESKTOP_CONNECTED" }));
        }
        ws.send(JSON.stringify({ type: "REGISTERED", sessionId }));
        return;
      }

      if (msg.type === "PING") {
        ws.send(JSON.stringify({ type: "PONG", version: "1.0" }));
        if (isDesktop && sessionId && wsSessions.has(sessionId)) {
          const p = wsSessions.get(sessionId);
          if (p.browser?.readyState === 1) p.browser.send(JSON.stringify({ type: "DESKTOP_CONNECTED" }));
        }
        if (isPlugin) {
          // Notify all browsers plugin is alive
          for (const [sid, pair] of wsSessions) {
            if (pair.browser?.readyState === 1) pair.browser.send(JSON.stringify({ type: "DESKTOP_CONNECTED" }));
          }
        }
        return;
      }

      // Route messages between browser and desktop/plugin
      if (sessionId && wsSessions.has(sessionId)) {
        const pair = wsSessions.get(sessionId);
        if (isDesktop) {
          // Desktop -> browser
          if (pair.browser?.readyState === 1) pair.browser.send(data.toString());
        } else {
          // Browser -> desktop or plugin
          const target = pair.desktop;
          if (target?.readyState === 1) {
            target.send(data.toString());
          } else if (pluginWs?.readyState === 1) {
            // No desktop — route to plugin instead, attach sessionId
            const withSession = { ...msg, sessionId };
            pluginWs.send(JSON.stringify(withSession));
          }
        }
      } else if (isPlugin) {
        // Plugin sending results — route to correct browser session
        const targetSessionId = msg.sessionId;
        if (targetSessionId && wsSessions.has(targetSessionId)) {
          const pair = wsSessions.get(targetSessionId);
          if (pair.browser?.readyState === 1) pair.browser.send(data.toString());
        }
      }
    } catch(e) { console.error("WS msg error:", e.message); }
  });

  ws.on("close", () => {
    if (isPlugin) {
      pluginWs = null;
      console.log("Plugin disconnected");
      for (const [sid, pair] of wsSessions) {
        if (pair.browser?.readyState === 1) pair.browser.send(JSON.stringify({ type: "DESKTOP_DISCONNECTED" }));
      }
      return;
    }
    if (sessionId && wsSessions.has(sessionId)) {
      const pair = wsSessions.get(sessionId);
      if (isDesktop) { delete pair.desktop; if (pair.browser?.readyState === 1) pair.browser.send(JSON.stringify({ type: "DESKTOP_DISCONNECTED" })); }
      else delete pair.browser;
      if (!pair.desktop && !pair.browser) wsSessions.delete(sessionId);
    }
  });
  ws.on("error", e => console.error("WS error:", e.message));
});

// ── Protobuf helpers ──────────────────────────────────────────────────────────
function encodeVarint(n) { const out=[]; n=Number(n); while(n>127){out.push((n&0x7f)|0x80);n=Math.floor(n/128);} out.push(n&0x7f); return Buffer.from(out); }
function decodeVarint(buf,pos) { let r=BigInt(0),s=BigInt(0); while(pos<buf.length){const b=buf[pos++];r|=BigInt(b&0x7f)<<s;s+=BigInt(7);if(!(b&0x80))break;} return[r,pos]; }
function pbDecode(buf) { const f={}; let pos=0; while(pos<buf.length){let tag; try{[tag,pos]=decodeVarint(buf,pos);}catch(e){break;} const fn=Number(tag>>BigInt(3)),wt=Number(tag&BigInt(7)); let val; if(wt===0){[val,pos]=decodeVarint(buf,pos);} else if(wt===2){let len;[len,pos]=decodeVarint(buf,pos);len=Number(len);val=buf.slice(pos,pos+len);pos+=len;} else if(wt===1){pos+=8;continue;} else if(wt===5){pos+=4;continue;} else break; if(val===undefined)continue; if(f[fn]===undefined)f[fn]=val; else if(Array.isArray(f[fn]))f[fn].push(val); else f[fn]=[f[fn],val];} return f; }
function pbField(fn,wt,val) { const tag=encodeVarint((fn<<3)|wt); if(wt===0)return Buffer.concat([tag,encodeVarint(val)]); const vb=Buffer.isBuffer(val)?val:Buffer.from(String(val),"utf8"); return Buffer.concat([tag,encodeVarint(vb.length),vb]); }
function i64field(fn,value) { const tag=encodeVarint((fn<<3)|0); const out=[]; let n=BigInt(value); while(n>BigInt(127)){out.push(Number(n&BigInt(0x7f))|0x80);n>>=BigInt(7);} out.push(Number(n&BigInt(0x7f))); return Buffer.concat([tag,Buffer.from(out)]); }
function frame(pt,payload) { const msg=Buffer.concat([pbField(1,0,pt),pbField(2,2,payload)]); const len=Buffer.alloc(4);len.writeUInt32BE(msg.length,0); return Buffer.concat([len,msg]); }

const buildAppAuth=(id,sec)=>frame(PT.APP_AUTH_REQ,Buffer.concat([pbField(2,2,id),pbField(3,2,sec)]));
const buildGetAccounts=(tok)=>frame(PT.GET_ACCOUNTS_REQ,pbField(2,2,tok));
const buildAccountAuth=(id,tok)=>frame(PT.ACCOUNT_AUTH_REQ,Buffer.concat([i64field(2,id),pbField(3,2,tok)]));
function buildDealList(id,fromMs,toMs) { const MAX=BigInt(2147483646000); const to=BigInt(toMs)>MAX?MAX:BigInt(toMs); const from=BigInt(fromMs)<BigInt(0)?BigInt(0):BigInt(fromMs); return frame(PT.DEAL_LIST_REQ,Buffer.concat([i64field(2,id),i64field(3,from),i64field(4,to),pbField(5,0,500)])); }

function connectCT(host) { return new Promise((res,rej)=>{ const s=tls.connect({host,port:CTRADER_PORT,rejectUnauthorized:false},()=>res(s)); s.on("error",rej); setTimeout(()=>{s.destroy();rej(new Error(`TCP timeout ${host}`));},20000); }); }
function sendRecv(socket,msg,expectedPT,ms=20000) { return new Promise((resolve,reject)=>{ let buf=Buffer.alloc(0); const timer=setTimeout(()=>{socket.removeListener("data",onData);reject(new Error(`Timeout PT ${expectedPT}`));},ms); function onData(chunk){buf=Buffer.concat([buf,chunk]);while(buf.length>=4){const mlen=buf.readUInt32BE(0);if(buf.length<4+mlen)break;const mbuf=buf.slice(4,4+mlen);buf=buf.slice(4+mlen);const outer=pbDecode(mbuf);const pt=Number(outer[1]);const payload=outer[2]||Buffer.alloc(0);if(pt===PT.HEARTBEAT){socket.write(frame(PT.HEARTBEAT,Buffer.alloc(0)));continue;}if(pt===PT.ERROR_RES){const e=pbDecode(payload);const code=e[2]?(Buffer.isBuffer(e[2])?e[2].toString():String(e[2])):"ERR";const desc=e[3]?(Buffer.isBuffer(e[3])?e[3].toString():String(e[3])):"";clearTimeout(timer);socket.removeListener("data",onData);return reject(new Error(`${code}: ${desc}`));}if(pt===expectedPT){clearTimeout(timer);socket.removeListener("data",onData);return resolve(payload);}}} socket.on("data",onData);socket.write(msg);}); }
function parseAccounts(payload) { const top=pbDecode(payload); const accounts=[]; const raw=top[4];if(!raw)return accounts; const items=Array.isArray(raw)?raw:[raw]; for(const item of items){if(!Buffer.isBuffer(item))continue;const acc=pbDecode(item);if(acc[1]!==undefined){const id=typeof acc[1]==='bigint'?acc[1]:BigInt(acc[1]);const isLive=acc[5]!==undefined?Number(acc[5])===1:false;accounts.push({id,isLive});}} return accounts; }
function parseDeals(payload) { const top=pbDecode(payload); const raw=top[3];if(!raw)return[]; return Array.isArray(raw)?raw:[raw]; }
function computeStats(dealBufs) { const profits=[]; for(const d of dealBufs){if(!Buffer.isBuffer(d))continue;const deal=pbDecode(d);if(Number(deal[7])!==2)continue;if(!deal[10]||!Buffer.isBuffer(deal[10]))continue;const cpd=pbDecode(deal[10]);const gp=cpd[3]!==undefined?Number(cpd[3]):0;const md=cpd[9]!==undefined?Number(cpd[9]):2;profits.push(gp/Math.pow(10,md));} if(!profits.length)return{trades:0,pf:0,wr:0,dd:0,netProfit:0}; const wins=profits.filter(p=>p>0),losses=profits.filter(p=>p<0); const gpp=wins.reduce((s,p)=>s+p,0),gll=Math.abs(losses.reduce((s,p)=>s+p,0)); const pf=gll>0?gpp/gll:gpp>0?9.99:0; const wr=(wins.length/profits.length)*100; const net=profits.reduce((s,p)=>s+p,0); let peak=0,maxDD=0,cum=0; profits.forEach(p=>{cum+=p;if(cum>peak)peak=cum;const dd=peak>0?((peak-cum)/peak)*100:0;if(dd>maxDD)maxDD=dd;}); return{trades:profits.length,wins:wins.length,losses:losses.length,pf:Math.round(pf*100)/100,wr:Math.round(wr*10)/10,dd:Math.round(maxDD*10)/10,netProfit:Math.round(net*100)/100}; }

async function fetchOOS(token,months) {
  const liveSocket=await connectCT("live.ctraderapi.com"); let accounts=[];
  try{await sendRecv(liveSocket,buildAppAuth(CLIENT_ID,CLIENT_SECRET),PT.APP_AUTH_RES);const accPay=await sendRecv(liveSocket,buildGetAccounts(token),PT.GET_ACCOUNTS_RES);accounts=parseAccounts(accPay);liveSocket.destroy();}catch(e){liveSocket.destroy();throw e;}
  if(!accounts.length)throw new Error("No accounts found");
  const chosen=accounts.find(a=>a.isLive)||accounts[0];
  const host=chosen.isLive?"live.ctraderapi.com":"demo.ctraderapi.com";
  const socket=await connectCT(host);
  try{
    await sendRecv(socket,buildAppAuth(CLIENT_ID,CLIENT_SECRET),PT.APP_AUTH_RES);
    await sendRecv(socket,buildAccountAuth(chosen.id,token),PT.ACCOUNT_AUTH_RES);
    const toMs=Date.now(),fromMs=toMs-(months*30*24*60*60*1000);
    let deals=[];
    try{const dealPay=await sendRecv(socket,buildDealList(chosen.id,fromMs,toMs),PT.DEAL_LIST_RES);deals=parseDeals(dealPay);}catch(dealErr){if(dealErr.message.includes("UNSUPPORTED_MESSAGE")||dealErr.message.includes("27684")){socket.destroy();return{trades:0,pf:0,wr:0,dd:0,netProfit:0,accountType:chosen.isLive?'live':'demo',note:"No trade history found"};} throw dealErr;}
    socket.destroy();
    return{...computeStats(deals),accountType:chosen.isLive?'live':'demo',period:`Last ${months} months`};
  }catch(e){socket.destroy();throw e;}
}

// ── HTTP endpoints ────────────────────────────────────────────────────────────
app.get("/health",(req,res)=>res.json({status:"ok",configured:!!(CLIENT_ID&&CLIENT_SECRET),sessions:wsSessions.size,pluginConnected:!!(pluginWs?.readyState===1)}));
app.get("/oos",async(req,res)=>{ const{token,months=3}=req.query; if(!token)return res.status(401).json({error:"No token"}); if(!CLIENT_ID||!CLIENT_SECRET)return res.status(500).json({error:"Env vars missing"}); try{const stats=await fetchOOS(token,parseInt(months));res.json(stats);}catch(e){console.error("OOS error:",e.message);res.status(500).json({error:e.message});} });

server.listen(PORT,()=>console.log(`KJC Arena proxy+bridge on port ${PORT}`));
