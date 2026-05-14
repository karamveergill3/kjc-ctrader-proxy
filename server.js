/**
 * KJC Arena — cTrader Open API Proxy Server v8
 * Auto-detects live vs demo account and routes correctly
 */
const tls = require("tls");
const express = require("express");
const app = express();
app.use(express.json());

const CLIENT_ID = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET;
const PORT = process.env.PORT || 3001;

const PT = { APP_AUTH_REQ:2100,APP_AUTH_RES:2101,ACCOUNT_AUTH_REQ:2102,ACCOUNT_AUTH_RES:2103,GET_ACCOUNTS_REQ:2149,GET_ACCOUNTS_RES:2150,DEAL_LIST_REQ:2140,DEAL_LIST_RES:2141,ERROR_RES:2142,HEARTBEAT:51 };

function encodeVarint(n) {
  const out=[]; n=Number(n);
  while(n>127){out.push((n&0x7f)|0x80);n=Math.floor(n/128);}
  out.push(n&0x7f); return Buffer.from(out);
}

function decodeVarint(buf,pos) {
  let result=BigInt(0),shift=BigInt(0);
  while(pos<buf.length){const b=buf[pos++];result|=BigInt(b&0x7f)<<shift;shift+=BigInt(7);if(!(b&0x80))break;}
  return[result,pos];
}

function pbDecode(buf) {
  const fields={}; let pos=0;
  while(pos<buf.length){
    let tag; try{[tag,pos]=decodeVarint(buf,pos);}catch(e){break;}
    const fn=Number(tag>>BigInt(3)),wt=Number(tag&BigInt(7)); let val;
    if(wt===0){[val,pos]=decodeVarint(buf,pos);}
    else if(wt===2){let len;[len,pos]=decodeVarint(buf,pos);len=Number(len);val=buf.slice(pos,pos+len);pos+=len;}
    else if(wt===1){pos+=8;continue;}
    else if(wt===5){pos+=4;continue;}
    else break;
    if(val===undefined)continue;
    if(fields[fn]===undefined)fields[fn]=val;
    else if(Array.isArray(fields[fn]))fields[fn].push(val);
    else fields[fn]=[fields[fn],val];
  }
  return fields;
}

function pbField(fn,wt,val) {
  const tag=encodeVarint((fn<<3)|wt);
  if(wt===0)return Buffer.concat([tag,encodeVarint(val)]);
  const vb=Buffer.isBuffer(val)?val:Buffer.from(String(val),"utf8");
  return Buffer.concat([tag,encodeVarint(vb.length),vb]);
}

function encodeInt64Field(fn,value) {
  const tag=encodeVarint((fn<<3)|0); const out=[];
  let n=BigInt(value);
  while(n>BigInt(127)){out.push(Number(n&BigInt(0x7f))|0x80);n>>=BigInt(7);}
  out.push(Number(n&BigInt(0x7f)));
  return Buffer.concat([tag,Buffer.from(out)]);
}

function frame(pt,payload) {
  const msg=Buffer.concat([pbField(1,0,pt),pbField(2,2,payload)]);
  const len=Buffer.alloc(4);len.writeUInt32BE(msg.length,0);
  return Buffer.concat([len,msg]);
}

const buildAppAuth=(id,sec)=>frame(PT.APP_AUTH_REQ,Buffer.concat([pbField(2,2,id),pbField(3,2,sec)]));
const buildGetAccounts=(tok)=>frame(PT.GET_ACCOUNTS_REQ,pbField(2,2,tok));
const buildAccountAuth=(accId,tok)=>frame(PT.ACCOUNT_AUTH_REQ,Buffer.concat([encodeInt64Field(2,accId),pbField(3,2,tok)]));
const buildDealList=(accId,from,to)=>frame(PT.DEAL_LIST_REQ,Buffer.concat([encodeInt64Field(2,accId),encodeInt64Field(3,from),encodeInt64Field(4,to),pbField(5,0,1000)]));

function connect(host) {
  return new Promise((resolve,reject)=>{
    const s=tls.connect({host,port:5035,rejectUnauthorized:false},()=>resolve(s));
    s.on("error",reject);
    setTimeout(()=>{s.destroy();reject(new Error(`TCP timeout ${host}`));},20000);
  });
}

function sendRecv(socket,msg,expectedPT,ms=20000) {
  return new Promise((resolve,reject)=>{
    let buf=Buffer.alloc(0);
    const timer=setTimeout(()=>{socket.removeListener("data",onData);reject(new Error(`Timeout PT ${expectedPT}`));},ms);
    function onData(chunk) {
      buf=Buffer.concat([buf,chunk]);
      while(buf.length>=4){
        const mlen=buf.readUInt32BE(0);
        if(buf.length<4+mlen)break;
        const mbuf=buf.slice(4,4+mlen);buf=buf.slice(4+mlen);
        const outer=pbDecode(mbuf);
        const pt=Number(outer[1]);const payload=outer[2]||Buffer.alloc(0);
        console.log(`RX pt=${pt}`);
        if(pt===PT.HEARTBEAT){socket.write(frame(PT.HEARTBEAT,Buffer.alloc(0)));continue;}
        if(pt===PT.ERROR_RES){
          const e=pbDecode(payload);
          const code=e[2]?(Buffer.isBuffer(e[2])?e[2].toString():String(e[2])):"ERR";
          const desc=e[3]?(Buffer.isBuffer(e[3])?e[3].toString():String(e[3])):"";
          clearTimeout(timer);socket.removeListener("data",onData);
          return reject(new Error(`cTrader ${code}: ${desc}`));
        }
        if(pt===expectedPT){clearTimeout(timer);socket.removeListener("data",onData);return resolve(payload);}
      }
    }
    socket.on("data",onData);socket.write(msg);
  });
}

function parseAccounts(payload) {
  const top=pbDecode(payload);
  const accounts=[];
  // field 4 = repeated ProtoOACtidTraderAccount
  // field 1 = ctidTraderAccountId, field 5 = isLive (bool)
  const raw=top[4];
  if(!raw)return accounts;
  const items=Array.isArray(raw)?raw:[raw];
  for(const item of items){
    if(!Buffer.isBuffer(item))continue;
    const acc=pbDecode(item);
    if(acc[1]!==undefined){
      const id=typeof acc[1]==='bigint'?acc[1]:BigInt(acc[1]);
      // field 5 = isLive (1=live, 0=demo)
      const isLive=acc[5]!==undefined?Number(acc[5])===1:false;
      accounts.push({id,isLive});
      console.log(`Account ID: ${id}, isLive: ${isLive}`);
    }
  }
  return accounts;
}

function parseDeals(payload) {
  const top=pbDecode(payload);
  const raw=top[3];if(!raw)return[];
  return Array.isArray(raw)?raw:[raw];
}

function computeStats(dealBufs) {
  const profits=[];
  for(const d of dealBufs){
    if(!Buffer.isBuffer(d))continue;
    const deal=pbDecode(d);
    if(Number(deal[7])!==2)continue;
    if(!deal[10]||!Buffer.isBuffer(deal[10]))continue;
    const cpd=pbDecode(deal[10]);
    const gp=cpd[3]!==undefined?Number(cpd[3]):0;
    const md=cpd[9]!==undefined?Number(cpd[9]):2;
    profits.push(gp/Math.pow(10,md));
  }
  if(!profits.length)return{trades:0,pf:0,wr:0,dd:0,netProfit:0};
  const wins=profits.filter(p=>p>0),losses=profits.filter(p=>p<0);
  const gpp=wins.reduce((s,p)=>s+p,0),gll=Math.abs(losses.reduce((s,p)=>s+p,0));
  const pf=gll>0?gpp/gll:gpp>0?9.99:0;
  const wr=(wins.length/profits.length)*100;
  const net=profits.reduce((s,p)=>s+p,0);
  let peak=0,maxDD=0,cum=0;
  profits.forEach(p=>{cum+=p;if(cum>peak)peak=cum;const dd=peak>0?((peak-cum)/peak)*100:0;if(dd>maxDD)maxDD=dd;});
  return{trades:profits.length,wins:wins.length,losses:losses.length,pf:Math.round(pf*100)/100,wr:Math.round(wr*10)/10,dd:Math.round(maxDD*10)/10,netProfit:Math.round(net*100)/100};
}

async function fetchOOS(token,months) {
  // Step 1: Connect to live to get account list
  console.log("Connecting to live server to get accounts...");
  const liveSocket=await connect("live.ctraderapi.com");
  let accounts=[];
  try {
    await sendRecv(liveSocket,buildAppAuth(CLIENT_ID,CLIENT_SECRET),PT.APP_AUTH_RES);
    const accPay=await sendRecv(liveSocket,buildGetAccounts(token),PT.GET_ACCOUNTS_RES);
    accounts=parseAccounts(accPay);
    liveSocket.destroy();
  } catch(e){liveSocket.destroy();throw e;}

  if(!accounts.length)throw new Error("No accounts found for this token");
  console.log("All accounts:",accounts.map(a=>`${a.id}(${a.isLive?'live':'demo'})`).join(', '));

  // Step 2: Pick best account — prefer live, fall back to demo
  const liveAcc=accounts.find(a=>a.isLive);
  const chosen=liveAcc||accounts[0];
  const host=chosen.isLive?"live.ctraderapi.com":"demo.ctraderapi.com";
  console.log(`Using account ${chosen.id} on ${host}`);

  // Step 3: Connect to correct server and fetch deals
  const socket=await connect(host);
  try {
    await sendRecv(socket,buildAppAuth(CLIENT_ID,CLIENT_SECRET),PT.APP_AUTH_RES);
    await sendRecv(socket,buildAccountAuth(chosen.id,token),PT.ACCOUNT_AUTH_RES);
    console.log("Account auth OK");
    const to=BigInt(Date.now()),from=to-BigInt(months)*BigInt(30*24*60*60*1000);
    const dealPay=await sendRecv(socket,buildDealList(chosen.id,from,to),PT.DEAL_LIST_RES);
    const deals=parseDeals(dealPay);
    console.log(`Got ${deals.length} deals`);
    socket.destroy();
    const stats=computeStats(deals);
    return{...stats,accountId:chosen.id.toString(),accountType:chosen.isLive?'live':'demo'};
  } catch(e){socket.destroy();throw e;}
}

app.get("/health",(req,res)=>res.json({status:"ok",configured:!!(CLIENT_ID&&CLIENT_SECRET)}));

app.get("/oos",async(req,res)=>{
  const{token,months=3}=req.query;
  if(!token)return res.status(401).json({error:"No token"});
  if(!CLIENT_ID||!CLIENT_SECRET)return res.status(500).json({error:"Env vars missing"});
  try{
    const stats=await fetchOOS(token,parseInt(months));
    res.json({...stats,period:`Last ${months} months`});
  }catch(e){console.error("Error:",e.message);res.status(500).json({error:e.message});}
});

app.listen(PORT,()=>console.log(`cTrader proxy v8 on port ${PORT}`));
