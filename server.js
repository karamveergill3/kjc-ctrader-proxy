/**
 * KJC Arena — cTrader Open API Proxy Server
 * Deploy to Railway: https://railway.app
 * 
 * Connects to cTrader via TCP/TLS + protobuf
 * Exposes a simple HTTP endpoint for Vercel to call
 */

const net = require("net");
const tls = require("tls");
const express = require("express");
const app = express();
app.use(express.json());

const CLIENT_ID = process.env.CTRADER_CLIENT_ID;
const CLIENT_SECRET = process.env.CTRADER_CLIENT_SECRET;
const PORT = process.env.PORT || 3001;

// ── cTrader API constants ────────────────────────────────────────────────────
const CTRADER_HOST = "live.ctraderapi.com";
const CTRADER_PORT = 5035;

// Payload type IDs (from cTrader proto definitions)
const PayloadType = {
  PROTO_OA_APPLICATION_AUTH_REQ: 2100,
  PROTO_OA_APPLICATION_AUTH_RES: 2101,
  PROTO_OA_ACCOUNT_AUTH_REQ: 2102,
  PROTO_OA_ACCOUNT_AUTH_RES: 2103,
  PROTO_OA_GET_ACCOUNT_LIST_BY_ACCESS_TOKEN_REQ: 2149,
  PROTO_OA_GET_ACCOUNT_LIST_BY_ACCESS_TOKEN_RES: 2150,
  PROTO_OA_DEAL_LIST_REQ: 2140,
  PROTO_OA_DEAL_LIST_RES: 2141,
  PROTO_OA_ERROR_RES: 2142,
  PROTO_MESSAGE: 5,
};

// ── Protobuf-lite encoder/decoder ────────────────────────────────────────────
// We implement a minimal protobuf encoder without the full library
// since we only need a handful of message types

function encodeVarint(value) {
  const bytes = [];
  while (value > 0x7f) {
    bytes.push((value & 0x7f) | 0x80);
    value = value >>> 7;
  }
  bytes.push(value & 0x7f);
  return Buffer.from(bytes);
}

function writeField(fieldNum, type, value) {
  // type: 0=varint, 2=length-delimited
  const tag = (fieldNum << 3) | type;
  const tagBuf = encodeVarint(tag);
  if (type === 0) {
    return Buffer.concat([tagBuf, encodeVarint(value)]);
  } else {
    const valBuf = Buffer.isBuffer(value) ? value : Buffer.from(value, "utf8");
    return Buffer.concat([tagBuf, encodeVarint(valBuf.length), valBuf]);
  }
}

function readVarint(buf, offset) {
  let result = 0, shift = 0;
  while (offset < buf.length) {
    const byte = buf[offset++];
    result |= (byte & 0x7f) << shift;
    shift += 7;
    if (!(byte & 0x80)) break;
  }
  return { value: result, offset };
}

function decodeMessage(buf) {
  const fields = {};
  let offset = 0;
  while (offset < buf.length) {
    const tag = readVarint(buf, offset);
    offset = tag.offset;
    const fieldNum = tag.value >> 3;
    const wireType = tag.value & 0x7;
    if (wireType === 0) {
      const v = readVarint(buf, offset);
      offset = v.offset;
      fields[fieldNum] = v.value;
    } else if (wireType === 2) {
      const len = readVarint(buf, offset);
      offset = len.offset;
      fields[fieldNum] = buf.slice(offset, offset + len.value);
      offset += len.value;
    } else {
      break; // unsupported wire type
    }
  }
  return fields;
}

// ── Build cTrader messages ────────────────────────────────────────────────────

function buildProtoMessage(payloadType, payload) {
  // ProtoMessage: field 3 = payloadType (varint), field 5 = payload (bytes)
  const typeField = writeField(3, 0, payloadType);
  const payloadField = writeField(5, 2, payload);
  const msg = Buffer.concat([typeField, payloadField]);
  // Prefix with 4-byte big-endian length
  const len = Buffer.alloc(4);
  len.writeUInt32BE(msg.length, 0);
  return Buffer.concat([len, msg]);
}

function buildAppAuthReq(clientId, clientSecret) {
  const payload = Buffer.concat([
    writeField(2, 2, clientId),
    writeField(3, 2, clientSecret),
  ]);
  return buildProtoMessage(PayloadType.PROTO_OA_APPLICATION_AUTH_REQ, payload);
}

function buildGetAccountListReq(accessToken) {
  const payload = writeField(2, 2, accessToken);
  return buildProtoMessage(PayloadType.PROTO_OA_GET_ACCOUNT_LIST_BY_ACCESS_TOKEN_REQ, payload);
}

function buildAccountAuthReq(ctidTraderAccountId, accessToken) {
  // ctidTraderAccountId is int64 — encode as two varints (lo, hi) or just use lo for accounts < 2^32
  const idBuf = Buffer.alloc(8);
  idBuf.writeBigInt64BE(BigInt(ctidTraderAccountId), 0);
  const payload = Buffer.concat([
    writeField(2, 0, ctidTraderAccountId),
    writeField(3, 2, accessToken),
  ]);
  return buildProtoMessage(PayloadType.PROTO_OA_ACCOUNT_AUTH_REQ, payload);
}

function buildDealListReq(ctidTraderAccountId, fromTimestamp, toTimestamp) {
  const payload = Buffer.concat([
    writeField(2, 0, ctidTraderAccountId),
    writeField(3, 0, fromTimestamp),
    writeField(4, 0, toTimestamp),
    writeField(5, 0, 1000), // maxRows
  ]);
  return buildProtoMessage(PayloadType.PROTO_OA_DEAL_LIST_REQ, payload);
}

// ── Parse response ────────────────────────────────────────────────────────────

function parseProtoMessage(buf) {
  const fields = decodeMessage(buf);
  const payloadType = fields[3];
  const payload = fields[5];
  return { payloadType, payload };
}

function parseAccountList(payload) {
  // Returns array of ctidTraderAccountId (field 3 in repeated ProtoOACtidTraderAccount, which has field 2 = ctidTraderAccountId)
  const accounts = [];
  let offset = 0;
  while (offset < payload.length) {
    const tag = readVarint(payload, offset);
    offset = tag.offset;
    const fieldNum = tag.value >> 3;
    const wireType = tag.value & 0x7;
    if (wireType === 2) {
      const len = readVarint(payload, offset);
      offset = len.offset;
      const subBuf = payload.slice(offset, offset + len.value);
      offset += len.value;
      if (fieldNum === 3) {
        // This is a ProtoOACtidTraderAccount message
        const sub = decodeMessage(subBuf);
        const accountId = sub[2]; // ctidTraderAccountId
        if (accountId) accounts.push(accountId);
      }
    } else if (wireType === 0) {
      const v = readVarint(payload, offset);
      offset = v.offset;
    } else break;
  }
  return accounts;
}

function parseDealList(payload) {
  const deals = [];
  let offset = 0;
  while (offset < payload.length) {
    const tag = readVarint(payload, offset);
    offset = tag.offset;
    const fieldNum = tag.value >> 3;
    const wireType = tag.value & 0x7;
    if (wireType === 2) {
      const len = readVarint(payload, offset);
      offset = len.offset;
      const subBuf = payload.slice(offset, offset + len.value);
      offset += len.value;
      if (fieldNum === 3) {
        // ProtoOADeal message
        const sub = decodeMessage(subBuf);
        deals.push(sub);
      }
    } else if (wireType === 0) {
      const v = readVarint(payload, offset);
      offset = v.offset;
    } else break;
  }
  return deals;
}

// ── TCP connection helper ─────────────────────────────────────────────────────

function connectCTrader() {
  return new Promise((resolve, reject) => {
    const socket = tls.connect({ host: CTRADER_HOST, port: CTRADER_PORT, rejectUnauthorized: true }, () => {
      resolve(socket);
    });
    socket.on("error", reject);
    setTimeout(() => reject(new Error("Connection timeout")), 10000);
  });
}

function sendAndReceive(socket, message, expectedPayloadType, timeout = 8000) {
  return new Promise((resolve, reject) => {
    let buffer = Buffer.alloc(0);
    const timer = setTimeout(() => reject(new Error(`Timeout waiting for ${expectedPayloadType}`)), timeout);

    const onData = (data) => {
      buffer = Buffer.concat([buffer, data]);
      while (buffer.length >= 4) {
        const msgLen = buffer.readUInt32BE(0);
        if (buffer.length < 4 + msgLen) break;
        const msgBuf = buffer.slice(4, 4 + msgLen);
        buffer = buffer.slice(4 + msgLen);
        const { payloadType, payload } = parseProtoMessage(msgBuf);
        if (payloadType === PayloadType.PROTO_OA_ERROR_RES) {
          clearTimeout(timer);
          socket.removeListener("data", onData);
          const errFields = decodeMessage(payload);
          reject(new Error(`cTrader error: ${errFields[2] ? errFields[2].toString() : "Unknown"}`));
          return;
        }
        if (payloadType === expectedPayloadType) {
          clearTimeout(timer);
          socket.removeListener("data", onData);
          resolve(payload);
          return;
        }
      }
    };

    socket.on("data", onData);
    socket.write(message);
  });
}

// ── Main fetch function ───────────────────────────────────────────────────────

async function fetchDeals(accessToken, months) {
  const socket = await connectCTrader();

  try {
    // 1. App auth
    await sendAndReceive(socket, buildAppAuthReq(CLIENT_ID, CLIENT_SECRET), PayloadType.PROTO_OA_APPLICATION_AUTH_RES);

    // 2. Get account list
    const accountListPayload = await sendAndReceive(socket, buildGetAccountListReq(accessToken), PayloadType.PROTO_OA_GET_ACCOUNT_LIST_BY_ACCESS_TOKEN_RES);
    const accounts = parseAccountList(accountListPayload);
    if (!accounts.length) throw new Error("No accounts found for this access token");

    const accountId = accounts[0];

    // 3. Account auth
    await sendAndReceive(socket, buildAccountAuthReq(accountId, accessToken), PayloadType.PROTO_OA_ACCOUNT_AUTH_RES);

    // 4. Fetch deals
    const toTs = Date.now();
    const fromTs = toTs - (months * 30 * 24 * 60 * 60 * 1000);
    const dealListPayload = await sendAndReceive(socket, buildDealListReq(accountId, fromTs, toTs), PayloadType.PROTO_OA_DEAL_LIST_RES);
    const deals = parseDealList(dealListPayload);

    socket.destroy();
    return { accounts, accountId, deals };
  } catch (e) {
    socket.destroy();
    throw e;
  }
}

// ── Compute stats from deals ──────────────────────────────────────────────────

function computeStats(deals, months) {
  // ProtoOADeal fields:
  // field 2 = dealId, field 3 = orderId, field 4 = positionId
  // field 7 = dealStatus (2=FILLED), field 10 = closePositionDetail (sub-message)
  // closePositionDetail field 3 = grossProfit (int64, in 1/100 currency units)
  // field 9 = moneyDigits

  const closedDeals = deals.filter(d => d[7] === 2); // FILLED
  if (!closedDeals.length) return { trades: 0, pf: 0, wr: 0, dd: 0, netProfit: 0 };

  const profits = closedDeals.map(d => {
    if (d[10] && Buffer.isBuffer(d[10])) {
      const closeDetail = decodeMessage(d[10]);
      const grossProfit = closeDetail[3] || 0;
      const moneyDigits = closeDetail[9] || 2;
      return grossProfit / Math.pow(10, moneyDigits);
    }
    return 0;
  });

  const wins = profits.filter(p => p > 0);
  const losses = profits.filter(p => p < 0);
  const grossProfit = wins.reduce((s, p) => s + p, 0);
  const grossLoss = Math.abs(losses.reduce((s, p) => s + p, 0));
  const pf = grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? 9.99 : 0;
  const wr = (wins.length / closedDeals.length) * 100;
  const netProfit = profits.reduce((s, p) => s + p, 0);

  // Max drawdown
  let peak = 0, maxDD = 0, cum = 0;
  profits.forEach(p => {
    cum += p;
    if (cum > peak) peak = cum;
    const dd = peak > 0 ? ((peak - cum) / peak) * 100 : 0;
    if (dd > maxDD) maxDD = dd;
  });

  return {
    trades: closedDeals.length,
    wins: wins.length,
    losses: losses.length,
    pf: Math.round(pf * 100) / 100,
    wr: Math.round(wr * 10) / 10,
    dd: Math.round(maxDD * 10) / 10,
    netProfit: Math.round(netProfit * 100) / 100,
  };
}

// ── HTTP endpoint ─────────────────────────────────────────────────────────────

app.get("/health", (req, res) => res.json({ status: "ok" }));

app.get("/oos", async (req, res) => {
  const { token, months = 3 } = req.query;
  if (!token) return res.status(401).json({ error: "No access token" });
  if (!CLIENT_ID || !CLIENT_SECRET) return res.status(500).json({ error: "Server not configured" });

  try {
    const { deals } = await fetchDeals(token, parseInt(months));
    const stats = computeStats(deals, parseInt(months));
    res.json({ ...stats, period: `Last ${months} months` });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`cTrader proxy running on port ${PORT}`);
});
