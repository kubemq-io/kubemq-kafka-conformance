// KafkaJS driver of the clients track (suites/clients/SCENARIO.md).
// The ledger goes to stdout, one JSON object per line. KafkaJS logs through console.* by default,
// so console output is sent to stderr here (where log lines go is not client behaviour; the Kafka
// configuration is untouched).
'use strict'

const { Kafka } = require('kafkajs')
const kafkajsVersion = require('kafkajs/package.json').version

for (const m of ['log', 'info', 'warn', 'error', 'debug']) {
  console[m] = (...a) => process.stderr.write(a.map(String).join(' ') + '\n')
}

const CLIENT = 'kafkajs'
const env = (k, d) => process.env[k] || d
const B = env('KCERT_BOOTSTRAP', '')
const MODE = env('KCERT_MODE', '')
const TOPIC = env('KCERT_TOPIC', '')
const GROUP = env('KCERT_GROUP', '')
const PARTITIONS = parseInt(env('KCERT_PARTITIONS', '0'), 10)
const RECORDS = parseInt(env('KCERT_RECORDS', '0'), 10)
const RECORDS2 = parseInt(env('KCERT_RECORDS2', '0'), 10)
const KEYS = parseInt(env('KCERT_KEYS', '1'), 10)
const TIMEOUT_MS = parseInt(env('KCERT_TIMEOUT_SEC', '60'), 10) * 1000
const ADMIN = env('KCERT_ADMIN', '1') !== '0'

const emit = (ev, f = {}) => process.stdout.write(JSON.stringify({ ev, ...f }) + '\n')
const code = e => (e && Number.isInteger(e.code) && e.code >= 0 ? e.code : e ? -1 : 0)
function fail (step, e) {
  emit('error', { step, error_code: code(e), error: String(e && e.message ? e.message : e) })
  emit('done')
  process.exit(1)
}
const value = s => 'value-' + String(s).padStart(6, '0')
const key = s => 'key-' + String(s % KEYS).padStart(2, '0')
const seqOf = v => { const m = /^value-(\d+)$/.exec(v || ''); return m ? parseInt(m[1], 10) : -1 }
const sleep = ms => new Promise(r => setTimeout(r, ms))
const until = async (cond, ms, what) => {
  const end = Date.now() + ms
  while (!cond()) {
    if (Date.now() > end) fail('consume', new Error(what))
    await sleep(50)
  }
}

async function create (kafka, numPartitions, replicationFactor) {
  const admin = kafka.admin()
  await admin.connect()
  let err = null
  try {
    const created = await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions, replicationFactor }], waitForLeaders: true })
    if (!created) err = new Error('createTopics returned false (topic already existed)')
  } catch (e) { err = e }
  emit('create', { topic: TOPIC, partitions: numPartitions, error_code: code(err), error: err ? String(err.message) : '' })
  if (err) fail('create', err)
  await admin.disconnect()
}

// produce sends one record per send call, so each acknowledgement names its own offset.
async function produce (producer, phase, lo, hi, keyed, acks) {
  for (let seq = lo; seq < hi; seq++) {
    const m = { value: value(seq) }
    if (keyed) {
      m.key = key(seq)
      m.headers = { seq: String(seq), client: CLIENT }
    }
    try {
      const md = await producer.send(acks === undefined ? { topic: TOPIC, messages: [m] } : { topic: TOPIC, acks, messages: [m] })
      emit('ack', { phase, seq, partition: md[0].partition, offset: parseInt(md[0].baseOffset, 10), error_code: md[0].errorCode })
    } catch (e) {
      emit('ack', { phase, seq, partition: -1, offset: -1, error_code: code(e), error: String(e.message) })
    }
  }
}

function recv (phase, got, partition, message) {
  const seq = seqOf(message.value && message.value.toString())
  got.add(seq)
  // KafkaJS 2.x has no ordered header list: keys of a plain object, in wire order.
  const headers = Object.entries(message.headers || {}).map(([k, v]) => [k, Buffer.isBuffer(v) ? v.toString() : String(v)])
  emit('recv', {
    phase, seq, partition, offset: parseInt(message.offset, 10),
    key: message.key === null ? null : message.key.toString(), value: message.value.toString(), headers
  })
}

function onJoin (consumer, phase, state) {
  consumer.on(consumer.events.GROUP_JOIN, e => {
    const ps = ((e.payload.memberAssignment || {})[TOPIC] || []).slice().sort((a, b) => a - b)
    emit('assigned', { phase, partitions: ps })
    if (ps.length > 0) state.joined = true // an empty assignment is not readiness
  })
  consumer.on(consumer.events.FETCH_START, () => { if (state.joined) state.fetching = true })
}

async function consume (kafka, phase, want, drain) {
  const consumer = kafka.consumer({ groupId: GROUP })
  const got = new Set()
  const last = new Map()
  onJoin(consumer, phase, {})
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ partition, message }) => {
      recv(phase, got, partition, message)
      last.set(partition, parseInt(message.offset, 10))
    }
  })
  await until(() => got.size >= want, TIMEOUT_MS, `received ${got.size} of ${want} records before the deadline`)
  if (drain) await sleep(2000)
  let err = null
  try {
    await consumer.commitOffsets([...last].map(([partition, o]) => ({ topic: TOPIC, partition, offset: String(o + 1) })))
  } catch (e) { err = e }
  emit('commit', { phase, error_code: code(err), error: err ? String(err.message) : '' })
  await consumer.disconnect()
}

async function scenario () {
  const kafka = new Kafka({ brokers: [B] })
  if (ADMIN) await create(kafka, PARTITIONS, 1)
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 })
  await producer.connect()
  await produce(producer, 1, 0, RECORDS, true, -1)
  await consume(kafka, 1, RECORDS, false)
  await produce(producer, 2, RECORDS, RECORDS + RECORDS2, true, -1)
  await producer.disconnect()
  await consume(kafka, 2, RECORDS2, true)
}

async function stock () {
  const kafka = new Kafka({ brokers: [B] })
  if (ADMIN) await create(kafka, undefined, undefined)
  const consumer = kafka.consumer({ groupId: GROUP })
  const got = new Set()
  const state = {}
  onJoin(consumer, 1, state)
  await consumer.connect()
  await consumer.subscribe({ topic: TOPIC })
  await consumer.run({ eachMessage: async ({ partition, message }) => recv(1, got, partition, message) })
  // KafkaJS resolves the start offsets (default: latest) before its first fetch after joining,
  // so the first FETCH_START after GROUP_JOIN is the readiness barrier.
  await until(() => state.fetching, TIMEOUT_MS, 'consumer never started fetching after joining')
  const producer = kafka.producer()
  await producer.connect()
  await produce(producer, 1, 0, RECORDS, false, undefined)
  await producer.disconnect()
  await until(() => got.size >= RECORDS, TIMEOUT_MS, `received ${got.size} of ${RECORDS} records before the deadline`)
  await consumer.disconnect()
}

async function main () {
  if (!B || !TOPIC || !GROUP || !['scenario', 'stock-defaults'].includes(MODE)) {
    fail('inputs', new Error('KCERT_BOOTSTRAP, KCERT_TOPIC, KCERT_GROUP and KCERT_MODE are required'))
  }
  // KafkaJS implements only the classic consumer group protocol.
  emit('start', { client: CLIENT, version: kafkajsVersion, mode: MODE, group_protocol: 'classic' })
  if (MODE === 'scenario') await scenario(); else await stock()
  emit('done')
  process.exit(0)
}

main().catch(e => fail('driver', e))
