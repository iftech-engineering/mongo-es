const fs = require('fs')

const Redis = require('ioredis')

const { Config, Task, run } = require('../dist/src/index')

const redis = new Redis('localhost')

Task.onSaveCheckpoint((name, checkpoint) => {
  return redis.set(`mongo-es:${name}`, JSON.stringify(checkpoint))
})

Task.onLoadCheckpoint((name) => {
  return redis.get(`mongo-es:${name}`).then(JSON.parse)
})

Task.onTransform((task, ir) => {
  if (ir.action === 'upsert' && task.extract.collection === 'user') {
    ir.data.type = 'USER'
  }
  return ir
})

run(new Config(fs.readFileSync('examples/config.json', 'utf8')))
