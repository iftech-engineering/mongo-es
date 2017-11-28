import MongoDB from './mongodb'
import Elasticsearch from './elasticsearch'
import Processor from './processor'
import { Config, Task } from './config'

export async function run(config: Config): Promise<void> {
  // init mongodb and elasticsearch connection
  console.log('run', new Date())
  const mongodb = await MongoDB.init(config)
  const elasticsearch = await Elasticsearch.init(config)

  // check and create indices
  for (let index of config.elasticsearch.indices) {
    index.index += config.controls.indexNameSuffix
    if (!await elasticsearch.exists(index)) {
      await elasticsearch.create(index)
      console.log('create index', index.index)
    }
  }

  // put mappings
  for (let task of config.tasks) {
    task.load.index += config.controls.indexNameSuffix
    await elasticsearch.putMapping(task.load)
    console.log('put mapping', task.load.index, task.load.type)
  }

  // load checkpoint
  for (let task of config.tasks) {
    const checkpoint = await Task.loadCheckpoint(task.name())
    if (checkpoint) {
      task.from = checkpoint
    }
    console.log('from checkpoint', task.name(), task.from)
  }

  // run tasks
  await Promise.all(config.tasks.map(async (task) => {
    const processor = new Processor(task, config.controls, mongodb, elasticsearch)
    if (task.from.phase === 'scan') {
      try {
        console.log('scan', task.name(), 'from', task.from.id)
        const time = new Date()
        await processor.scanDocument()
        await task.endScan(time)
        console.log('scan', task.name(), 'end')
      } catch (err) {
        console.error('scan', err)
      }
    }
    console.log('tail', task.name(), 'from', task.from.time)
    await processor.tailOpLog()
  }))
}

console.debug = process.env.NODE_ENV === 'dev' ? console.log : () => null

export { Config, Task }
