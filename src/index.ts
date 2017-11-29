import MongoDB from './mongodb'
import Elasticsearch from './elasticsearch'
import Indices from './indices'
import Processor from './processor'
import { Config, Task } from './config'

export async function run(config: Config): Promise<void> {
  console.log('run', new Date())

  // check and create indices, mappings
  await Indices.init(config)

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
    try {
      const mongodb = await MongoDB.init(config.mongodb, task)
      const elasticsearch = new Elasticsearch(config.elasticsearch, task)
      const processor = new Processor(task, config.controls, mongodb, elasticsearch)
      if (task.from.phase === 'scan') {
        console.log('scan', task.name(), 'from', task.from.id)
        const time = new Date()
        await processor.scanDocument()
        await task.endScan(time)
        console.log('scan', task.name(), 'end')
      }
      console.log('tail', task.name(), 'from', task.from.time)
      await processor.tailOpLog()
    } catch (err) {
      console.error('=====================')
      console.error('= ERROR', err)
      console.error('=====================')
    }
  }))
}

console.debug = process.env.NODE_ENV === 'dev' ? console.log : () => null

export { Config, Task }
