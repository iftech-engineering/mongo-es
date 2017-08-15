import MongoDB from './mongodb'
import Elasticsearch from './elasticsearch'
import Processor from './processor'
import Config from './config'

export async function run(config: Config) {
  // init mongodb and elasticsearch connection
  console.log('run', new Date())
  await MongoDB.init(config)
  await Elasticsearch.init(config)

  // check and create indices
  for (let index of config.elasticsearch.indices) {
    index.index += config.controls.indexNameSuffix
    if (!await Elasticsearch.exists(index)) {
      await Elasticsearch.create(index)
      console.log('create index', index.index)
    }
  }

  // put mappings
  for (let index in config.tasks) {
    const task = config.tasks[index]
    task.load.index += config.controls.indexNameSuffix
    await Elasticsearch.putMapping(task.load)
    console.log('put mapping', task.load.index, task.load.type)
  }

  // run tasks
  config.tasks.forEach(async (task) => {
    const processor = new Processor(task, config.controls)
    if (task.from.phase === 'scan') {
      try {
        console.log('scan', task.name(), 'start', 'from', task.from.id)
        await processor.scanDocument()
        console.log('scan', task.name(), 'end')
      } catch (err) {
        console.error('scan', err)
      }
    }
    console.log('tail', task.name(), 'start', 'from', task.from.time)
    await processor.tailOpLog()
  })
}

console.debug = process.env.NODE_ENV === 'dev' ? console.log : () => null
