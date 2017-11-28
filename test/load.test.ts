import test from 'ava'
import { Client } from 'elasticsearch'

import Processor from '../old/processor'
import Elasticsearch from '../old/elasticsearch'
import { Controls, Task } from '../old/config'

const client = new Client({
  host: 'localhost:9200',
})

const task: Task = new Task({
  from: {
    phase: 'scan',
  },
  extract: {},
  transform: {
    mapping: {
      "field0.field1": "field0.field1",
      "field0.field2": "field0.field2"
    }
  },
  load: {
    index: 'test',
    type: 'test',
  },
})

test('load', async t => {
  await Elasticsearch.init({
    elasticsearch: {
      host: 'localhost:9200',
    },
  } as any)
  const processor = new Processor(task, new Controls({}))
  await processor.load([{
    action: 'upsert',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    data: {
      field0: {
        field1: 1,
        field2: 2,
      },
    },
    parent: undefined,
  }])
  const data = await client.get<any>({
    index: 'test',
    type: 'test',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
  })
  t.deepEqual(data, {
    _index: 'test',
    _type: 'test',
    _id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    _version: 1,
    found: true,
    _source: {
      field0: {
        field1: 1,
        field2: 2,
      },
    }
  })
})

test.before('create index', t => {
  return client.indices.create({
    index: 'test',
  })
})

test.after.always('delete index', t => {
  return client.indices.delete({
    index: 'test'
  })
})
