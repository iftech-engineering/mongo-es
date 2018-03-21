import test from 'ava'
import { Client } from 'elasticsearch'

import Processor from '../src/processor'
import Elasticsearch from '../src/elasticsearch'
import { Controls, Task } from '../src/config'

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
      "field0.field1": "field1",
      "field0.field2": "field2"
    }
  },
  load: {
    index: 'test',
    type: 'test',
  },
})

test('load', async t => {
  const elasticsearch = new Elasticsearch({
    options: {
      host: 'localhost:9200',
    },
    indices: [],
  }, task)
  const processor = new Processor(task, new Controls({}), null as any, elasticsearch)
  await processor.load([{
    action: 'upsert',
    id: 'aaaaaaaaaaaaaaaaaaaaaaaa',
    data: {
      field1: 1,
      field2: 2,
    },
    parent: undefined,
    timestamp: 0
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
      field1: 1,
      field2: 2,
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
