import test from 'ava'
import * as Docker from 'dockerode'
import { Client } from 'elasticsearch'

import Processor from '../old/processor'
import Elasticsearch from '../old/elasticsearch'
import { Controls, Task } from '../old/config'

const docker = new Docker()
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

let container: Docker.Container

test.before.cb('start elasticsearch', t => {
  docker.createContainer({
    Image: 'docker.elastic.co/elasticsearch/elasticsearch-oss:6.0.0',
    Env: ['discovery.type=single-node'],
    HostConfig: {
      PortBindings: {
        '9200/tcp': [
          {
            HostPort: '9200',
          },
        ],
        '9300/tcp': [
          {
            HostPort: '9300',
          },
        ],
      },
    },
  }, function (err, c) {
    if (err || !c) {
      t.fail(err)
    } else {
      container = c
      c.start(function (err, data) {
        if (err) {
          t.fail(err)
        } else {
          // waiting es start
          setTimeout(async () => {
            await Elasticsearch.init({
              elasticsearch: {
                host: 'localhost:9200',
              },
            } as any)
            t.end()
          }, 10000)
        }
      })
    }
  })
})

test.after.always.cb('stop elasticsearch', t => {
  t.notThrows(async () => {
    await container.stop()
    await container.remove()
    t.end()
  })
})
