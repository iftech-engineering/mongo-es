import { Client, BulkIndexDocumentsParams } from 'elasticsearch'
import { ObjectID } from 'mongodb'

import { Document } from './types'
import { ElasticsearchConfig, Task } from './config'

export default class Elasticsearch {
  client: Client
  task: Task

  constructor(elasticsearch: ElasticsearchConfig, task: Task) {
    this.client = new Client(elasticsearch.options)
    this.task = task
  }

  async bulk(params: BulkIndexDocumentsParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.client.bulk(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  async search(id: ObjectID): Promise<Document | null> {
    return new Promise<Document | null>((resolve) => {
      this.client.search<Document>({
        index: this.task.load.index,
        type: this.task.load.type,
        body: {
          query: {
            term: {
              _id: id.toHexString(),
            },
          },
        },
      }, (err, response: any) => {
        if (err) {
          console.warn('search from elasticsearch', this.task.name(), id, err.message)
          resolve(null)
          return
        }
        if (response.hits.total === 0) {
          console.warn('search from elasticsearch', this.task.name(), id, 'not found')
          resolve(null)
          return
        }
        console.debug('search from elasticsearch', response)
        const doc = response.hits.hits[0]._source
        doc._id = new ObjectID(response.hits.hits[0]._id)
        if (this.task.transform.parent && response.hits.hits[0]._parent) {
          doc[this.task.transform.parent] = new ObjectID(response.hits.hits[0]._parent)
        }
        resolve(doc)
      })
    })
  }

  async retrieve(id: ObjectID): Promise<Document | null> {
    return new Promise<Document | null>((resolve) => {
      this.client.get<Document>({
        index: this.task.load.index as string,
        type: this.task.load.type,
        id: id.toHexString(),
      }, (err, response) => {
        if (err) {
          console.warn('retrieve from elasticsearch', this.task.name(), id, err.message)
          resolve(null)
          return
        }
        console.debug('retrieve from elasticsearch', response)
        resolve(response ? {
          ...response._source,
          _id: new ObjectID(response._id),
        } : null)
      })
    })
  }
}
