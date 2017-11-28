import {
  Client,
  IndicesCreateParams,
  IndicesPutMappingParams,
  IndicesExistsParams,
  BulkIndexDocumentsParams,
} from 'elasticsearch'
import { ObjectID } from 'mongodb'

import { Document } from './types'
import { Config, Task } from './config'

export default class Elasticsearch {
  client: Client

  constructor(client: Client) {
    this.client = client
  }

  static async init({ elasticsearch }: Config): Promise<Elasticsearch> {
    return new Elasticsearch(new Client(elasticsearch.options))
  }

  async create(params: IndicesCreateParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.client.indices.create(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  async putMapping(params: IndicesPutMappingParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.client.indices.putMapping(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  async exists(params: IndicesExistsParams): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      this.client.indices.exists(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  async bulk(params: BulkIndexDocumentsParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.client.bulk(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  async search(task: Task, id: ObjectID): Promise<Document | null> {
    return new Promise<Document | null>((resolve) => {
      this.client.search<Document>({
        index: task.load.index,
        type: task.load.type,
        body: {
          query: {
            term: {
              _id: id.toHexString(),
            },
          },
        },
      }, (err, response: any) => {
        if (err) {
          console.warn('search from elasticsearch', task.name(), id, err.message)
          resolve(null)
          return
        }
        if (response.hits.total === 0) {
          console.warn('search from elasticsearch', task.name(), id, 'not found')
          resolve(null)
          return
        }
        console.debug('search from elasticsearch', response)
        const doc = response.hits.hits[0]._source
        doc._id = new ObjectID(response.hits.hits[0]._id)
        if (task.transform.parent && response.hits.hits[0]._parent) {
          doc[task.transform.parent] = new ObjectID(response.hits.hits[0]._parent)
        }
        resolve(doc)
      })
    })
  }

  async retrieve(task: Task, id: ObjectID): Promise<Document | null> {
    return new Promise<Document | null>((resolve) => {
      this.client.get<Document>({
        index: task.load.index as string,
        type: task.load.type,
        id: id.toHexString(),
      }, (err, response) => {
        if (err) {
          console.warn('retrieve from elasticsearch', task.name(), id, err.message)
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
