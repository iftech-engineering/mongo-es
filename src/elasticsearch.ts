import { Client, BulkIndexDocumentsParams } from 'elasticsearch'
import { ObjectID } from 'mongodb'
import { keyBy, keys } from 'lodash'

import { Document } from './types'
import { ElasticsearchConfig, Task } from './config'

export default class Elasticsearch {
  static client: Client
  task: Task
  searchBuffer: { [id: string]: ((doc: Document | null) => void)[] } = {}
  searchRunning: boolean = false
  retrieveBuffer: { [id: string]: ((doc: Document | null) => void)[] } = {}
  retrieveRunning: boolean = false

  constructor(elasticsearch: ElasticsearchConfig, task: Task) {
    if (!Elasticsearch.client) {
      Elasticsearch.client = new Client({ ...elasticsearch.options })
    }
    this.task = task
  }

  async bulk(params: BulkIndexDocumentsParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      Elasticsearch.client.bulk(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  async search(id: ObjectID): Promise<Document | null> {
    return new Promise<Document | null>((resolve) => {
      this.searchBuffer[id.toHexString()] = this.searchBuffer[id.toHexString()] || []
      this.searchBuffer[id.toHexString()].push(resolve)
      if (!this.searchRunning) {
        this.searchRunning = true
        setImmediate(this._search.bind(this))
      }
    })
  }

  async _search(): Promise<void> {
    const ids = keys(this.searchBuffer)
    if (ids.length === 0) {
      this.searchRunning = false
      return
    }
    const docs = await this._searchBatchSafe(ids)
    ids.forEach((id) => {
      const cbs = this.searchBuffer[id]
      delete this.searchBuffer[id]
      cbs.forEach((cb) => {
        cb(docs[id] || null)
      })
    })
    setImmediate(this._search.bind(this))
  }

  async _searchBatchSafe(ids: string[]): Promise<{ [id: string]: Document }> {
    return new Promise<{ [id: string]: Document }>((resolve) => {
      Elasticsearch.client.search<Document>({
        index: this.task.load.index,
        type: this.task.load.type,
        body: {
          query: {
            terms: {
              _id: ids,
            },
          },
        },
      }, (err, response) => {
        try {
          if (err) {
            console.warn('search from elasticsearch', this.task.name(), ids, err.message)
            resolve({})
            return
          }
          console.debug('search from elasticsearch', response)
          const docs = response.hits.hits.map((hit: any) => {
            const doc = hit._source
            doc._id = new ObjectID(hit._id)
            if (this.task.transform.parent && hit._parent) {
              doc[this.task.transform.parent] = new ObjectID(hit._parent)
            }
            return doc as Document
          })
          resolve(keyBy(docs, doc => doc._id.toHexString()))
        } catch (err2) {
          console.error('search from elasticsearch', this.task.name(), ids, err2)
          resolve({})
        }
      })
    })
  }

  async retrieve(id: ObjectID): Promise<Document | null> {
    return new Promise<Document | null>((resolve) => {
      this.retrieveBuffer[id.toHexString()] = this.retrieveBuffer[id.toHexString()] || []
      this.retrieveBuffer[id.toHexString()].push(resolve)
      if (!this.retrieveRunning) {
        this.retrieveRunning = true
        setImmediate(this._retrieve.bind(this))
      }
    })
  }

  async _retrieve(): Promise<void> {
    const ids = keys(this.retrieveBuffer)
    if (ids.length === 0) {
      this.retrieveRunning = false
      return
    }
    const docs = await this._retrieveBatchSafe(ids)
    ids.forEach((id) => {
      const cbs = this.retrieveBuffer[id]
      delete this.retrieveBuffer[id]
      cbs.forEach((cb) => {
        cb(docs[id] || null)
      })
    })
    setImmediate(this._retrieve.bind(this))
  }

  async _retrieveBatchSafe(ids: string[]): Promise<{ [id: string]: Document }> {
    return new Promise<{ [id: string]: Document }>((resolve) => {
      Elasticsearch.client.mget<Document>({
        index: this.task.load.index as string,
        type: this.task.load.type,
        body: {
          ids,
        }
      }, (err, response) => {
        try {
          if (err || !response.docs) {
            console.warn('retrieve from elasticsearch', this.task.name(), ids, err.message)
            resolve({})
            return
          }
          console.debug('retrieve from elasticsearch', response)
          const docs = response.docs.map(doc => {
            return {
              ...doc._source,
              _id: new ObjectID(doc._id),
            }
          })
          resolve(keyBy(docs, doc => doc._id.toHexString()))
        } catch (err2) {
          console.error('retrieve from elasticsearch', this.task.name(), ids, err2)
          resolve({})
        }
      })
    })
  }
}
