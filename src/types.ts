import { MongoClientOptions, Db } from 'mongodb'
import { Client, ConfigOptions, IndicesCreateParams, IndicesPutMappingParams } from 'elasticsearch'
import { Timestamp, ObjectId } from 'bson'

export type Config = {
  mongo: MongoConfig
  elasticsearch: ElasticsearchConfig
  tasks: Task[]
  controls: Controls
}

export type MongoConfig = {
  url: string
  options: MongoClientOptions
}

export type ElasticsearchConfig = {
  options: ConfigOptions
  index: IndicesCreateParams
}

export type Task = {
  extract: ExtractTask
  transform: TransformTask
  load: LoadTask
}

export type Controls = {
  tailFromTime?: number | string
  mongoReadCapacity?: number
  elasticsearchBulkSize?: number
}

export type ExtractTask = {
  db: string
  collection: string
  query: any
  projection: {
    [key: string]: 1 | 0
  }
  sort: {
    [key: string]: 1 | -1
  }
}

export type TransformTask = {
  parent: string
  mapping: {
    [key: string]: string
  }
}

export type LoadTask = IndicesPutMappingParams

export type Document = {
  _id: ObjectId
  [key: string]: any
}

export type OpLog = {
  ts: Timestamp
  h: number
  v: number
  ns: string
  fromMigrate: boolean
} & (
    {
      op: 'i'
      o: {
        _id: ObjectId
        [key: string]: any
      }
    } | {
      op: 'u'
      o: {
        $set: any
        $unset: any
        [key: string]: any
      }
      o2: {
        _id: ObjectId
      }
    } | {
      op: 'd'
      o: {
        _id: ObjectId
      }
    }
  )

export type IntermediateRepresentation = {
  action: 'create' | 'update' | 'delete'
  id: string
  parent?: string
  data: any
}

export type Mongo = {
  [key: string]: Db
}

export type Elasticsearch = Client

export { ObjectId }
