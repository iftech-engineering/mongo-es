import { MongoClientOptions, Db, Timestamp, ObjectID } from 'mongodb'
import { Client, ConfigOptions, IndicesCreateParams, IndicesPutMappingParams } from 'elasticsearch'

export type Config = {
  mongodb: MongoConfig
  elasticsearch: ElasticsearchConfig
  tasks: Task[]
  controls: Controls
}

export type MongoConfig = {
  url: string
  options?: MongoClientOptions
}

export type ElasticsearchConfig = {
  options: ConfigOptions
  indices?: IndicesCreateParams[]
}

export type Task = {
  extract: ExtractTask
  transform: TransformTask
  load: LoadTask
}

export type Controls = {
  tailFromTime?: number | string
  mongodbReadCapacity?: number
  elasticsearchBulkSize?: number
  indexNameSuffix?: string
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
  _id: ObjectID
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
        _id: ObjectID
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
        _id: ObjectID
      }
    } | {
      op: 'd'
      o: {
        _id: ObjectID
      }
    }
  )

export type IntermediateRepresentation = {
  action: 'create' | 'update' | 'delete'
  id: string
  parent?: string
  data: any
}

export type MongoDB = {
  [key: string]: Db
}

export type Elasticsearch = Client

export { ObjectID }
