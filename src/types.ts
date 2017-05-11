import { MongoClientOptions, Db } from 'mongodb'
import { Client, ConfigOptions } from 'elasticsearch'
import { Timestamp, ObjectID } from 'bson'

export type Config = {
  mongo: MongoConfig
  elasticsearch: ElasticsearchConfig
  tasks: Task[]
}

export type MongoConfig = {
  url: string
  options: MongoClientOptions
  provisionedReadCapacity: number
}

export type ElasticsearchConfig = {
  options: ConfigOptions
}

export type Task = {
  extract: ExtractTask
  transform: TransformTask
  load: LoadTask
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

export type LoadTask = {
  index: string
  type: string
  mapping: {
    _parent?: {
      type: string
    }
    properties: {
      [key: string]: any
    }
  }
}

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

export type Mongo = {
  [key: string]: Db
}

export type Elasticsearch = Client

export type ObjectID = ObjectID
