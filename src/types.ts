import { MongoClientOptions } from 'mongodb'
import { ConfigOptions } from 'elasticsearch'
import { Timestamp } from 'bson'

export type Config = {
  mongo: MongoConfig
  es: ElasticsearchConfig
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
  mapping: {
    [key: string]: string | '_parent'
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
  _id: string
  [key: string]: any
}

export type OpLog = {
  ts: Timestamp
  h: number
  v: number
  op: 'i' | 'd' | 'u'
  ns: string
  fromMigrate: boolean
  o: {
    _id: string
    [key: string]: any
  }
  o2?: {
    _id: string
    [key: string]: any
  }
}

export type IntermediateRepresentation = {
  action: 'create' | 'update' | 'delete'
  id: string
  parent?: string
  data: any
}
