import { MongoClientOptions } from 'mongodb'
import { ConfigOptions } from 'elasticsearch'

export type Config = {
  mongo: MongoConfig
  es: ElasticsearchConfig
  tasks: Task[]
}

export type MongoConfig = {
  url: string
  options: MongoClientOptions
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
  fields: any
  sort: any
  maxDPS: number
}

export type TransformTask = {
  mapping: {
    [key: string]: string
  }
}

export type LoadTask = {
  index: string
  type: string
  mapping: any
}
