import { MongoClientOptions } from 'mongodb'

export type Config = {
  mongo: {
    uri: string
    options: MongoClientOptions
    db: string
    collection: string
    parent?: string
    query?: any
    fields: any
    dps?: number
  }
  elasticsearch: {
    index: string
    settings: any
    mapping: {
      dynamic?: boolean
      _parent?: {
        type: string
      }
      properties: {
        [key: string]: {
          type: string
        }
      }
    }
  }
}
