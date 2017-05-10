import { Readable } from 'stream'

export function speedLimit(stream: Readable, dps?: number): Readable {
  if (!dps) {
    return stream
  }
  let dataCount = 0
  const timer = setInterval(() => {
    dataCount = 0
    stream.resume()
  }, 1000)
  stream.addListener('data', (doc) => {
    dataCount++
    if (dataCount >= dps) {
      stream.pause()
    }
  })
  stream.addListener('end', () => {
    clearInterval(timer)
  })
  return stream
}
