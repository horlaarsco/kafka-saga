import { Kafka, Partitioners } from 'kafkajs'
import { TOPICS } from '.'

const kafka = new Kafka({
  clientId: 'merchant-service',
  brokers: ['localhost:9092'],
})

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

const produceEvent = async (topic: TOPICS, message: object) => {
  try {
    console.log('Connecting to Kafka producer...')
    await producer.connect()

    console.log('Sending message to Kafka topic:' + topic)
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(message) }],
    })
  } catch (error) {
    console.log(error, 'Failed to produce message...')
  } finally {
    console.log('Disconnecting from Kafka producer...')
    await producer.disconnect()
  }
}

export { produceEvent }
