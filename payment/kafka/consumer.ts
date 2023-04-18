import { Kafka } from 'kafkajs'
import { EVENTS, TOPICS } from '.'
import { produceEvent } from './producer'

const kafka = new Kafka({
  clientId: 'merchant-service',
  brokers: ['localhost:9092'],
})

const initConsumer = async () => {
  const consumer = kafka.consumer({ groupId: 'payment-service' })

  await consumer.connect()
  await consumer.subscribe({ topic: TOPICS.PAYMENT_SERVICE, fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      console.log('Message received from Kafka topic:' + topic)
      console.log(message?.value?.toString())

      const data = JSON.parse(message?.value?.toString()!)

      console.log('Waiting for 5 seconds...')
      setTimeout(() => {}, 5000)

      switch (data.type) {
        case EVENTS.ORDER_CREATED:
          console.log('Sending message to Kafka topic: ' + TOPICS.ORDER_SERVICE)

          await produceEvent(TOPICS.ORDER_SERVICE, {
            type: EVENTS.PAYMENT_SUCCESS,
            payload: data.payload,
          })

          console.log('Message sent to Kafka topic: ' + TOPICS.ORDER_SERVICE)
          break
        default:
          console.log('Invalid event type.....')
          break
      }
    },
  })
}

export { initConsumer }
